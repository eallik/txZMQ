"""
ZeroMQ connection.
"""
from collections import deque, namedtuple
from itertools import islice

from zmq.core import constants, error
from zmq.core.socket import Socket

from zope.interface import implements

from twisted.internet.interfaces import IFileDescriptor, IReadDescriptor
from twisted.internet import reactor
from twisted.python import log


class ZmqEndpointType(object):
    """
    Endpoint could be "bound" or "connected".
    """
    bind = "bind"
    connect = "connect"


ZmqEndpoint = namedtuple('ZmqEndpoint', ['type', 'address'])


class ZmqConnection(object):
    """
    Connection through ZeroMQ, wraps a ZeroMQ socket.

    Base class for all ZMQ connection classes with a uniform interface.

    Subclasses should override sendMsg and sendMultipart with their own
    connection-type specific implementations, which can have a different
    argument signature. Thus, ZmqBase does not attempt to hide away the
    differences between different connection classes--this is simply
    impossible--It simply attempts to provide a more consistent way of
    interacting with them.

    By default, these methods simply delegate to the unerlying low-level
    ZmqConnection.send, which handles both single and multi-part messages.

    Subclasses can/should add their own semantic aliases for sendMsg and
    sendMultipart, such as publish and publishMultipart for a PUB socket.

    @cvar socketType: socket type, from ZeroMQ
    @cvar allowLoopbackMulticast: is loopback multicast allowed?
    @type allowLoopbackMulticast: C{boolean}
    @cvar multicastRate: maximum allowed multicast rate, kbps
    @type multicastRate: C{int}
    @cvar highWaterMark: hard limit on the maximum number of outstanding
        messages 0MQ shall queue in memory for any single peer
    @type highWaterMark: C{int}

    @ivar factory: ZeroMQ Twisted factory reference
    @type factory: L{ZmqFactory}
    @ivar socket: ZeroMQ Socket
    @type socket: L{Socket}
    @ivar endpoints: ZeroMQ addresses for connect/bind
    @type endpoints: C{list} of L{ZmqEndpoint}
    @ivar fd: file descriptor of zmq mailbox
    @type fd: C{int}
    @ivar queue: output message queue
    @type queue: C{deque}
    """
    implements(IReadDescriptor, IFileDescriptor)

    socketType = None
    allowLoopbackMulticast = False
    multicastRate = 100
    highWaterMark = 0

    def __init__(self, factory, endpoint=None, identity=None):
        """
        Constructor.

        One endpoint is passed to the constructor, more could be added
        via call to C{addEndpoints}.

        @param factory: ZeroMQ Twisted factory
        @type factory: L{ZmqFactory}
        @param endpoint: ZeroMQ address for connect/bind
        @type endpoint: C{list} of L{ZmqEndpoint}
        @param identity: socket identity (ZeroMQ)
        @type identity: C{str}
        """
        self.factory = factory
        self.endpoints = []
        self.identity = identity
        self.socket = Socket(factory.context, self.socketType)
        self.queue = deque()
        self.recv_parts = []
        self.scheduled_doRead = None

        self.fd = self.socket.getsockopt(constants.FD)
        self.socket.setsockopt(constants.LINGER, factory.lingerPeriod)
        self.socket.setsockopt(
            constants.MCAST_LOOP, int(self.allowLoopbackMulticast))
        self.socket.setsockopt(constants.RATE, self.multicastRate)
        self.socket.setsockopt(constants.HWM, self.highWaterMark)
        if self.identity is not None:
            self.socket.setsockopt(constants.IDENTITY, self.identity)

        if endpoint:
            self.addEndpoints([endpoint])

        self.factory.connections.add(self)

        self.factory.reactor.addReader(self)

    def addEndpoints(self, endpoints):
        """
        Add more connection endpoints. Connection may have
        many endpoints, mixing protocols and types.

        @param endpoints: list of endpoints to add
        @type endpoints: C{list}
        """
        self.endpoints.extend(endpoints)
        self._connectOrBind(endpoints)

    def shutdown(self):
        """
        Shutdown connection and socket.
        """
        self.factory.reactor.removeReader(self)

        self.factory.connections.discard(self)

        self.socket.close()
        self.socket = None

        self.factory = None

        if self.scheduled_doRead is not None:
            self.scheduled_doRead.cancel()
            self.scheduled_doRead = None

    def __repr__(self):
        return "%s(%r, %r)" % (
            self.__class__.__name__, self.factory, self.endpoints)

    def fileno(self):
        """
        Part of L{IFileDescriptor}.

        @return: The platform-specified representation of a file descriptor
                 number.
        """
        return self.fd

    def connectionLost(self, reason):
        """
        Called when the connection was lost.

        Part of L{IFileDescriptor}.

        This is called when the connection on a selectable object has been
        lost.  It will be called whether the connection was closed explicitly,
        an exception occurred in an event handler, or the other end of the
        connection closed it first.

        @param reason: A failure instance indicating the reason why the
                       connection was lost.  L{error.ConnectionLost} and
                       L{error.ConnectionDone} are of special note, but the
                       failure may be of other classes as well.
        """
        if self.factory:
            self.factory.reactor.removeReader(self)

    def _readMultipart(self):
        """
        Read multipart in non-blocking manner, returns with ready message
        or raising exception (in case of no more messages available).
        """
        while True:
            self.recv_parts.append(self.socket.recv(constants.NOBLOCK))
            if not self.socket.getsockopt(constants.RCVMORE):
                result, self.recv_parts = self.recv_parts, []

                return result

    def doRead(self):
        """
        Some data is available for reading on your descriptor.

        ZeroMQ is signalling that we should process some events,
        we're starting to send queued messages and to receive
        incoming messages.

        Part of L{IReadDescriptor}.
        """
        if self.scheduled_doRead is not None:
            if not self.scheduled_doRead.called:
                self.scheduled_doRead.cancel()
            self.scheduled_doRead = None

        events = self.socket.getsockopt(constants.EVENTS)
        if (events & constants.POLLOUT) == constants.POLLOUT:
            while self.queue:
                try:
                    self.socket.send(
                        self.queue[0][1], constants.NOBLOCK | self.queue[0][0])
                except error.ZMQError as e:
                    if e.errno == constants.EAGAIN:
                        break
                    self.queue.popleft()
                    raise e

                self.queue.popleft()
        if (events & constants.POLLIN) == constants.POLLIN:
            while True:
                if self.factory is None:  # disconnected
                    return
                try:
                    message = self._readMultipart()
                except error.ZMQError as e:
                    if e.errno == constants.EAGAIN:
                        break

                    raise e

                log.callWithLogger(self, self.messageReceived, message)

    def logPrefix(self):
        """
        Part of L{ILoggingContext}.

        @return: Prefix used during log formatting to indicate context.
        @rtype: C{str}
        """
        return 'ZMQ'

    def _connectOrBind(self, endpoints):
        """
        Connect and/or bind socket to endpoints.
        """
        for endpoint in endpoints:
            if endpoint.type == ZmqEndpointType.connect:
                self.socket.connect(endpoint.address)
            elif endpoint.type == ZmqEndpointType.bind:
                self.socket.bind(endpoint.address)
            else:
                assert False, "Unknown endpoint type %r" % endpoint

    # What follows is the API that the users of this class and its subclasses
    # should use.

    def sendMsg(self, message):
        """
        Provides a higher level wrapper over ZmqConnection.send for sending
        single-part messages.

        @param message: message data
        @type message: C{str}
        """
        return self.sendMultipart([message])

    def sendMultipart(self, parts):
        """
        Send a multipart message via ZeroMQ.

        @param parts: message data
        """
        if not hasattr(parts, '__iter__'):
            raise TypeError("ZmqConnection.sendMultipart requires an iterable")
        self.queue.extend((constants.SNDMORE, m)
                          for m in islice(parts, len(parts) - 1))
        self.queue.append((0, parts[-1]))

        if self.scheduled_doRead is None:
            self.scheduled_doRead = reactor.callLater(0, self.doRead)

    def messageReceived(self, message):
        """
        Called on incoming message from ZeroMQ.

        Override this to handle generic messages received and pass them to
        gotMessage with a socket-type specific signature.

        @param message: message data
        """
        if len(message) > 1:
            self.gotMultipart(message)
        else:
            self.gotMessage(message[0])

    def gotMessage(self, *args, **kwargs):
        """
        Called on an incoming message.

        The default implementation delegates to `ZmqBase.gotMessage` to allow
        implementing message receiving logic in just one handler.
        """
        self.gotMultipart(*args, **kwargs)

    def gotMultipart(self, *args, **kwargs):
        """
        Called on an incoming multipart message.

        Unless ZmqBase.gotMessage is overridden, this method also gets called
        for single-part messages.
        """
        raise NotImplementedError
