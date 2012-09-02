"""
ZeroMQ REQ-REP-like wrappers and the deprecated XREQ-XREP aliases.

Caveat: the XREQ and XREP compatibility aliases do not behave the same way as
the original ZeroMQ XREQ and XREP sockets in the sense that ZeroMQ XREQ and
XREP behave like DEALER and ROUTER, but the aliases her behave just like REQ
and REP.
"""
import uuid
import warnings

from twisted.internet import defer

from zmq.core import constants
from txzmq.connection import ZmqConnection


# TODO: consider adding waitRequest to ZmqReplyConnection to avoid the need to
# subclass it to use it.


class ZmqRequestConnection(ZmqConnection):
    """
    A REQ-like connection.

    This is implemented with an underlying DEALER socket, even though
    semantics are closer to REQ socket.

    Unlike a true REQ socket, supports sending multiple simultaneous requests
    and receiving replies to them in any order chosen by the server. Not having
    support for this would defeat the purpose of Twisted.
    """
    socketType = constants.DEALER

    # the number of new UUIDs to generate when the pool runs out of them
    UUID_POOL_GEN_SIZE = 5

    def __init__(self, *args, **kwargs):
        ZmqConnection.__init__(self, *args, **kwargs)
        self._requests = {}
        self._uuids = []

    def _getNextId(self):
        """
        Returns an unique id.

        By default, generates pool of UUID in increments
        of C{UUID_POOL_GEN_SIZE}. Could be overridden to
        provide custom ID generation.

        @return: generated unique "on the wire" message ID
        @rtype: C{str}
        """
        if not self._uuids:
            for _ in xrange(self.UUID_POOL_GEN_SIZE):
                self._uuids.append(str(uuid.uuid4()))
        return self._uuids.pop()

    def _releaseId(self, msgId):
        """
        Release message ID to the pool.

        @param msgId: message ID, no longer on the wire
        @type msgId: C{str}
        """
        self._uuids.append(msgId)
        if len(self._uuids) > 2 * self.UUID_POOL_GEN_SIZE:
            self._uuids[-self.UUID_POOL_GEN_SIZE:] = []

    def sendMultipart(self, messageParts):
        """
        Send L{message} with specified L{tag}.

        @param messageParts: message data
        @type messageParts: C{tuple}
        """
        d = defer.Deferred()
        messageId = self._getNextId()
        self._requests[messageId] = d
        ZmqConnection.sendMultipart(self, [messageId, ''] + messageParts)
        return d

    def messageReceived(self, message):
        """
        Called on incoming message from ZeroMQ.

        @param message: message data
        """
        # Here, it is not possible to have separate handling of single- and
        # multipart messages because all we have is a single Deferred:
        msgId, _, msg = message[0], message[1], message[2:]
        d = self._requests.pop(msgId)
        self._releaseId(msgId)
        d.callback(msg)


class ZmqReplyConnection(ZmqConnection):
    """
    A REP-like connection.

    This is implemented with an underlying ROUTER socket, but the semantics
    are close to REP socket.

    Unlike a true REP socket, supports handling of multiple simultaneous
    incoming requests and replying to them in any order desired. Not having
    support for this would defeat the purpose of Twisted.
    """
    socketType = constants.ROUTER

    def __init__(self, *args, **kwargs):
        ZmqConnection.__init__(self, *args, **kwargs)
        self._routingInfo = {}  # keep track of routing info

    def sendMsg(self, messageId, message):
        self.sendMultipart(messageId, [message])

    def sendMultipart(self, messageId, messageParts):
        """
        Send L{message} with specified L{tag}.

        @param messageId: message uuid
        @type messageId: C{str}
        @param message: message data
        @type message: C{str}
        """
        routingInfo = self._routingInfo.pop(messageId)
        ZmqConnection.sendMultipart(self, routingInfo + [messageId, ''] + messageParts)

    def messageReceived(self, message):
        """
        Called on incoming message from ZeroMQ.

        @param message: message data
        """
        i = message.index('')
        assert i > 0
        (routingInfo, msgId, payload) = (
            message[:i - 1], message[i - 1], message[i + 1:])
        msgParts = payload[0:]
        self._routingInfo[msgId] = routingInfo
        if len(msgParts) > 1:
            self.gotMultipart(msgId, msgParts)
        else:
            self.gotMessage(msgId, msgParts[0])

    def gotMessage(self, messageId, message):
        """
        Called on incoming message.

        @param messageId: message uuid
        @type messageId: C{str}
        @param message: message
        """
        self.gotMultipart(messageId, [message])

    def gotMultipart(self, messageId, messageParts):
        """
        Called on incoming message.

        @param messageId: message uuid
        @type messageId: C{str}
        @param messageParts: message data
        """
        raise NotImplementedError(self)

    def reply(self, messageId, message):
        """
        Semantic alias for ZmqReplyConnection.sendMsg
        """
        self.sendMsg(messageId, message)

    def replyMultipart(self, messageId, messageParts):
        """
        Semantic alias for ZmqReplyConnection.sendMultipart
        """
        self.sendMultipart(messageId, messageParts)


# TODO: *if* we're going to break backwards compatibility anyway, we should
# just remove these 4 aliases, but I'll keep them for now.

class ZmqXREQConnection(ZmqRequestConnection):
    """
    Provided for backwards compatibility.

    Deprecated in favour of either ZmqRequestConnection or ZmqDealerConnection.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn("ZmqXREQConnection is deprecated in favour of "
                      "either ZmqReqConnection or ZmqDealerConnection",
                      DeprecationWarning)
        ZmqRequestConnection.__init__(self, *args, **kwargs)


class ZmqXREPConnection(ZmqReplyConnection):
    """
    Provided for backwards compatibility.

    Deprecated in favour of either ZmqReplyConnection or ZmqRouterConnection.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn("ZmqXREPConnection is deprecated in favour of "
                      "either ZmqReplyConnection or ZmqRouterConnection",
                      DeprecationWarning)
        ZmqReplyConnection.__init__(self, *args, **kwargs)


class ZmqREQConnection(ZmqRequestConnection):
    """
    Provided for backwards compatibility.

    Deprecated alias of ZmqRequestConnection.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn("ZmqREQConnection is a deprecated alias of "
                      "ZmqRequestConnection", DeprecationWarning)
        ZmqRequestConnection.__init__(self, *args, **kwargs)


class ZmqREPConnection(ZmqReplyConnection):
    """
    Provided for backwards compatibility.

    Deprecated alias of ZmqReplyConnection.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn("ZmqREPConnection is a deprecated alias of "
                      "ZmqReplyConnection", DeprecationWarning)
        ZmqReplyConnection.__init__(self, *args, **kwargs)
