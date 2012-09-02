from txzmq.connection import ZmqConnection


# TODO: ideally, all connection classes would inherit from this in the future
# so that we'd have a consistent wrapper API over all underlying socket types.
class ZmqBase(ZmqConnection):
    """
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
    """
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
        Provides a higher level wrapper over ZmqConnection.send for sending
        multipart messages.

        @param parts: message data
        """
        self.send(parts)

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
