"""
ZeroMQ PUB-SUB wrappers.
"""
from zmq.core import constants

from txzmq.base import ZmqBase


class ZmqPubConnection(ZmqBase):
    """
    Publishing in broadcast manner.
    """
    socketType = constants.PUB

    def sendMsg(self, message, tag=''):
        """
        Broadcast L{message} with specified L{tag}.

        @param message: message data
        @type message: C{str}
        @param tag: message tag
        @type tag: C{str}
        """
        self.send([tag + '\0' + message])

    def sendMultipart(self, messageParts, tag=''):
        # TODO:
        raise NotImplementedError

    def publish(self, message, tag=''):
        self.sendMsg(message, tag)

    def publishMultipart(self, messageParts, tag=''):
        self.sendMultipart(messageParts, tag)

    def __repr__(self):
        return 'PUB'


class ZmqSubConnection(ZmqBase):
    """
    Subscribing to messages.
    """
    socketType = constants.SUB

    def subscribe(self, tag):
        """
        Subscribe to messages with specified tag (prefix).

        @param tag: message tag
        @type tag: C{str}
        """
        self.socket.setsockopt(constants.SUBSCRIBE, tag)

    def unsubscribe(self, tag):
        """
        Unsubscribe from messages with specified tag (prefix).

        @param tag: message tag
        @type tag: C{str}
        """
        self.socket.setsockopt(constants.UNSUBSCRIBE, tag)

    def messageReceived(self, message):
        """
        Called on incoming message from ZeroMQ.

        @param message: message data
        """
        # TODO: fix the bug
        # TODO: support multipart messages
        # TODO: support multipart messages with 0-byte compatibility
        if len(message) == 2:  # XXX: this will be a bug with a 2 char string
            # compatibility receiving of tag as first part
            # of multi-part message
            tag, message = message
            self.gotMessage(message, tag)
        else:
            assert len(message) == 1
            tag, message = message[0].split('\0', 1)
            self.gotMessage(message, tag)

    def gotMessage(self, message, tag):
        """
        Called on incoming message recevied by subscriber

        @param message: message data
        @param tag: message tag
        """
        self.gotMultipart([message], tag)

    def gotMultipart(self, messageParts, tag):
        """
        Called on incoming multipart message recevied by subscriber

        @param messageParts: message data
        @param tag: message tag
        """
        raise NotImplementedError(self)
