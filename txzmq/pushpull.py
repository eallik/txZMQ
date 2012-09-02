"""
ZeroMQ PUSH-PULL wrappers.
"""
from zmq.core import constants

from txzmq.base import ZmqBase


class ZmqPushConnection(ZmqBase):
    """
    Publishing in broadcast manner.
    """
    socketType = constants.PUSH

    def push(self, message):
        """
        Push a message L{message}.

        Semantic alias for ZmqPushConnection.sendMsg

        @param message: message data
        @type message: C{str}
        """
        self.sendMsg(message)

    def pushMultipart(self, messageParts):
        """
        Push a multipart message L{messageParts}.

        Semantic alias for ZmqPushConnection.sendMultipart

        @param messageParts: message data
        @type message: C{list}
        """
        self.sendMultipart(messageParts)


class ZmqPullConnection(ZmqBase):
    """
    Pull messages from a socket
    """
    socketType = constants.PULL

    def gotMessage(self, message):
        """
        Called on incoming message received by puller.

        @param message: message
        """
        self.onPull(message)  # XXX: API inconsistency due to onPull/onPullMultipart

    def gotMultipart(self, messageParts):
        """
        Called on incoming multipart message received by puller.

        @param messageParts: message data
        """
        self.onPullMultipart(messageParts)

    # XXX: not sure if these are a good idea at all--they cause the generic
    # gotMessage->gotMultipart delegation to break thus introducing an API
    # inconsistency

    def onPull(self, message):
        """
        Semantic alias for ZmqPullConnection.gotMessage

        @param message: message
        """
        raise NotImplementedError(self)

    def onPullMultipart(self, messageParts):
        """
        Semantic alias for ZmqPullConnection.gotMultipart

        @param messageParts: message data
        """
        raise NotImplementedError(self)
