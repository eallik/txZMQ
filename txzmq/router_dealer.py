"""
ZeroMQ ROUTER and DEALER connection types.
"""
from zmq.core import constants
from txzmq.connection import ZmqConnection


class ZmqDealerConnection(ZmqConnection):
    """
    A DEALER connection.
    """
    socketType = constants.DEALER


class ZmqRouterConnection(ZmqConnection):
    """
    A ROUTER connection.
    """
    socketType = constants.ROUTER

    def sendMsg(self, recipientId, message):
        self.sendMultipart(recipientId, [message])

    def sendMultipart(self, recipientId, parts):
        ZmqConnection.sendMultipart(self, [recipientId] + parts)

    def messageReceived(self, message):
        senderId = message.pop(0)
        if len(message) == 1:
            self.gotMessage(senderId, message[0])
        else:
            self.gotMultipart(senderId, message)

    def gotMessage(self, senderId, message):
        self.gotMultipart(senderId, [message])
