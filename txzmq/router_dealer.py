"""
ZeroMQ ROUTER and DEALER connection types.
"""
from zmq.core import constants
from .base import ZmqBase


class ZmqDealerConnection(ZmqBase):
    """
    A DEALER connection.
    """
    socketType = constants.DEALER


class ZmqRouterConnection(ZmqBase):
    """
    A ROUTER connection.
    """
    socketType = constants.ROUTER

    def sendMsg(self, recipientId, message):
        self.send([recipientId, message])

    def sendMultipart(self, recipientId, parts):
        self.send([recipientId] + parts)

    def messageReceived(self, message):
        sender_id = message.pop(0)
        self.gotMessage(sender_id, message)
