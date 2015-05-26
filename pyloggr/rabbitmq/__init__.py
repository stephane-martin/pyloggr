# encoding: utf-8

"""
The pyloggr.rabbitmq subpackage provides classes for publishing and consuming to/from RabbitMQ.
Pika library is used, but the pika callback style has been workarounded in coroutines.

------------------------
"""
__author__ = 'stef'

import socket
import logging

class RabbitMQMessage(object):
    """
    Represents a message from RabbitMQ

    `Consumer.start_consuming` returns a queue. Elements in the queue have `RabbitMQMessage` type.
    """

    __slots__ = ('delivery_tag', 'props', 'body', 'channel')

    def __init__(self, delivery_tag, props, body, channel):
        self.delivery_tag = delivery_tag
        self.props = props
        self.body = body
        self.channel = channel

    def ack(self):
        """
        Acknowledge the message to RabbitMQ
        """
        logger = logging.getLogger(__name__)
        logger.info("Consumer: ACK message: {}".format(self.delivery_tag))
        if self.channel:
            if self.channel.is_open:
                self.channel.basic_ack(self.delivery_tag)
        else:
            logger.error("Can't ACK message: no channel")

    def nack(self):
        """
        Acknowledge NOT the message to RabbitMQ
        """
        logger = logging.getLogger(__name__)
        logger.warning("Consumer: NACK message: {}".format(self.delivery_tag))
        if self.channel:
            if self.channel.is_open:
                self.channel.basic_nack(self.delivery_tag, requeue=True)
        else:
            logger.error("Can't NACK message: no channel")


class RabbitMQConnectionError(socket.error):
    """
    Exception triggered when connection to RabbitMQ fails
    """
    pass


class Configuration(object):
    def __init__(self, host, port, user, password, vhost, queue=None, exchange=None, qos=None,
                 application_id='pyloggr', event_type='', binding_key=None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.vhost = vhost
        self.queue = queue if queue else None
        self.exchange = exchange if exchange else None
        self.qos = qos if qos else None
        self.application_id = application_id if application_id else ''
        self.event_type = event_type if event_type else ''
        self.binding_key = binding_key if binding_key else None


