# encoding: utf-8
__author__ = 'stef'

import logging

import pika
from pika.adapters.tornado_connection import TornadoConnection
from tornado.ioloop import IOLoop
from tornado.gen import coroutine, Task, Return
from tornado.concurrent import Future, chain_future
from toro import Event

from . import RabbitMQConnectionError

logger = logging.getLogger(__name__)


class Publisher(object):

    def __init__(self, rabbitmq_config):

        self._parameters = pika.ConnectionParameters(
            host=rabbitmq_config['host'],
            port=rabbitmq_config['port'],
            credentials=pika.PlainCredentials(rabbitmq_config['user'], rabbitmq_config['password']),
            virtual_host=rabbitmq_config['vhost']
        )
        self.rabbitmq_config = rabbitmq_config
        self.connection = None
        self.channel = None
        self.shutting_down = False

        self._reset_counters()

        # IOLoop.instance().add_callback(self.connect)

    def _reset_counters(self):
        self._delivery_tag = 0
        self._ack = 0
        self._nack = 0
        self._confirmation_callbacks = {}
        self.futures_ack = dict()

    def _open_channel(self, callback=None):
        logger.info("Opening channel to RabbitMQ publisher")
        self.connection.channel(on_open_callback=callback)

    @coroutine
    def start(self):
        """
        Starts the publisher

        Note
        ====
        Coroutine
        """
        logger.info("Connecting to RabbitMQ publisher")
        self.shutting_down = False
        error_connect_future = Future()
        connect_future = Future()

        def on_connect_error(conn, errormsg=None):
            error_connect_future.set_result((False, conn, errormsg))

        def on_connect_open(conn):
            connect_future.set_result((True, conn, None))

        ev = Event()

        def on_connection_close(conn, reply_code, reply_text):
            logger.info("Connection to RabbitMQ publisher has been closed")
            self.connection = None
            self.channel = None
            if self.shutting_down:
                # its a normal shutdown
                self.shutting_down = False
            else:
                # unexpected connection closed, let's notify the client
                ev.set()

        def on_channel_close(chan, reply_code, reply_text):
            logger.info("Channel to RabbitMQ publisher has been closed")
            self._reset_counters()
            self.channel = None

        self.connection = TornadoConnection(
            self._parameters,
            on_open_callback=on_connect_open,
            on_open_error_callback=on_connect_error,
            on_close_callback=None,
            custom_ioloop=IOLoop.instance(),
            stop_ioloop_on_close=False,
        )

        chain_future(error_connect_future, connect_future)
        (res, connection, error_msg) = yield connect_future
        if not res:
            raise RabbitMQConnectionError(error_msg)
        logger.info("Connection to RabbitMQ publisher has been opened")
        if connection is not None:
            connection.add_on_close_callback(on_connection_close)
        channel = yield Task(self._open_channel)
        logger.info("Channel to RabbitMQ publisher has been opened")
        self._reset_counters()
        self.channel = channel
        self.channel.confirm_delivery(self._on_delivery_confirmation)
        self.channel.add_on_close_callback(on_channel_close)
        # give an Event object to client, so that it can detect when connection has been closed
        raise Return(ev)

    @coroutine
    def publish(self, exchange, body, routing_key='', message_id=None, headers=None,
                content_type="application/json", content_encoding="utf-8", persistent=True):

        """
        publish(exchange, body, routing_key='', message_id=None, headers=None, content_type="application/json", content_encoding="utf-8", persistent=True)
        Publish a message to RabbitMQ

        :param exchange: publish to this exchange
        :type exchange: str
        :param body: message body
        :type body: str
        :param routing_key: optional routing key
        :type routing_key: str
        :param message_id: optional ID for the message
        :type message_id: str
        :param headers: optional headers
        :type headers: dict
        :param content_type: message content type
        :type content_type: str
        :param content_encoding: message charset
        :type content_encoding: str
        :param persistent: if True, message will be persisted in RabbitMQ
        :type persistent: bool
        :return: True if publication was acknowledged by RabbitMQ, False otherwise
        :rtype: bool

        Note
        ====
        Coroutine
        """

        if self.connection is None:
            logger.warning('Impossible to publish: no connection')
            raise Return(False)

        if not self.connection.is_open:
            logger.warning('Impossible to publish: connection is not opened')
            raise Return(False)

        if self.channel is None:
            logger.warning('Impossible to publish: no channel')
            self._open_channel()
            raise Return(False)

        if not self.channel.is_open:
            logger.warning('Impossible to publish: channel is not opened')
            self._open_channel()
            raise Return(False)

        mode = 2 if persistent else 1

        publish_properties = pika.BasicProperties(
            content_type=content_type,
            content_encoding=content_encoding,
            app_id=self.rabbitmq_config['application_id'],
            type=self.rabbitmq_config['event_type'],
            delivery_mode=mode,
            message_id=message_id,
            headers=headers
        )

        self._delivery_tag += 1
        tag = self._delivery_tag
        self.futures_ack[self._delivery_tag] = Future()
        logger.debug("Publishing message: {}".format(tag))
        self.channel.basic_publish(exchange, routing_key, body, publish_properties)
        res = yield self.futures_ack[tag]
        del self.futures_ack[tag]
        raise Return(res)

    def _on_delivery_confirmation(self, method_frame):
        confirmation_type = method_frame.method.NAME.split('.')[1].lower()
        tag = method_frame.method.delivery_tag
        multiple = method_frame.method.multiple

        if confirmation_type == 'ack':
            if multiple:
                logger.info("Publisher: Multiple ACK up to tag: {}".format(tag))
                all_confirmed_tags = [t for t in self._confirmation_callbacks.keys() if t <= tag]
                for t in all_confirmed_tags:
                    self.futures_ack[t].set_result(True)
                self._ack += len(all_confirmed_tags)
            else:
                logger.info("Publisher: Received ACK for message '{}'".format(tag))
                self.futures_ack[tag].set_result(True)
                self._ack += 1
        else:
            logger.warning("Publisher: Received NACK for message '{}'".format(tag))
            self.futures_ack[tag].set_result(False)
            self._nack += 1

        logger.info("'Publisher: {}' deliveries on '{}' messages".format(self._ack + self._nack, self._delivery_tag))

    def stop(self):
        """
        Stops the publisher
        """
        if self.connection is None:
            return
        if self.connection.is_closed or self.connection.is_closing:
            return
        self.shutting_down = True
        self.connection.close()

