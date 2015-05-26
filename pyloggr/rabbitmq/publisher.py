# encoding: utf-8

"""
The publisher module provides the `Publisher` class to publish messages to RabbitMQ
"""

__author__ = 'stef'

import logging
from datetime import timedelta

import pika
from pika.adapters.tornado_connection import TornadoConnection
from tornado.ioloop import IOLoop
from tornado.gen import coroutine, Task, Return, with_timeout, TimeoutError
from tornado.concurrent import Future, chain_future
from toro import Event

from . import RabbitMQConnectionError

logger = logging.getLogger(__name__)


class Publisher(object):
    """
    Publisher encapsulates the logic for async publishing to RabbitMQ

    Parameters
    ==========
    rabbitmq_config: pyloggr.config.RabbitMQBaseConfig
        RabbitMQ connection parameters
    base_routing_key: str
        This routing key will be used if no routing_key is provided to publish methods
    """

    def __init__(self, rabbitmq_config, base_routing_key=u''):
        """
        :type rabbitmq_config: pyloggr.config.RabbitMQBaseConfig
        :type base_routing_key: str
        """

        self._parameters = pika.ConnectionParameters(
            host=rabbitmq_config.host,
            port=rabbitmq_config.port,
            credentials=pika.PlainCredentials(rabbitmq_config.user, rabbitmq_config.password),
            virtual_host=rabbitmq_config.vhost
        )
        self.rabbitmq_config = rabbitmq_config
        self.connection = None
        self.channel = None
        self.closing = False
        self.connection_has_been_closed_event = None
        self.base_routing_key = base_routing_key

        self._reset_counters()
        # IOLoop.instance().add_callback(self.connect)

    def _reset_counters(self):
        self._delivery_tag = 0
        self._ack = 0
        self._nack = 0
        self.futures_ack = dict()

    def _open_channel(self, callback=None):
        logger.info("Opening channel to RabbitMQ publisher")
        self.connection.channel(on_open_callback=callback)

    @coroutine
    def start(self):
        """
        start()
        Starts the publisher.

        start() raises RabbitMQConnectionError if no connection can be established.
        If connection succeeds, it returns a toro.Event object that will resolve when connection will be lost

        Note
        ====
        Coroutine
        """
        logger.info("Connecting to RabbitMQ publisher")
        self._reset_counters()
        error_connect_future = Future()
        connect_future = Future()
        self.closing = False

        def _on_connect_error(conn, errormsg=None):
            error_connect_future.set_result((False, conn, errormsg))

        def _on_connect_open(conn):
            connect_future.set_result((True, conn, None))

        self.connection_has_been_closed_event = Event()

        # noinspection PyUnusedLocal
        def _on_connection_close(conn, reply_code, reply_text):
            logger.info("Connection to RabbitMQ publisher has been closed!")
            current_tags = self.futures_ack.keys()
            for tag in current_tags:
                if not self.futures_ack[tag].done():
                    self.futures_ack[tag].set_result(False)
            self.connection = None
            self.channel = None
            # notify who is using the publisher
            self.connection_has_been_closed_event.set()
            self.closing = False

        # noinspection PyUnusedLocal
        def _on_channel_close(chan, reply_code, reply_text):
            logger.info("Channel to RabbitMQ publisher has been closed!")
            current_tags = self.futures_ack.keys()
            for tag in current_tags:
                if not self.futures_ack[tag].done():
                    self.futures_ack[tag].set_result(False)
            self.channel = None
            self.closing = True
            self.connection.close()

        self.connection = TornadoConnection(
            self._parameters,
            on_open_callback=_on_connect_open,
            on_open_error_callback=_on_connect_error,
            on_close_callback=None,
            custom_ioloop=IOLoop.current(),
            stop_ioloop_on_close=False,
        )

        chain_future(error_connect_future, connect_future)
        try:
            (res, connection, error_msg) = yield with_timeout(timedelta(seconds=10), connect_future)
        except TimeoutError:
            logger.error("publisher: connect to rabbitmq timeout")
            raise
        else:
            if not res:
                raise RabbitMQConnectionError(error_msg)
            logger.info("Connection to RabbitMQ publisher has been opened")
            if connection is not None:
                connection.add_on_close_callback(_on_connection_close)
            channel = yield Task(self._open_channel)
            logger.info("Channel to RabbitMQ publisher has been opened")
            self._reset_counters()
            self.channel = channel
            self.channel.confirm_delivery(self._on_delivery_confirmation)
            self.channel.add_on_close_callback(_on_channel_close)
            # give an Event object to client, so that it can detect when connection has been closed
            raise Return(self.connection_has_been_closed_event)

    # noinspection PyDocstring
    @coroutine
    def publish(self, exchange, body, routing_key=u'', message_id=None, headers=None,
                content_type="application/json", content_encoding="utf-8", persistent=True,
                application_id=u'', event_type=u''):

        """
        publish(exchange, body, routing_key='', message_id=None, headers=None, content_type="application/json", content_encoding="utf-8", persistent=True, application_id=None, event_type=None)
        Publish a message to RabbitMQ

        Parameters
        ==========
        exchange: str
            publish to this exchange
        body: str
            message body
        routing_key: str
            optional routing key
        message_id: str
            optional ID for the message
        headers: dict
            optional message headers
        content_type: str
            message content type
        content_encoding: str
            message charset
        persistent: bool
            if True, message will be persisted in RabbitMQ
        application_id: str
            optional application ID
        event_type: str
            optional message type

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
        application_id = application_id if application_id else self.rabbitmq_config.application_id
        event_type = event_type if event_type else self.rabbitmq_config.event_type

        publish_properties = pika.BasicProperties(
            content_type=content_type,
            content_encoding=content_encoding,
            app_id=application_id,
            type=event_type,
            delivery_mode=mode,
            message_id=message_id,
            headers=headers
        )

        self._delivery_tag += 1
        current_tag = self._delivery_tag

        self.futures_ack[current_tag] = Future()
        logger.debug("Publishing message: {}".format(current_tag))
        if not routing_key:
            routing_key = self.base_routing_key
        self.channel.basic_publish(exchange, routing_key, body, publish_properties)
        res = yield self.futures_ack[current_tag]
        del self.futures_ack[current_tag]
        raise Return(res)

    @coroutine
    def publish_event(self, event, routing_key=u'', exchange=u'', application_id=u'', event_type=u''):
        """
        publish_event(event, routing_key=u'', exchange=u'', application_id=u'', event_type=u'')
        Publish an Event object in RabbitMQ. Always persistent.

        :param event: Event object
        :type event: pyloggr.event.Event
        :param routing_key: RabbitMQ routing key
        :type routing_key: str or unicode
        :param exchange: optional exchange (override global config)
        :type exchange: str or unicode
        :param application_id: optional application ID (override global config)
        :type application_id: str or unicode
        :param event_type: optional event type (override global config)
        :type event_type: str or unicode

        Note
        ====
        Tornado coroutine
        """
        json_event = event.dump_json()
        if not routing_key:
            routing_key = self.base_routing_key
        # publish the event in RabbitMQ in JSON format
        exchange = str(exchange) if exchange else self.rabbitmq_config.exchange
        result = yield self.publish(
            exchange=exchange,
            body=json_event,
            routing_key=routing_key,
            message_id=event.uuid,
            persistent=True,
            application_id=application_id,
            event_type=event_type
        )
        raise Return((result, event))

    def _on_delivery_confirmation(self, method_frame):
        confirmation_type = method_frame.method.NAME.split('.')[1].lower()
        tag = method_frame.method.delivery_tag
        multiple = method_frame.method.multiple

        if confirmation_type == 'ack':
            if multiple:
                logger.debug("Publisher: Multiple ACK up to tag: {}".format(tag))
                all_confirmed_tags = [t for t, f in self.futures_ack.items() if t <= tag and not f.done()]
                for t in all_confirmed_tags:
                    self.futures_ack[t].set_result(True)
                self._ack += len(all_confirmed_tags)
            else:
                logger.debug("Publisher: Received ACK for message '{}'".format(tag))
                self.futures_ack[tag].set_result(True)
                self._ack += 1
        else:
            logger.warning("Publisher: Received NACK for message '{}'".format(tag))
            self.futures_ack[tag].set_result(False)
            self._nack += 1

        logger.debug("'Publisher: {}' deliveries on '{}' messages".format(self._ack + self._nack, self._delivery_tag))

    @coroutine
    def stop(self):
        """
        stop()
        Stops the publisher

        Note
        ====
        Tornado coroutine
        """
        if not self.closing:
            self.closing = True
            if self.connection is None:
                return
            if self.connection.is_closed or self.connection.is_closing:
                return
            self.connection.close()
        yield self.connection_has_been_closed_event.wait()
