# encoding: utf-8

"""
Provide the Consumer class to manage a consumer connection to RabbitMQ
"""

__author__ = 'stef'

import logging
from random import randint
from datetime import timedelta

from pika import ConnectionParameters, PlainCredentials
from pika.exceptions import ChannelClosed, ConnectionClosed
from pika.adapters import TornadoConnection
from tornado.ioloop import IOLoop
from tornado.gen import coroutine, Future, chain_future, Task, Return, with_timeout, TimeoutError
from toro import Event

from . import RabbitMQConnectionError, RabbitMQMessage
from pyloggr.utils.simple_queue import SimpleToroQueue, TimeoutTask

logger = logging.getLogger(__name__)


class Consumer(object):
    """
    A consumer connects to some RabbitMQ instance and eats messages from it

    Parameters
    ==========
    rabbitmq_config: pyloggr.config.RabbitMQBaseConfig
        RabbitMQ consumer configuration
    """

    def __init__(self, rabbitmq_config):
        """
        :type rabbitmq_config: pyloggr.rabbitmq.Configuration
        """
        self._parameters = ConnectionParameters(
            host=rabbitmq_config.host,
            port=rabbitmq_config.port,
            credentials=PlainCredentials(rabbitmq_config.user, rabbitmq_config.password),
            virtual_host=rabbitmq_config.vhost
        )
        self.rabbitmq_config = rabbitmq_config
        self.connection = None
        self.channel = None
        self.closing = False
        self.message_queue = None
        self._consumer_tag = None
        self.consumer_closed_event = None

    def _open_channel(self, callback=None):
        logger.info("Opening channel to RabbitMQ consumer")
        self.connection.channel(on_open_callback=callback)

    @coroutine
    def start(self):
        """
        start()
        Opens the connection to RabbitMQ as a consumer

        :returns: a Toro.Event object that triggers when the connection to RabbitMQ is lost

        Note
        ====
        Coroutine
        """
        logger.info('Connecting to RabbitMQ')
        self.closing = False
        error_connect_future = Future()
        connect_future = Future()

        def _on_connect_error(conn, errormsg=None):
            error_connect_future.set_result((False, conn, errormsg))

        def _on_connect_open(conn):
            connect_future.set_result((True, conn, None))

        self.consumer_closed_event = Event()

        # noinspection PyUnusedLocal
        def _on_connection_closed(conn, reply_code, reply_text):
            logger.info("Connection to RabbitMQ consumer has been closed")
            self.connection = None
            self.channel = None
            self.closing = False
            # let's notify the client
            self.consumer_closed_event.set()

        # noinspection PyUnusedLocal
        def _on_channel_closed(chan, reply_code, reply_text):
            logger.info("Channel to RabbitMQ consumer has been closed")
            self.channel = None
            self.queue = None
            self.message_queue = None
            self.closing = True
            logger.warning('RabbitMQ consumer channel was closed: ({}) {}'.format(reply_code, reply_text))
            self._close_connection()

        self.connection = TornadoConnection(
            self._parameters,
            on_open_callback=_on_connect_open,
            on_open_error_callback=_on_connect_error,
            on_close_callback=None,
            custom_ioloop=IOLoop.instance(),
            stop_ioloop_on_close=False,
        )
        chain_future(error_connect_future, connect_future)
        try:
            (res, connection, error_msg) = yield with_timeout(timedelta(seconds=10), connect_future)
        except TimeoutError:
            logger.error("consumer: connection to rabbitmq timeout")
            raise
        if not res:
            raise RabbitMQConnectionError(error_msg)

        logger.info("Connection to RabbitMQ consumer has been opened")
        if connection is not None:
            connection.add_on_close_callback(_on_connection_closed)
        channel = yield Task(self._open_channel)
        if self.rabbitmq_config.qos:
            channel.basic_qos(prefetch_count=self.rabbitmq_config.qos)
        logger.info("Channel to RabbitMQ consumer has been opened")
        self.channel = channel
        self.channel.add_on_close_callback(_on_channel_closed)
        self.message_queue = SimpleToroQueue()
        if self.rabbitmq_config.queue:
            # durable queue has already been declared
            queue_name = self.rabbitmq_config.queue
            logger.info("Consumer: declaring queue '{}'".format(queue_name))
            try:
                yield TimeoutTask(
                    self.channel.queue_declare,
                    deadline=timedelta(seconds=5),
                    queue=queue_name,
                    passive=True,
                    exclusive=False,
                    auto_delete=False
                )
                logger.info("Queue has been checked and declared OK. Ready to consume.")
            except TimeoutError:
                # we wait at most 5 seconds for rabbitmq
                logger.error("RabbitMQ did not respond to declare queue. Does the queue exist?")
                raise

        else:
            # we build an transient queue
            queue_name = "consumer_" + str(randint(1, 1000000))
            self.rabbitmq_config.queue = queue_name
            logger.info("Consumer: declaring queue '{}'".format(queue_name))
            yield Task(
                self.channel.queue_declare,
                queue=queue_name,
                passive=False,
                durable=False,
                exclusive=True,
                auto_delete=True
            )
            yield Task(
                self.channel.queue_bind,
                queue=queue_name,
                exchange=self.rabbitmq_config.exchange,
                routing_key=self.rabbitmq_config.binding_key,
            )
            logger.info("Queue has been checked and declared OK and bound. Ready to consume.")

        # give an Event object to client, so that it can detect when connection has been closed
        raise Return(self.consumer_closed_event)

    def _close_connection(self):
        self.closing = True
        if self.connection:
            try:
                self.connection.close()
            except ConnectionClosed:
                logger.info('Connection already closed')

    def _close_channel(self):
        self.closing = True
        if self.channel:
            try:
                self.channel.close()
            except ChannelClosed:
                logger.info("Channel already closed")
                self._close_connection()
        else:
            self._close_connection()

    def start_consuming(self):
        """
        Starts consuming messages from RabbitMQ

        :returns: a Toro message queue that stores the messages when they arrive
        :rtype: SimpleToroQueue
        """
        if self.channel is None:
            return
        if not self.channel.is_open:
            return
        logger.info('Start consuming')
        self.channel.add_on_cancel_callback(self._on_consumer_cancelled_by_server)
        self._consumer_tag = self.channel.basic_consume(
            consumer_callback=self._on_message,
            queue=self.rabbitmq_config.queue
        )
        return self.message_queue

    # noinspection PyUnusedLocal
    def _on_message(self, unused_channel, basic_deliver, properties, body):
        """
        Invoked by pika when a message is delivered from RabbitMQ.
        """
        logger.debug('Consumer: received message # %s from %s', basic_deliver.delivery_tag, properties.app_id)

        self.message_queue.put(
            RabbitMQMessage(
                basic_deliver.delivery_tag, properties, body, self.channel
            )
        )

    @coroutine
    def stop_consuming(self):
        """
        stop_consuming()
        Stops consuming messages from RabbitMQ

        Note
        ====
        Tornado coroutine
        """
        if self._consumer_tag:
            if self.channel:
                logger.info('Asking RabbitMQ to cancel the consumer')
                yield Task(self.channel.basic_cancel, consumer_tag=self._consumer_tag)
                logger.info('RabbitMQ confirmed the cancellation of the consumer')
            self._consumer_tag = None
        self._close_channel()

    @property
    def consuming(self):
        """
        Returns true if the consumer is actually in consuming state
        """
        return (not self.closing) and (self._consumer_tag is not None)

    # noinspection PyUnusedLocal
    def _on_consumer_cancelled_by_server(self, method_frame=None):
        """
        Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer.
        """
        logger.info('Consumer was cancelled remotely, shutting down')
        self._consumer_tag = None
        self._close_channel()

    @coroutine
    def stop(self):
        """
        stop()
        Shutdowns the connection to RabbitMQ and stops the consumer

        Note
        ====
        Tornado coroutine
        """
        if not self.closing:
            logger.info('Stopping consumer')
            self.closing = True
            yield self.stop_consuming()

        yield self.consumer_closed_event.wait()
