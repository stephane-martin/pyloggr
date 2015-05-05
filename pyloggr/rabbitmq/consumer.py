# encoding: utf-8
__author__ = 'stef'

import logging
from random import randint

from pika import ConnectionParameters, PlainCredentials
from pika.exceptions import ChannelClosed, ConnectionClosed
from pika.adapters import TornadoConnection
from tornado.ioloop import IOLoop
from tornado.gen import coroutine, Future, chain_future, Task, Return
from toro import Event

from . import RabbitMQConnectionError
from pyloggr.utils.simple_queue import SimpleToroQueue

logger = logging.getLogger(__name__)


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
        logger.info("Consumer: ACK message: {}".format(self.delivery_tag))
        if self.channel:
            self.channel.basic_ack(self.delivery_tag)
        else:
            logger.error("Can't ACK message: no channel")

    def nack(self):
        """
        NOT acknowledge the message to RabbitMQ
        """
        logger.warning("Consumer: NACK message: {}".format(self.delivery_tag))
        if self.channel:
            self.channel.basic_nack(self.delivery_tag, requeue=True)
        else:
            logger.error("Can't NACK message: no channel")


class Consumer(object):
    """
    A consumer connects to some RabbitMQ instance and eats messages from it

    If binding_key is provided to the constructor, a transient queue is created, and bound to
    the exchange, using the given binding_key to filter messages.

    If binding_key is not provided, a queue has to prexist in RabbitMQ. The queue name must be
    given in rabbitmq_config
    """

    def __init__(self, rabbitmq_config, binding_key=None):
        """
        :param rabbitmq_config: RabbitMQ consumer configuration
        :type rabbitmq_config: pyloggr.config.RabbitMQBaseConfig
        :param binding_key: optional binding key for the consumer
        :type binding_key: str
        """
        self._parameters = ConnectionParameters(
            host=rabbitmq_config.host,
            port=rabbitmq_config.port,
            credentials=PlainCredentials(rabbitmq_config.user, rabbitmq_config.password),
            virtual_host=rabbitmq_config.vhost
        )
        self.rabbitmq_config = rabbitmq_config
        self.binding_key = binding_key
        self.connection = None
        self.channel = None
        self.shutting_down = False
        self.message_queue = None

        self._consumer_tag = None

    def _open_channel(self, callback=None):
        logger.info("Opening channel to RabbitMQ consumer")
        self.connection.channel(on_open_callback=callback)

    @coroutine
    def start(self, qos=None):
        """
        Opens the connection to RabbitMQ as a consumer

        :param qos: how many messages RabbitMQ should send at once ?
        :type qos: int
        :returns: a Toro event that triggers when the connection to RabbitMQ is lost

        Note
        ====
        Coroutine
        """
        logger.info('Connecting to RabbitMQ')
        self.shutting_down = False
        error_connect_future = Future()
        connect_future = Future()

        def on_connect_error(conn, errormsg=None):
            error_connect_future.set_result((False, conn, errormsg))

        def on_connect_open(conn):
            connect_future.set_result((True, conn, None))

        closed_event = Event()

        def on_connection_closed(conn, reply_code, reply_text):
            logger.info("Connection to RabbitMQ consumer has been closed")
            self.connection = None
            self.channel = None
            if self.shutting_down:
                # its a normal shutdown
                self.shutting_down = False
            else:
                # unexpected connection closed, let's notify the client
                closed_event.set()

        def on_channel_closed(chan, reply_code, reply_text):
            logger.info("Channel to RabbitMQ consumer has been closed")
            self.channel = None
            self.queue = None
            self.message_queue = None
            if self.shutting_down:
                logger.info("RabbitMQ consumer channel has been (willingly) closed")
            else:
                logger.warning('RabbitMQ consumer channel was closed: ({}) {}'.format(reply_code, reply_text))
            self._close_connection()

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
        logger.info("Connection to RabbitMQ consumer has been opened")
        if connection is not None:
            connection.add_on_close_callback(on_connection_closed)
        channel = yield Task(self._open_channel)
        if qos:
            channel.basic_qos(prefetch_count=qos)
        logger.info("Channel to RabbitMQ consumer has been opened")
        self.channel = channel
        self.channel.add_on_close_callback(on_channel_closed)
        self.message_queue = SimpleToroQueue()
        if getattr(self.rabbitmq_config, 'queue', None):
            # durable queue has already been declared
            queue_name = self.rabbitmq_config.queue
            logger.info("Consumer: declaring queue '{}'".format(queue_name))
            yield Task(
                self.channel.queue_declare,
                queue=queue_name,
                passive=True,
                exclusive=False,
                auto_delete=False
            )
            logger.info("Queue has been checked and declared OK. Ready to consume.")

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
                routing_key=self.binding_key,
            )
            logger.info("Queue has been checked and declared OK and bound. Ready to consume.")

        # give an Event object to client, so that it can detect when connection has been closed
        raise Return(closed_event)

    def _close_connection(self):
        if self.connection:
            try:
                self.connection.close()
            except ConnectionClosed:
                logger.info('Connection already closed')

    def _close_channel(self):
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

        Note
        ====
        Tornado coroutine
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

        self.message_queue.put_nowait(
            RabbitMQMessage(
                basic_deliver.delivery_tag, properties, body, self.channel
            )
        )

    @coroutine
    def stop_consuming(self):
        """
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

    def _on_consumer_cancelled_by_server(self, method_frame):
        """
        Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer.
        """
        logger.info('Consumer was cancelled remotely, shutting down')
        self._consumer_tag = None
        self._close_channel()

    @coroutine
    def stop(self):
        """
        Shutdowns the connection to RabbitMQ and stops the consumer

        Note
        ====
        Tornado coroutine
        """
        logger.info('Stopping consumer')
        self.shutting_down = True
        yield self.stop_consuming()
        logger.info('Stopped consumer')
