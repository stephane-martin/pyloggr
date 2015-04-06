# encoding: utf-8
__author__ = 'stef'

import logging

from tornado.gen import coroutine
from tornado.ioloop import IOLoop
from concurrent.futures import ThreadPoolExecutor

from ..rabbitmq import RabbitMQConnectionError
from ..rabbitmq.publisher import Publisher
from ..rabbitmq.consumer import Consumer
from pyloggr.filters import DropException, Filters
from ..event import Event, ParsingError, InvalidSignature
from ..config import SLEEP_TIME, CONFIG_DIR
from pyloggr.utils import sleep

logger = logging.getLogger(__name__)

# todo: choose final storage for events in filter configuration


class EventParser(object):
    """
    Implements an Event parser than retrieves events from RabbitMQ, apply filters, and pushes
    back events to RabbitMQ.
    """

    def __init__(self, from_rabbitmq_config, to_rabbitmq_config):
        """
        :type from_rabbitmq_config: pyloggr.config.RabbitMQBaseConfig
        :type to_rabbitmq_config: pyloggr.config.RabbitMQBaseConfig
        """
        self.from_rabbitmq_config = from_rabbitmq_config
        self.to_rabbitmq_config = to_rabbitmq_config
        self.consumer = None
        self.publisher = None
        self.shutting_down = None
        self.executor = ThreadPoolExecutor(max_workers=self.from_rabbitmq_config.qos + 5)
        self.filters = Filters(CONFIG_DIR)
        self.filters.open()

    @coroutine
    def launch(self):
        """
        Starts the parser

        Note
        ====
        Coroutine
        """

        self.publisher = Publisher(self.to_rabbitmq_config)
        try:
            closed_publisher_event = yield self.publisher.start()
        except RabbitMQConnectionError:
            logger.warning("Can't connect to publisher")
            logger.info("We will try to reconnect to RabbitMQ in {} seconds".format(SLEEP_TIME))
            yield self.stop()
            yield sleep(60)
            if not self.shutting_down:
                IOLoop.instance().add_callback(self.launch)
            return
        # here we use a callback, so that we can directly wait for the next closed_publisher_event
        IOLoop.instance().add_callback(self._start_consumer)
        yield closed_publisher_event.wait()
        yield self.stop()
        yield sleep(60)
        if not self.shutting_down:
            IOLoop.instance().add_callback(self.launch)

    @coroutine
    def _start_consumer(self):
        self.consumer = Consumer(self.from_rabbitmq_config)
        try:
            closed_connection_event = yield self.consumer.start(self.from_rabbitmq_config.qos)
        except RabbitMQConnectionError:
            logger.warning("Can't connect to consumer")
            logger.info("We will try to reconnect to RabbitMQ in {} seconds".format(SLEEP_TIME))
            # self.stop() stops the publisher too. so closed_publisher_event.wait() inside launch will return
            yield self.stop()
            return
        else:
            yield self._consume()           # only returns if we lose rabbitmq connection

    @coroutine
    def _consume(self):
        # this coroutine never returns (as long the rabbitmq connection lives)
        message_queue = self.consumer.start_consuming()
        while True:
            if (not self.consumer) or self.shutting_down:
                break
            message = yield message_queue.get()
            future = self.executor.submit(self._apply_filters, message)
            IOLoop.instance().add_future(future, self._publish)

    @coroutine
    def _publish(self, future):
        message, ev = future.result()
        if ev is None:
            message.ack()
            return
        res = yield self.publisher.publish(
            exchange=self.to_rabbitmq_config.exchange,
            body=ev.dumps()
        )
        if res:
            message.ack()
        else:
            message.nack()

    @coroutine
    def stop(self):
        """
        Stops the parser
        """
        if self.consumer:
            yield self.consumer.stop()
            self.consumer = None
        if self.publisher:
            yield self.publisher.stop()
            self.publisher = None

    @coroutine
    def shutdown(self):
        """
        Shutdowns (stops definitely) the parser
        """
        self.shutting_down = True
        yield self.stop()
        self.filters.close()

    def _apply_filters(self, message):
        """
        Apply filters to the event inside the RabbitMQ message.

        Note
        ====
        This method is executed in a separated thread.


        :param message: event to apply filters to, as a RabbitMQ message
        :type message: pyloggr.consumer.RabbitMQMessage
        :return: tuple(message, parsed event). parsed event is None when event couldn't be parsed.
        :rtype: tuple(pyloggr.consumer.RabbitMQMessage, pyloggr.event.Event)
        """
        try:
            ev = Event.parse_bytes_to_event(message.body, hmac=True, json=True)
        except ParsingError:
            # should not happen, as pyloggr's syslog server just sent the event
            logger.error("Dropping one unparsable event")
            logger.error(message)
            return message, None
        except InvalidSignature:
            # should not happen, the event is not supposed to have a HMAC yet
            logger.critical("Dropping one tampered event")
            logger.critical(message)
            return message, None

        try:
            self.filters.apply(ev)
        except DropException:
            logger.debug("DROP filter")
            return message, None

        return message, ev
