# encoding: utf-8
__author__ = 'stef'

import logging

from tornado.gen import coroutine
from tornado.ioloop import IOLoop
from concurrent.futures import ThreadPoolExecutor

from ..rabbitmq import RabbitMQConnectionError
from ..rabbitmq.publisher import Publisher
from ..rabbitmq.consumer import Consumer
from pyloggr.filters import DropException
from ..event import Event, ParsingError, InvalidSignature
from ..config import SLEEP_TIME

logger = logging.getLogger(__name__)


class EventParser(object):
    """
    Implements an Event parser than retrieves events from RabbitMQ, apply filters, and pushes
    back events to RabbitMQ.
    """

    def __init__(self, from_rabbitmq_config, to_rabbitmq_config, filters):
        """
        :type filters: pyloggr.filters.Filters
        """
        self.from_rabbitmq_config = from_rabbitmq_config
        self.to_rabbitmq_config = to_rabbitmq_config
        self.filters = filters
        self.consumer = None
        self.publisher = None
        self._publisher_later = None
        self.shutting_down = None
        self.executor = ThreadPoolExecutor(max_workers=self.from_rabbitmq_config['qos'] + 5)

    @coroutine
    def start(self):
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
            self._publisher_later = IOLoop.instance().call_later(SLEEP_TIME, self.start)
            return
        yield self._start_consumer()
        yield closed_publisher_event.wait()
        yield self.stop()
        if not self.shutting_down:
            self._publisher_later = IOLoop.instance().call_later(SLEEP_TIME, self.start)

    @coroutine
    def _start_consumer(self):
        self.consumer = Consumer(self.from_rabbitmq_config)
        try:
            yield self.consumer.start(self.from_rabbitmq_config['qos'])
        except RabbitMQConnectionError:
            logger.warning("Can't connect to consumer")
            logger.info("We will try to reconnect to RabbitMQ in {} seconds".format(SLEEP_TIME))
            yield self.stop()
            return
        yield self._consume()

    @coroutine
    def _consume(self):
        message_queue = self.consumer.start_consuming()
        while True:
            if (not self.consumer) or self.shutting_down:
                break
            message = yield message_queue.get()
            # todo: is apply_filters thread safe?
            future = self.executor.submit(self.apply_filters, message)
            IOLoop.instance().add_future(future, self._publish)
            # ev = self.apply_filters(message)
            # if ev is None:
            #     continue
            # res = yield self.publisher.publish(
            #     exchange=self.to_rabbitmq_config['exchange'],
            #     body=ev.dumps()
            # )
            # if res:
            #     message.ack()
            # else:
            #     message.nack()

    @coroutine
    def _publish(self, future):
        message, ev = future.result()
        if ev is None:
            # todo: instead of dropping the offending event, log it somewhere
            message.ack()
            return
        res = yield self.publisher.publish(
            exchange=self.to_rabbitmq_config['exchange'],
            body=ev.dumps()
        )
        if res:
            message.ack()
        else:
            message.nack()

    def stop(self):
        """
        Stops the parser
        """
        if self.consumer:
            self.consumer.stop()
            self.consumer = None
        if self.publisher:
            self.publisher.stop()
            self.publisher = None

    def shutdown(self):
        """
        Shutdowns (stops definitely) the parser
        """
        self.shutting_down = True
        self.stop()

    def apply_filters(self, message):
        try:
            ev = Event.load(message.body)
        except ParsingError:
            logger.warning("Dropping one unparsable event")
            return message, None

        try:
            ev.verify_hmac()
        except InvalidSignature:
            logger.critical("Dropping one tampered event")
            return message, None

        try:
            self.filters.apply(ev)
        except DropException:
            logger.debug("DROP filter!")
            return message, None

        return message, ev