# encoding: utf-8

"""
The Filter Machine process can be used to apply series of filters to events
"""
__author__ = 'stef'

import logging
from datetime import timedelta

from tornado.gen import coroutine, TimeoutError
from tornado.ioloop import IOLoop
# noinspection PyCompatibility
from concurrent.futures import ThreadPoolExecutor

from ..rabbitmq import RabbitMQConnectionError
from ..rabbitmq.publisher import Publisher
from ..rabbitmq.consumer import Consumer
from pyloggr.filters import DropException, Filters
from pyloggr.event import Event, ParsingError, InvalidSignature
from pyloggr.config import Config
from pyloggr.utils import sleep

logger = logging.getLogger(__name__)


class FilterMachine(object):
    """
    Implements an Event parser than retrieves events from RabbitMQ, apply filters, and pushes
    back events to RabbitMQ.
    """

    def __init__(self, consumer_config, publisher_config, filters_filename):
        """
        :type consumer_config: pyloggr.rabbitmq.Configuration
        :type publisher_config: pyloggr.rabbitmq.Configuration
        """
        self.consumer_config = consumer_config
        self.publisher_config = publisher_config
        self.consumer = None
        self.publisher = None
        self.shutting_down = None
        self.executor = ThreadPoolExecutor(max_workers=self.consumer_config.qos + 5)
        self.filters = None
        self.filters_filename = filters_filename

    @coroutine
    def launch(self):
        """
        Starts the parser

        Note
        ====
        Coroutine
        """
        self.filters = Filters(Config.CONFIG_DIR, self.filters_filename)
        self.filters.open()
        self.publisher = Publisher(self.publisher_config)
        try:
            closed_publisher_event = yield self.publisher.start()
        except RabbitMQConnectionError:
            logger.warning("Filter machine: Can't connect to publisher")
            logger.info("We will try to reconnect to RabbitMQ in {} seconds".format(Config.SLEEP_TIME))
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
        self.consumer = Consumer(self.consumer_config)
        try:
            closed_consumer_event = yield self.consumer.start()
        except (RabbitMQConnectionError, TimeoutError):
            logger.warning("Can't connect to consumer")
            logger.info("We will try to reconnect to RabbitMQ in {} seconds".format(Config.SLEEP_TIME))
            # self.stop() stops the publisher too. so closed_publisher_event.wait() inside launch will return
            yield self.stop()
            return
        else:
            IOLoop.instance().add_callback(self._consume)
            yield closed_consumer_event.wait()
            yield self.stop()

    @coroutine
    def _consume(self):
        # this coroutine doesn't return, as long the rabbitmq connection lives
        message_queue = self.consumer.start_consuming()
        while self.consumer.consuming and not self.shutting_down:
            try:
                message = yield message_queue.get_wait(deadline=timedelta(seconds=1))
            except TimeoutError:
                pass
            else:
                future = self.executor.submit(self._apply_filters, message)
                IOLoop.instance().add_future(future, self._publish)

    @coroutine
    def _publish(self, future):
        message, ev = future.result()
        if ev is None:
            # dropped event
            message.ack()
            return
        if self.publisher:

            # here we take into account the optional overrides of the router engine
            event_type = ev.override_event_type if ev.override_event_type else ''
            if ev.override_exchanges:
                # publish to many exchanges
                futures = [
                    self.publisher.publish_event(
                        ev, routing_key='pyloggr.machine', event_type=event_type, exchange=exchange
                    )
                    for exchange in ev.override_exchanges
                ]
                results = yield futures
                results = [res for res, _ in results]
                if any(results):
                    # if at least one publish succeeds, we ack the message
                    message.ack()
                    if not all(results):
                        logger.warning("Publication of event '{}' failed for at least one exchange".format(ev.uuid))
                else:
                    logger.warning("Publication of event '{}' failed for all exchanges".format(ev.uuid))
                    message.nack()

            else:
                # publish only to the default exchange, from machine configuration
                res, _ = yield self.publisher.publish_event(ev, routing_key='pyloggr.machine', event_type=event_type)
                if res:
                    message.ack()
                else:
                    logger.warning("Publication of event '{}' failed".format(ev.uuid))
                    message.nack()
        else:
            message.nack()

    @coroutine
    def stop(self):
        """
        Stops the parser
        """
        futures = []
        if self.consumer:
            futures.append(self.consumer.stop())
        if self.publisher:
            futures.append(self.publisher.stop())
        yield futures
        self.consumer = None
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
