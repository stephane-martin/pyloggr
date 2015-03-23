# encoding: utf-8

"""
Collect events from the Redis rescue queue and try to forward them to RabbitMQ
"""
__author__ = 'stef'

import logging
from tornado.gen import coroutine
from tornado.ioloop import IOLoop, PeriodicCallback
from pyloggr.rabbitmq.publisher import Publisher, RabbitMQConnectionError
from pyloggr.cache import cache
from pyloggr.config import SLEEP_TIME
from pyloggr.event import Event, ParsingError

logger = logging.getLogger(__name__)


class EventCollector(object):
    def __init__(self, rabbitmq_config):
        self.rabbitmq_config = rabbitmq_config
        self.publisher = None
        self._connect_rabbitmq_later = None
        self.shutting_down = False

    @coroutine
    def start(self):
        self.publisher = Publisher(self.rabbitmq_config)
        try:
            rabbit_close_ev = yield self.publisher.start()
        except RabbitMQConnectionError:
            logger.error("Can't connect to RabbitMQ")
            self.stop()
            self.connect_later()
            return

        self._start_periodic()

        yield rabbit_close_ev.wait()
        # we lost connection to RabbitMQ
        self.stop()
        self.connect_later()
        return

    def connect_later(self):
        if self._connect_rabbitmq_later is None and not self.shutting_down:
            logger.info("We will try to reconnect to RabbitMQ in {} seconds".format(SLEEP_TIME))
            self._connect_rabbitmq_later = IOLoop.instance().call_later(SLEEP_TIME, self.start)

    def _cancel_connect_later(self):
        """
        Used in normal shutdown process (dont try to reconnect when we have decided to shutdown)
        """
        if self._connect_rabbitmq_later is not None:
            IOLoop.instance().remove_timeout(self._connect_rabbitmq_later)
            self._connect_rabbitmq_later = None

    def stop(self):
        self._stop_periodic()
        if self.publisher:
            self.publisher.stop()
            self.publisher = None

    def shutdown(self):
        self.shutting_down = True
        self._cancel_connect_later()
        self.stop()

    def _start_periodic(self):
        """
        Every minute we check if there are some events in the rescue queue
        """
        if self.periodic_queue_saving is None:
            self.periodic_queue_saving = PeriodicCallback(
                callback=self._try_publish_again,
                callback_time=1000 * 60
            )
            self.periodic_queue_saving.start()

    def _stop_periodic(self):
        """
        Stop the periodic check
        """
        if self.periodic_queue_saving:
            if self.periodic_queue_saving.is_running:
                self.periodic_queue_saving.stop()
            self.periodic_queue_saving = None

    @coroutine
    def _try_publish_again(self):
        """
        _try_publish_again()
        Check the rescue queue, try to publish events in RabbitMQ if we find some of them

        Note
        ====
        Tornado coroutine
        """
        nb_events = len(cache.rescue)
        if nb_events is None:
            logger.info("Rescue queue: can't connect to Redis")
            return

        if self.publisher is None:
            logger.info("Rescue queue: no connection to RabbitMQ")
            return

        logger.info("{} elements in the rescue queue".format(nb_events))
        if nb_events == 0:
            return

        publish_futures = dict()
        for bytes_event in cache.rescue.get_generator():
            try:
                event = Event.parse_bytes_to_event(bytes_event)
            except ParsingError:
                # we silently drop the unparsable event
                logger.debug("Rescue queue: dropping one unparsable event")
                continue

            publish_futures[bytes_event] = self.publisher.publish_event(
                self.rabbitmq_config.exchange, event, 'pyloggr.syslog.0'
            )

        results = yield publish_futures
        failed_events = [bytes_event for (bytes_event, ack) in results.items() if not ack]
        map(cache.rescue.append, failed_events)
