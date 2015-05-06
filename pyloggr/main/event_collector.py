# encoding: utf-8

"""
Collect events from the rescue queue and try to forward them to RabbitMQ
"""
__author__ = 'stef'

import logging
from tornado.gen import coroutine
from tornado.ioloop import IOLoop, PeriodicCallback
from pyloggr.rabbitmq.publisher import Publisher, RabbitMQConnectionError
from pyloggr.cache import cache
from pyloggr.config import Config
from pyloggr.utils import sleep

logger = logging.getLogger(__name__)


class EventCollector(object):
    def __init__(self, rabbitmq_config):
        self.rabbitmq_config = rabbitmq_config
        self.publisher = None
        self._connect_rabbitmq_later = None
        self.collecting = False
        self.shutting_down = False

    @coroutine
    def start(self):
        self.publisher = Publisher(self.rabbitmq_config, base_routing_key='pyloggr.syslog.rescue')
        try:
            rabbit_close_ev = yield self.publisher.start()
        except RabbitMQConnectionError:
            logger.error("Can't connect to RabbitMQ")
            self.stop()
            yield sleep(Config.SLEEP_TIME)
            self.connect_later()
            return

        self._start_periodic()

        yield rabbit_close_ev.wait()
        # we lost connection to RabbitMQ
        self.stop()
        yield sleep(Config.SLEEP_TIME)
        self.connect_later()
        return

    def connect_later(self):
        if not self.shutting_down:
            self._connect_rabbitmq_later = IOLoop.instance().add_callback(self.start)

    def stop(self):
        self._stop_periodic()
        if self.publisher:
            self.publisher.stop()
            self.publisher = None

    def shutdown(self):
        self.shutting_down = True
        self.stop()

    def _start_periodic(self):
        """
        Every minute we check if there are some events in the rescue queue
        """
        if self.periodic_queue_saving is None:
            self.periodic_queue_saving = PeriodicCallback(
                callback=self._try_publish_again,
                callback_time=Config.SLEEP_TIME * 1000
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
        Check the rescue queue, try to publish events in RabbitMQ

        Note
        ====
        Tornado coroutine
        """
        if self.collecting:
            return
        self.collecting = True

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

        publish_futures = [
            self.publisher.publish_event(event, 'pyloggr.syslog.collector') for event in cache.rescue.get_generator()
        ]
        results = yield publish_futures
        failed_events = [event for (ack, event) in results if not ack]
        map(cache.rescue.append, failed_events)

        self.collecting = False
