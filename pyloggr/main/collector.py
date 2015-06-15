# encoding: utf-8

"""
Collect events from the rescue queue and try to forward them to RabbitMQ
"""
__author__ = 'stef'

import logging

# noinspection PyCompatibility
from concurrent.futures import ThreadPoolExecutor
from tornado.gen import coroutine, TimeoutError
from tornado.ioloop import IOLoop, PeriodicCallback
from pyloggr.rabbitmq.publisher import Publisher, RabbitMQConnectionError
from pyloggr.config import Config
from pyloggr.event import Event
from pyloggr.utils import sleep
from pyloggr.utils.lmdb_wrapper import LmdbWrapper

logger = logging.getLogger(__name__)


class EventCollector(object):
    def __init__(self, rabbitmq_config):
        self._rabbitmq_config = rabbitmq_config
        self._publisher = None
        self._collecting = False
        self._shutting_down = False
        self._periodic_collect = None

    @coroutine
    def launch(self):
        LmdbWrapper(Config.RESCUE_QUEUE_DIRNAME, size=52428800).open(
            sync=True, metasync=True, max_dbs=2
        )
        self._publisher = Publisher(self._rabbitmq_config)
        try:
            rabbit_close_ev = yield self._publisher.start()
        except (RabbitMQConnectionError, TimeoutError):
            logger.error("Can't connect to RabbitMQ")
            self.stop()
            yield sleep(Config.SLEEP_TIME)
            if not self._shutting_down:
                IOLoop.instance().add_callback(self.launch)
            return

        self._start_periodic()

        yield rabbit_close_ev.wait()
        # we lost connection to RabbitMQ
        yield self.stop()
        yield sleep(Config.SLEEP_TIME)
        if not self._shutting_down:
            IOLoop.instance().add_callback(self.launch)
        return

    @coroutine
    def stop(self):
        self._stop_periodic()
        if self._publisher:
            yield self._publisher.stop()
            self._publisher = None

    @coroutine
    def shutdown(self):
        self._shutting_down = True
        yield self.stop()
        LmdbWrapper.get_instance(Config.RESCUE_QUEUE_DIRNAME).close()

    def _start_periodic(self):
        """
        Every minute we check if there are some events in the rescue queue
        """
        if self._periodic_collect is None:
            self._periodic_collect = PeriodicCallback(
                callback=self._collect_and_publish,
                callback_time=Config.SLEEP_TIME * 1000
            )
            self._periodic_collect.start()

    def _stop_periodic(self):
        """
        Stop the periodic check
        """
        if self._periodic_collect:
            if self._periodic_collect.is_running:
                self._periodic_collect.stop()
            self._periodic_collect = None

    @coroutine
    def _collect_and_publish(self):
        """
        _try_publish_again()
        Check the rescue queue, try to publish events in RabbitMQ

        Note
        ====
        Tornado coroutine
        """
        if self._collecting:
            return
        if self._publisher is None:
            logger.info("Rescue queue: no connection to RabbitMQ")
            return

        self._collecting = True

        def _get_elements_from_lmdb_thread():
            lmdb = LmdbWrapper.get_instance(Config.RESCUE_QUEUE_DIRNAME)
            queue = lmdb.queue('pyloggr.rescue')

            nb_events = len(queue)
            logger.info("{} elements in the rescue queue".format(nb_events))

            if nb_events == 0:
                return None

            return queue.pop_all()

        with ThreadPoolExecutor(1) as exe:
            dict_events = yield exe.submit(_get_elements_from_lmdb_thread)
        if not dict_events:
            self._collecting = False
            return
        # parse events
        events = (
            Event.parse_bytes_to_event(dict_event, hmac=True, swallow_exceptions=True)
            for dict_event in dict_events
        )
        # wipe None values
        events = (event for event in events if event)
        publish_futures = [
            self._publisher.publish_event(event, 'pyloggr.syslog.collector')
            for event in events
        ]
        results = yield publish_futures
        failed_events = (event for (ack, event) in results if not ack)

        # put back events in lmdb if necessary
        def _put_back_events_in_lmdb_thread(evts):
            lmdb = LmdbWrapper.get_instance(Config.RESCUE_QUEUE_DIRNAME)
            queue = lmdb.queue('pyloggr.rescue')
            queue.extend(evts)

        with ThreadPoolExecutor(1) as exe:
            yield exe.submit(_put_back_events_in_lmdb_thread, failed_events)
        self._collecting = False
