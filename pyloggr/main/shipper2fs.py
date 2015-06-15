# encoding: utf-8
"""
Ships events from RabbitMQ to the filesystem
"""
from __future__ import absolute_import, division, print_function
__author__ = 'stef'

import logging
from datetime import timedelta
from os.path import dirname, exists, join
from io import open
import os

from tornado.concurrent import Future
from tornado.gen import coroutine, Return, TimeoutError
from tornado.ioloop import IOLoop, PeriodicCallback
from sortedcontainers import SortedSet
import lockfile
from future.utils import lmap, viewvalues

from pyloggr.rabbitmq.consumer import Consumer, RabbitMQConnectionError, RabbitMQMessage
from pyloggr.utils import sleep
from pyloggr.config import Config
from pyloggr.event import Event, ParsingError, InvalidSignature

logger = logging.getLogger(__name__)
security_logger = logging.getLogger('security')


class FilesystemShipper(object):
    """
    The FilesystemShipper takes events from a RabbitMQ queue and writes them on filesystem

    Parameters
    ==========
    rabbitmq_config: pyloggr.rabbitmq.Configuration
        RabbitMQ configuration
    export_fs_config: pyloggr.config.Shipper2FSConfig
        Log export configuration
    """

    def __init__(self, rabbitmq_config, export_fs_config):
        """
        :type rabbitmq_config: pyloggr.rabbitmq.Configuration
        :type export_fs_config: pyloggr.config.Shipper2FSConfig
        """
        self.consumer = Consumer(rabbitmq_config)
        self.export_fs_config = export_fs_config
        self.shutting_down = None
        self.event_queue = None
        self.closed_conn_event = None
        self.files_queues = {}

    @coroutine
    def launch(self):
        """
        launch()
        Start shipper2fs
        """
        # connect to RabbitMQ
        try:
            self.closed_conn_event = yield self.consumer.start()
        except (RabbitMQConnectionError, TimeoutError):
            logger.error("Can't connect to RabbitMQ")
            yield sleep(60)
            if not self.shutting_down:
                IOLoop.instance().add_callback(self.launch)
            return

        # consume events and put them in event_queue
        self.event_queue = self.consumer.start_consuming()
        IOLoop.instance().add_callback(self._consume)

        # wait until we lose rabbitmq connection
        yield self.closed_conn_event.wait()
        # we lost connection to RabbitMQ (by accident, or because stop() was called)
        yield self.stop()
        logger.info("Waiting {} seconds before trying to reconnect".format(Config.SLEEP_TIME))
        yield sleep(Config.SLEEP_TIME)
        if not self.shutting_down:
            # try to reconnect
            IOLoop.instance().add_callback(self.launch)

    @coroutine
    def stop(self):
        """
        stop()
        Stops the shipper
        """
        logger.info("Stopping shipper2fs")
        if self.consumer:
            yield self.consumer.stop()
            self.consumer = None
        # stop the "export to FS" queues
        lmap(lambda queue: queue.stop(), viewvalues(self.files_queues))

    @coroutine
    def shutdown(self):
        """
        shutdown()
        Shutdowns (stops definitely) the shipper.
        """
        logger.info("Shutting down shipper2fs")
        self.shutting_down = True
        yield self.stop()

    @coroutine
    def _consume(self):
        event_queue = self.event_queue
        if event_queue is None:
            return

        # loop until we lose rabbitmq connection
        while (not self.closed_conn_event.is_set()) or (event_queue.qsize() != 0):
            try:
                message = yield event_queue.get_wait(deadline=timedelta(seconds=1))
            except TimeoutError:
                pass
            else:
                IOLoop.instance().add_callback(self.export, message)

    @coroutine
    def export(self, message):
        """
        export(message)
        Export event to filesystem

        :param message: RabbitMQ message
        :type message: RabbitMQMessage
        """
        try:
            event = Event.parse_bytes_to_event(message.body, hmac=True)
        except ParsingError:
            logger.info("shipper2fs: dropping one unparsable message")
            message.ack()
        except InvalidSignature:
            logger.info("shipper2fs: dropping one message with invalid signature")
            message.ack()
            security_logger.critical("shipper2fs: dropping one message with invalid signature")
            security_logger.info(message.body)
        else:
            # put event in export queue and wait until it has been exported
            res = yield self._append(event)
            if res:
                message.ack()
            else:
                message.nack()

    @coroutine
    def _append(self, event):
        """
        :type event: pyloggr.event.Event
        """
        # replace fields: $SOURCE, $DATE, $SEVERITY, $FACILITY, $APP_NAME
        filename = self.export_fs_config.filename.replace(
            '$SOURCE', event.source
        ).replace(
            '$SEVERITY', event.severity
        ).replace(
            '$FACILITY', event.facility
        ).replace(
            '$APP_NAME', event.app_name
        ).replace(
            '$DATE', str(event.timereported.date())
        ).lstrip('/')
        filename = join(self.export_fs_config.directory, filename)
        if filename not in self.files_queues:
            self.files_queues[filename] = FSQueue(
                filename, self.export_fs_config.seconds_between_flush, self.export_fs_config.frmt
            )
        queue = self.files_queues[filename]
        res = yield queue.append(event)
        raise Return(res)


class FSQueue(object):
    """
    Store events that have to be exported to a given filename
    """
    def __init__(self, filename, period, frmt):
        self.filename = filename
        self.frmt = frmt
        self._queue = SortedSet()
        self.futures = {}
        self._periodic = PeriodicCallback(self.flush, period * 1000)
        self._periodic.start()
        self._flushing = False

    @coroutine
    def append(self, event):
        """
        append(event)
        Add an event to the queue. The coroutine resolves when the event has been exported.

        :param event: event
        :type event: pyloggr.event.Event
        """
        if event in self._queue:
            raise Return(True)
        self._queue.add(event)
        self.futures[event.uuid] = Future()
        res = yield self.futures[event.uuid]
        del self.futures[event.uuid]
        raise Return(res)

    def flush(self):
        """
        flush()
        Actually flush the events to the file
        """
        if self._flushing:
            return
        self._flushing = True
        if len(self._queue) == 0:
            return
        s = "\n".join(event.dump(frmt=self.frmt) for event in self._queue) + '\n'
        dname = dirname(self.filename)
        if not exists(dname):
            os.makedirs(dname)
        logger.debug("Flushing '{}'".format(self.filename))
        try:
            with lockfile.LockFile(self.filename):
                with open(self.filename, 'ab') as fh:
                    fh.write(s)
        except (OSError, lockfile.Error):
            logger.exception("shipper2fs: flushing failed")
            lmap(lambda event: self.futures[event.uuid].set_result(False), self._queue)
        else:
            lmap(lambda event: self.futures[event.uuid].set_result(True), self._queue)
        finally:
            self._queue = SortedSet()
            self._flushing = False

    def stop(self):
        """
        stop()
        Stop the queue
        """
        self._periodic.stop()
        # notify that the events inside this queue were not exported
        if self.futures:
            lmap(lambda future: future.set_result(False), viewvalues(self.futures))
