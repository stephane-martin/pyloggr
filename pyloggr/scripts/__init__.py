# encoding: utf-8

"""
The `script` subpackage contains launchers for the pyloggr's processes.

"""

__author__ = 'stef'

import logging
import logging.config
import time
from signal import signal, SIGTERM, SIGINT

from tornado.ioloop import IOLoop
from tornado.process import fork_processes
from tornado.gen import coroutine

from pyloggr.config import MAX_WAIT_SECONDS_BEFORE_SHUTDOWN
from pyloggr.cache import cache, CacheError


class PyloggrProcess(object):
    def __init__(self, name, fork=True):
        self.name = name
        self.logger = None
        self.fork = fork
        self.task_id = None
        self.pyloggr_process = None

    def main(self):
        self.logger = logging.getLogger('pyloggr')

        try:
            cache.initialize()
        except CacheError as err:
            self.logger.error(err)
            return

        signal(SIGTERM, self.sig_handler)
        signal(SIGINT, self.sig_handler)

        if self.fork:
            self.task_id = fork_processes(0)

        IOLoop.instance().add_callback(self.launch)
        self.logger.info("Starting the IOLoop")
        IOLoop.instance().start()

    def sig_handler(self, sig, frame):
        self.logger.info('Caught signal: {}'.format(sig))
        IOLoop.instance().add_callback_from_signal(self.shutdown)

    @coroutine
    def launch(self):
        raise NotImplementedError

    @coroutine
    def shutdown(self):

        self.logger.info("Shutting down...")
        yield self.pyloggr_process.shutdown()
        self.logger.info('Will stop Tornado in {} seconds...'.format(MAX_WAIT_SECONDS_BEFORE_SHUTDOWN))

        io_loop = IOLoop.instance()

        deadline = time.time() + MAX_WAIT_SECONDS_BEFORE_SHUTDOWN
        countdown = MAX_WAIT_SECONDS_BEFORE_SHUTDOWN

        def stop_loop(counter):
            self.logger.debug(counter)
            now = time.time()
            if now < deadline and (io_loop._callbacks or io_loop._timeouts):
                io_loop.call_later(1, stop_loop, counter-1)
            else:
                io_loop.stop()
                self.logger.info("We stopped the IOLoop")

        stop_loop(countdown)
        cache.shutdown()
