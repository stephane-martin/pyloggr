# encoding: utf-8

"""
The `script` subpackage contains launchers for the pyloggr's processes.

"""

__author__ = 'stef'

import os
import logging
import logging.config
import time
import psutil
import sys
from signal import signal, SIGTERM, SIGINT, SIG_IGN

from tornado.ioloop import IOLoop
from tornado.process import fork_processes
from tornado.gen import coroutine

from pyloggr.config import MAX_WAIT_SECONDS_BEFORE_SHUTDOWN, PIDS_DIRECTORY
from pyloggr.cache import cache, CacheError


class PyloggrProcess(object):
    def __init__(self, name, fork=True):
        self.name = name
        self.logger = None
        self.fork = fork
        self.task_id = -1
        self.pyloggr_process = None


    def main(self):
        self.logger = logging.getLogger('pyloggr')

        try:
            cache.initialize()
        except CacheError as err:
            self.logger.error(err)
            return

        signal(SIGTERM, self.parent_sig_handler)
        signal(SIGINT, self.parent_sig_handler)

        if self.fork:
            try:
                self.task_id = fork_processes(0)
            except OSError as ex:
                if ex.errno == 10:
                    return
                raise

            # fork_processes returns as a child process
            # set signals for tornado children
            signal(SIGTERM, self.child_sig_handler)
            signal(SIGINT, SIG_IGN)

        # child process
        IOLoop.instance().add_callback(self.launch)
        self.logger.info("Starting the IOLoop for {}".format(self.task_id))
        IOLoop.instance().start()
        # cleanly exits the child
        sys.exit(0)

    def child_sig_handler(self, sig, frame):
        self.logger.info('Child caught signal: {}'.format(sig))
        IOLoop.instance().add_callback_from_signal(self.shutdown)

    def parent_sig_handler(self, sig, frame):
        self.logger.info('Parent caught signal: {}'.format(sig))
        if self.fork:
            # ask the children to stop
            current_process = psutil.Process()
            children = [child for child in current_process.children() if child.name() == 'python']
            [child.send_signal(SIGTERM) for child in children]
            # wait until everyone is dead
            [child.wait() for child in children]
            from pyloggr.utils import remove_pid_file
            remove_pid_file(self.name)
        else:
            # single process tornado
            IOLoop.instance().add_callback_from_signal(self.shutdown)

    @coroutine
    def launch(self):
        raise NotImplementedError

    @coroutine
    def shutdown(self):

        self.logger.info("Shutting down '{}'...".format(self.task_id))
        yield self.pyloggr_process.shutdown()

        io_loop = IOLoop.instance()

        deadline = time.time() + MAX_WAIT_SECONDS_BEFORE_SHUTDOWN
        countdown = MAX_WAIT_SECONDS_BEFORE_SHUTDOWN

        # get rid of sleepers
        sleepers = [timeout for timeout in io_loop._timeouts if getattr(timeout.callback, 'sleeper', None)]
        map(io_loop.remove_timeout, sleepers)

        def stop_loop(counter):
            self.logger.debug(counter)
            now = time.time()
            if now < deadline and (io_loop._callbacks or io_loop._timeouts):
                io_loop.call_later(1, stop_loop, counter - 1)
            else:
                io_loop.stop()
                self.logger.info("We stopped the IOLoop for '{}'".format(self.task_id))

        stop_loop(countdown)

        cache.shutdown()
        if (self.task_id == -1) and (not self.fork):
            from pyloggr.utils import remove_pid_file
            remove_pid_file(self.name)
