# encoding: utf-8

"""
The `script` subpackage contains launchers for the pyloggr's processes.
"""

__author__ = 'stef'

import logging
import logging.config
import time
import psutil
import sys
from signal import signal, SIGTERM, SIGINT, SIG_IGN

from tornado.ioloop import IOLoop
from tornado.process import fork_processes
from tornado.gen import coroutine
from future.utils import lmap

from pyloggr.config import Config
from pyloggr.cache import Cache
from pyloggr.event import Event

class PyloggrProcess(object):
    """
    Boilerplate for starting the different pyloggr processes
    """
    def __init__(self, fork=True):
        self.name = self.__class__.__name__
        self.logger = None
        self.fork = fork
        self.task_id = -1
        self.pyloggr_process = None

    def main(self):
        """
        main method

        - Initialize Redis cache
        - set up signal handlers
        - fork if necessary
        - run the `launch` method
        """
        self.logger = logging.getLogger('pyloggr')

        Event.set_hmac_key(Config.HMAC_KEY)

        Cache.initialize()

        signal(SIGTERM, self._parent_sig_handler)
        signal(SIGINT, self._parent_sig_handler)

        if self.fork:
            try:
                self.task_id = fork_processes(0)
            except OSError as ex:
                if ex.errno == 10:
                    return
                raise

            # fork_processes returns as a child process
            # set signals for tornado children
            signal(SIGTERM, self._child_sig_handler)
            signal(SIGINT, SIG_IGN)

        # child process
        IOLoop.instance().add_callback(self._launch)
        self.logger.info("Starting the IOLoop for {}".format(self.task_id))
        IOLoop.instance().start()
        # cleanly exits the child
        sys.exit(0)

    # noinspection PyUnusedLocal
    def _child_sig_handler(self, sig, frame):
        self.logger.info('Child caught signal: {}'.format(sig))
        IOLoop.instance().add_callback_from_signal(self.shutdown)

    # noinspection PyUnusedLocal
    def _parent_sig_handler(self, sig, frame):
        self.logger.info('Parent caught signal: {}'.format(sig))
        if self.fork:
            # ask the children to stop
            current_process = psutil.Process()
            current_name = current_process.name()
            children = [child for child in current_process.children() if child.name() == current_name]
            [child.send_signal(SIGTERM) for child in children]
            # wait until everyone is dead
            [child.wait() for child in children]
            from pyloggr.utils import remove_pid_file
            remove_pid_file(self.name)
        else:
            # single process tornado
            IOLoop.instance().add_callback_from_signal(self.shutdown)

    @coroutine
    def _launch(self):
        """
        launch()
        Abstract method

        Note
        ====
        Tornado coroutine
        """
        raise NotImplementedError

    @coroutine
    def shutdown(self):
        """
        shutdown()
        Cleanly shutdown the process

        Note
        ====
        Tornado coroutine
        """

        self.logger.info("Shutting down '{}'...".format(self.task_id))
        if self.pyloggr_process:
            if isinstance(self.pyloggr_process, list):
                futures = map(lambda p: p.shutdown(), self.pyloggr_process)
                yield futures
            else:
                yield self.pyloggr_process.shutdown()

        io_loop = IOLoop.instance()

        def _stop_sleepers():
            # noinspection PyUnresolvedReferences,PyProtectedMember
            sleepers = [timeout for timeout in io_loop._timeouts if getattr(timeout.callback, 'sleeper', None)]
            lmap(io_loop.remove_timeout, sleepers)

        deadline = time.time() + Config.MAX_WAIT_SECONDS_BEFORE_SHUTDOWN
        countdown = Config.MAX_WAIT_SECONDS_BEFORE_SHUTDOWN

        def _stop_loop(counter):
            self.logger.debug(counter)
            # get rid of sleepers
            _stop_sleepers()
            now = time.time()
            # noinspection PyProtectedMember
            if now < deadline and (io_loop._callbacks or io_loop._timeouts):
                io_loop.call_later(1, _stop_loop, counter - 1)
            else:
                io_loop.stop()
                self.logger.info("We stopped the IOLoop for '{}'".format(self.task_id))

        _stop_loop(countdown)

        Cache.shutdown()
        if (self.task_id == -1) and (not self.fork):
            from pyloggr.utils import remove_pid_file
            remove_pid_file(self.name)
