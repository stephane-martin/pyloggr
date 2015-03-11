#!/usr/bin/env python
# encoding: utf-8
__author__ = 'stef'

import time
import signal
import logging
import logging.config

from tornado.ioloop import IOLoop
from tornado.process import fork_processes
from tornado.gen import coroutine

from pyloggr.main.syslog_server import SyslogServer
from pyloggr.config import MAX_WAIT_SECONDS_BEFORE_SHUTDOWN, FROM_RSYSLOG_TO_RABBITMQ_CONFIG, SYSLOG_CONF
from pyloggr.config import LOGGING_CONFIG
from pyloggr.cache import cache


RELP_LOGGING_FILENAME = "/tmp/relp_server.log"

server = None
logger = logging.getLogger('relp_server')

@coroutine
def shutdown():
    global server

    logger.info("Stopping all operations...")
    yield server.shutdown()

    logger.info('Will stop Tornado in {} seconds...'.format(MAX_WAIT_SECONDS_BEFORE_SHUTDOWN))
    io_loop = IOLoop.instance()

    deadline = time.time() + MAX_WAIT_SECONDS_BEFORE_SHUTDOWN
    countdown = MAX_WAIT_SECONDS_BEFORE_SHUTDOWN

    def stop_loop(counter):
        logger.debug(counter)
        now = time.time()
        if now < deadline and (io_loop._callbacks or io_loop._timeouts):
            io_loop.call_later(1, stop_loop, counter-1)
        else:
            io_loop.stop()
            logger.info("Stopped the IOLoop")

    stop_loop(countdown)


def sig_handler(sig, frame):
    logger.info('Caught signal: {}'.format(sig))
    IOLoop.instance().add_callback_from_signal(shutdown)


@coroutine
def start():
    global server
    cache.initialize()
    logger.info("Starting Syslog Server")
    yield server.launch()


def main():
    global server

    LOGGING_CONFIG['handlers']['tofile']['filename'] = RELP_LOGGING_FILENAME
    logging.config.dictConfig(LOGGING_CONFIG)
    SyslogServer.bind_all_sockets()
    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)

    # start as many SyslogServer process as number of CPUs
    task_id = fork_processes(0)

    server = SyslogServer(
        rabbitmq_config=FROM_RSYSLOG_TO_RABBITMQ_CONFIG,
        relp_server_config=SYSLOG_CONF,
        task_id=task_id
    )

    IOLoop.instance().add_callback(start)
    logger.info("Starting the IOLoop")
    IOLoop.instance().start()


if __name__ == "__main__":
    main()
