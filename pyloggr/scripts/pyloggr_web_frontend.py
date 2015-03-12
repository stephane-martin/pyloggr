#!/usr/bin/env python
# encoding: utf-8
__author__ = 'stef'

import logging
import logging.config
import time
from signal import SIGTERM, SIGINT, signal

from tornado.ioloop import IOLoop
from tornado.gen import coroutine

from pyloggr.main.web_frontend import WebServer
from pyloggr.config import MAX_WAIT_SECONDS_BEFORE_SHUTDOWN, LOGGING_CONFIG
from pyloggr.cache import cache, CacheError

HTTP_LOGGING_FILENAME = "/tmp/pyloggr.http.log"
LOGGING_CONFIG['handlers']['tofile']['filename'] = HTTP_LOGGING_FILENAME
logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger('http')
webserver = None


@coroutine
def shutdown():
    global webserver
    yield webserver.stop()

    logger.info("Stopping all operations...")
    logger.info('Will stop Tornado in {} seconds ...'.format(MAX_WAIT_SECONDS_BEFORE_SHUTDOWN))
    io_loop = IOLoop.instance()

    deadline = time.time() + MAX_WAIT_SECONDS_BEFORE_SHUTDOWN

    def stop_loop():
        now = time.time()
        if now < deadline and (io_loop._callbacks or io_loop._timeouts):
            io_loop.call_later(1, stop_loop)
        else:
            io_loop.stop()
            logger.info("Stopped the IOLoop")

    stop_loop()
    cache.shutdown()


def sig_handler(sig, frame):
    logger.info('Caught signal: {}'.format(sig))
    IOLoop.instance().add_callback_from_signal(shutdown)


@coroutine
def start():
    global webserver
    try:
        cache.initialize()
    except CacheError as err:
        logger.error(err)
        return
    webserver = WebServer()
    signal(SIGTERM, sig_handler)
    signal(SIGINT, sig_handler)
    yield webserver.start()


def main():
    IOLoop.instance().add_callback(start)
    logger.info("Starting the IOLoop")
    IOLoop.instance().start()


if __name__ == "__main__":
    main()
