#!/usr/bin/env python
# encoding: utf-8
__author__ = 'stef'

import logging
import logging.config
import time
import signal

from tornado.ioloop import IOLoop
from tornado.gen import coroutine
from tornado.process import fork_processes

from pyloggr.main.shipper2pgsql import PgsqlShipper
from pyloggr.config import MAX_WAIT_SECONDS_BEFORE_SHUTDOWN, FROM_RABBITMQ_TO_PGSQL_CONFIG
from pyloggr.config import LOGGING_CONFIG, PGSQL_CONFIG
from pyloggr.cache import cache, CacheError


CONSUMER_TO_PG_LOGGING_FILENAME = "/tmp/consumer_to_pg.log"
LOGGING_CONFIG['handlers']['tofile']['filename'] = CONSUMER_TO_PG_LOGGING_FILENAME
logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger('consumer_to_pg')
shipper = None


def shutdown():
    global shipper

    logger.info("Stopping all operations...")
    shipper.shutdown()
    logger.info('Will stop Tornado in {} seconds ...'.format(MAX_WAIT_SECONDS_BEFORE_SHUTDOWN))
    io_loop = IOLoop.instance()

    deadline = time.time() + MAX_WAIT_SECONDS_BEFORE_SHUTDOWN

    def stop_loop():
        now = time.time()
        if now < deadline and (io_loop._callbacks or io_loop._timeouts):
            io_loop.add_timeout(now + 1, stop_loop)
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
    global shipper

    shipper = PgsqlShipper(FROM_RABBITMQ_TO_PGSQL_CONFIG, PGSQL_CONFIG)
    yield shipper.start()


def main():
    try:
        cache.initialize()
    except CacheError as err:
        logger.error(err)
        return

    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)

    fork_processes(0)


    ioloop = IOLoop.instance()
    ioloop.add_callback(start)
    logger.info("Starting the IOLoop")
    ioloop.start()


if __name__ == "__main__":
    main()
