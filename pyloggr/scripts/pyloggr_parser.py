#!/usr/bin/env python
# encoding: utf-8
__author__ = 'stef'

import logging
import logging.config
import time
import signal

from tornado.ioloop import IOLoop
from tornado.gen import coroutine

from pyloggr.config import MAX_WAIT_SECONDS_BEFORE_SHUTDOWN, LOGGING_CONFIG, CONFIG_DIR
from pyloggr.config import FROM_PARSER_TO_RABBITMQ_CONFIG, FROM_RABBITMQ_TO_PARSER_CONFIG
from pyloggr.main.event_parser import EventParser
from pyloggr.filters import Filters


PARSER_LOGGING_FILENAME = "/tmp/parser.log"
logger = logging.getLogger('parser')
event_parser = None
filters = None


def shutdown():
    global event_parser, filters

    logger.info("Stopping all operations...")
    event_parser.shutdown()
    filters.close()
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


def sig_handler(sig, frame):
    logger.info('Caught signal: {}'.format(sig))
    IOLoop.instance().add_callback_from_signal(shutdown)

@coroutine
def start_parser():
    global event_parser, filters

    logger.info("I think so i parse")
    filters = Filters(CONFIG_DIR)
    filters.open()
    event_parser = EventParser(
        from_rabbitmq_config=FROM_RABBITMQ_TO_PARSER_CONFIG,
        to_rabbitmq_config=FROM_PARSER_TO_RABBITMQ_CONFIG,
        filters=filters
    )
    yield event_parser.start()


def main():
    # todo: fork process
    LOGGING_CONFIG['handlers']['tofile']['filename'] = PARSER_LOGGING_FILENAME
    logging.config.dictConfig(LOGGING_CONFIG)

    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)

    ioloop = IOLoop.instance()
    ioloop.add_callback(start_parser)
    logger.info("Starting the IOLoop")
    ioloop.start()


if __name__ == "__main__":
    main()
