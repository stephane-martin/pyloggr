# encoding: utf-8
"""
Ship events to a PostgreSQL database
"""

__author__ = 'stef'

import logging

from tornado.gen import coroutine
from tornado.ioloop import PeriodicCallback, IOLoop
# noinspection PyCompatibility
from concurrent.futures import ThreadPoolExecutor
from psycopg2.pool import ThreadedConnectionPool, PoolError
# noinspection PyCompatibility
import psycopg2
from sortedcontainers import SortedSet
from tornado.gen import Return, TimeoutError

from pyloggr.utils.constants import SQL_INSERT_QUERY, SQL_COLUMNS_STR, D_COLUMNS
from pyloggr.rabbitmq.consumer import Consumer, RabbitMQConnectionError
from pyloggr.event import Event, ParsingError, InvalidSignature
from pyloggr.utils import sleep
from pyloggr.config import Config

logger = logging.getLogger(__name__)


class PostgresqlShipper(object):
    """
    PostgresqlShipper retrieves events from RabbitMQ, and inserts them in PostgreSQL
    """

    def __init__(self, rabbitmq_config, pgsql_config):
        """
        :type rabbitmq_config: pyloggr.rabbitmq.Configuration
        :type pgsql_config: pyloggr.config.Shipper2PGSQL
        """
        self.pgsql_config = pgsql_config
        self.syslog_ev_queue = None
        self.periodic_check_queue_size = None
        self.dsn = 'dbname={} user={} password={} host={} port={} connect_timeout={}'.format(
            self.pgsql_config.dbname, self.pgsql_config.user, self.pgsql_config.password,
            self.pgsql_config.host, self.pgsql_config.port, self.pgsql_config.connect_timeout
        )
        self.db_pool = None
        self.shutting_down = None
        self._times = 0
        self.consumer = Consumer(rabbitmq_config)

    @coroutine
    def launch(self):
        """
        Starts the shipper

        - Opens a connection to RabbitMQ
        - Opens a pool to PostgreSQL
        - Consumes messages from RabbitMQ
        - Parses messages as regular syslog events
        - Periodically ships the events to PostgreSQL
        """
        self.periodic_check_queue_size = None

        # connect to RabbitMQ
        try:
            closed_conn_event = yield self.consumer.start()
        except (RabbitMQConnectionError, TimeoutError):
            logger.error("Can't connect to RabbitMQ")
            yield sleep(60)
            if not self.shutting_down:
                IOLoop.instance().add_callback(self.launch)
            return

        # connect to PGSQL
        while self.db_pool is None:
            try:
                yield self._get_db_pool()
            except psycopg2.Error:
                logger.error("shipper: can't connect to PGSQL")
                yield sleep(Config.SLEEP_TIME)

        self.syslog_ev_queue = self.consumer.start_consuming()
        self.periodic_check_queue_size = PeriodicCallback(self._check_queue_size, callback_time=2000)
        self.periodic_check_queue_size.start()

        yield closed_conn_event.wait()
        # we lost connection to RabbitMQ (by accident, or because stop() was called)
        yield self.stop()
        logger.info("Waiting {} seconds before trying to reconnect".format(Config.SLEEP_TIME))
        yield sleep(Config.SLEEP_TIME)
        if not self.shutting_down:
            IOLoop.instance().add_callback(self.launch)

    @coroutine
    def _get_db_pool(self):
        if not self.db_pool:
            # we try to connect to PGSQL in a thread, because connection timeouts can block
            executor = ThreadPoolExecutor(max_workers=1)
            try:
                self.db_pool = yield executor.submit(
                    ThreadedConnectionPool, 1, self.pgsql_config.max_pool, self.dsn, async=False
                )
            finally:
                executor.shutdown()
        raise Return(self.db_pool)

    @coroutine
    def stop(self):
        """
        Stops the shipper
        """
        logger.info("Stopping the shipper2pgsql")
        if self.periodic_check_queue_size:
            self.periodic_check_queue_size.stop()
            self.periodic_check_queue_size = None
        if self.consumer:
            yield self.consumer.stop()
            self.consumer = None
        if self.db_pool:
            if not self.db_pool.closed:
                self.db_pool.closeall()
            self.db_pool = None

    @coroutine
    def shutdown(self):
        """
        Shutdowns (stops definitely) the shipper.
        """
        logger.info("Shutting down shipper2pgsql")
        self.shutting_down = True
        yield self.stop()

    @coroutine
    def _check_queue_size(self):
        self._times += 2
        # todo: configurable 500 and 60
        if self._times >= 60 or self.syslog_ev_queue.qsize() >= 500:
            self._times = 0
            IOLoop.instance().add_callback(self._flush_messages)

    @coroutine
    def _flush_messages(self):
        size = self.syslog_ev_queue.qsize()
        if size == 0:
            logger.debug("No event to flush")
            return

        logger.info("Flushing events to PGSQL")
        if self.db_pool is None:
            logger.warning("We don't have a pool to PGSQL. Giving up flush. Stopping the consumer.")
            yield self.stop()
            return
        if self.db_pool.closed:
            logger.warning("PGSQL pool is closed. Giving up flush. Stopping the consumer.")
            yield self.stop()
            return

        # get_all_nowait pops all the events in syslog_ev_queue
        msgs = self.syslog_ev_queue.get_all()
        if not msgs:
            return

        logger.info("{} events to forward to PGSQL".format(len(msgs)))

        def _flush_backthread(rabbitmq_messages, tablename):
            # parse the bytes messages into real events
            # we use a SortedSet to get rid of duplicates
            events = SortedSet()
            for rabbit_message in rabbitmq_messages:
                try:
                    ev = Event.parse_bytes_to_event(rabbit_message.body, hmac=True)
                except ParsingError:
                    # should not happen, messages are coming from pyloggr
                    logger.info("shipper: dropping one unparsable message")
                except InvalidSignature:
                    security_logger = logging.getLogger('security')
                    logger.error("Dropping one tampered event, see security logs")
                    security_logger.critical("Dropping one tampered event")
                    security_logger.critical(rabbit_message.body)
                else:
                    events.add(ev)
            try:
                conn = self.db_pool.getconn()
            except PoolError:
                logging.exception("flush_backthread: can't get a PGSQL connection from the pool")
                raise
            try:
                conn.autocommit = False
                with conn.cursor() as cur:
                    # build the SQL insert query
                    values = ','.join([evt.dump_sql(cur) for evt in events])
                    # query = "INSERT INTO {} {} VALUES ".format(tablename, SQL_COLUMNS) + values
                    query = SQL_INSERT_QUERY.format(
                        SQL_COLUMNS_STR, values, tablename, SQL_COLUMNS_STR, D_COLUMNS, tablename
                    )
                    cur.execute(query)
                    conn.commit()
            except psycopg2.Error:
                logger.exception("flush_backthread: flushing to PGSQL failed")
                raise
            finally:
                if conn:
                    self.db_pool.putconn(conn)

        executor = ThreadPoolExecutor(max_workers=1)
        # noinspection PyBroadException
        try:
            yield executor.submit(
                _flush_backthread, rabbitmq_messages=msgs, tablename=self.pgsql_config.tablename
            )
        except (psycopg2.Error, PoolError):
            logger.exception("Flushing to PGSQL failed (probably PGSQL connection problem)")
            map(lambda message: message.nack(), msgs)
            yield self.stop()
        except:
            logger.error("Admin should review this")
            logger.exception("shipper: unexpected Exception while flushing events to PGSQL")
            map(lambda message: message.nack(), msgs)
            yield self.stop()
        else:
            map(lambda message: message.ack(), msgs)
        finally:
            executor.shutdown()
