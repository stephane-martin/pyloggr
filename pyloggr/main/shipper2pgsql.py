# encoding: utf-8
__author__ = 'stef'

import logging

from tornado.gen import coroutine
from tornado.ioloop import PeriodicCallback, IOLoop
from concurrent.futures import ThreadPoolExecutor
from psycopg2.pool import ThreadedConnectionPool, PoolError
import psycopg2
from sortedcontainers import SortedSet
from toro import Empty

from ..utils.constants import SQL_INSERT_QUERY, SQL_COLUMNS_STR, D_COLUMNS
from ..rabbitmq.consumer import Consumer
from ..rabbitmq import RabbitMQConnectionError
from ..event import Event, ParsingError, InvalidSignature
from pyloggr.utils import sleep


logger = logging.getLogger(__name__)


class PostgresqlShipper(object):
    """
    PgsqlShipper gets events from RabbitMQ, and inserts them in PostgreSQL
    """

    def __init__(self, rabbitmq_config, pgsql_config):
        """
        :type rabbitmq_config: pyloggr.config.RabbitMQBaseConfig
        :type pgsql_config: pyloggr.config.PostgresqlConfig
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
        try:
            closed_conn_event = yield self.consumer.start()
        except RabbitMQConnectionError:
            logger.error("Can't connect to RabbitMQ")
            yield sleep(60)
            if not self.shutting_down:
                IOLoop.instance().add_callback(self.launch)
            return
        yield self._get_db_pool()
        self.syslog_ev_queue = self.consumer.start_consuming()
        self.periodic_check_queue_size = PeriodicCallback(
            self._check_queue_size, callback_time=1000
        )
        self.periodic_check_queue_size.start()
        yield closed_conn_event.wait()
        # we lost connection to RabbitMQ
        self.stop()
        yield sleep(60)
        if not self.shutting_down:
            IOLoop.instance().add_callback(self.launch)

    @coroutine
    def _get_db_pool(self):
        # we try to connect to PGSQL in a thread, because timeouts can block
        executor = ThreadPoolExecutor(max_workers=1)
        try:
            self.db_pool = yield executor.submit(
                ThreadedConnectionPool, 1, self.pgsql_config.max_pool, self.dsn, async=False
            )
        except:
            logger.exception("Can't connect to PGSQL")
            sleep(60)
            yield self._get_db_pool()
            return
        finally:
            executor.shutdown()

    def stop(self):
        """
        Stops the shipper
        """
        if self.periodic_check_queue_size:
            logger.info("Stopping the periodic flush to PGSQL")
            self.periodic_check_queue_size.stop()
            self.periodic_check_queue_size = None
        self.consumer.stop()

    @coroutine
    def shutdown(self):
        """
        Shutdowns (stops definitely) the shipper.
        """
        self.shutting_down = True
        self.stop()

    @coroutine
    def _check_queue_size(self):
        self._times += 1
        if self._times >= self.pgsql_config.max_seconds_without_flush or self.syslog_ev_queue.qsize() >= 500:
            self._times = 0
            yield self._flush_messages()

    @coroutine
    def _flush_messages(self):
        size = self.syslog_ev_queue.qsize()
        if size == 0:
            logger.debug("No event to flush")
            return

        logger.info("Flushing events to PGSQL")
        if self.db_pool is None:
            logger.warning("We don't have a pool to PGSQL. Giving up flush. Stopping the consumer.")
            self.stop()
            yield sleep(60)
            yield self.launch()
            return
        if self.db_pool.closed:
            self.stop()
            yield sleep(60)
            yield self.launch()
            return

        logger.info("{} events to forward to PGSQL".format(size))
        msgs = list()
        # todo: write less redundant code
        try:
            for _ in range(size):
                msgs.append(self.syslog_ev_queue.get_nowait())
        except Empty:
            pass

        if not msgs:
            return

        def flush_backthread(rabbitmq_messages, tablename):
            events = SortedSet()
            for rabbit_message in rabbitmq_messages:
                try:
                    ev = Event.parse_bytes_to_event(rabbit_message.body, hmac=True, json=True)
                except ParsingError:
                    # should not happen, messages are coming from pyloggr
                    logger.info("Dropping one message after parsing error")
                except InvalidSignature:
                    security_logger = logging.getLogger('security')
                    logger.critical("Dropping one tampered event, see security logs")
                    security_logger.critical("Dropping one tampered event")
                    security_logger.critical(rabbit_message.body)
                else:
                    events.add(ev)

            try:
                conn = self.db_pool.getconn()
            except PoolError as ex:
                logging.exception("Can't get a PGSQL connection from the pool")
                raise ex
            try:
                conn.autocommit = False
                with conn.cursor() as cur:
                    # eliminate potentially duplicated events from the list
                    # build the SQL insert query
                    values = ','.join([evt.dump_sql(cur) for evt in events])
                    # query = "INSERT INTO {} {} VALUES ".format(tablename, SQL_COLUMNS) + values
                    query = SQL_INSERT_QUERY.format(
                        SQL_COLUMNS_STR, values, tablename, SQL_COLUMNS_STR, D_COLUMNS, tablename
                    )
                    cur.execute(query)
                    conn.commit()
            except psycopg2.Error as ex:
                logger.exception("Flushing to PGSQL failed")
                raise ex
            finally:
                if conn:
                    self.db_pool.putconn(conn)

        executor = ThreadPoolExecutor(max_workers=1)
        try:
            yield executor.submit(
                flush_backthread, rabbitmq_messages=msgs, tablename=self.pgsql_config.tablename
            )
        except:
            logger.exception("Flushing to PGSQL failed")
            for msg in msgs:
                msg.nack()
            self.stop()
            yield sleep(60)
            yield self.launch()
        else:
            for msg in msgs:
                msg.ack()
        finally:
            executor.shutdown()
