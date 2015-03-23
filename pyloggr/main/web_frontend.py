# encoding: utf-8
__author__ = 'stef'

# todo: show RabbitMQ queues
# todo: metrics

import logging

from pkg_resources import resource_filename
from tornado.web import RequestHandler, Application, url
from tornado.websocket import WebSocketHandler
from tornado.httpserver import HTTPServer
from tornado.netutil import bind_sockets
from tornado.gen import coroutine, Task
from tornado.ioloop import PeriodicCallback, IOLoop
from jinja2 import Environment, PackageLoader
import momoko
import psycopg2


from ..rabbitmq import management
from ..rabbitmq.notifications_consumer import NotificationsConsumer
from ..rabbitmq import RabbitMQConnectionError
from ..utils.observable import Observable
from ..config import NOTIFICATIONS, RABBITMQ_HTTP
from ..config import PARSER_CONSUMER, PGSQL_CONSUMER
from ..config import POSTGRESQL
from ..cache import cache

# todo: integrate in config
COOKIE_SECRET = "lkqsdhfosqfhqz:foez"

DSN = 'dbname={} user={} password={} host={} port={} connect_timeout={}'.format(
    POSTGRESQL.dbname, POSTGRESQL.user, POSTGRESQL.password,
    POSTGRESQL.host, POSTGRESQL.port, POSTGRESQL.connect_timeout
)

logger = logging.getLogger(__name__)
PERIODIC_RABBIT_STATUS_TIME = 10 * 1000


# noinspection PyAbstractClass
class SyslogClientsFeed(WebSocketHandler):
    """
    a Websocket used to talk with the browser
    """

    def open(self):
        logger.debug("Websocket is opened")
        self.set_nodelay(True)
        # get notifications from other pyloggr process
        self.application.consumer.register(self)
        # get notifications from RabbitMQ management API
        self.application.rabbitmq_stats.register(self)
        self.application.pgsql_stats.register(self)

    def notified(self, d):
        """
        `notified` is called by observables, when some event is meant to be communicated to the web frontend

        :param d: the event data to transmit to the web frontend
        :type d: dict
        """
        self.write_message(d)

    def on_message(self, message):
        # browser sent a message
        logger.debug("Browser said: {}".format(message))

    def on_close(self):
        logger.debug("WebSocket closed")
        self.application.consumer.unregister(self)
        self.application.rabbitmq_stats.unregister(self)
        self.application.pgsql_stats.unregister(self)

    def check_origin(self, origin):
        return True


class QueryLogs(RequestHandler):
    """
    Query the log database
    """
    pass


class Status(RequestHandler):
    """
    Displays a status page
    """

    def get(self):
        # todo: refactoring to use react.js instead of server templating
        cache_status = cache.available
        rabbitmq_status = self.application.consumer.channel is not None
        syslogs = []
        if cache_status:
            syslogs = cache.syslog

        html_output = self.application.templates['status'].render(
            syslogs=syslogs,
            cache_status=cache_status,
            rabbitmq_status=rabbitmq_status,
            rabbitmq_queues=self.application.rabbitmq_stats.queues
        )
        self.write(html_output)


class Hello(RequestHandler):
    def get(self):
        self.write("Hello, world")


class PyloggrApplication(Application):

    def __init__(self, url_prefix, consumer):
        self.consumer = consumer
        self.rabbitmq_stats = None

        urls = [
            (r'/?', Hello, 'index'),
            ('/status/?', Status, 'status'),
            ('/query/?', QueryLogs, 'query'),
            ('/websocket/?', SyslogClientsFeed, 'websocket'),
        ]
        handlers = [url(url_prefix + path, method, name=name) for (path, method, name) in urls]
        settings = {
            'autoreload': False,
            'debug': True,
            'static_path': resource_filename('pyloggr', '/static'),
            'static_url_prefix': url_prefix + '/static/',
            'cookie_secret': COOKIE_SECRET
        }

        template_loader = PackageLoader('pyloggr', 'templates')
        template_env = Environment(loader=template_loader)
        template_env.globals['reverse'] = self.reverse_url
        template_env.globals['prefix'] = url_prefix
        self.templates = {
            'status': template_env.get_template('status.html')
        }

        Application.__init__(self, handlers, **settings)


class WebServer(object):
    """
    Pyloggr process for the web frontend part
    """
    def __init__(self):
        self.consumer = NotificationsConsumer(NOTIFICATIONS, 'pyloggr.*.*')
        self.app = PyloggrApplication('/syslog', self.consumer)
        self.http_server = HTTPServer(self.app)
        self.sockets = bind_sockets(8888)
        self._periodic_rabbit = None
        self._periodic_pgsql = None

    @coroutine
    def launch(self):
        try:
            lost_rabbit_connection = yield self.consumer.start()
        except RabbitMQConnectionError:
            # no rabbitmq connection
            pass
        else:
            IOLoop.instance().add_callback(self.consumer.start_consuming)
        self.http_server.add_sockets(self.sockets)
        self.start_periodics()

    @coroutine
    def shutdown(self):
        yield self.http_server.close_all_connections()
        self.stop_periodic_rabbitmq_stats()
        self.consumer.stop()
        self.http_server.stop()

    def start_periodics(self):
        self.app.rabbitmq_stats = RabbitMQStats()
        self.app.rabbitmq_stats.update()
        self.app.pgsql_stats = PgSQLStats()
        self.app.pgsql_stats.update()
        if self._periodic_rabbit is None:
            self._periodic_rabbit = PeriodicCallback(self.app.rabbitmq_stats.update, PERIODIC_RABBIT_STATUS_TIME)
            self._periodic_rabbit.start()
        if self._periodic_pgsql is None:
            self._periodic_pgsql = PeriodicCallback(self.app.pgsql_stats.update, PERIODIC_RABBIT_STATUS_TIME)
            self._periodic_pgsql.start()

    def stop_periodic_rabbitmq_stats(self):
        if self._periodic_rabbit is not None:
            self._periodic_rabbit.stop()
            self.app.rabbitmq_stats = None
        if self._periodic_pgsql is not None:
            self._periodic_pgsql.stop()
            self.app.pgsql_stats = None


class PgSQLStats(Observable):
    """
    Gather information from the database
    """
    def __init__(self):
        Observable.__init__(self)
        self.available = None
        self._updating = False

    @coroutine
    def update(self):
        if self._updating:
            return
        self._updating = True
        db_conn = None
        stats = None
        try:
            db_conn = yield momoko.Op(momoko.Connection().connect, DSN)
            cursor = yield momoko.Op(db_conn.execute, 'SELECT COUNT(*) FROM {};'.format(POSTGRESQL.tablename))
            stats = cursor.fetchone()[0]
        except psycopg2.Error:
            logger.exception("Database seems down")
            self.available = False
        else:
            self.available = True
        finally:
            if db_conn is not None:
                db_conn.close()

        if stats:
            logger.debug("{} lines in PGSQL".format(stats))
        # notify the websocket
        yield self.notify_observers({
            'action': 'pgsql.stats',
            'loglines': stats,
            'available': self.available
        })

        self._updating = False


class RabbitMQStats(Observable):
    """
    Gather information from RabbitMQ management API
    """
    queue_names = [PARSER_CONSUMER.queue, PGSQL_CONSUMER.queue]

    def __init__(self):
        self.queues = {name: {} for name in self.queue_names}
        self.available = None
        self._updating = False

        self._rabbitmq_api_client = management.Client(
            host=RABBITMQ_HTTP,
            user=NOTIFICATIONS.user,
            passwd=NOTIFICATIONS.password,
            timeout=13
        )
        Observable.__init__(self)

    @coroutine
    def update(self):
        if self._updating:
            return

        self._updating = True
        results = dict()
        try:
            for name in self.queue_names:
                results[name] = yield self._rabbitmq_api_client.get_queue(NOTIFICATIONS.vhost, name)
        except management.NetworkError:
            logger.warning("RabbitMQ management API does not seem available")
            self.available = False
        except management.HTTPError as ex:
            logger.warning("Management API answered error code: '{}'".format(ex.status))
            logger.debug("Reason: {}".format(ex.reason))
            self.available = False
        else:
            self.available = True

        if self.available:
            for name in self.queue_names:
                self.queues[name]['messages'] = results[name]['messages']
        else:
            self.queues = {name: {} for name in self.queue_names}

        # notify the websocket
        yield self.notify_observers({
            'action': 'queues.stats',
            'queues': self.queues,
            'available': self.available
        })

        self._updating = False
