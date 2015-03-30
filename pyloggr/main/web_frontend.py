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
from tornado.gen import coroutine
from tornado.ioloop import PeriodicCallback, IOLoop
from jinja2 import Environment, PackageLoader
import momoko
import psycopg2


from ..rabbitmq import management
from ..rabbitmq.notifications_consumer import NotificationsConsumer
from ..rabbitmq import RabbitMQConnectionError
from ..utils.observable import Observable, Observer
from ..config import NOTIFICATIONS, RABBITMQ_HTTP
from ..config import PARSER_CONSUMER, PGSQL_CONSUMER
from ..config import POSTGRESQL
from ..cache import cache
from pyloggr.utils import sleep

# todo: integrate in config
COOKIE_SECRET = "lkqsdhfosqfhqz:foez"

DSN = 'dbname={} user={} password={} host={} port={} connect_timeout={}'.format(
    POSTGRESQL.dbname, POSTGRESQL.user, POSTGRESQL.password,
    POSTGRESQL.host, POSTGRESQL.port, POSTGRESQL.connect_timeout
)

logger = logging.getLogger(__name__)
PERIODIC_RABBIT_STATUS_TIME = 10 * 1000


class SyslogServers(Observable, Observer):
    """
    Data about the running Pyloggr's syslog servers

    Notified by rabbitmq
    Observed by Websocket
    """
    def __init__(self):
        Observable.__init__(self)
        self.servers = dict()
        # get initial data from Redis

        if not cache.available:
            self.servers = {}

        self.servers = {syslog_server.server_id: {
            'id': syslog_server.server_id,
            'ports': syslog_server.ports,
            'clients': {client['id']: {
                'id': client['id'],
                'host': client['host'],
                'client_port': client['client_port'],
                'server_port': client['server_port']
            } for client in syslog_server.clients}
        } for syslog_server in cache.syslog_list.values()}

    def notified(self, d):
        # get updates from rabbitmq
        if d['action'] == "add_client":
            self.servers[d['server_id']]['clients'][d['id']] = {
                'id': d['id'],
                'host': d['host'],
                'client_port': d['client_port'],
                'server_port': d['server_port']
            }
        elif d['action'] == "remove_client":
            del self.servers[d['server_id']]['clients'][d['id']]
        elif d['action'] == "add_server":
            self.servers[d['server_id']] = {
                'id': d['server_id'],
                'clients': {},
                'ports': d['ports']
            }
        elif d['action'] == "remove_server":
            del self.servers[d['server_id']]
        else:
            logger.debug("Action: '{}'".format(d['action']))

        self.notify()

    def notify(self):
        status.syslog = bool(self.servers)
        d = dict()
        d['action'] = 'syslogs'
        d['servers'] = self.servers
        self.notify_observers(d)


class Status(Observable):
    """
    Encapsulates the status of the the various pyloggr components
    """
    labels = ['rabbitmq', 'redis', 'postgresql', 'syslog', 'parser', 'shipper']

    def __init__(self):
        Observable.__init__(self)

    def __setattr__(self, key, value):
        object.__setattr__(self, key, value)
        if key in self.labels:
            self.notify()

    def notify(self):
        d = dict()
        for f in ['rabbitmq', 'redis', 'postgresql', 'syslog', 'parser', 'shipper']:
            d[f] = "On" if getattr(self, f, None) else "Off"
        d['action'] = 'status'
        self.notify_observers(d)


# noinspection PyAbstractClass
class SyslogClientsFeed(WebSocketHandler, Observer):
    """
    a Websocket used to talk with the browser
    """

    def open(self):
        logger.debug("Websocket is opened")
        self.set_nodelay(True)
        # get notifications from other pyloggr process
        # get notifications from RabbitMQ management API
        status.register(self)
        syslog_servers.register(self)
        rabbitmq_stats.register(self)
        pgsql_stats.register(self)

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
        if message == "getStatus":
            status.notify()
            syslog_servers.notify()
            rabbitmq_stats.notify()
            pgsql_stats.notify()

    def on_close(self):
        logger.debug("WebSocket closed")
        syslog_servers.unregister(self)
        status.unregister(self)
        rabbitmq_stats.unregister(self)
        pgsql_stats.unregister(self)

    def check_origin(self, origin):
        return True


class QueryLogs(RequestHandler):
    """
    Query the log database
    """
    pass


class StatusPage(RequestHandler):
    """
    Displays a status page
    """

    def get(self):
        cache_status = cache.available
        # noinspection PyUnresolvedReferences
        rabbitmq_status = status.rabbitmq is not None
        syslogs = []
        if cache_status:
            syslogs = cache.syslog_list

        html_output = self.application.templates['status'].render(
            syslogs=syslogs,
            cache_status=cache_status,
            rabbitmq_status=rabbitmq_status,
            rabbitmq_queues=rabbitmq_stats.queues
        )
        self.write(html_output)


class PyloggrApplication(Application):

    def __init__(self, url_prefix):

        urls = [
            ('/status/?', StatusPage, 'status'),
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
        self.app = PyloggrApplication('/syslog')
        self.http_server = HTTPServer(self.app)
        self.sockets = bind_sockets(8888)
        self._periodic = None

    @coroutine
    def launch(self):
        # at this point redis has been initialized
        global syslog_servers, notifications_consumer
        syslog_servers = SyslogServers()
        notifications_consumer.register(syslog_servers)

        IOLoop.instance().add_callback(self.get_rabbit_notifications)
        self.http_server.add_sockets(self.sockets)
        self.start_periodic()

    @coroutine
    def get_rabbit_notifications(self):
        try:
            lost_rabbit_connection = yield notifications_consumer.start()
        except RabbitMQConnectionError:
            # no rabbitmq connection ==> no notifications
            yield sleep(60)
            IOLoop.instance().add_callback(self.get_rabbit_notifications)
            return
        else:
            # we use a callback to immediately return (start_consuming never returns)
            IOLoop.instance().add_callback(notifications_consumer.start_consuming)
        yield lost_rabbit_connection.wait()
        yield sleep(60)
        IOLoop.instance().add_callback(self.get_rabbit_notifications)

    @coroutine
    def shutdown(self):
        yield self.http_server.close_all_connections()
        self.stop_periodic()
        notifications_consumer.stop()
        self.http_server.stop()

    def start_periodic(self):
        self.app.rabbitmq_stats = RabbitMQStats()
        self.app.pgsql_stats = PgSQLStats()
        self.update_stats()
        if self._periodic is None:
            self._periodic = PeriodicCallback(self.update_stats, PERIODIC_RABBIT_STATUS_TIME)
            self._periodic.start()

    def stop_periodic(self):
        if self._periodic is not None:
            self._periodic.stop()
            self._periodic = None

    @coroutine
    def update_stats(self):
        yield rabbitmq_stats.update()
        yield pgsql_stats.update()
        status.redis = cache.available


class PgSQLStats(Observable):
    """
    Gather information from the database
    """
    def __init__(self):
        Observable.__init__(self)
        self._updating = False
        self.stats = None

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
            status.postgresql = False
        else:
            status.postgresql = True
            self.stats = stats
        finally:
            if db_conn is not None:
                db_conn.close()

        self._updating = False
        self.notify()

    def notify(self):
        d = dict()
        d['action'] = 'pgsql.stats'
        # noinspection PyUnresolvedReferences
        d['status'] = status.postgresql
        d['stats'] = self.stats
        self.notify_observers(d)


class RabbitMQStats(Observable):
    """
    Gather information from RabbitMQ management API
    """
    queue_names = [PARSER_CONSUMER.queue, PGSQL_CONSUMER.queue]

    def __init__(self):
        Observable.__init__(self)
        self.queues = {name: {} for name in self.queue_names}
        self._updating = False

        self._rabbitmq_api_client = management.Client(
            host=RABBITMQ_HTTP,
            user=NOTIFICATIONS.user,
            passwd=NOTIFICATIONS.password,
            timeout=13
        )

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
            status.rabbitmq = False
        except management.HTTPError as ex:
            logger.warning("Management API answered error code: '{}'".format(ex.status))
            logger.debug("Reason: {}".format(ex.reason))
            status.rabbitmq = False
        else:
            status.rabbitmq = True

        if status.rabbitmq:
            for name in self.queue_names:
                self.queues[name]['messages'] = results[name]['messages']
        else:
            self.queues = {name: {} for name in self.queue_names}

        self._updating = False
        self.notify()

    def notify(self):
        d = dict()
        d['action'] = 'queues.stats'
        # noinspection PyUnresolvedReferences
        d['status'] = status.rabbitmq
        d['queues'] = self.queues
        self.notify_observers(d)

status = Status()
rabbitmq_stats = RabbitMQStats()
pgsql_stats = PgSQLStats()
syslog_servers = None
notifications_consumer = NotificationsConsumer(NOTIFICATIONS, 'pyloggr.*.*')

