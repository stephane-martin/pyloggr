# encoding: utf-8
"""
Pyloggr Web interface
"""
from __future__ import absolute_import, division, print_function

__author__ = 'stef'

# todo: metrics

import logging

from pkg_resources import resource_filename
from tornado.web import RequestHandler, Application, url
from tornado.websocket import WebSocketHandler
from tornado.httpserver import HTTPServer
from tornado.gen import coroutine, TimeoutError
from tornado.ioloop import PeriodicCallback, IOLoop
from jinja2 import Environment, PackageLoader
import momoko
# noinspection PyCompatibility
import psycopg2

from pyloggr.rabbitmq import management
from pyloggr.rabbitmq import Configuration as RMQConfig
from pyloggr.rabbitmq.notifications_consumer import NotificationsConsumer
from pyloggr.rabbitmq import RabbitMQConnectionError
from pyloggr.utils.observable import Observable, Observer
from pyloggr.config import Config
from pyloggr.cache import Cache
from pyloggr.utils import sleep

logger = logging.getLogger(__name__)
PERIODIC_RABBIT_STATUS_TIME = 10 * 1000


class SyslogServers(Observable, Observer):
    """
    Data about the running Pyloggr's syslog servers

    Notified by Rabbitmq
    Observed by Websocket
    """
    def __init__(self):
        Observable.__init__(self)
        self.servers = dict()
        # get initial data from Redis

        self.servers = {syslog_server.server_id: {
            'id': syslog_server.server_id,
            'ports': syslog_server.ports,
            'clients': {client['id']: {
                'id': client['id'],
                'host': client['host'],
                'client_port': client['client_port'],
                'server_port': client['server_port']
            } for client in syslog_server.clients}
        } for syslog_server in Cache.syslog_list.values()}

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
    Websocket used to talk with the browser
    """

    def open(self):
        logger.debug("Websocket is opened")
        self.set_nodelay(True)

        # get status of pyloggr processes
        status.register(self)
        # get notifications from syslog servers
        syslog_servers.register(self)
        # get stats from RabbitMQ management API
        rabbitmq_stats.register(self)
        # get stats from PGSQL
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
        html_output = self.application.templates['status'].render()
        self.write(html_output)


class Upload(RequestHandler):
    pass
    # todo: upload log files via form or via POST API


class PgSQLStats(Observable):
    """
    Gather information from the database
    """
    def __init__(self):
        Observable.__init__(self)
        self._updating = False
        self.stats = 0

    @coroutine
    def update(self):
        if self._updating:
            return
        self._updating = True
        db_conn = None
        stats = 0
        status_inc = True
        for shipper in Config.SHIPPER2PGSQL:

            try:
                db_conn = yield momoko.Op(momoko.Connection().connect, Config.SHIPPER2PGSQL[shipper].dsn)
                cursor = yield momoko.Op(db_conn.execute, 'SELECT COUNT(*) FROM {};'.format(
                    Config.SHIPPER2PGSQL[shipper].tablename
                ))
                stats += cursor.fetchone()[0]
            except psycopg2.Error:
                logger.exception("Database seems down")
                status_inc = False
            finally:
                if db_conn is not None:
                    db_conn.close()

        status.postgresql = status_inc
        self.stats = stats
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
    queue_names = [machine.source.queue for machine in Config.MACHINES.values()]
    queue_names.extend(shipper.source_queue for shipper in Config.SHIPPER2PGSQL.values())

    def __init__(self):
        Observable.__init__(self)
        self.queues = {name: {} for name in self.queue_names}
        self._updating = False

        self._rabbitmq_api_client = management.Client(
            host=Config.RABBITMQ_HTTP,
            user=Config.RABBITMQ.user,
            passwd=Config.RABBITMQ.password,
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
                results[name] = yield self._rabbitmq_api_client.get_queue(Config.RABBITMQ.vhost, name)
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
            'cookie_secret': Config.COOKIE_SECRET
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
    def __init__(self, sockets):
        self.app = PyloggrApplication('/syslog')
        self.http_server = HTTPServer(self.app)
        self._periodic = None
        self.sockets = sockets

    @coroutine
    def launch(self):
        # at this point, redis has been initialized...
        global syslog_servers, notifications_consumer
        # ... so we can get information about syslog servers from redis
        syslog_servers = SyslogServers()
        # ... and notifications (updates) from rabbitmq are sent to syslog_servers
        notifications_consumer.register(syslog_servers)
        IOLoop.instance().add_callback(self.get_rabbit_notifications)

        self.http_server.add_sockets(self.sockets)
        # periodic updates of stats
        self.start_periodic()

    @coroutine
    def get_rabbit_notifications(self):
        try:
            lost_rabbit_connection = yield notifications_consumer.start()
        except (RabbitMQConnectionError, TimeoutError):
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
        logger.debug("Dropping HTTP clients")
        yield self.http_server.close_all_connections()
        logger.debug("Stopping stats refresh")
        self.stop_periodic()
        logger.debug("Asking the notification consumer to stop")
        yield notifications_consumer.stop()
        logger.debug("Stopping the HTTP server")
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
        status.redis = True

# todo: perform init in Webserver instead

status = Status()
rabbitmq_stats = RabbitMQStats()
pgsql_stats = PgSQLStats()
syslog_servers = None
notifications_consumer = NotificationsConsumer(
    rabbitmq_config=RMQConfig(
        host=Config.RABBITMQ.host,
        port=Config.RABBITMQ.port,
        user=Config.RABBITMQ.user,
        password=Config.RABBITMQ.password,
        vhost=Config.RABBITMQ.vhost,
        exchange=Config.NOTIFICATIONS.exchange,
        binding_key=Config.NOTIFICATIONS.binding_key
    )
)
