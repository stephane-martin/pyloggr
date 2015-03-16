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
from tornado.ioloop import PeriodicCallback
from concurrent.futures import ThreadPoolExecutor
from jinja2 import Environment, PackageLoader
import pyrabbit.api

from ..rabbitmq.notifications_consumer import NotificationsConsumer
from pyloggr.utils.observable import Observable
from ..config import RABBITMQ_NOTIFICATIONS_CONFIG, COOKIE_SECRET, RABBITMQ_HTTP, RABBITMQ_PASSWORD, RABBITMQ_USER
from ..config import RABBITMQ_VHOST, FROM_RABBITMQ_TO_PARSER_CONFIG, FROM_RABBITMQ_TO_PGSQL_CONFIG
from ..cache import cache

logger = logging.getLogger(__name__)
PERIODIC_RABBIT_STATUS_TIME = 10 * 1000


class SyslogClientsFeed(WebSocketHandler):
    def open(self):
        logger.debug("Websocket is opened")
        self.set_nodelay(True)
        # get notifications from other pyloggr process
        self.application.consumer.register(self)
        # get notifications from RabbitMQ management API
        self.application.rabbitmq_stats.register(self)

    def notified(self, d):
        # syslog client was added or removed
        self.write_message(d)

    def on_message(self, message):
        # browser sent a message
        logger.debug("Browser said: {}".format(message))

    def on_close(self):
        logger.debug("WebSocket closed")
        self.application.consumer.unregister(self)

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
    def __init__(self):
        self.consumer = NotificationsConsumer(RABBITMQ_NOTIFICATIONS_CONFIG, 'pyloggr.*.*')
        self.app = PyloggrApplication('/syslog', self.consumer)
        self.http_server = HTTPServer(self.app)
        self.sockets = bind_sockets(8888)
        self._periodic = None
        self.rabbitmq_stats = None

    @coroutine
    def start(self):
        yield self.consumer.start()
        self.http_server.add_sockets(self.sockets)
        self.consumer.start_consuming()
        self.start_periodic_rabbitmq_stats()

    @coroutine
    def stop(self):
        yield self.http_server.close_all_connections()
        self.stop_periodic_rabbitmq_stats()
        self.consumer.stop()
        self.http_server.stop()

    def start_periodic_rabbitmq_stats(self):
        self.app.rabbitmq_stats = RabbitMQStats()
        self.app.rabbitmq_stats.update()
        if self._periodic is None:
            self._periodic = PeriodicCallback(self.get_rabbitmq_stats, PERIODIC_RABBIT_STATUS_TIME)
            self._periodic.start()

    @coroutine
    def get_rabbitmq_stats(self):
        yield self.app.rabbitmq_stats.update()

    def stop_periodic_rabbitmq_stats(self):
        if self._periodic is not None:
            self._periodic.stop()
            self.app.rabbitmq_stats = None


class RabbitMQStats(Observable):
    queue_names = [FROM_RABBITMQ_TO_PARSER_CONFIG['queue'], FROM_RABBITMQ_TO_PGSQL_CONFIG['queue']]

    def __init__(self):
        self.queues = {name: {} for name in self.queue_names}
        self.executor = ThreadPoolExecutor(max_workers=len(self.queues)+1)

        self._rabbitmq_api_client = pyrabbit.api.Client(
            host=RABBITMQ_HTTP,
            user=RABBITMQ_USER,
            passwd=RABBITMQ_PASSWORD,
            timeout=30
        )
        Observable.__init__(self)

    @coroutine
    def update(self):
        # pyrabbit is not async, so lets use threads
        results = dict()
        for name in self.queue_names:
            # httplib is not thread safe, so lets do the requests in sequence
            results[name] = yield self.executor.submit(self._rabbitmq_api_client.get_queue, RABBITMQ_VHOST, name)

        for name in self.queue_names:
            self.queues[name]['messages'] = results[name]['messages']
        self.notify_observers({'action': 'queues.stats', 'queues': self.queues})
