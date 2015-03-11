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
from future.builtins import bytes
from jinja2 import Environment, PackageLoader

from ..rabbitmq.notifications_consumer import NotificationsConsumer
from ..config import RABBITMQ_NOTIFICATIONS_CONFIG, COOKIE_SECRET
from ..cache import cache

logger = logging.getLogger(__name__)


class SyslogClientsFeed(WebSocketHandler):
    def open(self):
        logger.debug("Websocket is opened")
        self.set_nodelay(True)
        self.application.consumer.register(self)

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
        cache_status = cache.available
        rabbitmq_status = self.application.consumer.channel is not None
        syslogs = []
        if cache_status:
            syslogs = cache.syslog

        html_output = self.application.templates['status'].render(
            syslogs=syslogs,
            cache_status=cache_status,
            rabbitmq_status=rabbitmq_status
        )
        self.write(html_output)


class Hello(RequestHandler):
    def get(self):
        self.write("Hello, world")


class PyloggrApplication(Application):

    def __init__(self, url_prefix, consumer):
        self.consumer = consumer

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
        self.consumer = NotificationsConsumer(RABBITMQ_NOTIFICATIONS_CONFIG, 'pyloggr.*')
        self.app = PyloggrApplication('/syslog', self.consumer)
        self.http_server = HTTPServer(self.app)
        self.sockets = bind_sockets(8888)

    @coroutine
    def start(self):
        yield self.consumer.start()
        self.http_server.add_sockets(self.sockets)
        self.consumer.start_consuming()

    @coroutine
    def stop(self):
        yield self.http_server.close_all_connections()
        self.http_server.stop()
        self.consumer.stop()
