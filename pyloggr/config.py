# encoding: utf-8
"""
Small hack to be able to import configuration from environment variable.
"""

__author__ = 'stef'

import os
import logging
import logging.config
import sys
from os.path import dirname, exists, abspath, join, expanduser
import ssl
from base64 import b64decode

from configobj import ConfigObj
from marshmallow import Schema, fields


class RabbitMQBaseSchema(Schema):
    class Meta:
        strict = True

    host = fields.String(required=True)
    port = fields.Integer(default=5672)
    vhost = fields.String(required=True)
    user = fields.String(required=True)
    password = fields.String(required=True)


class RabbitMQBaseConfig(object):
    schema = RabbitMQBaseSchema()

    def __init__(self, host, port, vhost, user, password):
        self.host = host
        self.port = port
        self.vhost = vhost
        self.user = user
        self.password = password


class ConsumerSchema(RabbitMQBaseSchema):
    queue = fields.String(required=True)
    qos = fields.Integer(required=True)

    def make_object(self, data):
        return ConsumerConfig(**data)


class ConsumerConfig(RabbitMQBaseConfig):
    schema = ConsumerSchema()

    def __init__(self, host, port, vhost, user, password, queue, qos):
        RabbitMQBaseConfig.__init__(self, host, port, vhost, user, password)
        self.queue = queue
        self.qos = qos


class PublisherSchema(RabbitMQBaseSchema):
    application_id = fields.String(default='')
    event_type = fields.String(default='')
    exchange = fields.String(required=True)

    def make_object(self, data):
        return PublisherConfig(**data)


class PublisherConfig(RabbitMQBaseConfig):
    schema = PublisherSchema()

    def __init__(self, host, port, vhost, user, password, application_id, event_type, exchange):
        RabbitMQBaseConfig.__init__(self, host, port, vhost, user, password)
        self.application_id = application_id
        self.event_type = event_type
        self.exchange = exchange


class NotificationsSchema(RabbitMQBaseSchema):
    exchange = fields.String(required=True)

    def make_object(self, data):
        return NotificationsConfig(**data)


class NotificationsConfig(RabbitMQBaseConfig):
    def __init__(self, host, port, vhost, user, password, exchange):
        RabbitMQBaseConfig.__init__(self, host, port, vhost, user, password)
        self.exchange = exchange


class PostgresqlSchema(Schema):
    class Meta:
        strict = True

    host = fields.String(required=True)
    port = fields.Integer(default=5432)
    user = fields.String(required=True)
    password = fields.String(required=True)
    dbname = fields.String(required=True)
    tablename = fields.String(required=True)
    max_pool = fields.Integer(default=10)
    events_stack = fields.Integer(default=500)
    max_seconds_without_flush = fields.Integer(default=60)
    connect_timeout = fields.Integer(default=10)

    def make_object(self, data):
        return PostgresqlConfig(**data)


class PostgresqlConfig(object):
    schema = PostgresqlSchema()

    def __init__(self, host, port, user, password, dbname, tablename, max_pool, events_stack, max_seconds_without_flush,
                 connect_timeout):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.dbname = dbname
        self.tablename = tablename
        self.max_pool = max_pool
        self.events_stack = events_stack
        self.max_seconds_without_flush = max_seconds_without_flush
        self.connect_timeout = connect_timeout


class RedisSchema(Schema):
    class Meta:
        strict = True

    config_file = fields.String(default=u'/usr/local/etc/redis.conf')
    host = fields.String(default=u'127.0.0.1')
    port = fields.Integer(default=6379)
    password = fields.String(default='')
    try_spawn_redis = fields.Boolean(default=False)
    path = fields.String(default=u'/usr/local/bin/redis-server')

    def make_object(self, data):
        return RedisConfig(**data)


class RedisConfig(object):
    schema = RedisSchema()

    def __init__(self, config_file, host, port, password, try_spawn_redis, path):
        self.config_file = config_file
        self.host = host
        self.port = port
        self.password = password
        self.try_spawn_redis = try_spawn_redis
        self.path = path


class SSLSchema(Schema):
    class Meta:
        strict = True

    certfile = fields.String(required=True)
    keyfile = fields.String(required=True)
    ssl_version = fields.String(default=u'PROTOCOL_SSLv23')
    ca_certs = fields.String(default='None')
    cert_reqs = fields.Select(default=u'CERT_OPTIONAL', choices=[u'CERT_OPTIONAL', 'CERT_REQUIRED'])

    def make_object(self, data):
        return SSLConfig(**data)


class SSLConfig(object):
    schema = SSLSchema()

    def __init__(self, certfile, keyfile, ssl_version, ca_certs, cert_reqs):
        self.certfile = certfile
        self.keyfile = keyfile
        self.ssl_version = ssl_version
        self.ca_certs = ca_certs
        self.cert_reqs = cert_reqs

class LoggingSchema(Schema):
    class Meta:
        strict = True

    security = fields.String(default=u"~/logs/pyloggr.security.log")
    syslog = fields.String(default=u"~/logs/pyloggr.syslog.log")
    parser = fields.String(default=u"~/logs/pyloggr.parser.log")
    frontend = fields.String(default=u"~/logs/pyloggr.frontend.log")
    pgsql_shipper = fields.String(default=u"~/logs/pyloggr.pgsql_shipper.log")
    level = fields.String(default=u"INFO")

    def make_object(self, data):
        return LoggingConfig(**data)


class LoggingConfig(object):
    def __init__(self, security, syslog, parser, frontend, pgsql_shipper, level):
        self.security = security
        self.syslog = syslog
        self.parser = parser
        self.frontend = frontend
        self.pgsql_shipper = pgsql_shipper
        self.level = level


class SyslogSchema(Schema):
    class Meta:
        strict = True

    localhost_only = fields.Boolean(default=False)
    relp_port = fields.Integer(default=-1)
    relpssl_port = fields.Integer(default=-1)
    tcp_port = fields.Integer(default=-1)
    tcpssl_port = fields.Integer(default=-1)
    unix_socket = fields.String(default=u'')
    SSL = fields.Nested(SSLSchema)

    def make_object(self, data):
        return SyslogConfig(**data)


class SyslogConfig(object):
    schema = SyslogSchema()

    def __init__(self, localhost_only, relp_port, relpssl_port, tcp_port, tcpssl_port, unix_socket, SSL=None):
        self.localhost_only = localhost_only
        self.unix_socket = unix_socket
        self.SSL = SSL
        self.relp_port = relp_port
        self.relpssl_port = relpssl_port
        self.tcp_port = tcp_port
        self.tcpssl_port = tcpssl_port


class ConfigSchema(Schema):
    class Meta:
        strict = True

    MAX_WAIT_SECONDS_BEFORE_SHUTDOWN = fields.Integer(default=10)
    SLEEP_TIME = fields.Integer(default=60)
    HMAC_KEY = fields.String(required=True)
    RABBITMQ_HTTP = fields.String(required=True)

    POSTGRESQL = fields.Nested(PostgresqlSchema)
    NOTIFICATIONS = fields.Nested(NotificationsSchema)
    PARSER_CONSUMER = fields.Nested(ConsumerSchema)
    PARSER_PUBLISHER = fields.Nested(PublisherSchema)
    PGSQL_CONSUMER = fields.Nested(ConsumerSchema)
    SYSLOG_PUBLISHER = fields.Nested(PublisherSchema)
    REDIS = fields.Nested(RedisSchema)
    SYSLOG = fields.Nested(SyslogSchema)
    LOGGING_FILES = fields.Nested(LoggingSchema)

    def make_object(self, data):
        return Config(**data)

slots = [
    'MAX_WAIT_SECONDS_BEFORE_SHUTDOWN', 'SLEEP_TIME', 'NOTIFICATIONS', 'PARSER_CONSUMER',
    'PARSER_PUBLISHER', 'PGSQL_CONSUMER', 'SYSLOG_PUBLISHER', 'REDIS', 'SYSLOG', 'HMAC_KEY',
    'RABBITMQ_HTTP', 'POSTGRESQL', 'LOGGING_FILES'
]

class Config(object):
    schema = ConfigSchema()
    __slots__ = slots

    def __init__(self, MAX_WAIT_SECONDS_BEFORE_SHUTDOWN, SLEEP_TIME,
                 NOTIFICATIONS, PARSER_CONSUMER, PARSER_PUBLISHER, PGSQL_CONSUMER, SYSLOG_PUBLISHER,
                 REDIS, SYSLOG, HMAC_KEY, RABBITMQ_HTTP, POSTGRESQL, LOGGING_FILES):

        self.MAX_WAIT_SECONDS_BEFORE_SHUTDOWN = MAX_WAIT_SECONDS_BEFORE_SHUTDOWN
        self.SLEEP_TIME = SLEEP_TIME
        self.NOTIFICATIONS = NOTIFICATIONS
        self.PARSER_CONSUMER = PARSER_CONSUMER
        self.PARSER_PUBLISHER = PARSER_PUBLISHER
        self.PGSQL_CONSUMER = PGSQL_CONSUMER
        self.SYSLOG_PUBLISHER = SYSLOG_PUBLISHER
        self.REDIS = REDIS
        self.SYSLOG = SYSLOG
        self.HMAC_KEY = b64decode(HMAC_KEY)
        self.RABBITMQ_HTTP = RABBITMQ_HTTP
        self.POSTGRESQL = POSTGRESQL
        self.LOGGING_FILES = LOGGING_FILES


    @classmethod
    def load(cls, d):
        return cls.schema.load(d).data

    @classmethod
    def load_config_from_directory(cls, directory):
        """
        :rtype: Config
        """
        config_file = join(directory, 'pyloggr_config')
        config = ConfigObj(infile=config_file, interpolation=False, encoding="utf-8", write_empty_values=True,
                           raise_errors=True, file_error=True)
        d = config.dict()
        c = cls.load(d)
        if c.SYSLOG.SSL is not None:
            if c.SYSLOG.SSL.ca_certs == "None":
                c.SYSLOG.SSL.ca_certs = ssl.CERT_NONE
            c.SYSLOG.SSL.ssl_version = getattr(ssl, c.SYSLOG.SSL.ssl_version)
            c.SYSLOG.SSL.cert_reqs = getattr(ssl, c.SYSLOG.SSL.cert_reqs)

        if not c.REDIS.password:
            c.REDIS.password = None

        return c


def set_logging(filename):
    logging_config_dict = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'fileformat': {
                'format': '%(asctime)s --- %(name)s --- %(process)d --- %(levelname)s --- %(message)s'
            },
            'consoleformat': {
                'format': '%(levelname)s --- %(message)s'
            }
        },
        'handlers': {
            'console': {
                'level': 'DEBUG',
                'class': 'logging.StreamHandler',
                'formatter': 'consoleformat'
            },
            'tofile': {
                'level': 'DEBUG',
                'class': 'logging.FileHandler',
                'formatter': 'fileformat',
                'filename': '',
                'encoding': 'utf-8'
            },
            'security_handler': {
                'level': 'DEBUG',
                'class': 'logging.FileHandler',
                'formatter': 'fileformat',
                'filename': '',
                'encoding': 'utf-8'
            }

        },
        'loggers': {
            '': {
                'handlers': ['console', 'tofile'],
                'level': "INFO"
            },
            'pyloggr': {
                'handlers': ['console', 'tofile'],
                'level': LOGGING_FILES.level,
                'propagate': False
            },
            'security': {
                'handlers': ['console', 'security_handler'],
                'level': 'INFO',
                'propagate': False
            }

        }

    }

    filename = abspath(expanduser(filename))
    security_filename = abspath(expanduser(LOGGING_FILES.security))
    if not exists(dirname(filename)):
        os.makedirs(dirname(filename))
    if not exists(dirname(security_filename)):
        os.makedirs(dirname(security_filename))

    logging_config_dict['handlers']['tofile']['filename'] = filename
    logging_config_dict['handlers']['security_handler']['filename'] = security_filename

    logging.config.dictConfig(logging_config_dict)


CONFIG_DIR = os.environ.get('PYLOGGR_CONFIG_DIR')
thismodule = sys.modules[__name__]
if os.environ.get('SPHINX_BUILD'):
    # mock the config object when we are just building sphinx documentation
    for attr in slots:
        setattr(thismodule, attr, 'Mock')
else:
    # inject the config_obj attributes in this module, so that other modules can do things like
    # from config import PARAMETER
    config_obj = Config.load_config_from_directory(CONFIG_DIR)
    for attr in slots:
        setattr(thismodule, attr, getattr(config_obj, attr))



