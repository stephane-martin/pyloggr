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
import ssl as ssl_module
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
    def __init__(self, host, port, vhost, user, password):
        self.host = host
        self.port = port
        self.vhost = vhost
        self.user = user
        self.password = password
        self.queue = None
        self.qos = None
        self.application_id = None
        self.event_type = None
        self.exchange = None


class ConsumerSchema(RabbitMQBaseSchema):
    queue = fields.String(required=True)
    qos = fields.Integer(required=True)

    def make_object(self, data):
        return ConsumerConfig(**data)


class ConsumerConfig(RabbitMQBaseConfig):
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
    harvest = fields.String(default=u"~/logs/pyloggr.harvest.log")
    level = fields.String(default=u"INFO")

    def make_object(self, data):
        return LoggingConfig(**data)


class LoggingConfig(object):
    def __init__(self, security, syslog, parser, frontend, pgsql_shipper, harvest, level):
        self.security = security
        self.syslog = syslog
        self.parser = parser
        self.frontend = frontend
        self.pgsql_shipper = pgsql_shipper
        self.level = level
        self.harvest = harvest


class SyslogServerSchema(Schema):
    class Meta:
        strict = True

    name = fields.String(required=True)
    port = fields.List(fields.Integer, allow_none=False)
    stype = fields.String(required=True)
    localhost_only = fields.Boolean(default=False)
    socket = fields.String(required=False, default='')
    ssl = fields.Nested(SSLSchema, allow_none=True)

    def make_object(self, data):
        return SyslogServerConfig(**data)


class SyslogServerConfig(object):
    def __init__(self, name, port=None, stype='tcp', localhost_only=False, socket='', ssl=None):
        self.name = name
        if port is None:
            self.port = []
        elif isinstance(port, list):
            self.port = [int(p) for p in port]
        else:
            self.port = [int(port)]
        self.stype = stype
        self.localhost_only = localhost_only
        self.ssl = ssl
        self.socket = socket


class SyslogSchema(Schema):
    class Meta:
        strict = True

    servers = fields.Nested(SyslogServerSchema, allow_null=False, many=True)

    def make_object(self, data):
        return SyslogConfig(**data)


class SyslogConfig(object):
    def __init__(self, servers):
        self.servers = servers


class HarvestConfig(object):
    def __init__(self, directory="~/harvest", remove_after=True):
        self.directory = expanduser(directory)
        self.remove_after = remove_after


class HarvestSchema(Schema):
    class Meta:
        strict = True

    directory = fields.String(default='~/harvest')
    remove_after = fields.Boolean(default=True)

    def make_object(self, data):
        return HarvestConfig(**data)


class ConfigSchema(Schema):
    class Meta:
        strict = True

    MAX_WAIT_SECONDS_BEFORE_SHUTDOWN = fields.Integer(default=10)
    SLEEP_TIME = fields.Integer(default=60)
    HMAC_KEY = fields.String(required=True)
    RABBITMQ_HTTP = fields.String(required=True)
    COOKIE_SECRET = fields.String(required=True)
    PIDS_DIRECTORY = fields.String(default=u'/tmp/pids')

    POSTGRESQL = fields.Nested(PostgresqlSchema)
    NOTIFICATIONS = fields.Nested(NotificationsSchema)
    PARSER_CONSUMER = fields.Nested(ConsumerSchema)
    PARSER_PUBLISHER = fields.Nested(PublisherSchema)
    PGSQL_CONSUMER = fields.Nested(ConsumerSchema)
    SYSLOG_PUBLISHER = fields.Nested(PublisherSchema)
    REDIS = fields.Nested(RedisSchema)
    SYSLOG = fields.Nested(SyslogSchema)
    LOGGING_FILES = fields.Nested(LoggingSchema)
    HARVEST = fields.Nested(HarvestSchema)

    def make_object(self, data):
        return Config(**data)

config_slots = [
    'MAX_WAIT_SECONDS_BEFORE_SHUTDOWN', 'SLEEP_TIME', 'NOTIFICATIONS', 'PARSER_CONSUMER',
    'PARSER_PUBLISHER', 'PGSQL_CONSUMER', 'SYSLOG_PUBLISHER', 'REDIS', 'SYSLOG', 'HMAC_KEY',
    'RABBITMQ_HTTP', 'POSTGRESQL', 'LOGGING_FILES', 'COOKIE_SECRET', 'PIDS_DIRECTORY', 'HARVEST'
]


class Config(object):
    __slots__ = config_slots

    def __init__(self, **kw):

        for slot in config_slots:
            self.__setattr__(slot, kw.get(slot, None))

    @classmethod
    def load(cls, d):
        return ConfigSchema().load(d).data

    @classmethod
    def load_config_from_directory(cls, directory):
        """
        :rtype: Config
        """
        config_file = join(directory, 'pyloggr_config')
        config = ConfigObj(infile=config_file, interpolation=False, encoding="utf-8", write_empty_values=True,
                           raise_errors=True, file_error=True)
        d = config.dict()
        for server_name in d['SYSLOG']:
            d['SYSLOG'][server_name]['name'] = server_name
        d['SYSLOG'] = {'servers': d['SYSLOG'].values()}
        c = cls.load(d)
        for server in c.SYSLOG.servers:
            if server.ssl is not None:
                if server.ssl.ca_certs == "None":
                    server.ssl.ca_certs = ssl_module.CERT_NONE
                server.ssl.ssl_version = getattr(ssl_module, server.ssl.ssl_version)
                server.ssl.cert_reqs = getattr(ssl_module, server.ssl.cert_reqs)

        if not c.REDIS.password:
            c.REDIS.password = None

        c.HMAC_KEY = b64decode(c.HMAC_KEY)
        c.PIDS_DIRECTORY = expanduser(c.PIDS_DIRECTORY)

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
    for attr in config_slots:
        setattr(thismodule, attr, 'Mock')
else:
    if CONFIG_DIR is None:
        raise RuntimeError("Configuration directory is not specified")

    # inject the config_obj attributes in this module, so that other modules can do things like
    # from config import PARAMETER
    config_obj = Config.load_config_from_directory(CONFIG_DIR)
    for attr in config_slots:
        setattr(thismodule, attr, getattr(config_obj, attr))
    POSTGRESQL.DSN = 'dbname={} user={} password={} host={} port={} connect_timeout={}'.format(
        POSTGRESQL.dbname, POSTGRESQL.user, POSTGRESQL.password, POSTGRESQL.host, POSTGRESQL.port,
        POSTGRESQL.connect_timeout
    )


