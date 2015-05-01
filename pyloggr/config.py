# encoding: utf-8
"""
Small hack to be able to import configuration from environment variable.
"""

__author__ = 'stef'

import os
import logging
import logging.config
from os.path import dirname, exists, abspath, join, expanduser
import ssl as ssl_module
from base64 import b64decode

from configobj import ConfigObj
from marshmallow import Schema, fields

from pyloggr.utils.constants import FACILITY, SEVERITY


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
    ports = fields.List(fields.Integer, allow_none=False)
    stype = fields.String(required=True)
    localhost_only = fields.Boolean(default=False)
    socketname = fields.String(required=False, default=u'')
    ssl = fields.Nested(SSLSchema, allow_none=True)
    packer_groups = fields.List(fields.String, allow_none=True)

    def make_object(self, data):
        return SyslogServerConfig(**data)


class SyslogServerConfig(object):
    def __init__(self, name, ports=None, stype='tcp', localhost_only=False, socketname='', ssl=None,
                 packer_groups=None):
        self.name = name
        if ports is None:
            self.ports = []
        elif isinstance(ports, list):
            self.ports = [int(p) for p in ports]
        else:
            self.ports = [int(ports)]
        self.stype = stype
        self.localhost_only = localhost_only
        self.ssl = ssl
        self.socketname = socketname
        self.packer_groups = packer_groups if packer_groups else []


class SyslogSchema(Schema):
    class Meta:
        strict = True

    servers = fields.Nested(SyslogServerSchema, allow_null=False, many=True)

    def make_object(self, data):
        return SyslogConfig(**data)


class SyslogConfig(object):
    def __init__(self, servers):
        self.servers = servers


class HarvestDirectorySchema(Schema):
    class Meta(object):
        strict = True

    packer_group = fields.String(default=u'')
    remove_after = fields.Boolean(default=True)
    directory_name = fields.String(required=True)
    recursive = fields.Boolean(default=False)
    facility = fields.String(default=u'')
    severity = fields.String(default=u'')
    app_name = fields.String(default=u'')
    source = fields.String(default=u'')
    events_stack = fields.Integer(default=1000)

    def make_object(self, data):
        return HarvestDirectory(**data)


class HarvestDirectory(object):
    def __init__(self, directory_name, app_name=u'', remove_after=True, packer_group=u'', recursive=False, facility=u'',
                 severity=u'', source=u'', events_stack=1000):
        self.packer_group = packer_group
        self.remove_after = remove_after
        self.directory_name = directory_name
        self.recursive = recursive
        if facility:
            if facility not in FACILITY.values():
                raise ValueError("HARVEST configuration: invalid 'facility' value")
        self.facility = facility
        if severity:
            if severity not in SEVERITY.values():
                raise ValueError("HARVEST configuration: invalid 'severity' value")
        self.severity = severity
        self.app_name = app_name
        self.source = source
        self.events_stack = events_stack


class HarvestConfig(object):
    def __init__(self, directories):
        self.directories = directories


class HarvestSchema(Schema):
    class Meta(object):
        strict = True
    directories = fields.Nested(HarvestDirectorySchema, allow_null=True, many=True)

    def make_object(self, data):
        return HarvestConfig(**data)


class ConfigSchema(Schema):
    class Meta(object):
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
    RESCUE_QUEUE_FNAME = fields.String(default=u"~/rescue")

    def make_object(self, data):
        return GlobalConfig(**data)

config_slots = [
    'MAX_WAIT_SECONDS_BEFORE_SHUTDOWN', 'SLEEP_TIME', 'NOTIFICATIONS', 'PARSER_CONSUMER',
    'PARSER_PUBLISHER', 'PGSQL_CONSUMER', 'SYSLOG_PUBLISHER', 'REDIS', 'SYSLOG', 'HMAC_KEY',
    'RABBITMQ_HTTP', 'POSTGRESQL', 'LOGGING_FILES', 'COOKIE_SECRET', 'PIDS_DIRECTORY', 'HARVEST', 'RESCUE_QUEUE_FNAME'
]


class GlobalConfig(object):
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
        :rtype: GlobalConfig
        """
        config_file = join(directory, 'pyloggr_config')
        config = ConfigObj(infile=config_file, interpolation=False, encoding="utf-8", write_empty_values=True,
                           raise_errors=True, file_error=True)
        d = config.dict()

        for server_name in d['SYSLOG']:
            d['SYSLOG'][server_name]['name'] = server_name
        d['SYSLOG'] = {'servers': d['SYSLOG'].values()}

        new = {}
        for harvest_directory in d['HARVEST']:
            directory = abspath(expanduser(harvest_directory))
            d['HARVEST'][harvest_directory]['directory_name'] = directory
            new[directory] = d['HARVEST'][harvest_directory]
        d['HARVEST'] = {'directories': new.values()}

        c = cls.load(d)
        for server in c.SYSLOG.servers:
            if server.ssl is not None:
                if server.ssl.ca_certs == "None":
                    server.ssl.ca_certs = ssl_module.CERT_NONE
                server.ssl.ssl_version = getattr(ssl_module, server.ssl.ssl_version)
                server.ssl.cert_reqs = getattr(ssl_module, server.ssl.cert_reqs)
        c.SYSLOG.servers = {server.name: server for server in c.SYSLOG.servers}
        c.HARVEST.directories = {directory_obj.directory_name: directory_obj for directory_obj in c.HARVEST.directories}

        if not c.REDIS.password:
            c.REDIS.password = None

        c.HMAC_KEY = b64decode(c.HMAC_KEY)
        c.PIDS_DIRECTORY = abspath(expanduser(c.PIDS_DIRECTORY))
        # todo: make utils to check if directory exists and is writeable
        if not exists(c.PIDS_DIRECTORY):
            os.makedirs(c.PIDS_DIRECTORY)
        if not os.access(c.PIDS_DIRECTORY, os.W_OK | os.X_OK):
            raise ValueError("PID directory '{}' must be writeable".format(c.PIDS_DIRECTORY))
        c.RESCUE_QUEUE_FNAME = abspath(expanduser(c.RESCUE_QUEUE_FNAME))
        rescue_dir = dirname(c.RESCUE_QUEUE_FNAME)
        if not exists(rescue_dir):
            os.makedirs(rescue_dir)

        return c


def set_logging(filename, level="DEBUG"):
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
                'level': level,
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
    security_filename = abspath(expanduser(Config.LOGGING_FILES.security))
    if not exists(dirname(filename)):
        os.makedirs(dirname(filename))
    if not exists(dirname(security_filename)):
        os.makedirs(dirname(security_filename))

    logging_config_dict['handlers']['tofile']['filename'] = filename
    logging_config_dict['handlers']['security_handler']['filename'] = security_filename

    logging.config.dictConfig(logging_config_dict)


class Config(object):
    pass


def set_configuration(configuration_directory):
    config_obj = GlobalConfig.load_config_from_directory(configuration_directory)
    # copy the parameters in the Config object
    for attr in config_slots:
        setattr(Config, attr, getattr(config_obj, attr))

    # PGSQL DSN
    Config.POSTGRESQL.DSN = 'dbname={} user={} password={} host={} port={} connect_timeout={}'.format(
        Config.POSTGRESQL.dbname, Config.POSTGRESQL.user, Config.POSTGRESQL.password, Config.POSTGRESQL.host,
        Config.POSTGRESQL.port, Config.POSTGRESQL.connect_timeout
    )

    Config.CONFIG_DIR = configuration_directory

    # set HMAC key so that Event can compute hmacs
    from pyloggr.event import Event
    # noinspection PyUnresolvedReferences
    Event.HMAC_KEY = Config.HMAC_KEY

    # read packers_config and inject it in Config.SYSLOG.servers and Config.HARVEST.directories
    from pyloggr.packers.build_packers_config import parse_config_file
    packers_config = parse_config_file(join(configuration_directory, 'packers_config'))
    # Config.SYSLOG.servers ...
    syslog_servers_with_packers = [server for server in Config.SYSLOG.servers.values() if server.packer_groups]
    for syslog_server in syslog_servers_with_packers:
        syslog_server.packer_groups = [
            packers_config[packer_group_name] for packer_group_name in syslog_server.packer_groups
        ]
    # Config.HARVEST.directories ...
    harvest_directories_with_packers = [
        directory_obj for directory_obj in Config.HARVEST.directories.values() if directory_obj.packer_group
    ]
    for directory_obj in harvest_directories_with_packers:
        directory_obj.packer_group = packers_config[directory_obj.packer_group]

