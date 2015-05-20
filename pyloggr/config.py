# encoding: utf-8
"""
Small hack to be able to import configuration from environment variable.
"""

__author__ = 'stef'

from os.path import abspath, expanduser, dirname, join, exists
import logging
import logging.config
import ssl as ssl_module
from base64 import b64decode
from itertools import ifilter, imap

from configobj import ConfigObj

from pyloggr.utils import check_directory
from pyloggr.utils.constants import FACILITY, SEVERITY


class GenericConfig(object):
    """Base class for configurations"""
    @classmethod
    def from_dict(cls, d):
        """
        Build configuration object from a dictionnary

        :param d: dictionnary
        """
        return cls(**d)


class RabbitMQConfig(GenericConfig):
    """RabbitMQ connection parameters"""
    def __init__(self, host, user, password, port=5672, vhost='pyloggr'):
        self.host = str(host)
        self.port = int(port) if port else 5672
        self.vhost = str(vhost) if vhost else 'pyloggr'
        self.user = str(user)
        self.password = password


class ConsumerConfig(GenericConfig):
    """Parameters for RabbitMQ consumers"""

    def __init__(self, queue, qos=None, binding_key=None):
        self.queue = str(queue)
        self.qos = int(qos) if qos is not None else None
        self.binding_key = str(binding_key) if binding_key else None


class PublisherConfig(GenericConfig):
    """Parameters for RabbitMQ publishers"""
    def __init__(self, exchange, application_id='pyloggr', event_type='', binding_key=''):
        self.application_id = str(application_id) if application_id else 'pyloggr'
        self.event_type = str(event_type) if event_type else ''
        self.exchange = str(exchange)
        self.binding_key = str(binding_key) if binding_key else None


class FilterMachineConfig(GenericConfig):
    """
    Filter machines configuration
    """
    def __init__(self, source, destination, filters):
        self.source = ConsumerConfig.from_dict(source)
        self.destination = PublisherConfig.from_dict(destination)
        self.filters = str(filters)


class Shipper2PGSQL(GenericConfig):
    """
    Parameters for PostgreSQL shippers
    """
    def __init__(self, host, user, password, source_queue, event_stack_size=500, port=5432, dbname="pyloggr",
                 tablename="events", max_pool=10, connect_timeout=10):
        self.host = str(host)
        self.port = int(port) if port else 5432
        self.user = str(user)
        self.password = password
        self.dbname = str(dbname) if dbname else 'pyloggr'
        self.tablename = str(tablename) if tablename else 'events'
        self.max_pool = int(max_pool) if max_pool else 10
        self.connect_timeout = int(connect_timeout) if connect_timeout else 10
        self.source_queue = str(source_queue)
        self.event_stack_size = int(event_stack_size) if event_stack_size else 500
        self.dsn = 'dbname={} user={} password={} host={} port={} connect_timeout={}'.format(
            self.dbname, self.user, self.password, self.host, self.port, self.connect_timeout
        )


class Shipper2FSConfig(GenericConfig):
    """
    Parameters for filesystem shippers
    """
    def __init__(self, directory, filename, source_queue, seconds_between_flush=10, frmt="RSYSLOG"):
        self.directory = str(directory)
        self.filename = str(filename)
        self.frmt = str(frmt) if frmt else "RSYSLOG"
        self.source_queue = str(source_queue)
        self.seconds_between_flush = int(seconds_between_flush) if seconds_between_flush else 10


class Shipper2SyslogConfig(GenericConfig):
    """
    Parameters for syslog shippers
    """
    def __init__(self, host, port, source_queue, use_ssl=False, protocol="tcp", frmt="RFC5424", source_qos=500):
        self.host = str(host)
        self.port = int(port)
        self.source_queue = str(source_queue)
        self.use_ssl = bool(use_ssl) if use_ssl is not None else False
        self.protocol = str(protocol) if protocol else "tcp"
        self.frmt = str(frmt) if frmt else "RFC5424"
        self.source_qos = int(source_qos)if source_qos is not None else 500


class RedisConfig(GenericConfig):
    """
    Redis connection parameters
    """
    def __init__(self, config_file="/etc/redis.conf", host="127.0.0.1", port=6379, password=None,
                 try_spawn_redis=False, path=None):
        self.config_file = str(config_file) if config_file else "/etc/redis.conf"
        self.host = str(host) if host else "127.0.0.1"
        self.port = int(port) if port else 6379
        self.password = password if password else None
        self.try_spawn_redis = bool(try_spawn_redis) if try_spawn_redis is not None else False
        self.path = str(path) if path else None


class SSLConfig(GenericConfig):
    """
    Syslog servers SSL configuration
    """
    def __init__(self, certfile, keyfile, ssl_version="PROTOCOL_SSLv23", ca_certs=ssl_module.CERT_NONE,
                 cert_reqs=ssl_module.CERT_NONE):

        certfile = abspath(expanduser(certfile))
        if not exists(certfile):
            raise ValueError("In SSL configuration, cerfile '{}' does not exist".format(certfile))
        self.certfile = certfile

        keyfile = abspath(expanduser(keyfile))
        if not exists(keyfile):
            raise ValueError("In SSL configuration, keyfile '{}' does not exist".format(keyfile))
        self.keyfile = keyfile

        if (ssl_version is None) or (ssl_version == "PROTOCOL_SSLv23") or (ssl_version == ''):
            self.ssl_version = ssl_module.PROTOCOL_SSLv23
        else:
            self.ssl_version = getattr(ssl.module, ssl_version, ssl_module.PROTOCOL_SSLv23)
        if (ca_certs is None) or (ca_certs == "CERT_NONE") or (ca_certs == ''):
            self.ca_certs = ssl_module.CERT_NONE
        else:
            self.ca_certs = getattr(ssl_module, ca_certs, ssl_module.CERT_NONE)
        if (cert_reqs is None) or (cert_reqs == "CERT_NONE") or (cert_reqs == ''):
            self.cert_reqs = ssl_module.CERT_NONE
        else:
            self.cert_reqs = getattr(ssl_module, cert_reqs, ssl_module.CERT_NONE)


class LoggingConfig(GenericConfig):
    """
    Where to log
    """
    def __init__(self, level="DEBUG", **kwargs):
        for name in ['security', 'syslog', 'filtermachine', 'frontend', 'shipper2fs', 'shipper2pgsql', 'harvest',
                     'collector']:
            self.__setattr__(name, str(kwargs.get(name, "~/logs/pyloggr.{}.log".format(name))))
        self.level = str(level) if level else "DEBUG"


class SyslogServerConfig(GenericConfig):
    """
    Parameters for syslog servers
    """
    def __init__(self, name, ports=None, stype='tcp', localhost_only=False, socketname='', ssl=None,
                 packer_groups=None):
        self.name = str(name)
        if ports is None:
            self.ports = []
        elif isinstance(ports, list):
            self.ports = [int(p) for p in ports]
        else:
            self.ports = [int(ports)]
        self.stype = str(stype) if stype else 'tcp'
        self.localhost_only = bool(localhost_only) if localhost_only else False
        self.ssl = SSLConfig.from_dict(ssl) if ssl else None
        self.socketname = str(socketname) if socketname else ''
        self.packer_groups = packer_groups.split(',') if packer_groups else []


class HarvestDirectory(GenericConfig):
    """
    Directories to harvest file logs from
    """
    def __init__(self, directory_name, app_name=u'', packer_group=u'', recursive=False, facility=u'',
                 severity=u'', source=u''):
        self.packer_group = str(packer_group) if packer_group else ''
        self.directory_name = str(directory_name)
        self.recursive = bool(recursive) if recursive is not None else False
        if facility:
            if facility not in FACILITY.values():
                raise ValueError("HARVEST configuration: invalid 'facility' value")
        self.facility = str(facility) if facility else ''
        if severity:
            if severity not in SEVERITY.values():
                raise ValueError("HARVEST configuration: invalid 'severity' value")
        self.severity = str(severity) if severity else ''
        self.app_name = str(app_name) if app_name else ''
        self.source = str(source) if source else ''


def _config_file_to_dict(filename):
    config = ConfigObj(
        infile=filename, interpolation=False, encoding="utf-8", write_empty_values=True,
        raise_errors=True, file_error=True
    )
    return config.dict()


class GlobalConfig(object):
    """
    Placeholder for all configuration parameters
    """

    # noinspection PyUnusedLocal,PyPep8Naming
    def __init__(self, HMAC_KEY, RABBITMQ_HTTP, COOKIE_SECRET, MAX_WAIT_SECONDS_BEFORE_SHUTDOWN=10,
                 PIDS_DIRECTORY='~/pids', RESCUE_QUEUE_DIRNAME="~/rescue", SLEEP_TIME=60, **kwargs):
        self.MAX_WAIT_SECONDS_BEFORE_SHUTDOWN = int(MAX_WAIT_SECONDS_BEFORE_SHUTDOWN)
        self.HMAC_KEY = b64decode(HMAC_KEY)
        self.PIDS_DIRECTORY = check_directory(PIDS_DIRECTORY)
        self.RABBITMQ_HTTP = RABBITMQ_HTTP
        self.RESCUE_QUEUE_DIRNAME = check_directory(RESCUE_QUEUE_DIRNAME)
        self.COOKIE_SECRET = COOKIE_SECRET
        self.SLEEP_TIME = int(SLEEP_TIME)

    @classmethod
    def load(cls, config_dirname):
        """
        :param config_dirname: configuration directory path
        :rtype: GlobalConfig
        """
        config_dirname = check_directory(config_dirname)
        main_config_filename = join(config_dirname, 'pyloggr_config')
        d = _config_file_to_dict(main_config_filename)
        c = cls(**d)
        c.CONFIG_DIR = config_dirname
        c.NOTIFICATIONS = PublisherConfig.from_dict(d['NOTIFICATIONS'])
        c._load_redis_conf()
        c._load_rabbitmq_conf()
        c._load_shipper2pgsql_conf()
        c._load_shipper2fs_conf()
        c._load_logging_conf()
        c._load_harvest_conf()
        c._load_syslog_servers_conf()
        c._load_machines_conf()
        c._load_shipper2syslog_conf()
        return c

    def _load_redis_conf(self):
        redis_config_filename = join(self.CONFIG_DIR, 'redis.conf')
        d = _config_file_to_dict(redis_config_filename)
        self.REDIS = RedisConfig.from_dict(d)

    def _load_rabbitmq_conf(self):
        rabbitmq_config_filename = join(self.CONFIG_DIR, 'rabbitmq.conf')
        d = _config_file_to_dict(rabbitmq_config_filename)
        self.RABBITMQ = RabbitMQConfig.from_dict(d)

    def _load_shipper2pgsql_conf(self):
        pgsql_config_filename = join(self.CONFIG_DIR, 'shipper2pgsql.conf')
        d = _config_file_to_dict(pgsql_config_filename)
        self.SHIPPER2PGSQL = {shipper: Shipper2PGSQL.from_dict(d[shipper]) for shipper in d}

    def _load_shipper2fs_conf(self):
        fs_config_filename = join(self.CONFIG_DIR, 'shipper2fs.conf')
        d = _config_file_to_dict(fs_config_filename)
        self.SHIPPER2FS = {shipper: Shipper2FSConfig.from_dict(d[shipper]) for shipper in d}
        for shipper in self.SHIPPER2FS.values():
            shipper.directory = check_directory(shipper.directory)

    def _load_logging_conf(self):
        logging_config_filename = join(self.CONFIG_DIR, 'logging.conf')
        d = _config_file_to_dict(logging_config_filename)
        self.LOGGING_FILES = LoggingConfig.from_dict(d)

    def _load_harvest_conf(self):
        self.HARVEST = {}
        harvest_conf_filename = join(self.CONFIG_DIR, 'harvest.conf')
        d = _config_file_to_dict(harvest_conf_filename)
        for (directory_name, options) in d.items():
            directory_name = check_directory(directory_name)
            options['directory_name'] = directory_name
            self.HARVEST[directory_name] = HarvestDirectory.from_dict(options)

    def _load_syslog_servers_conf(self):
        syslog_servers_conf_filename = join(self.CONFIG_DIR, 'syslog_servers.conf')
        d = _config_file_to_dict(syslog_servers_conf_filename)
        server_names = [server_name for server_name in d if isinstance(d[server_name], dict)]
        others = {key: value for key, value in d.items() if key not in server_names}
        self.SYSLOG_PUBLISHER = PublisherConfig.from_dict(others)
        self.SYSLOG = {}
        for server_name in server_names:
            d[server_name]['name'] = server_name
            self.SYSLOG[server_name] = SyslogServerConfig.from_dict(d[server_name])

    def _load_machines_conf(self):
        syslog_servers_conf_filename = join(self.CONFIG_DIR, 'machines.conf')
        d = _config_file_to_dict(syslog_servers_conf_filename)
        self.MACHINES = {name: FilterMachineConfig.from_dict(machine) for name, machine in d.items()}

    def _load_shipper2syslog_conf(self):
        shipper2syslog_conf_filename = join(self.CONFIG_DIR, 'shipper2syslog.conf')
        d = _config_file_to_dict(shipper2syslog_conf_filename)
        self.SHIPPER2SYSLOG = {name: Shipper2SyslogConfig.from_dict(shipper) for name, shipper in d.items()}


def set_logging(filename, level="DEBUG"):
    """
    Set logging configuration

    :param filename: logs file name
    :param level: logs verbosity
    :type filename: str
    :type level: str
    """
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
    check_directory(dirname(filename))
    check_directory(dirname(security_filename))

    logging_config_dict['handlers']['tofile']['filename'] = filename
    logging_config_dict['handlers']['security_handler']['filename'] = security_filename

    logging.config.dictConfig(logging_config_dict)


class Config(object):
    """
    Config object can be imported and contains configuration parameters
    """
    pass


def set_configuration(configuration_directory):
    """
    Set up configuration

    :param configuration_directory: configuration parent directory
    :return:
    """
    config_obj = GlobalConfig.load(configuration_directory)
    # copy configuration to Config object
    attrs = ifilter(lambda attr: attr.isupper(), vars(config_obj))
    list(imap(lambda attr: setattr(Config, attr, config_obj.__getattribute__(attr)), attrs))

    # set HMAC key so that Event can compute hmacs
    from pyloggr.event import Event
    # noinspection PyUnresolvedReferences
    Event.HMAC_KEY = Config.HMAC_KEY

    # read packers_config and inject it in Config.SYSLOG.servers and Config.HARVEST.directories
    from pyloggr.packers.build_packers_config import parse_config_file
    packers_config = parse_config_file(join(configuration_directory, 'packers_config'))
    # in Config.SYSLOG.servers ...
    syslog_servers_with_packers = [server for server in Config.SYSLOG.values() if server.packer_groups]
    for syslog_server in syslog_servers_with_packers:
        syslog_server.packer_groups = [
            packers_config[packer_group_name] for packer_group_name in syslog_server.packer_groups
        ]
    # in Config.HARVEST.directories ...
    harvest_directories_with_packers = [
        directory_obj for directory_obj in Config.HARVEST.values() if directory_obj.packer_group
    ]
    for directory_obj in harvest_directories_with_packers:
        directory_obj.packer_group = packers_config[directory_obj.packer_group]
