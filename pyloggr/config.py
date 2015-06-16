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
import certifi
from future.builtins import str as real_unicode
from future.builtins import bytes as real_bytes

from pyloggr.utils import check_directory
from pyloggr.utils.constants import FACILITY, SEVERITY
from pwd import getpwnam
from grp import getgrnam


def _make_bool(b):
    if isinstance(b, bool):
        return b
    elif isinstance(b, int):
        return bool(b)
    if isinstance(b, real_unicode) or isinstance(b, real_bytes):
        b = b.lower().strip()
        if b in ('false', 'no'):
            return False
        elif b in ('true', 'yes'):
            return True
        elif b == "none":
            return None
        elif b == '':
            return None
        else:
            raise ValueError("Strange boolean value ?!")
    if b is None:
        return None

    raise ValueError("Strange boolean value ?!")


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
    def __init__(self, host, port, source_queue, use_ssl=False, protocol="tcp", frmt="RFC5424", source_qos=500,
                 verify=True, hostname='', ca_certs=None, client_cert=None, client_key=None):

        self.host = str(host)
        self.port = int(port)
        self.source_queue = str(source_queue)
        self.protocol = str(protocol) if protocol else "tcp"
        self.frmt = str(frmt) if frmt else "RFC5424"
        self.source_qos = int(source_qos) if source_qos is not None else 500

        use_ssl = _make_bool(use_ssl)
        verify = _make_bool(verify)
        self.use_ssl = use_ssl if use_ssl is not None else False
        self.verify = verify if verify is not None else True

        self.hostname = str(hostname) if hostname else self.host
        use_certifi = False
        if ca_certs is None:
            ca_certs = certifi.where()
            use_certifi = True
        elif ca_certs.lower() == "none" or ca_certs.lower() == "false":
            ca_certs = certifi.where()
            use_certifi = True
        else:
            ca_certs = abspath(expanduser(ca_certs))
            if not exists(ca_certs):
                raise ValueError("Shipper2SyslogConfig: ca_certs file '{}' does not exist".format(
                    ca_certs
                ))
        self.ca_certs = ca_certs

        if client_cert is not None:
            if client_cert.lower() == "none" or client_cert.lower() == 'false' or client_cert == '':
                client_cert = None
            else:
                client_cert = abspath(expanduser(client_cert))
                if not exists(client_cert):
                    raise ValueError("Shipper2SyslogConfig: client_cert file '{}' does not exist".format(
                        client_cert
                    ))
        self.client_cert = client_cert

        if client_key is not None:
            if client_key.lower() == "none" or client_key.lower() == 'false' or client_key == '':
                client_key = None
            else:
                client_key = abspath(expanduser(client_key))
                if not exists(client_key):
                    raise ValueError("Shipper2SyslogConfig: client_key file '{}' does not exist".format(
                        client_key
                    ))
        self.client_key = client_key

        if (client_key is None and client_cert is not None) or (client_key is not None and client_cert is None):
            raise ValueError("client_key and client_cert should be both filled or both empty")

        if self.use_ssl and (self.client_key is not None) and (not self.verify):
            raise ValueError("Nonsense configuration: syslog client wants to use certificates, "
                             "but does not check the server certificate's validity")

        if self.use_ssl and self.verify and use_certifi:
            print("Warning: Shipper2SyslogConfig: ca_certs is empty, that means that CA "
                  "certificates from certifi will be used to verify the server certificate")


class SSLConfig(GenericConfig):
    """
    Syslog servers SSL configuration
    """
    def __init__(self, certfile, keyfile, ssl_version="PROTOCOL_SSLv23", ca_certs='',
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
        if ca_certs is None:
            self.ca_certs = ''
        elif (ca_certs.lower() == "none") or (ca_certs == ''):
            self.ca_certs = ''
        else:
            certs_path = abspath(expanduser(ca_certs))
            if not exists(certs_path):
                raise ValueError("ca_certs file '{}' does not exist".format(ca_certs))
            self.ca_certs = certs_path
        if cert_reqs is None:
            self.cert_reqs = ssl_module.CERT_NONE
        elif (cert_reqs.lower() == "cert_none") or (cert_reqs == '') or (cert_reqs.lower() == "none"):
            self.cert_reqs = ssl_module.CERT_NONE
        elif (cert_reqs.lower() == "cert_optional") or (cert_reqs.lower() == "optional"):
            self.cert_reqs = ssl_module.CERT_OPTIONAL
        elif (cert_reqs.lower() == "cert_required") or (cert_reqs.lower() == "required"):
            self.cert_reqs = ssl_module.CERT_REQUIRED
        else:
            self.cert_reqs = getattr(ssl_module, cert_reqs, ssl_module.CERT_NONE)


class LoggingConfig(GenericConfig):
    """
    Where to log
    """
    def __init__(self, level="DEBUG", **kwargs):
        for name in ['security', 'syslog', 'filtermachine', 'frontend', 'shipper2fs', 'shipper2pgsql', 'harvest',
                     'collector', 'shipper2syslog']:
            self.__setattr__(name, str(kwargs.get(name, "~/logs/pyloggr.{}.log".format(name))))
        self.level = str(level) if level else "DEBUG"


class SyslogServerConfig(GenericConfig):
    """
    Parameters for syslog servers
    """
    def __init__(self, name, ports=None, stype='tcp', localhost_only=False, socket_names=None, ssl=None,
                 packer_groups=None, compress=False):
        self.name = str(name)
        if ports is None:
            self.ports = []
        elif isinstance(ports, list):
            self.ports = [int(p) for p in ports]
        else:
            self.ports = [int(ports)]
        self.stype = str(stype)
        localhost_only = _make_bool(localhost_only)
        compress = _make_bool(compress)
        self.localhost_only = localhost_only if localhost_only is not None else False
        self.compress = compress if compress is not None else False
        self.ssl = SSLConfig.from_dict(ssl) if ssl else None
        self.packer_groups = packer_groups.split(',') if packer_groups else []
        if not socket_names:
            self.socket_names = []
        elif isinstance(socket_names, list):
            self.socket_names = [abspath(expanduser(str(socket_name))) for socket_name in socket_names]
        else:
            self.socket_names = [abspath(expanduser(str(socket_names)))]


class SyslogAgentDestination(GenericConfig):
    def __init__(self, host, port, protocol="relp", frmt="RFC5424", tls=False, tls_hostname="",
                 verify_server_cert=True, compress=False):
        self.host = str(host)
        self.port = int(port)
        self.protocol = str(protocol).lower() if protocol else "relp"
        self.frmt = str(frmt) if frmt else "RFC5424"
        self.tls_hostname = str(tls_hostname) if tls_hostname else None

        verify_server_cert = _make_bool(verify_server_cert)
        compress = _make_bool(compress)
        tls = _make_bool(tls)
        self.tls = tls if tls is not None else False
        self.verify_server_cert = verify_server_cert if verify_server_cert is not None else True
        self.compress = compress if compress is not None else False


class SyslogAgentConfig(GenericConfig):
    """
    Parameters for the syslog agent
    """
    def __init__(self, UID=None, GID=None, destinations=None, tcp_ports=None, udp_ports=None, relp_ports=None, pause=5,
                 lmdb_db_name="~/lmdb/agent_queue", localhost_only=True, server_deadline=120, socket_names=None,
                 pids_directory="~/pids", logs_directory="~/logs", HMAC_KEY=None, logs_level="DEBUG"):

        self.UID = getpwnam(str(UID)).pw_uid if UID else None
        self.GID = getgrnam(str(GID)).gr_gid if GID else None

        self.logs_level = logs_level if logs_level else "DEBUG"
        pids_directory = pids_directory if pids_directory else "~/pids"
        self.pids_directory = check_directory(pids_directory, self.UID, self.GID, create=True)
        logs_directory = logs_directory if logs_directory else "~/logs"
        self.logs_directory = check_directory(logs_directory, self.UID, self.GID, create=True)
        lmdb_db_name = lmdb_db_name if lmdb_db_name else "~/lmdb/agent_queue"
        self.lmdb_db_name = check_directory(lmdb_db_name, self.UID, self.GID, create=True)

        self.logs_file = join(self.logs_directory, 'pyloggr.agent.log')
        if not tcp_ports:
            self.tcp_ports = []
        elif isinstance(tcp_ports, list):
            self.tcp_ports = [int(p) for p in tcp_ports]
        else:
            self.tcp_ports = [int(tcp_ports)]

        if not udp_ports:
            self.udp_ports = []
        elif isinstance(udp_ports, list):
            self.udp_ports = [int(p) for p in udp_ports]
        else:
            self.udp_ports = [int(udp_ports)]

        if not relp_ports:
            self.relp_ports = []
        elif isinstance(relp_ports, list):
            self.relp_ports = [int(p) for p in relp_ports]
        else:
            self.relp_ports = [int(relp_ports)]

        if not socket_names:
            self.socket_names = []
        elif isinstance(socket_names, list):
            self.socket_names = [abspath(expanduser(str(socket_name))) for socket_name in socket_names]
        else:
            self.socket_names = [abspath(expanduser(str(socket_names)))]

        self.pause = int(pause) if pause else 5
        self.server_deadline = int(server_deadline) if server_deadline is not None else 120

        localhost_only = _make_bool(localhost_only)
        self.localhost_only = localhost_only if localhost_only is not None else True
        self.destinations = destinations
        self.HMAC_KEY = b64decode(HMAC_KEY) if HMAC_KEY else None
        if not HMAC_KEY:
            logging.warning("You should specify a HMAC_KEY")

class HarvestDirectory(GenericConfig):
    """
    Directories to harvest file logs from
    """
    def __init__(self, directory_name, app_name=u'', packer_group=u'', recursive=False, facility=u'',
                 severity=u'', source=u''):
        self.packer_group = str(packer_group) if packer_group else ''
        self.directory_name = str(directory_name)
        recursive = _make_bool(recursive)
        self.recursive = recursive if recursive is not None else False
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
    return ConfigObj(
        infile=filename, interpolation=False, encoding="utf-8", write_empty_values=True,
        raise_errors=True, file_error=True
    ).dict()


class GlobalConfig(object):
    """
    Placeholder for all configuration parameters
    """

    # noinspection PyUnusedLocal,PyPep8Naming
    def __init__(self, HMAC_KEY, RABBITMQ_HTTP, COOKIE_SECRET, MAX_WAIT_SECONDS_BEFORE_SHUTDOWN=10,
                 PIDS_DIRECTORY='~/pids', SLEEP_TIME=60, UID=None, GID=None, HTTP_PORT=8080,
                 EXCHANGE_SPACE="~/lmdb/exchange", RESCUE_QUEUE_DIRNAME="~/lmdb/rescue",
                 **kwargs):
        self.UID = getpwnam(str(UID)).pw_uid if UID else None
        self.GID = getgrnam(str(GID)).gr_gid if GID else None
        self.MAX_WAIT_SECONDS_BEFORE_SHUTDOWN = int(MAX_WAIT_SECONDS_BEFORE_SHUTDOWN)
        self.HMAC_KEY = b64decode(HMAC_KEY)
        self.PIDS_DIRECTORY = check_directory(PIDS_DIRECTORY, self.UID, self.GID)
        self.RABBITMQ_HTTP = RABBITMQ_HTTP
        self.RESCUE_QUEUE_DIRNAME = check_directory(RESCUE_QUEUE_DIRNAME, self.UID, self.GID)
        self.EXCHANGE_SPACE = check_directory(EXCHANGE_SPACE, self.UID, self.GID)
        self.COOKIE_SECRET = COOKIE_SECRET
        self.SLEEP_TIME = int(SLEEP_TIME)
        self.HTTP_PORT = int(HTTP_PORT) if HTTP_PORT else 8080

    @classmethod
    def load(cls, config_dirname):
        """
        :param config_dirname: configuration directory path
        :rtype: GlobalConfig
        """
        config_dirname = check_directory(config_dirname, create=False)
        main_config_filename = join(config_dirname, 'pyloggr_config')
        d = _config_file_to_dict(main_config_filename)
        c = cls(**d)
        c.CONFIG_DIR = config_dirname
        c.NOTIFICATIONS = PublisherConfig.from_dict(d['NOTIFICATIONS'])
        c._load_rabbitmq_conf()
        c._load_shipper2pgsql_conf()
        c._load_shipper2fs_conf()
        c._load_logging_conf()
        c._load_harvest_conf()
        c._load_syslog_servers_conf()
        c._load_machines_conf()
        c._load_shipper2syslog_conf()
        return c

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
            shipper.directory = check_directory(shipper.directory, self.UID, self.GID)

    def _load_logging_conf(self):
        logging_config_filename = join(self.CONFIG_DIR, 'logging.conf')
        d = _config_file_to_dict(logging_config_filename)
        self.LOGGING_FILES = LoggingConfig.from_dict(d)

    def _load_harvest_conf(self):
        self.HARVEST = {}
        harvest_conf_filename = join(self.CONFIG_DIR, 'harvest.conf')
        d = _config_file_to_dict(harvest_conf_filename)
        for (directory_name, options) in d.items():
            directory_name = check_directory(directory_name, self.UID, self.GID)
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
    if hasattr(Config, "LOGGING_FILES"):
        security_filename = abspath(expanduser(Config.LOGGING_FILES.security))
    else:
        security_filename = filename
    check_directory(dirname(filename), Config.UID, Config.GID)
    check_directory(dirname(security_filename), Config.UID, Config.GID)

    logging_config_dict['handlers']['tofile']['filename'] = filename
    logging_config_dict['handlers']['security_handler']['filename'] = security_filename

    logging.config.dictConfig(logging_config_dict)


class Config(object):
    """
    Config object can be imported and contains configuration parameters
    """
    pass


def set_agent_configuration(config_file):
    d = _config_file_to_dict(config_file)
    dest_names = [dest_name for dest_name in d if isinstance(d[dest_name], dict)]
    others = {key: value for key, value in d.items() if key not in dest_names}
    Config.SYSLOG_AGENT = SyslogAgentConfig.from_dict(others)
    Config.SYSLOG_AGENT.destinations = [SyslogAgentDestination.from_dict(d[dest_name]) for dest_name in dest_names]
    Config.PIDS_DIRECTORY = Config.SYSLOG_AGENT.pids_directory
    Config.HMAC_KEY = Config.SYSLOG_AGENT.HMAC_KEY
    Config.UID = Config.SYSLOG_AGENT.UID
    Config.GID = Config.SYSLOG_AGENT.GID

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

    # read packers_config and inject it in Config.SYSLOG.servers and Config.HARVEST.directories
    from pyloggr.packers.build_packers_config import parse_config_file as parse_packer_config_file
    packers_config = parse_packer_config_file(join(configuration_directory, 'packers_config'))
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
