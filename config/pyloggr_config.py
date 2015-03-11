# encoding: utf-8
__author__ = 'stef'

from ssl import PROTOCOL_SSLv23, CERT_NONE
from secrets import RABBITMQ_PASSWORD, HMAC_KEY, PGSQL_PASSWORD, COOKIE_SECRET

MAX_WAIT_SECONDS_BEFORE_SHUTDOWN = 10

SLEEP_TIME = 60

RABBITMQ_HOST = '10.71.0.1'
RABBITMQ_PORT = 5672
RABBITMQ_HTTP_PORT = 80
RABBITMQ_USER = 'python'
RABBITMQ_VHOST = 'syslog'




# to get a good key: base64.b64encode(os.urandom(32))

FROM_RSYSLOG_TO_RABBITMQ_CONFIG = {
    'host': RABBITMQ_HOST,
    'port': RABBITMQ_PORT,
    'user': RABBITMQ_USER,
    'password': RABBITMQ_PASSWORD,
    'vhost': RABBITMQ_VHOST,
    'exchange': 'from_rsyslog',
    'application_id': 'Pyloggr RELP Server',
    'event_type': 'RSyslog event'
}

FROM_PARSER_TO_RABBITMQ_CONFIG = {
    'host': RABBITMQ_HOST,
    'port': RABBITMQ_PORT,
    'user': RABBITMQ_USER,
    'password': RABBITMQ_PASSWORD,
    'vhost': RABBITMQ_VHOST,
    'exchange': 'from_parser',
    'application_id': 'Pyloggr parser',
    'event_type': 'Parsed event'
}

FROM_RABBITMQ_TO_PARSER_CONFIG = {
    'host': RABBITMQ_HOST,
    'port': RABBITMQ_PORT,
    'user': RABBITMQ_USER,
    'password': RABBITMQ_PASSWORD,
    'vhost': RABBITMQ_VHOST,
    'queue': 'to_parser',
}

FROM_RABBITMQ_TO_PGSQL_CONFIG = {
    'host': RABBITMQ_HOST,
    'port': RABBITMQ_PORT,
    'user': RABBITMQ_USER,
    'password': RABBITMQ_PASSWORD,
    'vhost': RABBITMQ_VHOST,
    'queue': 'to_pgsql',
}

RABBITMQ_NOTIFICATIONS_CONFIG = {
    'host': RABBITMQ_HOST,
    'port': RABBITMQ_PORT,
    'user': RABBITMQ_USER,
    'password': RABBITMQ_PASSWORD,
    'vhost': RABBITMQ_VHOST,
    'exchange': 'pyloggr.pubsub'
}

SYSLOG_CONF = {
    'tcp_port': 1514,
    'tcpssl_port': 6514,
    'relp_port': 1515,
    'relpssl_port': 6515,
    'unix_socket': '/tmp/pyloggr.sock',
    'management_socket': '/tmp/pylogger_management.sock',
    'localhost_only': False,
    'ssl': {
        'certfile': '/home/stef/src/config/logcentral_chain.pem',
        'keyfile': '/home/stef/src/config/logcentral.key',
        'ssl_version': PROTOCOL_SSLv23,
        'cert_reqs': CERT_NONE,
        'ca_certs': None
    }
    # to disable TLS support you can use 'ssl': None

    # ssl_version
    # PROTOCOL_SSLv23: "the highest protocol version that both the client and server support"

    # cert_reqs
    # - CERT_OPTIONAL In this mode no certificates will be required from the other side of the socket connection;
    # but if they are provided, validation will be attempted and an SSLError will be raised on failure.
    # - CERT_REQUIRED In this mode, certificates are required from the other side of the socket connection;
    # an SSLError will be raised if no certificate is provided, or if its validation fails.

    # ca_certs
    # If the value of cert_reqs is not CERT_NONE, then the ca_certs parameter must point to a file of CA certificates.
    # The ca_certs file contains a set of concatenated “certification authority” certificates, which are used to
    # validate certificates passed from the other end of the connection.


}

PGSQL_CONFIG = {
    'host': '10.71.0.1',
    'port': 5432,
    'dbname': 'syslog',
    'user': 'syslog',
    'password': PGSQL_PASSWORD,
    'connect_timeout': 5,
    'max_pool': 10,
    'events_stack': 500,
    'max_seconds_without_flush': 60,
    'tablename': 'public.sysglogtest'
}

REDIS_CONFIG = {
    'host': '127.0.0.1',
    'port': 6379,
    'password': None,
    'rescue_queue_name': 'pyloggr.rescue_queue',
    'syslog_clients': 'pyloggr.syslog_clients',
    'syslog_status': 'pyloggr.syslog_status',
    'try_spawn_redis': True
}


LOGGING_LEVEL = "DEBUG"

LOGGING_CONFIG = {
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
            'filename': '/tmp/pylogger.log',
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
            'level': LOGGING_LEVEL,
            'propagate': False
        },
        'relp_server': {
            'handlers': ['console', 'tofile'],
            'level': LOGGING_LEVEL,
            'propagate': False
        },
        'consumer_to_pg': {
            'handlers': ['console', 'tofile'],
            'level': LOGGING_LEVEL,
            'propagate': False
        },
        'parser': {
            'handlers': ['console', 'tofile'],
            'level': LOGGING_LEVEL,
            'propagate': False
        },
        'http': {
            'handlers': ['console', 'tofile'],
            'level': LOGGING_LEVEL,
            'propagate': False
        },


    }

}

