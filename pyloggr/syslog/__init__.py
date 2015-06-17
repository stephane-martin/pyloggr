# encoding: utf-8

"""
The pyloggr.syslog subpackage provides syslog client and syslog server implementations

------------------------
"""

__author__ = 'stef'

from .relp_client import RELPClient
from .tcp_syslog_client import SyslogClient
from server import BaseSyslogClientConnection, BaseSyslogServer

def client_factory(servr, port, protocol, use_ssl=False, verify_cert=True, hostname=None, ca_certs=None, client_key=None,
                   client_cert=None, server_deadline=120):

    protocol = str(protocol).lower()
    if protocol == "relp":
        return RELPClient(servr, port, use_ssl, verify_cert, hostname, ca_certs, client_key, client_cert, server_deadline)
    elif protocol == "tcp" or protocol == 'syslog':
        return SyslogClient(servr, port, use_ssl, verify_cert, hostname, ca_certs, client_key, client_cert, server_deadline)



