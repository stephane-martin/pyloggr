# encoding: utf-8

"""
The pyloggr.syslog subpackage provides syslog client and syslog server code.

------------------------
"""

__author__ = 'stef'

from .relp_client import RELPClient
from .tcp_syslog_client import SyslogClient
from server import BaseSyslogClientConnection, BaseSyslogServer
