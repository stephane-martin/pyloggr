# encoding: utf-8

"""
Utility script to start the pyloggr's processes.
"""

__author__ = 'stef'

from tornado.gen import coroutine
from argh.helpers import ArghParser

from pyloggr.config import FROM_RSYSLOG_TO_RABBITMQ_CONFIG, SYSLOG_CONF, FROM_RABBITMQ_TO_PARSER_CONFIG
from pyloggr.config import FROM_PARSER_TO_RABBITMQ_CONFIG, FROM_RABBITMQ_TO_PGSQL_CONFIG, PGSQL_CONFIG
from pyloggr.main.syslog_server import SyslogServer, SyslogConfig
from pyloggr.main.event_parser import EventParser
from pyloggr.main.shipper2pgsql import PostgresqlShipper
from pyloggr.main.web_frontend import WebServer
from pyloggr.scripts import PyloggrProcess


class SyslogProcess(PyloggrProcess):
    def __init__(self):
        PyloggrProcess.__init__(self, name="syslog", logging_file="/tmp/pyloggr_syslog_server.log", fork=True)
        self.syslog_config = SyslogConfig(SYSLOG_CONF)
        self.syslog_config.bind_all_sockets()

    @coroutine
    def launch(self):
        self.pyloggr_process = SyslogServer(
            rabbitmq_config=FROM_RSYSLOG_TO_RABBITMQ_CONFIG,
            syslog_config=self.syslog_config,
            task_id=self.task_id
        )
        self.logger.info("Starting {}".format(self.name))
        yield self.pyloggr_process.launch()


class ParserProcess(PyloggrProcess):
    def __init__(self):
        PyloggrProcess.__init__(self, name="parser", logging_file="/tmp/pyloggr_parser.log", fork=True)

    @coroutine
    def launch(self):
        self.pyloggr_process = EventParser(
            from_rabbitmq_config=FROM_RABBITMQ_TO_PARSER_CONFIG,
            to_rabbitmq_config=FROM_PARSER_TO_RABBITMQ_CONFIG
        )
        self.logger.info("Starting {}".format(self.name))
        yield self.pyloggr_process.launch()


class PgSQLShipperProcess(PyloggrProcess):
    def __init__(self):
        PyloggrProcess.__init__(self, name="PGSQL shipper", logging_file="/tmp/pyloggr_pgsql_shipper.log", fork=True)

    @coroutine
    def launch(self):
        self.pyloggr_process = PostgresqlShipper(FROM_RABBITMQ_TO_PGSQL_CONFIG, PGSQL_CONFIG)
        self.logger.info("Starting {}".format(self.name))
        yield self.pyloggr_process.launch()


class FrontendProcess(PyloggrProcess):
    def __init__(self):
        PyloggrProcess.__init__(self, name="HTTP frontend", logging_file="/tmp/pyloggr_frontend.log", fork=False)

    @coroutine
    def launch(self):
        self.pyloggr_process = WebServer()
        self.logger.info("Starting {}".format(self.name))
        yield self.pyloggr_process.launch()


def parser(config_dir=None):
    print "im the parser"


def syslog(config_dir=None):
    print "im the syslog server"


def pgsql_shipper(config_dir=None):
    print "im the pgsql shipper"

p = ArghParser()
p.add_commands([parser, syslog, pgsql_shipper])

if __name__ == '__main__':
    FrontendProcess().main()
