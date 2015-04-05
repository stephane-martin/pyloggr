# encoding: utf-8

"""
Describe the pyloggr's processes.
"""

__author__ = 'stef'

from tornado.gen import coroutine

from pyloggr.scripts import PyloggrProcess
from pyloggr.main.syslog_server import SyslogServer, SyslogParameters
from pyloggr.main.event_parser import EventParser
from pyloggr.main.shipper2pgsql import PostgresqlShipper
from pyloggr.main.web_frontend import WebServer
from pyloggr.main.harvest import Harvest


class SyslogProcess(PyloggrProcess):
    def __init__(self, name):
        PyloggrProcess.__init__(self, name=name, fork=True)
        from pyloggr.config import SYSLOG
        self.syslog_config = SyslogParameters(SYSLOG)
        self.syslog_config.bind_all_sockets()

    @coroutine
    def launch(self):
        from pyloggr.config import SYSLOG_PUBLISHER
        self.pyloggr_process = SyslogServer(
            rabbitmq_config=SYSLOG_PUBLISHER,
            syslog_config=self.syslog_config,
            server_id=self.task_id
        )
        self.logger.info("Starting {}".format(self.name))
        yield self.pyloggr_process.launch()


class ParserProcess(PyloggrProcess):
    def __init__(self, name):
        PyloggrProcess.__init__(self, name=name, fork=True)

    @coroutine
    def launch(self):
        from pyloggr.config import PARSER_CONSUMER, PARSER_PUBLISHER
        self.pyloggr_process = EventParser(
            from_rabbitmq_config=PARSER_CONSUMER,
            to_rabbitmq_config=PARSER_PUBLISHER
        )
        self.logger.info("Starting {}".format(self.name))
        yield self.pyloggr_process.launch()


class PgSQLShipperProcess(PyloggrProcess):
    def __init__(self, name):
        PyloggrProcess.__init__(self, name=name, fork=True)

    @coroutine
    def launch(self):
        from pyloggr.config import PGSQL_CONSUMER, POSTGRESQL
        self.pyloggr_process = PostgresqlShipper(PGSQL_CONSUMER, POSTGRESQL)
        self.logger.info("Starting {}".format(self.name))
        yield self.pyloggr_process.launch()


class FrontendProcess(PyloggrProcess):
    def __init__(self, name):
        PyloggrProcess.__init__(self, name=name, fork=False)

    @coroutine
    def launch(self):
        self.pyloggr_process = WebServer()
        self.logger.info("Starting {}".format(self.name))
        yield self.pyloggr_process.launch()


class HarvestProcess(PyloggrProcess):
    def __init__(self, name):
        PyloggrProcess.__init__(self, name=name, fork=False)

    @coroutine
    def launch(self):
        from pyloggr.config import HARVEST
        self.pyloggr_process = Harvest(HARVEST)
        self.logger.info("Starting {}".format(self.name))
        yield self.pyloggr_process.launch()
