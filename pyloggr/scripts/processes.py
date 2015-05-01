# encoding: utf-8

"""
Describe the pyloggr's processes.
"""

__author__ = 'stef'

import logging

from tornado.gen import coroutine
from tornado.ioloop import IOLoop

from pyloggr.scripts import PyloggrProcess
from pyloggr.main.syslog_server import SyslogServer, SyslogParameters
from pyloggr.main.event_parser import EventParser
from pyloggr.main.shipper2pgsql import PostgresqlShipper
from pyloggr.main.web_frontend import WebServer
from pyloggr.main.harvest import Harvest
from pyloggr.config import Config

logger = logging.getLogger(__name__)


class SyslogProcess(PyloggrProcess):
    def __init__(self, name):
        PyloggrProcess.__init__(self, name=name, fork=True)
        self.syslog_config = SyslogParameters(Config.SYSLOG)
        self.syslog_config.bind_all_sockets()

    @coroutine
    def launch(self):
        self.pyloggr_process = SyslogServer(
            rabbitmq_config=Config.SYSLOG_PUBLISHER,
            syslog_parameters=self.syslog_config,
            server_id=self.task_id
        )
        self.logger.info("Starting {}".format(self.name))
        yield self.pyloggr_process.launch()


class ParserProcess(PyloggrProcess):
    def __init__(self, name):
        PyloggrProcess.__init__(self, name=name, fork=True)
        self.pyloggr_process = EventParser(
            from_rabbitmq_config=Config.PARSER_CONSUMER,
            to_rabbitmq_config=Config.PARSER_PUBLISHER
        )

    @coroutine
    def launch(self):
        self.logger.info("Starting {}".format(self.name))
        yield self.pyloggr_process.launch()


class PgSQLShipperProcess(PyloggrProcess):
    def __init__(self, name):
        PyloggrProcess.__init__(self, name=name, fork=True)

    @coroutine
    def launch(self):
        self.pyloggr_process = PostgresqlShipper(Config.PGSQL_CONSUMER, Config.POSTGRESQL)
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
        try:
            self.pyloggr_process = Harvest(
                harvest_config=Config.HARVEST,
                to_rabbitmq_config=Config.SYSLOG_PUBLISHER
            )
        except OSError:
            logger.exception("Harvest Initialization failed")
            IOLoop.instance().add_callback(self.shutdown)
        else:
            self.logger.info("Starting {}".format(self.name))
            yield self.pyloggr_process.launch()
