# encoding: utf-8

"""
Describe the pyloggr's processes.
"""

__author__ = 'stef'

import logging

import ftfy
from future.builtins import chr

from tornado.gen import coroutine
from tornado.ioloop import IOLoop

from pyloggr.scripts import PyloggrProcess
from pyloggr.main.syslog_server import MainSyslogServer, SyslogParameters
from pyloggr.main.filter_machine import FilterMachine
from pyloggr.main.shipper2pgsql import PostgresqlShipper
from pyloggr.main.shipper2fs import FilesystemShipper
from pyloggr.main.shipper2syslog import SyslogShipper
from pyloggr.main.web_frontend import WebServer
from pyloggr.main.harvest import Harvest
from pyloggr.main.collector import EventCollector
from pyloggr.rabbitmq import Configuration as RabbitConfig
from pyloggr.config import Config, Shipper2FSConfig, Shipper2SyslogConfig

logger = logging.getLogger(__name__)


class SyslogProcess(PyloggrProcess):
    """
    Implements the syslog server
    """
    def __init__(self):
        PyloggrProcess.__init__(self, fork=True)
        self.syslog_config = SyslogParameters(Config.SYSLOG)
        self.syslog_config.bind_all_sockets()

    @coroutine
    def _launch(self):
        publisher_config = RabbitConfig(
            host=Config.RABBITMQ.host,
            port=Config.RABBITMQ.port,
            user=Config.RABBITMQ.user,
            password=Config.RABBITMQ.password,
            vhost=Config.RABBITMQ.vhost,
            exchange=Config.SYSLOG_PUBLISHER.exchange,
            application_id=Config.SYSLOG_PUBLISHER.application_id,
            event_type=Config.SYSLOG_PUBLISHER.event_type
        )
        self.pyloggr_process = MainSyslogServer(
            rabbitmq_config=publisher_config,
            syslog_parameters=self.syslog_config,
            server_id=self.task_id
        )
        self.logger.info("Starting {}".format(self.name))
        yield self.pyloggr_process.launch()


class FilterMachineProcess(PyloggrProcess):
    """
    Apply filters to each event found in RabbitMQ, post back into RabbitMQ

    Parameters
    ==========
    name: str
        process name
    """
    def __init__(self):
        PyloggrProcess.__init__(self, fork=True)

    @coroutine
    def _launch(self):
        self.pyloggr_process = []
        for name, machine_config in Config.MACHINES.items():
            consumer_config = RabbitConfig(
                host=Config.RABBITMQ.host,
                port=Config.RABBITMQ.port,
                user=Config.RABBITMQ.user,
                password=Config.RABBITMQ.password,
                vhost=Config.RABBITMQ.vhost,
                queue=machine_config.source.queue,
                qos=machine_config.source.qos,
                binding_key=machine_config.source.binding_key
            )
            publisher_config = RabbitConfig(
                host=Config.RABBITMQ.host,
                port=Config.RABBITMQ.port,
                user=Config.RABBITMQ.user,
                password=Config.RABBITMQ.password,
                vhost=Config.RABBITMQ.vhost,
                exchange=machine_config.destination.exchange,
                application_id=machine_config.destination.application_id,
                event_type=machine_config.destination.event_type
            )
            process = FilterMachine(
                consumer_config=consumer_config,
                publisher_config=publisher_config,
                filters_filename=machine_config.filters
            )
            self.pyloggr_process.append(process)
            self.logger.info("Starting machine '{}'".format(name))
            yield process.launch()


class PgSQLShipperProcess(PyloggrProcess):
    """
    Ships events to PostgreSQL
    """
    def __init__(self):
        PyloggrProcess.__init__(self, fork=False)

    @coroutine
    def _launch(self):
        # start multiple pyloggr shippers if needed
        self.pyloggr_process = []
        for name, shipper_config in Config.SHIPPER2PGSQL.items():
            consumer_config = RabbitConfig(
                host=Config.RABBITMQ.host,
                port=Config.RABBITMQ.port,
                user=Config.RABBITMQ.user,
                password=Config.RABBITMQ.password,
                vhost=Config.RABBITMQ.vhost,
                queue=shipper_config.source_queue,
                qos=shipper_config.event_stack_size + 10,
                binding_key=None
            )
            process = PostgresqlShipper(consumer_config, shipper_config)
            self.pyloggr_process.append(process)
            self.logger.info("Starting PGSQL shipper '{}'".format(name))
            IOLoop.instance().add_callback(process.launch)


class FSShipperProcess(PyloggrProcess):
    """
    Dumps events to the filesystem
    """
    def __init__(self):
        PyloggrProcess.__init__(self, fork=False)

    @coroutine
    def _launch(self):
        # start multiple pyloggr shippers if needed
        self.pyloggr_process = []
        for name, shipper_config in Config.SHIPPER2FS.items():
            assert(isinstance(shipper_config, Shipper2FSConfig))
            consumer_config = RabbitConfig(
                host=Config.RABBITMQ.host,
                port=Config.RABBITMQ.port,
                user=Config.RABBITMQ.user,
                password=Config.RABBITMQ.password,
                vhost=Config.RABBITMQ.vhost,
                queue=shipper_config.source_queue
                # qos=shipper_config.event_stack_size + 10,
                # binding_key=None
            )
            process = FilesystemShipper(consumer_config, shipper_config)
            self.pyloggr_process.append(process)
            self.logger.info("Starting FS shipper '{}'".format(name))
            IOLoop.instance().add_callback(process.launch)


class SyslogShipperProcess(PyloggrProcess):
    """Ships events to a remote syslog server"""

    def __init__(self):
        PyloggrProcess.__init__(self, fork=False)

    @coroutine
    def _launch(self):
        # start multiple pyloggr shippers if needed
        self.pyloggr_process = []
        for name, shipper_config in Config.SHIPPER2SYSLOG.items():
            assert(isinstance(shipper_config, Shipper2SyslogConfig))
            consumer_config = RabbitConfig(
                host=Config.RABBITMQ.host,
                port=Config.RABBITMQ.port,
                user=Config.RABBITMQ.user,
                password=Config.RABBITMQ.password,
                vhost=Config.RABBITMQ.vhost,
                queue=shipper_config.source_queue,
                qos=shipper_config.source_qos
                # binding_key=None ?
            )
            process = SyslogShipper(consumer_config, shipper_config)
            self.pyloggr_process.append(process)
            self.logger.info("Starting syslog shipper '{}'".format(name))
            IOLoop.instance().add_callback(process.launch)


class FrontendProcess(PyloggrProcess):
    """
    Web frontend to Pyloggr
    """
    def __init__(self):
        PyloggrProcess.__init__(self, fork=False)

    @coroutine
    def _launch(self):
        self.pyloggr_process = WebServer()
        self.logger.info("Starting {}".format(self.name))
        yield self.pyloggr_process.launch()


class HarvestProcess(PyloggrProcess):
    """
    Monitor directories and inject files as logs in Pyloggr
    """
    def __init__(self):
        PyloggrProcess.__init__(self, fork=False)

    @coroutine
    def _launch(self):
        publisher_config = RabbitConfig(
            host=Config.RABBITMQ.host,
            port=Config.RABBITMQ.port,
            user=Config.RABBITMQ.user,
            password=Config.RABBITMQ.password,
            vhost=Config.RABBITMQ.vhost,
            exchange=Config.SYSLOG_PUBLISHER.exchange,
            application_id=Config.SYSLOG_PUBLISHER.application_id,
            event_type=Config.SYSLOG_PUBLISHER.event_type
        )
        try:
            self.pyloggr_process = Harvest(
                harvest_config=Config.HARVEST,
                publisher_config=publisher_config
            )
        except OSError:
            logger.exception("Harvest Initialization failed")
            IOLoop.instance().add_callback(self.shutdown)
        else:
            self.logger.info("Starting {}".format(self.name))
            yield self.pyloggr_process.launch()


class CollectorProcess(PyloggrProcess):
    """
    Collect events from the "rescue queue" and inject them back in pyloggr
    """
    def __init__(self):
        PyloggrProcess.__init__(self, fork=False)

    @coroutine
    def _launch(self):
        publisher_config = RabbitConfig(
            host=Config.RABBITMQ.host,
            port=Config.RABBITMQ.port,
            user=Config.RABBITMQ.user,
            password=Config.RABBITMQ.password,
            vhost=Config.RABBITMQ.vhost,
            exchange=Config.SYSLOG_PUBLISHER.exchange,
            application_id=Config.SYSLOG_PUBLISHER.application_id,
            event_type=Config.SYSLOG_PUBLISHER.event_type
        )
        self.pyloggr_process = EventCollector(rabbitmq_config=publisher_config)
        self.logger.info("Starting {}".format(self.name))
        yield self.pyloggr_process.launch()
