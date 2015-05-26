# encoding: utf-8

"""
Ships events from RabbitMQ to a Syslog server
"""

__author__ = 'stef'

import logging
import socket
from datetime import timedelta

from tornado.gen import coroutine, TimeoutError
from tornado.ioloop import IOLoop

from pyloggr.rabbitmq.consumer import Consumer, RabbitMQConnectionError
from pyloggr.event import Event, ParsingError, InvalidSignature
from pyloggr.syslog import RELPClient, SyslogClient
from pyloggr.utils import sleep
from pyloggr.config import Config


logger = logging.getLogger(__name__)
security_logger = logging.getLogger('security')


class SyslogShipper(object):
    """
    SyslogShipper retrieves events from RabbitMQ, and forwards them to a Syslog server
    """
    def __init__(self, rabbitmq_config, shipper_config):
        """
        :type rabbitmq_config: pyloggr.rabbitmq.Configuration
        :type shipper_config: pyloggr.config.Shipper2SyslogConfig
        """
        self.consumer = None
        self.shipper_config = shipper_config
        self.rabbitmq_config = rabbitmq_config
        self.ev_queue = None
        self.closed_syslog_event = None
        self.shutting_down = False
        self.stopping = False
        self.syslog_client = None

    @coroutine
    def launch(self):
        """
        Starts the shipper

        - Open a connection to the remote syslog server
        - Open a connection to RabbitMQ
        - Consume messages from RabbitMQ
        - Parse messages as regular syslog events
        - Ship events to the remote syslog server
        """
        if self.shutting_down or self.stopping:
            return
        self.syslog_client = RELPClient(
            self.shipper_config.host, self.shipper_config.port, self.shipper_config.use_ssl
        ) if self.shipper_config.protocol.lower() == "relp" else SyslogClient(
            self.shipper_config.host, self.shipper_config.port, self.shipper_config.use_ssl
        )
        try:
            self.closed_syslog_event = yield self.syslog_client.start()
        except (socket.error, TimeoutError):
            logger.error("Shipper2syslog: can't connect to the remote syslog server")
            yield sleep(Config.SLEEP_TIME)
            if not self.shutting_down:
                IOLoop.instance().add_callback(self.launch)
                return
        # here we use a callback, so that we can directly wait for the next closed_publisher_event
        IOLoop.instance().add_callback(self._start_consumer)
        # wait until we lose syslog connection
        yield self.closed_syslog_event.wait()
        yield self.stop()
        yield sleep(Config.SLEEP_TIME)
        if not self.shutting_down:
            IOLoop.instance().add_callback(self.launch)

    @coroutine
    def _start_consumer(self):
        self.consumer = Consumer(self.rabbitmq_config)
        try:
            closed_consumer_event = yield self.consumer.start()
        except (TimeoutError, RabbitMQConnectionError):
            logger.error("Shipper2syslog: can't connect to consumer to RabbitMQ")
            yield self.stop()
            # stop() will stop the syslog connection too, so closed_syslog_event is going to trigger
            return
        else:
            IOLoop.instance().add_callback(self._consume)
            # wait until we lose the connection to rabbitmq
            yield closed_consumer_event.wait()
            yield self.stop()

    @coroutine
    def _consume(self):
        self.ev_queue = self.consumer.start_consuming()             # SimpleToroQueue
        while self.consumer.consuming and not self.shutting_down:
            try:
                message = yield self.ev_queue.get_wait(deadline=timedelta(seconds=1))
            except TimeoutError:
                pass
            else:
                IOLoop.instance().add_callback(self._forward_message, message)

    @coroutine
    def _forward_message(self, message):
        """
        :type message: pyloggr.rabbitmq.RabbitMQMessage
        """
        try:
            ev = Event.parse_bytes_to_event(message.body, hmac=True)
        except ParsingError:
            logger.info("shipper2syslog: dropping one unparsable message")
            message.ack()
        except InvalidSignature:
            logger.info("shipper2syslog: dropping one message with invalid signature")
            message.ack()
            security_logger.critical("shipper2syslog: dropping one message with invalid signature")
            security_logger.info(message.body)
        else:
            status, _ = yield self.syslog_client.publish_event(ev, frmt=self.shipper_config.frmt)
            if status:
                message.ack()
            else:
                message.nack()

    @coroutine
    def stop(self):
        """Stop the shipper"""
        if self.stopping:
            return
        self.stopping = True
        if self.consumer:
            yield self.consumer.stop()
            self.consumer = None
        if self.syslog_client:
            yield self.syslog_client.stop()
            self.syslog_client = None
        self.stopping = False

    @coroutine
    def shutdown(self):
        """Shutdown the shipper"""
        self.shutting_down = True
        yield self.stop()
