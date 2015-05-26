# encoding: utf-8

"""
TCP syslog client
"""

__author__ = 'stef'

import socket
import logging
import ssl
from itertools import imap
from datetime import timedelta

import arrow
import certifi
from tornado.gen import coroutine, Return, with_timeout, TimeoutError
from tornado.iostream import StreamClosedError
from tornado.tcpclient import TCPClient
from tornado.netutil import ssl_match_hostname, SSLCertificateError
from toro import Event as ToroEvent

from pyloggr.event import Event

logger = logging.getLogger(__name__)
security_logger = logging.getLogger("security")


class SyslogClient(object):
    """
    Utility class to send messages or whole files to a syslog server, using TCP

    Parameters
    ==========
    server: str
        RELP server hostname or IP
    port: int
        RELP server port
    use_ssl: bool
        Should the client connect with SSL
    """

    def __init__(self, server, port, use_ssl=False, verify_cert=True, hostname=None, ca_certs=None):
        self.server = server
        self.port = port
        self.stream = None
        self.use_ssl = use_ssl
        self.client = None
        self.closed_connection_event = None
        self.hostname = hostname if hostname else server
        self.verify_cert = verify_cert
        self.ca_certs = ca_certs if ca_certs else certifi.where()

    @coroutine
    def start(self):
        """
        start()
        Connect to the syslog server
        """
        self.closed_connection_event = ToroEvent()
        self.client = TCPClient()
        try:
            connect_future = with_timeout(
                timeout=timedelta(seconds=10),
                future=self.client.connect(self.server, self.port)
            )
            self.stream = yield connect_future
        except socket.error:
            logger.error("TCP syslog client could not connect (socket.error)")
            raise
        except TimeoutError:
            logger.error("TCP syslog client could not connect (timeout)")
            raise

        if self.use_ssl:
            if self.verify_cert:
                ssl_options = {
                    'cert_reqs': ssl.CERT_REQUIRED,
                    'ca_certs': self.ca_certs
                }
            else:
                ssl_options = {
                    'cert_reqs': ssl.CERT_NONE
                }
            try:
                self.stream = yield self.stream.start_tls(
                    server_side=False,
                    server_hostname=self.hostname if self.verify_cert else None,
                    ssl_options=ssl_options
                )
            except ssl.SSLError:
                security_logger.exception("Error happened when opening SSL connection")
                raise socket.error("Error happened when opening SSL connection")

        self.stream.set_close_callback(self._on_stream_closed)
        raise Return(self.closed_connection_event)

    def _on_stream_closed(self):
        self.closed_connection_event.set()

    @coroutine
    def stop(self):
        """
        stop()
        Disconnect from the syslog server
        """
        if not self.stream.closed():
            self.stream.close()
        if self.client:
            self.client.close()

    @coroutine
    def send_events(self, events, frmt="RFC5424"):
        """
        send_events(events, frmt="RFC5424")
        Send multiple events to the syslog server

        :param events: events to send (iterable of :py:class:`Event`)
        """
        f = list(imap(lambda event: self.publish_event(event, frmt=frmt), events))
        results = yield f
        results = [status for status, _ in results]
        raise Return(all(results))

    @coroutine
    def publish_event(self, event, routing_key=None, frmt="RFC5424"):
        """
        publish_event(event, routing_key=None, frmt="RFC5424")
        Send a single event to the syslog server

        :param event: event to send
        :type event: Event
        """
        bytes_event = event.dump(frmt=frmt)
        syslog_line = str(len(bytes_event)) + ' ' + bytes_event
        try:
            yield self.stream.write(syslog_line)
        except StreamClosedError:
            raise Return((False, event))
        else:
            raise Return((True, event))

    @coroutine
    def send_message(self, message, source, severity, facility, app_name, frmt="RFC5424"):
        """
        send_message(message, source, severity, facility, app_name, frmt="RFC5424")
        Send a single message to the syslog server

        :param message: message to send
        :param source: event source
        :param severity: event severity
        :param facility: event facility
        :param app_name: event application name
        """
        message = message.strip('\r\n ')
        if not message:
            logger.info("Empty message was not sent")
            return
        event = Event(severity=severity, facility=facility, app_name=app_name, source=source,
                      timereported=arrow.utcnow(), message=message)
        res, _ = yield self.publish_event(event, frmt=frmt)
        raise Return(res)

    @coroutine
    def send_file(self, filename, source, severity, facility, app_name, frmt="RFC5424"):
        """
        send_file(filename, source, severity, facility, app_name, frmt="RFC5424")
        Send file to syslog server

        :param filename: file to send
        :param source: log source
        :param severity: log severity
        :param facility: log facility
        :param app_name: log application name
        """
        def _file_to_events_generator(stream):
            for line in stream:
                line = line.strip(' \r\n')
                if line:
                    event = Event(severity=severity, facility=facility, app_name=app_name, source=source,
                                  timereported=arrow.utcnow(), message=line)
                    yield event
        with open(filename, 'rb') as fhandle:
            res = yield self.send_events(_file_to_events_generator(fhandle), frmt=frmt)
        raise Return(res)
