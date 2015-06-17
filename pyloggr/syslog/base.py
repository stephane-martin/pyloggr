# encoding: utf-8

"""
Base class for syslog TCP and RELP clients
"""

from __future__ import absolute_import, division, print_function
__author__ = 'stef'

import ssl
import logging
import socket
from datetime import timedelta

# noinspection PyCompatibility
from concurrent.futures import ThreadPoolExecutor
from tornado.gen import coroutine, Return, with_timeout, TimeoutError
from tornado.tcpclient import TCPClient
from tornado.concurrent import Future
from tornado.ioloop import IOLoop
from toro import Event as ToroEvent
import certifi
import arrow
import lz4

from pyloggr.event import Event

security_logger = logging.getLogger('security')


class GenericClient(object):

    def __init__(self, server, port, use_ssl=False, verify_cert=True, hostname=None, ca_certs=None, client_key=None,
                 client_cert=None):
        self.server = server
        self.port = port
        self.use_ssl = use_ssl
        self._lz4_queue = []
        self._flush_lz4_queue = None
        self.stream = None
        self.hostname = hostname if hostname else server
        self.verify_cert = verify_cert
        self.ca_certs = ca_certs if ca_certs else certifi.where()
        self.client_key = client_key
        self.client_cert = client_cert
        self.client = None
        self.compress_thread = None
        self.closed_connection_event = None

    @coroutine
    def _do_flush_lz4_queue(self):
        self._flush_lz4_queue = None
        logger = logging.getLogger(__name__)
        logger.debug("LZ4 queue flush: %s events", len(self._lz4_queue))
        if len(self._lz4_queue) == 0:
            return
        queue, self._lz4_queue = self._lz4_queue, []
        events = [event for event, _ in queue]
        status, _ = yield self.send_events(events, compress=True)
        # notify callers via Future
        [future.set_result(status) for _, future in queue]

    @coroutine
    def _read_next_token(self):
        token = ''
        while True:
            token = yield self.stream.read_until_regex(r'\s')
            token = token.strip(' \r\n')
            if token:
                break
        raise Return(token)

    def set_tls(self):
        if self.use_ssl:
            ssl_options = {}
            if self.verify_cert:
                ssl_options['cert_reqs'] = ssl.CERT_REQUIRED,
                ssl_options['ca_certs'] = self.ca_certs
            else:
                ssl_options['cert_reqs'] = ssl.CERT_NONE
            if self.client_key is not None and self.client_cert is not None:
                ssl_options['certfile'] = self.client_cert
                ssl_options['keyfile'] = self.client_key
            try:
                self.stream = yield self.stream.start_tls(
                    server_side=False,
                    server_hostname=self.hostname if self.verify_cert else None,
                    ssl_options=ssl_options
                )
            except ssl.SSLError:
                security_logger.exception("Error happened when opening SSL connection")
                raise socket.error("Error happened when opening SSL connection")

    def _on_stream_closed(self):
        if self.closed_connection_event is not None:
            self.closed_connection_event.set()
        self._cleanup()

    @coroutine
    def start(self):
        logger = logging.getLogger(__name__)
        self.client = TCPClient()
        self.closed_connection_event = ToroEvent()
        try:
            connect_future = with_timeout(
                timeout=timedelta(seconds=10),
                future=self.client.connect(self.server, self.port)
            )
            self.stream = yield connect_future
        except socket.error:
            logger.error("RELP client could not connect (socket.error)")
            raise
        except TimeoutError:
            logger.error("RELP client could not connect (timeout)")
            raise

        self.set_tls()
        self.stream.set_close_callback(self._on_stream_closed)
        self.compress_thread = ThreadPoolExecutor(max_workers=1)
        raise Return(self.closed_connection_event)

    @coroutine
    def stop(self):
        self._cleanup()

    def _cleanup(self):
        if self.client:
            self.client.close()
        if self.compress_thread is not None:
            self.compress_thread.shutdown()
            self.compress_thread = None

    def pack_event(self, event):
        logger = logging.getLogger(__name__)
        logger.debug("RELP client: queueing event '%s' for LZ4", event.uid)
        # pack this event with previous ones in _lz4_queue
        future_publish_status = Future()
        self._lz4_queue.append((event, future_publish_status))
        if len(self._lz4_queue) >= 10000:
            # send the _lz4_queue
            if self._flush_lz4_queue is not None:
                IOLoop.current().remove_handler(self._flush_lz4_queue)
                self._flush_lz4_queue = None
            IOLoop.current().add_callback(self._do_flush_lz4_queue)
        if self._flush_lz4_queue is None:
            self._flush_lz4_queue = IOLoop.current().add_timeout(timedelta(seconds=30), self._do_flush_lz4_queue)
        return future_publish_status

    @coroutine
    def send_message(self, message, source, severity, facility, app_name, frmt="RFC5424"):
        """
        send_message(message, source, severity, facility, app_name, frmt="RFC5424")
        Send a single message to the RELP server

        :param message: message to send
        :param source: event source
        :param severity: event severity
        :param facility: event facility
        :param app_name: event application name
        :param frmt: event dumping format
        """
        logger = logging.getLogger(__name__)
        if self.closed_connection_event.is_set():
            raise Return(False)
        if not message:
            logger.info("Empty message was not sent")
            raise Return(True)

        message = message.strip('\r\n ')
        event = Event(severity=severity, facility=facility, app_name=app_name, source=source,
                      timereported=arrow.utcnow(), message=message)
        status, _ = yield self.publish_event(event, frmt=frmt)
        raise Return(status)

    @coroutine
    def send_file(self, filename, source, severity, facility, app_name, frmt="RFC5424"):
        """
        send_file(filename, source, severity, facility, app_name, frmt="RFC5424")
        Send file to RELP server

        :param filename: file to send
        :param source: log source
        :param severity: log severity
        :param facility: log facility
        :param app_name: log application name
        :param frmt: event dumping format
        """
        if self.closed_connection_event.is_set():
            raise Return((False, None))

        def _file_to_events_generator(stream):
            for line in stream:
                line = line.strip(' \r\n')
                if line:
                    yield Event(severity=severity, facility=facility, app_name=app_name, source=source,
                                timereported=arrow.utcnow(), message=line)

        with open(filename, 'rb') as fhandle:
            status, acks = yield self.send_events(_file_to_events_generator(fhandle), frmt=frmt)
        raise Return((status, acks))

    # noinspection PyUnusedLocal
    @coroutine
    def publish_event(self, event, routing_key=None, frmt="RFC5424", compress=False):
        """
        publish_event(event, routing_key=None, frmt="RFC5424")
        Send one event to the RELP server

        :param event: event
        :param routing_key: not used, just here for method signature
        :param frmt: event dumping format
        :param compress: should the RELP client pack messages and compress them
        :type event: Event
        :type routing_key: str
        :type frmt: str
        :type compress: bool
        """
        logger = logging.getLogger(__name__)
        if self.closed_connection_event.is_set():
            raise Return((False, event))
        if compress and frmt != "RFC5424":
            raise ValueError("With compress=True, frmt must be RFC5424")
        if compress:
            future_publish_status = self.pack_event(event)
            status = yield future_publish_status
            raise Return((status, event))
        else:
            logger.debug("RELP client: sending event '%s' without compression", event.uid)
            # just sent the single event
            status, acks = yield self.send_events([event], frmt=frmt, compress=False)
            if not status:
                raise Return((False, event))
            else:
                raise Return((acks[0], event))

    @coroutine
    def send_events(self, events, frmt="RFC5424", compress=False):
        raise NotImplementedError

    @staticmethod
    def _compress(uncompressed_buf):
        return lz4.compress(uncompressed_buf)
