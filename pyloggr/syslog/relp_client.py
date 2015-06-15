# encoding: utf-8

"""
RELP client
"""

from __future__ import absolute_import, division, print_function
__author__ = 'stef'

from io import open
import socket
import logging
from datetime import timedelta
import ssl

import arrow
import lz4
import certifi
# noinspection PyCompatibility
from concurrent.futures import ThreadPoolExecutor
from tornado.gen import coroutine, Return, with_timeout, TimeoutError
from tornado.tcpclient import TCPClient
from tornado.concurrent import Future
from tornado.ioloop import IOLoop
from tornado.iostream import StreamClosedError
from toro import Event as ToroEvent
from future.utils import raise_from

from pyloggr.utils.constants import RELP_OPEN_COMMAND, RELP_CLOSE_COMMAND
from pyloggr.utils import sleep
from pyloggr.event import Event


security_logger = logging.getLogger("security")


def _compress(uncompressed_buf):
    return lz4.compress(uncompressed_buf)


class RELPException(Exception):
    pass


class ServerClose(RELPException):
    pass


class ServerBoo(RELPException):
    pass


class RELPClient(object):
    """
    Utility class to send messages or whole files to a RELP server, using an asynchrone TCP client

    Parameters
    ==========
    server: str
        RELP server hostname or IP
    port: int
        RELP server port
    use_ssl: bool
        Should the client connect with SSL
    """

    def __init__(self, server, port, use_ssl=False, verify_cert=True, hostname=None, ca_certs=None,
                 client_key=None, client_cert=None, server_deadline=120):
        self.server = server
        self.port = port
        self.stream = None
        self.closed_connection_event = None
        self.client = None
        self.current_relp_id = 1
        self.use_ssl = use_ssl
        self.hostname = hostname if hostname else server
        self.verify_cert = verify_cert
        self.ca_certs = ca_certs if ca_certs else certifi.where()
        self.client_key = client_key
        self.client_cert = client_cert
        self.server_deadline = server_deadline
        self._lz4_queue = []
        self._flush_lz4_queue = None
        self.compress_thread = None
        self.acks = None
        self.unexpected_close = None

    @coroutine
    def start(self):
        """
        start()
        Connect to the RELP server and send 'open' command

        :raises socket.error: if TCP connection fails
        Note
        ====
        Tornado coroutine
        """
        self.client = TCPClient()
        self.closed_connection_event = ToroEvent()
        self.current_relp_id = 1
        logger = logging.getLogger(__name__)
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

        self.stream.set_close_callback(self._on_stream_closed)

        try:
            yield self.stream.write(str(self.current_relp_id) + " " + RELP_OPEN_COMMAND)
            response_id, code, data = yield self._read_one_response()
            if code != 200:
                logger.error("RELP server sent a BOO after the 'open' command")
                raise ServerBoo(data)
        except (ServerClose, StreamClosedError, ServerBoo) as ex:
            logger.error("RELP opening connection failed")
            raise_from(ServerClose("RELP opening connection failed"), ex)

        self.compress_thread = ThreadPoolExecutor(max_workers=1)

        # everything OK, increment the counter
        self.current_relp_id += 1

        self.acks = {}
        self.unexpected_close = ToroEvent()
        # receive the responses in background
        IOLoop.current().add_callback(self._read_streaming_responses)

        raise Return(self.closed_connection_event)

    def _on_stream_closed(self):
        self.closed_connection_event.set()
        self._cleanup()

    @coroutine
    def _read_streaming_responses(self):
        logger = logging.getLogger(__name__)
        while True:
            try:
                response_id, code, data = yield self._read_one_response()
            except ServerClose:
                logger.error("_read_streaming_responses: server announced unexpected close")
                self.stream.close()
                self.unexpected_close.set()
                return
            except StreamClosedError:
                logger.error("_read_streaming_responses: stream closed error")
                self.unexpected_close.set()
                return
            except ServerBoo:
                logger.error("_read_streaming_responses: did not understand response")
                self.stream.close()
                self.unexpected_close.set()
                return
            if code == 200:
                logger.debug("RELP client: remote RELP server ACKed one message")
            self.acks[response_id] = True if code == 200 else False

    @coroutine
    def _read_one_response(self):
        response_id = yield self._read_next_token()
        if response_id == "0":
            raise ServerClose("RELP server announced a serverclose")
        try:
            response_id = int(response_id)
            yield self._read_next_token()              # rsp
            length = yield self._read_next_token()
            length = int(length)
            if length > 0:
                data = yield self.stream.read_bytes(length)
                data = data.strip('\r\n ').split(None, 1)
                code = int(data[0])
                cmddata = ''
                if len(data) > 0:
                    cmddata = data[1].strip('\r\n ')
            else:
                code = None
                cmddata = ''
        except (ValueError, TypeError) as ex:
            raise_from(ServerBoo("did not understand relp server response"), ex)
            return
        raise Return((response_id, code, cmddata))

    @coroutine
    def _read_next_token(self):
        token = ''
        while True:
            token = yield self.stream.read_until_regex(r'\s')
            token = token.strip(' \r\n')
            if token:
                break
        raise Return(token)

    @coroutine
    def stop(self):
        """
        stop()
        Disconnect from the RELP server
        """
        if not self.stream.closed():
            try:
                yield self.stream.write(str(self.current_relp_id) + " " + RELP_CLOSE_COMMAND)
            except StreamClosedError:
                pass
            else:
                # yield self._read_one_response()
                self.stream.close()
        self._cleanup()

    def _cleanup(self):
        if self.client:
            self.client.close()
        if self.compress_thread is not None:
            self.compress_thread.shutdown()
            self.compress_thread = None

    @coroutine
    def send_events(self, events, frmt="RFC5424", compress=False):
        """
        send_events(events, frmt="RFC5424")
        Send multiple events to the RELP server

        :param events: events to send (iterable of :py:class:`Event`)
        :param frmt: event dumping format
        :param compress: if True, send the events as one LZ4-compressed line
        :type events: iterable of Event
        :type frmt: str
        :type compress: bool
        """

        if self.closed_connection_event.is_set():
            raise Return((False, None))

        n_start = self.current_relp_id
        logger = logging.getLogger(__name__)
        relp_ids = set()
        nb_total_events = 0

        if compress:
            # same relp ID for every line
            current_relp_id_str = str(self.current_relp_id)
            relp_ids.add(self.current_relp_id)
            # increment current_relp_if *before* any yield !
            self.current_relp_id += 1
            nb_total_events = 1

            bytes_events = (event.dump(frmt=frmt) for event in events)
            lines = (str(len(bytes_event)) + ' ' + bytes_event for bytes_event in bytes_events)
            relp_lines = (current_relp_id_str + " syslog " + line + "\n" for line in lines)
            uncompressed_buf = "".join(relp_lines)
            # compress the buf with LZ4
            compressed_buf = yield self.compress_thread.submit(_compress, uncompressed_buf)
            ratio = int(100 - (100 * len(compressed_buf) // len(uncompressed_buf)))
            logger.debug("RELP client: LZ4 compression ratio: %s", ratio)
            relp_line = current_relp_id_str + " lz4 " + str(len(compressed_buf)) + ' ' + compressed_buf  + "\n"

            try:
                yield self.stream.write(relp_line)
            except StreamClosedError:
                logger.info("Relp client sending events: Stream closed error ?!")
                self.unexpected_close.set()
        else:
            # send the events separately, without compression
            relp_lines = []
            # there is no "yield" in the for loop, so the self.current_relp_id can be incremented without
            # any race condition (at the cost of consumed RAM)
            for event in events:
                bytes_event = event.dump(frmt=frmt)
                line = str(len(bytes_event)) + ' ' + bytes_event
                relp_lines.append(str(self.current_relp_id) + " " + "syslog " + line + "\n")
                relp_ids.add(self.current_relp_id)
                self.current_relp_id += 1
                nb_total_events += 1
            try:
                yield self.stream.write("".join(relp_lines))
            except StreamClosedError:
                logger.info("Relp client sending events: Stream closed error ?!")
                self.unexpected_close.set()

        # wait that until we have all answers
        elapsed = 0
        while (not relp_ids.issubset(set(self.acks.keys()))) and (not self.unexpected_close.is_set()):
            yield sleep(1, wake_event=self.unexpected_close)
            elapsed += 1
            if elapsed >= self.server_deadline:
                logger.warning("RELP client: the server did not sent all answers before deadline. Giving up.")
                self.stream.close()
                self.unexpected_close.set()

        if compress:
            if n_start in self.acks:
                status = [self.acks[n_start] and not self.unexpected_close.is_set()]
                acks = nb_total_events * status
                raise Return((status, acks))
            else:
                raise Return((False, nb_total_events * [False]))
        else:
            # return all the ACKs that we've got
            acks = [self.acks.get(relp_id, False) for relp_id in sorted(relp_ids)]
            raise Return((not self.unexpected_close.is_set(), acks))

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
            logger.debug("RELP client: queueing event '%s' for LZ4", event.uid)
            # pack this event with previous ones in _lz4_queue
            future_publish_status = Future()
            self._lz4_queue.append((event, future_publish_status))
            if len(self._lz4_queue) >= 10000:
                # send the _lz4_queue
                if self._flush_lz4_queue is not None:
                    IOLoop.current().remove_handler(self._flush_lz4_queue)
                    self._flush_lz4_queue = None
                yield self._do_flush_lz4_queue()
            if self._flush_lz4_queue is None:
                self._flush_lz4_queue = IOLoop.current().add_timeout(timedelta(seconds=30), self._do_flush_lz4_queue)
            # wait that our event is actually published
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
        if self.closed_connection_event.is_set():
            raise Return(False)

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
