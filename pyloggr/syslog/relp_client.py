# encoding: utf-8

"""
RELP client
"""

__author__ = 'stef'

from io import open
import socket
import logging
from datetime import timedelta
import ssl

import arrow
import certifi
from tornado.gen import coroutine, Return, sleep, with_timeout, TimeoutError
from tornado.tcpclient import TCPClient
from tornado.ioloop import IOLoop
from tornado.iostream import StreamClosedError
from toro import Event as ToroEvent
from future.utils import raise_from

from pyloggr.utils.constants import RELP_OPEN_COMMAND, RELP_CLOSE_COMMAND
from pyloggr.event import Event


logger = logging.getLogger(__name__)
security_logger = logging.getLogger("security")


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
                 client_key=None, client_cert=None):
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

        # everything OK, increment the counter
        self.current_relp_id += 1
        raise Return(self.closed_connection_event)

    def _on_stream_closed(self):
        self.closed_connection_event.set()
        if self.client:
            self.client.close()

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
        if self.client:
            self.client.close()

    @coroutine
    def send_events(self, events, frmt="RFC5424"):
        """
        send_events(events, frmt="RFC5424")
        Send multiple events to the RELP server

        :param events: events to send (iterable of :py:class:`Event`)
        :param frmt: event dumping format
        """

        acks = {}
        unexpected_close = ToroEvent()
        n_start = self.current_relp_id
        got_all_answers = ToroEvent()

        @coroutine
        def _read_streaming_responses():
            while not got_all_answers.is_set():
                try:
                    response_id, code, data = yield self._read_one_response()
                except ServerClose:
                    logger.error("_read_streaming_responses: server announced unexpected close")
                    self.stream.close()
                    unexpected_close.set()
                    return
                except StreamClosedError:
                    logger.error("_read_streaming_responses: stream closed error")
                    unexpected_close.set()
                    return
                except ServerBoo:
                    logger.error("_read_streaming_responses: did not understand response")
                    self.stream.close()
                    unexpected_close.set()
                    return
                acks[response_id - n_start] = True if code == 200 else False

        # receive the responses in background
        IOLoop.current().add_callback(_read_streaming_responses)

        # send the events
        try:
            for event in events:
                bytes_event = event.dump(frmt=frmt)
                line = str(len(bytes_event)) + ' ' + bytes_event
                relp_line = str(self.current_relp_id) + " " + "syslog " + line + "\n"
                yield self.stream.write(relp_line)
                self.current_relp_id += 1
        except StreamClosedError:
            unexpected_close.set()

        nb_total_events = self.current_relp_id - n_start

        # wait that until we have all answers
        while (len(acks) != nb_total_events) and (not unexpected_close.set()):
            yield sleep(1)
        if len(acks) == nb_total_events:
            got_all_answers.set()
        raise Return((not unexpected_close.is_set(), acks))

    # noinspection PyUnusedLocal
    @coroutine
    def publish_event(self, event, routing_key=None, frmt="RFC5424"):
        """
        publish_event(event, routing_key=None, frmt="RFC5424")
        Send a single event to the RELP server

        :param event: event to send
        :param routing_key: not used, just here for signature
        :param frmt: event dumping format
        :type event: Event
        """
        status, _ = yield self.send_events([event], frmt=frmt)
        raise Return((status, event))

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

        def _file_to_events_generator(stream):
            for line in stream:
                line = line.strip(' \r\n')
                if line:
                    yield Event(severity=severity, facility=facility, app_name=app_name, source=source,
                                timereported=arrow.utcnow(), message=line)

        with open(filename, 'rb') as fhandle:
            status, acks = yield self.send_events(_file_to_events_generator(fhandle), frmt=frmt)
        raise Return((status, acks))
