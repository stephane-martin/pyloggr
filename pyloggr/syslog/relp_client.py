# encoding: utf-8

"""
RELP client
"""

__author__ = 'stef'

from io import open
import socket
import logging
import arrow
from tornado.gen import coroutine, Return, sleep
from tornado.tcpclient import TCPClient
from tornado.ioloop import IOLoop
from tornado.iostream import StreamClosedError
from toro import Event as ToroEvent
from pyloggr.utils.constants import RELP_OPEN_COMMAND, RELP_CLOSE_COMMAND
from pyloggr.event import Event


logger = logging.getLogger(__name__)


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
    """

    def __init__(self, server, port):
        self.server = server
        self.port = port
        self.stream = None
        self.closed_connection_event = ToroEvent()
        self.client = TCPClient()
        self.current_relp_id = 1

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
    def _read_one_response(self):
        response_id = yield self._read_next_token()
        if response_id == "0":
            raise ServerClose()
            pass
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
        raise Return((response_id, code, cmddata))

    @coroutine
    def connect(self):
        """
        connect()
        Connect to the RELP server and send 'open' command

        :raises socket.error: if TCP connection fails


        Note
        ====
        Tornado coroutine
        """
        try:
            self.stream = yield self.client.connect(self.server, self.port)
        except socket.error:
            logger.error("RELP client could not connect (socket.error)")
            raise

        yield self.stream.write(str(self.current_relp_id) + " " + RELP_OPEN_COMMAND)
        try:
            response_id, code, data = yield self._read_one_response()
        except ServerClose:
            logger.error("RELP server refused the 'open' command and sent 'serverclose'")
            raise

        if code != 200:
            logger.error("RELP server sent a BOO after the 'open' command")
            raise ServerBoo(data)

        # everything OK, increment the counter
        self.current_relp_id += 1

    @coroutine
    def disconnect(self):
        """
        Disconnect from the RELP server
        """
        if self.stream:
            try:
                yield self.stream.write(str(self.current_relp_id) + " " + RELP_CLOSE_COMMAND)
            except StreamClosedError:
                pass
            else:
                #yield self._read_one_response()
                self.stream.close()
        if self.client:
            self.client.close()

    @coroutine
    def send_events(self, events):
        """
        send_events(events)
        Send multiple events to the RELP server

        :param events: events to send (iterable of :py:class:`Event`)
        """

        sent_messages_id = []
        ack_messages_id = []
        nack_messages_id = []
        got_all_ack = ToroEvent()
        unexpected_close = ToroEvent()

        @coroutine
        def _read_streaming_responses():
            try:
                while not got_all_ack.is_set():
                    try:
                        response_id, code, data = yield self._read_one_response()
                    except ServerClose:
                        logger.error("_read_streaming_responses: server announced unexpected close")
                        unexpected_close.set()
                        return
                    if code == 200 and response_id in sent_messages_id:
                        ack_messages_id.append(response_id)
                    elif code == 500 and response_id in sent_messages_id:
                        nack_messages_id.append(response_id)
                    else:
                        logger.warning("What ?!")
                        # todo: appropriate action
            except StreamClosedError:
                logger.error("_read_streaming_responses: stream closed error")
                pass

        # receive the responses in background
        IOLoop.current().add_callback(_read_streaming_responses)
        # send the events
        for event in events:
            bytes_event = event.dump_rfc5424()
            line = str(len(bytes_event)) + ' ' + bytes_event
            relp_line = str(self.current_relp_id) + " " + "syslog " + line + "\n"
            yield self.stream.write(relp_line)
            sent_messages_id.append(self.current_relp_id)
            self.current_relp_id += 1

        # wait that until we have all responses
        while (not got_all_ack.is_set()) and (not unexpected_close.set()):
            if len(sent_messages_id) == (len(ack_messages_id) + len(nack_messages_id)):
                got_all_ack.set()
            yield sleep(1)
        if unexpected_close.set():
            raise ServerClose()
        raise Return(nack_messages_id)

    @coroutine
    def send_event(self, event):
        """
        send_event(event)
        Send a single event to the RELP server

        :param event: event to send
        :type event: Event
        """
        nack_messages_id = yield self.send_events([event])
        raise Return(len(nack_messages_id) == 0)

    @coroutine
    def send_message(self, message, source, severity, facility, app_name):
        """
        send_message(message, source, severity, facility, app_name)
        Send a single message to the RELP server

        :param message: message to send
        :param source: event source
        :param severity: event severity
        :param facility: event facility
        :param app_name: event application name
        """
        event = Event(severity=severity, facility=facility, app_name=app_name, source=source, timereported=arrow.utcnow(),
                      message=message)
        status = yield self.send_event(event)
        raise Return(status)

    @coroutine
    def send_file(self, filename, source, severity, facility, app_name):
        """
        sendfile(filename, source, severity, facility, app_name)
        Send file to RELP server

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
            nack_messages_id = yield self.send_events(_file_to_events_generator(fhandle))
        raise Return(nack_messages_id)

