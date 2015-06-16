# encoding: utf-8

"""
RELP client
"""

from __future__ import absolute_import, division, print_function
__author__ = 'stef'

import logging
from tornado.gen import coroutine, Return
from tornado.ioloop import IOLoop
from tornado.iostream import StreamClosedError
from toro import Event as ToroEvent
from future.utils import raise_from

from pyloggr.utils.constants import RELP_OPEN_COMMAND, RELP_CLOSE_COMMAND
from pyloggr.utils import sleep
from .base import GenericClient


security_logger = logging.getLogger("security")


class RELPException(Exception):
    pass


class ServerClose(RELPException):
    pass


class ServerBoo(RELPException):
    pass


class RELPClient(GenericClient):
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

        super(RELPClient, self).__init__(server, port, use_ssl, verify_cert, hostname, ca_certs, client_key,
                                         client_cert)
        self.current_relp_id = 1
        self.server_deadline = server_deadline
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
        yield super(RELPClient, self).start()
        self.current_relp_id = 1
        self._say_hello()

        self.current_relp_id += 1
        self.acks = {}
        self.unexpected_close = ToroEvent()
        # receive the responses in background
        IOLoop.current().add_callback(self._read_streaming_responses)
        raise Return(self.closed_connection_event)

    def _say_hello(self):
        logger = logging.getLogger(__name__)
        try:
            yield self.stream.write(str(self.current_relp_id) + " " + RELP_OPEN_COMMAND)
            response_id, code, data = yield self._read_one_response()
            if code != 200:
                logger.error("RELP server sent a BOO after the 'open' command")
                raise ServerBoo(data)
        except (ServerClose, StreamClosedError, ServerBoo) as ex:
            logger.error("RELP opening connection failed")
            raise_from(ServerClose("RELP opening connection failed"), ex)

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
        yield super(RELPClient, self).stop()

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
            raise Return((False, len(events) * [False]))

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

            bytes_events = (event.dump(frmt=frmt) + "\n" for event in events)
            lines = (str(len(bytes_event)) + ' ' + bytes_event for bytes_event in bytes_events)
            relp_lines = (current_relp_id_str + " syslog " + line + "\n" for line in lines)
            uncompressed = "".join(relp_lines)
            # compress the buf with LZ4
            compressed = yield self.compress_thread.submit(self._compress, uncompressed)
            ratio = int(100 - (100 * len(compressed) // len(uncompressed)))
            logger.debug("RELP client: LZ4 compression ratio: %s", ratio)
            relp_line = current_relp_id_str + " lz4 " + str(len(compressed)) + ' ' + compressed  + "\n"

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
