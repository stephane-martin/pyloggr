# encoding: utf-8

"""
TCP syslog client
"""

__author__ = 'stef'

import logging
from io import BytesIO
from tornado.gen import coroutine, Return
from tornado.iostream import StreamClosedError
from .base import GenericClient

logger = logging.getLogger(__name__)
security_logger = logging.getLogger("security")


class SyslogClient(GenericClient):
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

    def __init__(self, server, port, use_ssl=False, verify_cert=True, hostname=None, ca_certs=None,
                 client_key=None, client_cert=None, server_deadline=None):
        super(SyslogClient, self).__init__(server, port, use_ssl, verify_cert, hostname, ca_certs, client_key,
                                           client_cert)

    @coroutine
    def start(self):
        """
        start()
        Connect to the syslog server
        """
        yield super(SyslogClient, self).start()
        raise Return(self.closed_connection_event)

    @coroutine
    def stop(self):
        """
        stop()
        Disconnect from the syslog server
        """
        if not self.stream.closed():
            self.stream.close()
        yield super(SyslogClient, self).stop()

    @coroutine
    def send_events(self, events, frmt="RFC5424", compress=False):
        """
        send_events(events, frmt="RFC5424")
        Send multiple events to the syslog server

        :param events: events to send (iterable of :py:class:`Event`)
        :param frmt: event dumping format
        """

        if self.closed_connection_event.is_set():
            raise Return((False, len(events) * [False]))

        buf = BytesIO()
        nb_events = 0
        for event in events:
            bytes_event = event.dump(frmt=frmt) + "\n"
            syslog_line = str(len(bytes_event)) + ' ' + bytes_event
            buf.write(syslog_line)
            nb_events += 1

        if compress:
            s = yield self.compress_thread.submit(self._compress, buf.getvalue())
            s = str(len(s)) + ' ' + s
        else:
            s = buf.getvalue()

        try:
            if nb_events > 0:
                try:
                    yield self.stream.write(s)
                except StreamClosedError:
                    raise Return((False, nb_events * [False]))
                else:
                    raise Return((True, nb_events * [True]))
            else:
                raise Return((True, []))
        finally:
            buf.close()
