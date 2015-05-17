# encoding: utf-8

"""
TCP syslog client
"""

__author__ = 'stef'

import socket
import logging
import arrow

from pyloggr.event import Event

logger = logging.getLogger(__name__)


class SyslogClient(object):
    """
    Utility class to send messages or whole files to a syslog server, using TCP

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
        self.sock = None

    def connect(self):
        """
        Connect to the syslog server
        """
        try:
            self.sock = socket.create_connection((self.server, self.port), timeout=10)
        except socket.error:
            logger.error("SyslogClient: Connection error")
            raise

    def disconnect(self):
        """
        Disconnect from the syslog server
        """
        if self.sock:
            self.sock.close()
            self.sock = None

    def send_events(self, events):
        """
        Send multiple events to the syslog server

        :param events: events to send (iterable of :py:class:`Event`)
        """
        for event in events:
            self.send_event(event)

    def send_event(self, event):
        """
        Send a single event to the syslog server

        :param event: event to send
        :type event: Event
        """
        bytes_event = event.dump_rfc5424()
        syslog_line = str(len(bytes_event)) + ' ' + bytes_event
        self.sock.sendall(syslog_line)

    def send_message(self, message, source, severity, facility, app_name):
        """
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
        event = Event(severity=severity, facility=facility, app_name=app_name, source=source, timereported=arrow.utcnow(),
                      message=message)
        self.send_event(event)

    def send_file(self, filename, source, severity, facility, app_name):
        """
        sendfile(filename, source, severity, facility, app_name)
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
            self.send_events(_file_to_events_generator(fhandle))
