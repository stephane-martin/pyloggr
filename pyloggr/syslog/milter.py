# encoding: utf-8

"""
Milter example with tornado framework (http://www.tornadoweb.org/)
"""

from __future__ import absolute_import, division, print_function
__author__ = 'Stephane Martin'

import struct
from io import BytesIO
import logging
import email
from email.header import Header, decode_header, make_header

# noinspection PyUnresolvedReferences,PyCompatibility
from concurrent.futures import ThreadPoolExecutor
from tornado.gen import coroutine
from tornado.iostream import StreamClosedError
from tornado.ioloop import IOLoop

from pyloggr.event import Event
from pyloggr.utils.milter_base import MilterDispatcher, MilterBase, MilterCloseConnection

MILTER_LEN_BYTES = 4  # from sendmail's include/libmilter/mfdef.h
MAX_SIZE = 40 * 1024 * 1024


def parse_email(headers, body, event, queue):
    # rebuild the full email message

    mail_as_string = headers + b"\n" + body
    try:
        mail = email.message_from_string(mail_as_string)
    except Exception:
        logging.getLogger(__name__).exception("Error while parsing email")
    else:
        for part in mail.walk():
            filename = part.get_filename("")
            if filename:
                filename = unicode(make_header(decode_header(filename)))
                content_type = part.get_content_type()
                event['attachments'].add(content_type + ' ' + filename)
    finally:
        del body, headers, mail_as_string
    queue.put_nowait(event.dump_json())


def after_body_parsed(f):
    try:
        f.result()
    except Exception:
        logging.getLogger(__name__).exception("Milter: exception happened when parsing mail")


class MilterClient(object):
    """
    MilterClient represents a milter client to take care about
    """
    def __init__(self, stream, address, milter_class, context):
        self.stream = stream
        self.address = address
        self.client_host = None
        self.milter_dispatcher = MilterDispatcher(milter_class, context)

    @coroutine
    def on_connect(self):
        try:
            server_sockname = self.stream.socket.getsockname()
            if len(server_sockname) == 2:
                (self.client_host, client_port) = self.stream.socket.getpeername()                      # ipv4
            elif len(server_sockname) == 4:
                (self.client_host, client_port, flowinfo, scopeid) = self.stream.socket.getpeername()   # ipv6
            else:
                return
        except StreamClosedError:
            # The client went away before it we could do anything with it
            self.disconnect_milter()
            return

        yield self.handle_milter()

    @coroutine
    def handle_milter(self):
        logger = logging.getLogger(__name__)
        try:
            while True:
                packetlen = yield self.stream.read_bytes(MILTER_LEN_BYTES)
                try:
                    packetlen = int(struct.unpack('!I', packetlen)[0])
                except ValueError:
                    self.disconnect_milter()
                    return
                if packetlen > MAX_SIZE:
                    logger.warning('MilterClient: packetlen is too big: abort')
                    self.disconnect_milter()
                    return
                data = yield self.stream.read_bytes(packetlen)
                try:
                    response = self.milter_dispatcher.dispatch(data)
                    if isinstance(response, list):
                        for r in response:
                            yield self.send_milter_response(r)
                    elif response:
                        # response can be None, if no response is expected by postfix (macro)
                        yield self.send_milter_response(response)
                except StreamClosedError:
                    raise
                except MilterCloseConnection:
                    logger.debug("MilterCloseConnection")
                    self.disconnect_milter()
                    raise StreamClosedError
                except:
                    logger.exception("Milter: Something bad happened")
                    self.disconnect_milter()
                    raise StreamClosedError

        except StreamClosedError:
            logger.info("StreamClosedError")
            self.disconnect_milter()

    @coroutine
    def send_milter_response(self, response):
        length = struct.pack('!I', len(response))
        yield self.stream.write(length + response)

    def disconnect_milter(self):
        self.stream.close()
        self.milter_dispatcher.milter_obj.close()


class Milter2Syslog(MilterBase):
    """
    Receives mail notifications with milter protocol, and stores them in a queue
    """

    def __init__(self, context=None):
        super(Milter2Syslog, self).__init__()
        self.queue, self.client_host = context
        self.mutations = []
        self.event = None
        self.body = None
        self.hostname = None
        self.address = None
        self.helo_hostname = None
        self.logger = logging.getLogger(__name__)
        self.exe = ThreadPoolExecutor(max_workers=10)
        self.headers = ''

    def init_event(self):
        if self.event is None:
            self.event = Event(facility="mail", app_name="pyloggr.milter", source=self.client_host)
        if self.hostname is not None:
            self.event['milter.connect_from'] = self.hostname
        if self.address is not None:
            self.event['milter.connect_from_ip'] = self.address
        if self.helo_hostname is not None:
            self.event['milter.helo_hostname'] = self.helo_hostname

        if self.body is None:
            self.body = BytesIO()

    def on_connect(self, cmd, hostname, family, port, address):
        self.logger.debug("on connect")
        self.hostname = hostname
        self.address = address
        return self.response_continue()

    def on_helo(self, cmd, helo_hostname):
        self.helo_hostname = helo_hostname
        return self.response_continue()

    def on_mail_from(self, cmd, mailfrom, esmtp_info):
        self.init_event()
        self.event['milter.mail_from'] = mailfrom
        return self.response_continue()

    def on_rcpt_to(self, cmd, rcptto, esmtp_info):
        self.init_event()
        self.event['milter.rcpt_to'].add(rcptto)
        return self.response_continue()

    def on_header(self, cmd, key, val):
        self.init_event()
        header = make_header(decode_header(val))
        unicode_header = unicode(header)
        self.headers += key + ": " + str(header) + "\n"
        key = key.lower().replace('-', '_').strip(" \r\n")
        self.event['milter.header.' + key].add(unicode_header)
        if key == "subject":
            self.event.message = unicode_header
        return self.response_continue()

    def on_end_headers(self, cmd):
        self.logger.debug("end header")
        return self.response_continue()

    def on_body(self, cmd, data):
        self.init_event()
        self.body.write(data)
        return self.response_continue()

    def on_reset_state(self):
        self.mutations = list()
        self.event = None
        self.headers = ''
        if self.body is not None:
            if not self.body.closed:
                self.body.close()
            self.body = None

    def on_end_body(self, cmd):
        if self.body is not None and self.event is not None:
            if not self.body.closed:
                body = self.body.getvalue()
                self.body.close()
                if len(body) > 0:
                    f = self.exe.submit(parse_email, self.headers, body, self.event, self.queue)
                    IOLoop.current().add_future(f, after_body_parsed)

        tmp_mutations, self.mutations = self.mutations, []
        return self.return_on_end_body_actions(tmp_mutations)

    def close(self):
        self.on_reset_state()
        self.exe.shutdown(wait=False)
