# encoding: utf-8

"""
This module provides stuff to implement a the main syslog/RELP server with Tornado
"""

__author__ = 'stef'

import logging
import ssl
import socket
import errno
from itertools import chain
from os.path import expanduser
import uuid
import threading

# noinspection PyCompatibility,PyPackageRequirements
from past.builtins import basestring as basestr
from tornado.iostream import SSLIOStream, StreamClosedError
from tornado.tcpserver import TCPServer
from tornado.gen import coroutine, Return
from tornado.netutil import ssl_wrap_socket, errno_from_exception, bind_sockets, bind_unix_socket

logger = logging.getLogger(__name__)
security_logger = logging.getLogger('security')


class BaseSyslogServer(TCPServer):

    def __init__(self, syslog_parameters):
        """
        :type syslog_parameters: SyslogParameters
        """
        TCPServer.__init__(self)

        self.syslog_parameters = syslog_parameters
        self.listening = False
        self.shutting_down = False
        self.list_of_clients = []

    @coroutine
    def launch(self):
        """
        Starts the server

        - First we try to connect to RabbitMQ
        - If successfull, we start to listen for syslog clients

        Note
        ====
        Tornado coroutine

        """
        self.listening = False
        yield self._start_syslog()

    @coroutine
    def _start_syslog(self):
        """
        _start_syslog()
        Start to listen for syslog clients
        """
        if not self.listening:
            self.listening = True
            self.add_sockets(self.syslog_parameters.list_of_sockets)

    @coroutine
    def handle_stream(self, stream, address):
        """
        handle_stream(stream, address)
        Called by tornado when we have a new client.

        :param stream: IOStream for the new connection
        :type stream: `IOStream`
        :param address: tuple (client IP, client source port)
        :type address: tuple

        Note
        ====
        Tornado coroutine
        """
        connection = BaseSyslogClientConnection(
            stream=stream,
            address=address,
            syslog_parameters=self.syslog_parameters
        )
        self.list_of_clients.append(connection)
        yield connection.on_connect()
        self.list_of_clients.remove(connection)

    def _stop_listen_sockets(self):
        for fd, sock in self._sockets.items():
            self.io_loop.remove_handler(fd)

    @coroutine
    def _stop_syslog(self):
        """
        _stop_syslog()
        Stop listening for syslog connections

        Note
        ====
        Tornado coroutine
        """
        if self.listening:
            self.listening = False
            logger.info("Closing RELP clients connections")
            for client in self.list_of_clients:
                client.disconnect()
            logger.info("Stopping the RELP server")
            # instead of calling self.stop(): we don't want to close the sockets
            self._stop_listen_sockets()

    @coroutine
    def stop_all(self):
        """
        stop_all()
        Stops completely the server. Stop listening for syslog clients. Close connection to RabbitMQ.

        Note
        ====
        Tornado coroutine
        """

        yield self._stop_syslog()

    @coroutine
    def shutdown(self):
        """
        Authoritarian shutdown
        """
        self.shutting_down = True
        yield self.stop_all()
        self.stop()

    def _handle_connection(self, connection, address):
        """
        Inherits _handle_connection from parent TCPServer to manage SSL connections. Called by
        Tornado when a client connects.
        """

        port = connection.getsockname()[1]
        if port not in self.syslog_parameters.ssl_ports:
            return super(BaseSyslogServer, self)._handle_connection(connection, address)

        try:
            connection = ssl_wrap_socket(
                connection,
                self.syslog_parameters.port_to_ssl_config[port],
                server_side=True,
                do_handshake_on_connect=False
            )
        except ssl.SSLError as err:
            if err.args[0] == ssl.SSL_ERROR_EOF:
                return connection.close()
            else:
                raise
        except socket.error as err:
            if errno_from_exception(err) in (errno.ECONNABORTED, errno.EINVAL):
                return connection.close()
            else:
                raise
        except IOError:
            logger.error("IOError happened when client connected with TLS. Check the SSL configuration")
            return connection.close()

        try:
            stream = SSLIOStream(
                connection,
                io_loop=self.io_loop,
                max_buffer_size=self.max_buffer_size,
                read_chunk_size=self.read_chunk_size
            )
            self.handle_stream(stream, address)
        except Exception:
            logger.error("Error in connection callback", exc_info=True)
            raise


class SyslogParameters(object):
    """
    Encapsulates the syslog configuration
    """
    def __init__(self, conf):
        """
        :type conf: pyloggr.config.SyslogConfig
        """

        # todo: simplify the mess
        self.conf = conf
        self.list_of_sockets = None
        self.port_to_protocol = {}
        self.unix_socket_names = []
        self.port_to_ssl_config = {}

        for server in conf.servers.values():
            if server.stype == 'unix':
                self.unix_socket_names.append(server.socketname)
                self.port_to_protocol[server.socketname] = 'socket'
            else:
                for port in server.ports:
                    self.port_to_protocol[port] = server.stype

        self.ssl_ports = list(
            chain.from_iterable([server.ports for server in conf.servers.values() if server.ssl is not None])
        )

        for port in self.ssl_ports:
            self.port_to_ssl_config[port] = [server.ssl for server in conf.servers.values() if port in server.ports][0]

    def bind_all_sockets(self):
        """
        Bind the sockets to the current server

        :return: list of bound sockets
        :rtype: list
        """

        list_of_sockets = list()
        for server in self.conf.servers.values():
            address = '127.0.0.1' if server.localhost_only else ''
            for port in server.ports:
                list_of_sockets.append(
                    bind_sockets(port, address)
                )

        if self.unix_socket_names:
            list_of_sockets.append(
                [bind_unix_socket(expanduser(sock), mode=0o666) for sock in self.unix_socket_names]
            )

        self.list_of_sockets = list(chain.from_iterable(list_of_sockets))
        logger.info("Pyloggr syslog will listen on: {}".format(','.join(str(x) for x in self.port_to_protocol.keys())))
        logger.info("SSL ports: {}".format(','.join(str(x) for x in self.ssl_ports)))
        return self.list_of_sockets


class BaseSyslogClientConnection(object):
    """
    Encapsulates a connection with a syslog client
    """
    def __init__(self, stream, address, syslog_parameters):
        """
        :type syslog_parameters: SyslogParameters
        """
        self.syslog_parameters = syslog_parameters
        self.stream = stream
        self.address = address
        if address:
            # ipv4 or ipv6
            self.stream.socket.setsockopt(socket.IPPROTO_TCP, socket.SO_KEEPALIVE, 1)
            self.stream.set_nodelay(True)
        self.stream.set_close_callback(self.on_disconnect)
        self.client_id = str(uuid.uuid4())

        self.client_host = ''
        self.client_port = None
        self.flowinfo = None
        self.scopeid = None
        self.server_port = None

        self.nb_messages_received = 0
        self.nb_messages_transmitted = 0
        self.packer_groups = []

        self.dispatch_dict = {
            'tcp': self.dispatch_tcp_client,
            'relp': self.dispatch_relp_client,
            'socket': self.dispatch_tcp_client
        }

        self.disconnecting = threading.Event()

    @coroutine
    def _read_next_token(self):
        """
        _read_next_token()
        Reads the stream until we get a space delimiter

        Note
        ====
        Tornado coroutine
        """
        # todo: perf optimisation ?
        token = ''
        stream = self.stream
        while True:
            token = yield stream.read_until_regex(r'\s')
            token = token.strip(' \r\n')
            if token:
                break
        raise Return(token)

    @coroutine
    def on_disconnect(self):
        """
        on_disconnect()
        Called when a client has been disconnected

        .. note:: Tornado coroutine
        """
        self.disconnecting.set()
        logger.info("Syslog client has been disconnected {}:{}".format(self.client_host, self.client_port))
        yield []

    def disconnect(self):
        """
        Disconnects the client
        """
        if not self.disconnecting.is_set():
            self.disconnecting.set()
            if not self.stream.closed():
                self.stream.close()

    @coroutine
    def dispatch_relp_client(self):
        """
        dispatch_relp_client()
        Implements RELP protocol

        Note
        ====
        Tornado coroutine


        From http://www.rsyslog.com/doc/relp.html::

            Request:
            RELP-FRAME = RELPID SP COMMAND SP DATALEN [SP DATA] TRAILER

            DATA = [SP 1*OCTET] ; command-defined data, if DATALEN is 0, no data is present
            COMMAND = 1*32ALPHA
            TRAILER = LF

            Response:
            RSP-HEADER = TXNR SP RSP-CODE [SP HUMANMSG] LF [CMDDATA]

            RSP-CODE = 200 / 500 ; 200 is ok, all the rest currently erros
            HUAMANMSG = *OCTET ; a human-readble message without LF in it
            CMDDATA = *OCTET ; semantics depend on original command
        """
        read_next_token = self._read_next_token
        try:
            while not self.disconnecting.is_set():
                relp_event_id = yield self._read_next_token()
                try:
                    relp_event_id = int(relp_event_id)
                except ValueError:
                    # bad client, let's disconnect
                    log_msg = "Relp ID ({}) was not an integer. We disconnect the RELP client {}:{}".format(
                        relp_event_id, self.client_host, self.client_port
                    )
                    logger.warning(log_msg)
                    security_logger.warning(log_msg)
                    self.disconnect()
                    break
                command = yield read_next_token()
                length = yield read_next_token()
                try:
                    length = int(length)
                except ValueError:
                    # bad client, let's disconnect
                    log_msg = "Relp length ({}) was not an integer. We disconnect the RELP client {}:{}".format(
                        length, self.client_host, self.client_port
                    )
                    logger.warning(log_msg)
                    security_logger.warning(log_msg)
                    self.disconnect()
                    break
                data = b''
                if length > 0:
                    data = yield self.stream.read_bytes(length)
                    data = data.strip(b' \r\n')

                self._process_relp_command(relp_event_id, command, data)

        except StreamClosedError:
            logger.info("Stream was closed {}:{}".format(self.client_host, self.client_port))
            self.disconnect()
        except ssl.SSLError:
            logger.warning("Something bad happened in the TLS conversation")
            security_logger.exception("Something bad happened in the TLS conversation")
            self.disconnect()

    @coroutine
    def dispatch_tcp_client(self):
        """
        dispatch_tcp_client()
        Implements Syslog/TCP protocol

        Note
        ====
        Tornado coroutine


        From RFC 6587::

            It can be assumed that octet-counting framing is used if a syslog
            frame starts with a digit.

            TCP-DATA = *SYSLOG-FRAME
            SYSLOG-FRAME = MSG-LEN SP SYSLOG-MSG
            MSG-LEN = NONZERO-DIGIT *DIGIT
            NONZERO-DIGIT = %d49-57
            SYSLOG-MSG is defined in the syslog protocol [RFC5424] and may also be considered to be the payload in [RFC3164]
            MSG-LEN is the octet count of the SYSLOG-MSG in the SYSLOG-FRAME.

            A transport receiver can assume that non-transparent-framing is used
            if a syslog frame starts with the ASCII character "<" (%d60).

            TCP-DATA = *SYSLOG-FRAME
            SYSLOG-FRAME = SYSLOG-MSG TRAILER
            TRAILER = LF / APP-DEFINED
            APP-DEFINED = 1*2OCTET
            SYSLOG-MSG is defined in the syslog protocol [RFC5424] and may also be considered to be the payload in [RFC3164]
        """
        read_next_token = self._read_next_token
        stream = self.stream
        try:
            while not self.disconnecting.is_set():
                first_token = yield read_next_token()
                if first_token[0] == b'<':
                    # non-transparent framing
                    rest_of_line = yield stream.read_until(b'\n')
                    syslog_msg = first_token + b' ' + rest_of_line
                else:
                    # octet framing
                    try:
                        msg_len = int(first_token)
                    except ValueError:
                        log_msg = u"Syntax error from TCP client '{}:{}'. We disconnect it.".format(
                            self.client_host, self.client_port
                        )
                        logger.warning(log_msg)
                        security_logger.warning(log_msg)
                        self.disconnect()
                        break
                    syslog_msg = yield stream.read_bytes(msg_len)
                self._process_event(syslog_msg.strip(b' \r\n'), 'tcp')

        except StreamClosedError:
            logger.info(u"TCP stream was closed {}:{}".format(self.client_host, self.client_port))
            self.disconnect()
        except ssl.SSLError:
            logger.warning("Something bad happened in the TLS conversation")
            security_logger.exception("Something bad happened in the TLS conversation")
            self.disconnect()

    def _process_relp_command(self, relp_event_id, command, data):
        """
        _process_relp_command(relp_event_id, command, data)
        RELP client has sent a command. Find the type and make the right answer.

        :param relp_event_id: RELP ID, sent by client
        :param command: the RELP command
        :param data: data transmitted after command (can be empty)
        """
        if command == 'open':
            answer = '{} rsp {} 200 OK\n{}\n'.format(relp_event_id, len(data) + 7, data)
            self.stream.write(answer)
        elif command == 'close':
            self.stream.write('{} rsp 0\n0 serverclose 0\n'.format(relp_event_id))
            self.disconnect()
        elif command == 'syslog':
            self._process_event(data.strip(b'\r\n '), 'relp', relp_event_id)
        else:
            log_msg = "Unknown command '{}' from {}:{}".format(command, self.client_host, self.client_port)
            security_logger.warning(log_msg)
            logger.warning(log_msg)
            self.stream.write('{} rsp 6 200 OK\n'.format(relp_event_id))

    def _process_event(self, bytes_event, protocol, relp_event_id=None):
        raise NotImplementedError

    def _set_socket(self):
        try:
            server_sockname = self.stream.socket.getsockname()
            if isinstance(server_sockname, basestr):
                self.server_port = server_sockname       # socket unix
            elif len(server_sockname) == 2:
                self.server_port = server_sockname[1]    # ipv4
                (self.client_host, self.client_port) = self.stream.socket.getpeername()
            elif len(server_sockname) == 4:
                self.server_port = server_sockname[1]    # ipv6
                (self.client_host, self_client_port, self.flowinfo, self.scopeid) = self.stream.socket.getpeername()
            else:
                raise ValueError

        except StreamClosedError:
            logger.info("The client went away before it could be dispatched")
            self.disconnect()
            return
        except ValueError:
            logger.warning("Unknown socket type")
            self.disconnect()
            return

    @coroutine
    def on_connect(self):
        """
        on_connect()
        Called when a client connects to SyslogServer.

        We find the protocol by looking at the connecting port. Then run the appropriate dispatch method.

        Note
        ====
        Tornado coroutine
        """
        self._set_socket()

        try:
            server_sockname = self.stream.socket.getsockname()
            if isinstance(server_sockname, basestr):
                server_port = server_sockname       # socket unix
            elif len(server_sockname) == 2:
                server_port = server_sockname[1]    # ipv4
                (self.client_host, self.client_port) = self.stream.socket.getpeername()
            elif len(server_sockname) == 4:
                server_port = server_sockname[1]    # ipv6
                (self.client_host, self_client_port, self.flowinfo, self.scopeid) = self.stream.socket.getpeername()
            else:
                raise ValueError

        except StreamClosedError:
            logger.info("The client went away before it could be dispatched")
            self.disconnect()
            return
        except ValueError:
            logger.warning("Unknown socket type")
            self.disconnect()
            return
        t = self.syslog_parameters.port_to_protocol.get(self.server_port, None)

        if t is None:
            logger.warning("Don't know what to do with port '{}'".format(self.server_port))
            self.disconnect()
            return

        dispatch_function = self.dispatch_dict.get(t, None)
        if dispatch_function is None:
            self.disconnect()
            return

        self.server_port = server_port

        logger.info('New client is connected {}:{} to {}'.format(
            self.client_host, self.client_port, server_port
        ))

        # noinspection PyCallingNonCallable
        yield dispatch_function()
