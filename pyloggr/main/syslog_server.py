# encoding: utf-8


__author__ = 'stef'

import socket
from itertools import chain
import ssl
import logging
import uuid
import errno

from tornado.gen import coroutine, Return
from tornado.ioloop import IOLoop
from tornado.tcpserver import TCPServer
from tornado.netutil import bind_unix_socket, ssl_wrap_socket, errno_from_exception, bind_sockets
from tornado.iostream import SSLIOStream, StreamClosedError
# noinspection PyPackageRequirements
from past.builtins import basestring

from pyloggr.event import Event, ParsingError
from pyloggr.rabbitmq.publisher import Publisher, RabbitMQConnectionError
from pyloggr.config import SLEEP_TIME
from pyloggr.utils.observable import NotificationProducer
from pyloggr.cache import cache

logger = logging.getLogger(__name__)

# todo: push unparsable messages to some special logfile


class Clients(NotificationProducer):
    """
    Stores the current Syslog clients, sends notifications to observers, publishes the list of clients in Redis
    """
    task_id = None

    @classmethod
    def set_task_id(cls, task_id):
        """
        :param task_id: process number
        :type task_id: int
        """
        cls.task_id = task_id

    def __init__(self):
        super(Clients, self).__init__()
        self._clients = dict()

    def add(self, client_id, client):
        """
        :type client_id: str
        :type client: SyslogClientConnection
        """
        self._clients[client_id] = client
        cache.syslog[self.task_id].clients = self
        props = client.props
        props['task_id'] = self.task_id
        IOLoop.instance().add_callback(
            self.notify_observers,
            {'action': 'add_client', 'client': props},
            'pyloggr.syslog.clients'
        )

    def remove(self, client_id):
        """
        :type client_id: str
        """
        if client_id in self._clients:
            props = self._clients[client_id].props
            props['task_id'] = self.task_id
            del self._clients[client_id]
            cache.syslog[self.task_id].clients = self
            IOLoop.instance().add_callback(
                self.notify_observers,
                {'action': 'remove_client', 'client': props},
                'pyloggr.syslog.clients'
            )

    def __getitem__(self, client_id):
        """
        :type client_id: str
        """
        return self._clients[client_id]

    def __iter__(self):
        return iter(self._clients)

    def values(self):
        return self._clients.values()

    def keys(self):
        return self._clients.keys()


clients = Clients()


class SyslogClientConnection(object):
    """
    Encapsulates a connection with a syslog client
    """

    def __init__(self, stream, address, syslog_config, rabbitmq_config, publisher):
        """
        :type syslog_config: SyslogConfig
        """
        self.syslog_config = syslog_config
        self.stream = stream
        self.address = address
        if address:
            # ipv4 or ipv6
            self.stream.socket.setsockopt(socket.IPPROTO_TCP, socket.SO_KEEPALIVE, 1)
            self.stream.set_nodelay(True)
        self.stream.set_close_callback(self.on_disconnect)
        self.client_id = str(uuid.uuid4())
        self.rabbitmq_config = rabbitmq_config
        self.publisher = publisher

        self.client_host = ''
        self.client_port = None
        self.flowinfo = None
        self.scopeid = None
        self.server_port = None

        self.nb_messages_received = 0
        self.nb_messages_transmitted = 0

        self.dispatch_dict = {
            'tcp': self.dispatch_tcp_client,
            'relp': self.dispatch_relp_client,
            'tcpssl': self.dispatch_tcp_client,
            'relpssl': self.dispatch_relp_client,
            'socket': self.dispatch_tcp_client
        }

    @property
    def props(self):
        return {
            'host': self.client_host,
            'client_port': self.client_port,
            'server_port': self.server_port,
            'id': self.client_id,
        }

    @coroutine
    def on_disconnect(self):
        """
        on_disconnect()
        Called when a client has been disconnected

        .. note:: Tornado coroutine
        """
        logger.info("Syslog client has been disconnected {}:{}".format(self.client_host, self.client_port))
        clients.remove(self.client_id)
        yield []

    @coroutine
    def _process_tcp_event(self, bytes_event):
        """
        _process_tcp_event(bytes_event)
        Process a syslog/TCP event.

        We got a Syslog event with TCP protocol. Event was transmitted via TCP, so the original
        sender hasn't kept the event. We must take care that it is really published in RabbitMQ
        or it will be lost!

        :param bytes_event: the event as bytes

        Note
        ====
        Tornado coroutine
        """
        if len(bytes_event) > 0:
            logger.info("Got an event from TCP client {}:{}".format(self.client_host, self.client_port))
            try:
                event = Event.parse_bytes_to_event(bytes_event)
            except ParsingError as ex:
                if ex.json:
                    logger.warning("JSON decoding failed. We log the event, drop it and continue")
                    logger.warning(bytes_event)
                    return
                else:
                    raise

            event['syslog_server_port'] = self.server_port
            event['syslog_client_host'] = self.client_host
            self.nb_messages_received += 1

            status = yield self.publisher.publish_event(
                exchange=self.rabbitmq_config['exchange'],
                event=event,
                routing_key='pyloggr.syslog.{}'.format(self.server_port)
            )
            if status:
                self.nb_messages_transmitted += 1
            else:
                logger.warning("RabbitMQ publisher said NACK :(")
                if cache.rescue.append(bytes_event):
                    logger.info("Published in RabbitMQ failed, but we pushed the event in the rescue queue")
                else:
                    logger.error("Failed to save event in redis. Event has been lost")

    @coroutine
    def _process_relp_event(self, bytes_event, relp_event_id):
        """
        _process_relp_event(bytes_event, relp_event_id)
        Process a RELP event.

        We have got a Syslog event in RELP transaction

        :param bytes_event: the event as bytes
        :param relp_event_id: event ID

        Note
        ====
        Tornado coroutine
        """
        if len(bytes_event) > 0:
            logger.info("Got an event from RELP client {}:{}".format(self.client_host, self.client_port))
            try:
                event = Event.parse_bytes_to_event(bytes_event)
            except ParsingError as ex:
                if ex.json:
                    logger.warning("JSON decoding failed. We send 500 to RELP client and continue")
                    logger.warning(bytes_event)
                    self.stream.write('{} rsp 6 500 KO\n'.format(relp_event_id))
                    return
                else:
                    raise

            event['syslog_server_port'] = self.server_port
            event['syslog_client_host'] = self.client_host

            self.nb_messages_received += 1

            status = yield self.publisher.publish_event(
                exchange=self.rabbitmq_config['exchange'],
                event=event,
                routing_key='pyloggr.syslog.{}'.format(self.server_port)
            )
            if status:
                self.stream.write('{} rsp 6 200 OK\n'.format(relp_event_id))
                self.nb_messages_transmitted += 1
            else:
                logger.warning(
                    "RabbitMQ publisher said NACK, sending 500 to RELP client. Event ID: {}".format(relp_event_id)
                )
                self.stream.write('{} rsp 6 500 KO\n'.format(relp_event_id))

    @coroutine
    def _process_relp_command(self, relp_event_id, command, data):
        """
        _process_relp_command(relp_event_id, command, data)
        RELP client has sent a command. Find the type and make the right answer.

        :param relp_event_id: RELP ID, sent by client
        :param command: the RELP command
        :param data: data transmitted after command (can be empty)

        Note
        ====
        Tornado coroutine
        """
        if command == 'open':
            answer = '%s rsp %d 200 OK\n%s\n' % (relp_event_id, len(data) + 7, data)
            self.stream.write(answer)
        elif command == 'close':
            self.stream.write('{} rsp 0\n0 serverclose 0\n'.format(relp_event_id))
            self.disconnect()
        elif command == 'syslog':
            yield self._process_relp_event(data.strip(), relp_event_id)
        else:
            logger.info("Unknown command '{}' from {}:{}".format(command, self.client_host, self.client_port))
            self.stream.write('{} rsp 6 200 OK\n'.format(relp_event_id))

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
        try:
            while True:
                first_token = yield self._read_next_token()
                if first_token[0] == b'<':
                    # non-transparent framing
                    rest_of_line = yield self.stream.read_until(b'\n')
                    syslog_msg = first_token + b' ' + rest_of_line
                else:
                    # octet framing
                    try:
                        msg_len = int(first_token)
                    except ValueError:
                        logger.warning(u"Syntax error from TCP client. We disconnect it.")
                        self.disconnect()
                        break
                    syslog_msg = yield self.stream.read_bytes(msg_len)
                syslog_msg = syslog_msg.strip(b' \r\n')
                try:
                    yield self._process_tcp_event(syslog_msg)
                except ParsingError:
                    logger.warning(u"TCP client sent a malformed event. We disconnect it.")
                    self.disconnect()
                    break

        except StreamClosedError:
            logger.info(u"TCP stream was closed {}:{}".format(self.client_host, self.client_port))
            self.disconnect()
        except ssl.SSLError:
            logger.exception("Something bad happened in the TLS conversation")
            self.disconnect()

    @coroutine
    def _read_next_token(self):
        """
        _read_next_token()
        Reads the stream until we get a space delimiter

        Note
        ====
        Tornado coroutine
        """
        # todo: perf optimisation
        token = ''
        while True:
            token = yield self.stream.read_until(b' ')
            token = token.strip(' \r\n')
            if token:
                break
        raise Return(token)

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
        try:
            while True:
                relp_event_id = yield self._read_next_token()
                try:
                    relp_event_id = int(relp_event_id)
                except ValueError:
                    # bad client, let's disconnect
                    logger.warning("Relp ID ({}) was not an integer".format(relp_event_id))
                    logger.warning("We disconnect the RELP client {}:{}".format(self.client_host, self.client_port))
                    self.disconnect()
                    break
                command = yield self._read_next_token()
                length = yield self._read_next_token()
                try:
                    length = int(length)
                except ValueError:
                    # bad client, let's disconnect
                    logger.warning("Relp length ({}) was not an integer".format(length))
                    logger.warning("We disconnect the RELP client {}:{}".format(self.client_host, self.client_port))
                    self.disconnect()
                    break
                data = b''
                if length > 0:
                    data = yield self.stream.read_bytes(length)
                    data = data.strip(b' \r\n')
                try:
                    yield self._process_relp_command(relp_event_id, command, data)
                except ParsingError:
                    logger.warning(u"RELP client sent a malformed event. We disconnect it.")
                    self.disconnect()
                    break

        except StreamClosedError:
            logger.info("Stream was closed {}:{}".format(self.client_host, self.client_port))
            self.disconnect()

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

        try:
            server_sockname = self.stream.socket.getsockname()
            if isinstance(server_sockname, basestring):
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

        t = self.syslog_config.get_connection_type(server_port)

        if t is None:
            logger.warning("Don't know what to do with port '{}'".format(self.server_port))
            self.disconnect()
            return

        dispatch_function = self.dispatch_dict.get(t, None)
        if dispatch_function is None:
            self.disconnect()
            return

        self.server_port = server_port
        clients.add(self.client_id, self)
        logger.info('New client is connected {}:{} to {}'.format(
            self.client_host, self.client_port, server_port
        ))
        assert(callable(dispatch_function))
        yield dispatch_function()

    def disconnect(self):
        """
        Disconnects the client
        """
        self.stream.close()
        clients.remove(self.client_id)


class SyslogConfig(object):
    """
    Encapsulates the syslog server configuration

    """
    def __init__(self, conf):
        self.conf = conf
        protocol_to_port = dict()
        port_to_protocol = dict()

        if conf.get('relp_port', None) is not None:
            protocol_to_port['relp'] = int(conf['relp_port'])
            port_to_protocol[int(conf['relp_port'])] = 'relp'
        if conf.get('tcp_port', None) is not None:
            protocol_to_port['tcp'] = int(conf['tcp_port'])
            port_to_protocol[int(conf['tcp_port'])] = 'tcp'
        if conf.get('ssl', None) is not None:
            if conf.get('tcpssl_port', None) is not None:
                protocol_to_port['tcpssl'] = int(conf['tcpssl_port'])
                port_to_protocol[int(conf['tcpssl_port'])] = 'tcpssl'
            if conf.get('relpssl_port', None) is not None:
                protocol_to_port['relpssl'] = int(conf['relpssl_port'])
                port_to_protocol[int(conf['relpssl_port'])] = 'relpssl'

        self.unix_socket_name = conf.get('unix_socket', None)
        if self.unix_socket_name is not None:
            protocol_to_port['socket'] = self.unix_socket_name
            port_to_protocol[self.unix_socket_name] = 'socket'

        self.protocol_to_port = protocol_to_port
        self.port_to_protocol = port_to_protocol

        self.list_of_sockets = list()

    @property
    def ssl(self):
        return self.conf.get('ssl', None)

    def is_port_ssl(self, port):
        """
        Is the given port used for SSL connections ?
        :param port: port
        :type port: int
        :return: True if port is used for SSL
        :rtype: bool

        """
        if 'ssl' not in self.conf:
            return False
        if self.conf['ssl'] is None:
            return False
        return port == self.conf.get('tcpssl_port', None) or port == self.conf.get('relpssl_port', None)

    def get_connection_type(self, port):
        """
        :rtype: str
        """
        return self.port_to_protocol.get(port, None)

    def bind_all_sockets(self):
        """
        Bind the sockets to the current server
        :return: list of bound sockets
        :rtype: list

        """
        address = ''
        if self.conf.get('localhost_only', None):
            address = '127.0.0.1'

        numeric_ports = [port for port in self.port_to_protocol if str(port).isdigit()]
        list_of_sockets = [bind_sockets(port, address) for port in numeric_ports]
        logger.info("Syslog Pyloggr will listen on {}".format(str(numeric_ports)))

        if self.unix_socket_name is not None:
            list_of_sockets.append([bind_unix_socket(self.unix_socket_name, mode=0o666)])
            logger.info("Syslog Pyloggr will listen on unix socket '{}'".format(self.unix_socket_name))
        self.list_of_sockets = list(chain.from_iterable(list_of_sockets))
        return self.list_of_sockets


class SyslogServer(TCPServer, NotificationProducer):
    """
    Tornado syslog server

    `SyslogServer` listens for syslog messages (RELP, RELP/TLS, TCP, TCP/TLS, Unix socket) and
    sends messages to RabbitMQ.

    :todo: receive Linux kernel messages
    """

    def __init__(self, rabbitmq_config, syslog_config, task_id):
        """
        :type syslog_config: SyslogConfig
        :type task_id: int
        """
        assert(isinstance(syslog_config, SyslogConfig))
        TCPServer.__init__(self)
        NotificationProducer.__init__(self)

        self.rabbitmq_config = rabbitmq_config
        self.syslog_config = syslog_config
        self.task_id = task_id
        Clients.set_task_id(task_id)

        self.publisher = None
        self._connect_rabbitmq_later = None
        self.listening = False
        self.shutting_down = False

        self._reset()

    def _reset(self):
        self.listening = False
        self.publisher = None
        self._connect_rabbitmq_later = None

    def _cancel_connect_later(self):
        """
        Used in normal shutdown process (dont try to reconnect when we have decided to shutdown)
        """
        if self._connect_rabbitmq_later is not None:
            IOLoop.instance().remove_timeout(self._connect_rabbitmq_later)
            self._connect_rabbitmq_later = None

    def connect_later(self):
        if self._connect_rabbitmq_later is None and not self.shutting_down:
            logger.info("We will try to reconnect to RabbitMQ in {} seconds".format(SLEEP_TIME))
            self._connect_rabbitmq_later = IOLoop.instance().call_later(SLEEP_TIME, self.launch)

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
        self.publisher = Publisher(self.rabbitmq_config)
        try:
            rabbit_close_ev = yield self.publisher.start()
        except RabbitMQConnectionError:
            logger.error("Can't connect to RabbitMQ")
            yield self.stop_all()
            self.connect_later()
            return

        clients.register_queue(self.publisher)
        self.register_queue(self.publisher)
        yield self._start_syslog()
        # the next yield only returns if rabbitmq connection has been lost
        yield rabbit_close_ev.wait()
        # don't notify to RabbitMQ... as we lost the connection to it
        clients.unregister_queue()
        self.unregister_queue()
        yield self.stop_all()
        self.connect_later()

    @coroutine
    def _start_syslog(self):
        """
        Start to listen for syslog connections
        """
        if not self.listening:
            self.listening = True
            self.add_sockets(self.syslog_config.list_of_sockets)

            cache.syslog[self.task_id].status = True
            yield self.notify_observers(
                {'action': 'add_server', 'ports': self.syslog_config.port_to_protocol.keys(), 'id': self.task_id},
                'pyloggr.syslog.servers'
            )

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
        connection = SyslogClientConnection(
            stream=stream,
            address=address,
            syslog_config=self.syslog_config,
            rabbitmq_config=self.rabbitmq_config,
            publisher=self.publisher,
        )
        yield connection.on_connect()

    @coroutine
    def _stop_syslog(self):
        """
        Stop listening for syslog connections

        Note
        ====
        Tornado coroutine
        """
        if self.listening:
            self.listening = False
            logger.info("Closing RELP clients connections")
            for client in clients.values():
                client.stream.close()
            logger.info("Stopping the RELP server")
            self.stop()
            cache.syslog[self.task_id].status = False
            yield self.notify_observers({
                'action': 'remove_server', 'subject': 'pyloggr.syslog.servers', 'id': self.task_id
            })

    @coroutine
    def stop_all(self):
        """
        Stops completely the server. Stop listening for syslog clients. Close connection to RabbitMQ.

        Note
        ====
        Tornado coroutine
        """
        yield self._stop_syslog()
        del cache.syslog[self.task_id]
        clients.unregister_queue()
        if self.publisher:
            yield self.publisher.stop()
            self.publisher = None

        self._reset()

    @coroutine
    def shutdown(self):
        self.shutting_down = True
        self._cancel_connect_later()
        yield self.stop_all()

    def _handle_connection(self, connection, address):
        """
        Inherits _handle_connection from parent TCPServer to manage SSL connections. Called by
        Tornado when a client connects.
        """

        port = connection.getsockname()[1]
        if not self.syslog_config.is_port_ssl(port):
            return super(SyslogServer, self)._handle_connection(connection, address)

        try:
            connection = ssl_wrap_socket(
                connection,
                self.syslog_config.ssl,
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
