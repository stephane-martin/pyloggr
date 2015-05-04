# encoding: utf-8

"""
This module provides stuff to implement a Syslog server with Tornado
"""

__author__ = 'stef'

import socket
from itertools import chain
import ssl
import logging
import uuid
import errno
from os.path import expanduser
import threading
import queue
from tornado.gen import coroutine, Return
from tornado.ioloop import IOLoop
from tornado.tcpserver import TCPServer
from tornado.netutil import bind_unix_socket, ssl_wrap_socket, errno_from_exception, bind_sockets
from tornado.iostream import SSLIOStream, StreamClosedError
# noinspection PyPackageRequirements,PyCompatibility
from past.builtins import basestring as basestr
import ujson

from pyloggr.event import Event, ParsingError
from pyloggr.rabbitmq.publisher import Publisher, RabbitMQConnectionError
from pyloggr.config import SyslogServerConfig, Config
from pyloggr.utils import sleep
from pyloggr.utils.observable import NotificationProducer
from pyloggr.cache import cache

logger = logging.getLogger(__name__)
security_logger = logging.getLogger('security')


class ListOfClients(NotificationProducer):
    """
    Stores the current Syslog clients, sends notifications to observers, publishes the list of clients in Redis
    """
    server_id = None

    @classmethod
    def set_server_id(cls, server_id):
        """
        :param server_id: process number
        :type server_id: int
        """
        cls.server_id = server_id

    def __init__(self):
        super(ListOfClients, self).__init__()
        self._clients = dict()

    def add(self, client_id, client):
        """
        :type client_id: str
        :type client: SyslogClientConnection
        """
        self._clients[client_id] = client
        cache.syslog_list[self.server_id].clients = self
        d = {'action': 'add_client'}
        d.update(client.props)
        if publications:
            publications.notify_observers(d, 'pyloggr.syslog.clients')

    def remove(self, client_id):
        """
        :type client_id: str
        """
        if client_id in self._clients:
            d = {'action': 'remove_client'}
            d.update(self._clients[client_id].props)
            del self._clients[client_id]
            cache.syslog_list[self.server_id].clients = self
            if publications:
                publications.notify_observers(d, 'pyloggr.syslog.clients')

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


list_of_clients = ListOfClients()


class Publications(object):
    def __init__(self, syslog_servers_conf, rabbitmq_config):
        self.rabbitmq_config = rabbitmq_config
        self.publisher = None
        self.syslog_servers_conf = syslog_servers_conf
        self.publication_ioloop = None
        self.publication_queue = queue.Queue()
        self.notifications_queue = queue.Queue()
        self.publication_thread = threading.Thread(target=self.publication_init_thread)
        self.shutting_down = threading.Event()
        self.servers = {}
        self.rabbitmq_status = threading.Event()

    def publication_init_thread(self):
        # make a specific IOLoop in this thread for RabbitMQ publishing
        self.publication_ioloop = IOLoop()
        self.publication_ioloop.make_current()
        self.publication_ioloop.add_callback(self.start)
        self.publication_ioloop.start()

    @coroutine
    def start(self):
        lost_rabbit_connection = yield self.connect_to_rabbit()
        IOLoop.current().add_callback(self.publication_loop)
        list_of_clients.register_queue(self.publisher)
        yield lost_rabbit_connection.wait()
        # if we get here, it means we lost rabbitmq
        self.rabbitmq_status.clear()
        list_of_clients.unregister_queue()

    def notify_observers(self, d, routing_key=''):
        self.notifications_queue.put((d, routing_key))

    @coroutine
    def connect_to_rabbit(self):
        rabbit_close_ev = None
        self.publisher = Publisher(self.rabbitmq_config)
        while True:
            try:
                rabbit_close_ev = yield self.publisher.start()
            except RabbitMQConnectionError:
                logger.error("local thread: can't connect to RabbitMQ")
                yield sleep(Config.SLEEP_TIME)
            else:
                break
        self.rabbitmq_status.set()
        raise Return(rabbit_close_ev)

    def initialize_packers(self):
        for server in self.syslog_servers_conf:
            new_packer_groups = []
            for packer_group in server.packer_groups:
                current_publisher = self.publisher
                for packer_partial in reversed(packer_group.packers):
                    current_publisher = packer_partial(current_publisher)
                new_packer_groups.append((packer_group.condition, current_publisher))
            for port in server.ports:
                self.servers[str(port)] = new_packer_groups

    def _publish(self, message):
        tcp_or_relp, server_port, client_host, bytes_event, relp_id, client = message
        try:
            event = Event.parse_bytes_to_event(bytes_event, hmac=True)
        except ParsingError as ex:
            if ex.json:
                logger.warning("JSON decoding failed. We log the event, drop it and continue")
                logger.warning(bytes_event)
                return
            else:
                IOLoop.instance().add_callback(client.disconnect)
                return

        event.relp_id = relp_id
        event['syslog_server_port'] = server_port
        event['syslog_client_host'] = client_host
        event['syslog_protocol'] = tcp_or_relp

        # pick the right publisher/packer
        chosen_publisher = self.publisher
        for condition, publisher in self.servers[str(server_port)]:
            if condition is None:
                chosen_publisher = publisher
            elif condition.eval(event):
                chosen_publisher = publisher
                break

        future = chosen_publisher.publish_event(event, 'pyloggr.syslog.{}'.format(server_port))
        future.tcp_or_relp = tcp_or_relp
        future.client = client
        IOLoop.current().add_future(future, self.after_published)

    def _publish_notification(self, message):
        d, routing_key = message
        json_message = ujson.dumps(d)
        IOLoop.current().add_callback(
            self.publisher.publish,
            exchange='pyloggr.pubsub',
            body=json_message,
            routing_key=routing_key,
            persistent=False
        )

    @coroutine
    def publication_loop(self):
        self.initialize_packers()
        while not self.shutting_down.is_set():
            try:
                message = self.publication_queue.get_nowait()
            except queue.Empty:
                try:
                    message = self.notifications_queue.get_nowait()
                except queue.Empty:
                    yield sleep(1)
                else:
                    self._publish_notification(message)
            else:
                self._publish(message)

        if self.publisher:
            yield self.publisher.stop()
        for packer_groups in self.servers.values():
            for _, packer in packer_groups:
                packer.shutdown()
        self.publication_ioloop.stop()

    def after_published(self, f):
        status, event = f.result()
        if event is None or status is None:
            return
        # give control back to main thread
        if f.tcp_or_relp == "tcp":
            IOLoop.instance().add_callback(f.client.after_published_tcp, status, event)
        else:
            IOLoop.instance().add_callback(f.client.after_published_relp, status, event)

publications = None


class SyslogClientConnection(object):
    """
    Encapsulates a connection with a syslog client
    """

    def __init__(self, stream, address, syslog_parameters, rabbitmq_config):
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
        self.rabbitmq_config = rabbitmq_config

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


    @property
    def props(self):
        return {
            'host': self.client_host,
            'client_port': self.client_port,
            'server_port': self.server_port,
            'id': self.client_id,
            'server_id': ListOfClients.server_id
        }

    @coroutine
    def on_disconnect(self):
        """
        on_disconnect()
        Called when a client has been disconnected

        .. note:: Tornado coroutine
        """
        logger.info("Syslog client has been disconnected {}:{}".format(self.client_host, self.client_port))
        list_of_clients.remove(self.client_id)
        yield []

    def _process_tcp_event(self, bytes_event):
        """
        _process_tcp_event(bytes_event)
        Process a syslog/TCP event.

        We got a Syslog event with TCP protocol. Event was transmitted via TCP, so the original
        sender hasn't kept the event. We must take care that it is really published in RabbitMQ
        or it will be lost!

        :param bytes_event: the event as bytes
        """
        if len(bytes_event) > 0:
            logger.info("Got an event from TCP client {}:{}".format(self.client_host, self.client_port))
            self.nb_messages_received += 1
            # put the bytes on the queue
            publications.publication_queue.put(
                ('tcp', self.server_port, self.client_host, bytes_event, None, self)
            )

    def after_published_tcp(self, status, event):
        if status:
            self.nb_messages_transmitted += 1
        else:
            logger.warning("RabbitMQ publisher said NACK :(")
            if cache.rescue.append(event):
                logger.info("Published in RabbitMQ failed, but we pushed the event in the rescue queue")
            else:
                logger.error("Failed to save event in redis. Event has been lost")

    def _process_relp_event(self, bytes_event, relp_event_id):
        """
        _process_relp_event(bytes_event, relp_event_id)
        Process a RELP event.

        We have got a Syslog event in RELP transaction

        :param bytes_event: the event as `bytes`
        :param relp_event_id: event ID given by the RELP client

        Note
        ====
        Tornado coroutine
        """
        if len(bytes_event) > 0:
            logger.info("Got an event from RELP client {}:{}".format(self.client_host, self.client_port))
            self.nb_messages_received += 1
            publications.publication_queue.put(
                ('relp', self.server_port, self.client_host, bytes_event, relp_event_id, self)
            )

    def after_published_relp(self, status, event):
        relp_event_id = event.relp_id
        if relp_event_id is None:
            return
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
            self._process_relp_event(data.strip(b'\r\n '), relp_event_id)
        else:
            log_msg = "Unknown command '{}' from {}:{}".format(command, self.client_host, self.client_port)
            security_logger.warning(log_msg)
            logger.warning(log_msg)
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
                        log_msg = u"Syntax error from TCP client '{}:{}'. We disconnect it.".format(
                            self.client_host, self.client_port
                        )
                        logger.warning(log_msg)
                        security_logger.warning(log_msg)
                        self.disconnect()
                        break
                    syslog_msg = yield self.stream.read_bytes(msg_len)
                self._process_tcp_event(syslog_msg.strip(b' \r\n'))

        except StreamClosedError:
            logger.info(u"TCP stream was closed {}:{}".format(self.client_host, self.client_port))
            self.disconnect()
        except ssl.SSLError:
            logger.warning("Something bad happened in the TLS conversation")
            security_logger.exception("Something bad happened in the TLS conversation")
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
        # todo: perf optimisation ?
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
                    log_msg = "Relp ID ({}) was not an integer. We disconnect the RELP client {}:{}".format(
                        relp_event_id, self.client_host, self.client_port
                    )
                    logger.warning(log_msg)
                    security_logger.warning(log_msg)
                    self.disconnect()
                    break
                command = yield self._read_next_token()
                length = yield self._read_next_token()
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
                try:
                    yield self._process_relp_command(relp_event_id, command, data)
                except ParsingError:
                    log_msg = u"RELP client '{}:{}' sent a malformed event. We disconnect it.".format(
                        self.client_host, self.client_port
                    )
                    logger.warning(log_msg)
                    security_logger.warning(log_msg)
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

        t = self.syslog_parameters.port_to_protocol.get(server_port, None)

        if t is None:
            logger.warning("Don't know what to do with port '{}'".format(self.server_port))
            self.disconnect()
            return

        dispatch_function = self.dispatch_dict.get(t, None)
        if dispatch_function is None:
            self.disconnect()
            return

        self.server_port = server_port
        # todo: problem, we modify the unique publisher
        #self.publisher.base_routing_key = 'pyloggr.syslog.{}'.format(self.server_port)

        # notify that we have a new client
        list_of_clients.add(self.client_id, self)
        logger.info('New client is connected {}:{} to {}'.format(
            self.client_host, self.client_port, server_port
        ))

        # noinspection PyCallingNonCallable
        yield dispatch_function()

    def disconnect(self):
        """
        Disconnects the client
        """
        self.stream.close()
        list_of_clients.remove(self.client_id)
        self.disconnecting.set()


class SyslogParameters(object):
    """
    Encapsulates the syslog server configuration
    """
    def __init__(self, conf):
        """
        :type conf: pyloggr.config.SyslogConfig
        """

        # todo: simplify the mess
        self.conf = conf
        self.list_of_sockets = None
        port_to_protocol = {}
        unix_socket_names = []

        for server in conf.servers.values():
            assert(isinstance(server, SyslogServerConfig))
            if server.stype == 'unix':
                unix_socket_names.append(server.socketname)
                port_to_protocol[server.socketname] = 'socket'
            else:
                for port in server.ports:
                    port_to_protocol[port] = server.stype

        self.port_to_protocol = port_to_protocol
        self.unix_socket_names = unix_socket_names
        self.ssl_ports = list(
            chain.from_iterable([server.ports for server in conf.servers.values() if server.ssl is not None])
        )
        port_to_ssl_config = {}
        for port in self.ssl_ports:
            port_to_ssl_config[port] = [server.ssl for server in conf.servers.values() if port in server.ports][0]
        self.port_to_ssl_config = port_to_ssl_config

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


class SyslogServer(TCPServer, NotificationProducer):
    """
    Tornado syslog server

    `SyslogServer` listens for syslog messages (RELP, RELP/TLS, TCP, TCP/TLS, Unix socket) and
    sends messages to RabbitMQ.

    :todo: receive Linux kernel messages
    """

    def __init__(self, rabbitmq_config, syslog_parameters, server_id):
        """
        :type syslog_parameters: SyslogParameters
        :type rabbitmq_config: pyloggr.config.RabbitMQBaseConfig
        :type server_id: int
        """
        TCPServer.__init__(self)
        NotificationProducer.__init__(self)

        self.rabbitmq_config = rabbitmq_config
        self.syslog_parameters = syslog_parameters
        self.server_id = server_id
        ListOfClients.set_server_id(server_id)

        self.listening = False
        self.shutting_down = False

        self._reset()

    def _reset(self):
        self.listening = False
        self._connect_rabbitmq_later = None

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
        global publications
        publications = Publications(self.syslog_parameters.conf.servers.values(), self.rabbitmq_config)
        publications.publication_thread.start()

        while not publications.rabbitmq_status.is_set():
            yield sleep(1)

        #self.register_queue(self.publisher)

        yield self._start_syslog()

        while publications.rabbitmq_status.is_set():
            yield sleep(1)

        #self.unregister_queue()
        yield self.stop_all()
        yield sleep(Config.SLEEP_TIME)
        if not self.shutting_down:
            IOLoop.instance().add_callback(self.launch)

    @coroutine
    def _start_syslog(self):
        """
        _start_syslog()
        Start to listen for syslog clients
        """
        if not self.listening:
            self.listening = True
            self.add_sockets(self.syslog_parameters.list_of_sockets)

            cache.syslog_list[self.server_id].status = True
            cache.syslog_list[self.server_id].ports = self.syslog_parameters.port_to_protocol.keys()
            if publications:
                publications.notify_observers(
                    {
                        'action': 'add_server',
                        'ports': self.syslog_parameters.port_to_protocol.keys(),
                        'server_id': self.server_id
                    },
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
            syslog_parameters=self.syslog_parameters,
            rabbitmq_config=self.rabbitmq_config
        )
        yield connection.on_connect()

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
            for client in list_of_clients.values():
                client.stream.close()
            logger.info("Stopping the RELP server")
            self.stop()
            cache.syslog_list[self.server_id].status = False
            if publications:
                publications.notify_observers(
                    {'action': 'remove_server', 'server_id': self.server_id},
                    'pyloggr.syslog.servers'
                )

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
        del cache.syslog_list[self.server_id]

        self._reset()

    @coroutine
    def shutdown(self):
        """
        Authoritarian shutdown
        """
        self.shutting_down = True
        if publications:
            publications.shutting_down.set()
        yield self.stop_all()

    def _handle_connection(self, connection, address):
        """
        Inherits _handle_connection from parent TCPServer to manage SSL connections. Called by
        Tornado when a client connects.
        """

        port = connection.getsockname()[1]
        if port not in self.syslog_parameters.ssl_ports:
            return super(SyslogServer, self)._handle_connection(connection, address)

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
