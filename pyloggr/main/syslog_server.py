# encoding: utf-8
__author__ = 'stef'

import socket
import ssl
import logging
import uuid
import errno

from tornado.gen import coroutine, Return, sleep
from tornado.ioloop import IOLoop, PeriodicCallback
from tornado.tcpserver import TCPServer
from tornado.netutil import bind_unix_socket, ssl_wrap_socket, errno_from_exception, bind_sockets
from tornado.iostream import SSLIOStream, StreamClosedError
from past.builtins import basestring

from pyloggr.event import Event, ParsingError
from pyloggr.rabbitmq.publisher import Publisher, RabbitMQConnectionError
from pyloggr.utils.fix_unicode import to_unicode
from pyloggr.config import SLEEP_TIME, SYSLOG_CONF
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
            {'action': 'add_client', 'client': props, 'subject': 'pyloggr.syslog_clients'}
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
                {'action': 'remove_client', 'client': props, 'subject': 'pyloggr.syslog_clients'}
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


@coroutine
def publish_syslog_event(rabbitmq_connection, exchange, event, routing_key='', persistent=True):
    """
    publish_syslog_event(rabbitmq_connection, exchange, event, routing_key='', persistent=True)
    Publish an Event object in RabbitMQ

    :param rabbitmq_connection: RabbitMQ connection
    :type rabbitmq_connection: Publisher
    :param exchange: RabbitMQ exchange
    :type exchange: str
    :param event: Event object
    :type event: Event
    :param routing_key: RabbitMQ routing key
    :type routing_key: str
    :param persistent: Should the event be saved on disk by RabbitMQ
    :type persistent: bool

    Note
    ====
    Tornado coroutine
    """
    json_event = event.dumps()
    logger.debug(json_event)
    # publish the event in RabbitMQ in JSON format
    result = yield rabbitmq_connection.publish(
        exchange=exchange,
        body=json_event,
        routing_key=routing_key,
        message_id=event.uuid,
        persistent=persistent
    )
    raise Return(result)


def bytes_to_event(bytes_event):
    """
    Parse some bytes into an :py:class:`pyloggr.event.Event` object

    .. note:: We generate a HMAC for this new event

    :param bytes_event: the event as bytes
    :type bytes_event: bytes
    :return: Event object
    :raise ParsingError: if bytes could not be parsed correctly
    """
    try:
        event = Event.load(bytes_event)
    except ParsingError:
        logger.warning(u"Pyloggr RELP Server: could not unmarshall a sylog event")
        logger.debug(to_unicode(bytes_event))
        raise
    else:
        event.generate_hmac()
        return event


class SyslogClientConnection(object):
    """
    Encapsulates a connection with a syslog client
    """

    def __init__(self, stream, address, relp_server_config, rabbitmq_config, rabbitmq_connection):
        self.relp_server_config = relp_server_config
        self.stream = stream
        self.address = address
        if address:
            # ipv4 or ipv6
            self.stream.socket.setsockopt(socket.IPPROTO_TCP, socket.SO_KEEPALIVE, 1)
            self.stream.set_nodelay(True)
        self.stream.set_close_callback(self.on_disconnect)
        self.client_id = str(uuid.uuid4())
        self.rabbitmq_config = rabbitmq_config
        self.rabbitmq_connection = rabbitmq_connection

        self.client_host = ''
        self.client_port = None
        self.flowinfo = None
        self.scopeid = None
        self.server_port = None

        self.nb_messages_received = 0
        self.nb_messages_transmitted = 0

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

        We have got a Syslog event with TCP protocol
        Event was transmitted via TCP, so the original sender hasn't kept the event
        We must take care that it is really published in RabbitMQ or it will be lost!

        :param bytes_event: the event as bytes

        .. note:: Tornado coroutine
        """
        if len(bytes_event) > 0:
            logger.info("Got an event from TCP client {}:{}".format(self.client_host, self.client_port))
            event = bytes_to_event(bytes_event)
            self.nb_messages_received += 1

            status = yield publish_syslog_event(self.rabbitmq_connection, self.rabbitmq_config['exchange'], event)
            if status:
                self.nb_messages_transmitted += 1
            else:
                logger.warning("RabbitMQ publisher said NACK :(")
                if cache.save_in_rescue(bytes_event):
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

        .. note:: Tornado coroutine
        """
        if len(bytes_event) > 0:
            logger.info("Got an event from RELP client {}:{}".format(self.client_host, self.client_port))
            event = bytes_to_event(bytes_event)
            self.nb_messages_received += 1

            status = yield publish_syslog_event(self.rabbitmq_connection, self.rabbitmq_config['exchange'], event)
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

        .. note:: Tornado coroutine
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

        .. note:: Tornado coroutine

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
        Reads the stream until we meet a space delimiter

        .. note:: Tornado coroutine
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

        .. note:: Tornado coroutine

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

        We find the protocol by the connecting port
        Then run the appropriate dispatch method

        .. note:: Tornado coroutine
        """

        try:
            server_sockname = self.stream.socket.getsockname()
            if isinstance(server_sockname, basestring):
                # socket unix
                self.server_port = server_sockname
            elif len(server_sockname) == 2:
                # ipv4
                self.server_port = server_sockname[1]
                (self.client_host, self.client_port) = self.stream.socket.getpeername()
            elif len(server_sockname) == 4:
                # ipv6
                self.server_port = server_sockname[1]
                (self.client_host, self_client_port, self.flowinfo, self.scopeid) = self.stream.socket.getpeername()
            else:
                raise ValueError

        except StreamClosedError:
            logger.info("The client went away before it could be dispatched")
            self.disconnect()
        except ValueError:
            logger.warning("Unknown socket type")
            self.disconnect()
        else:
            if self.server_port not in [
                self.relp_server_config['relp_port'],
                self.relp_server_config['relpssl_port'],
                self.relp_server_config['tcp_port'],
                self.relp_server_config['tcpssl_port'],
                self.relp_server_config['unix_socket']
            ]:
                logger.warning("Don't know what to do with port '{}'".format(self.server_port))
                return

            clients.add(self.client_id, self)

            if self.server_port == self.relp_server_config['relp_port']:
                logger.info('New RELP client is connected {}:{}'.format(self.client_host, self.client_port))
                yield self.dispatch_relp_client()
            if self.server_port == self.relp_server_config['relpssl_port']:
                logger.info('New RELP/TLS client is connected {}:{}'.format(self.client_host, self.client_port))
                yield self.dispatch_relp_client()
            elif self.server_port == self.relp_server_config['tcp_port']:
                logger.info('New TCP client is connected {}:{}'.format(self.client_host, self.client_port))
                yield self.dispatch_tcp_client()
            elif self.server_port == self.relp_server_config['tcpssl_port']:
                logger.info('New TCP/TLS client is connected {}:{}'.format(self.client_host, self.client_port))
                yield self.dispatch_tcp_client()
            elif self.server_port == self.relp_server_config['unix_socket']:
                logger.info("New Unix Socket client is connected")
                yield self.dispatch_tcp_client()
            else:
                self.disconnect()

    def disconnect(self):
        """
        Disconnects the client
        """
        self.stream.close()
        clients.remove(self.client_id)


class SyslogServer(TCPServer, NotificationProducer):
    """
    Tornado syslog server

    `SyslogServer` listens for syslog messages (RELP, RELP/TLS, TCP, TCP/TLS, Unix socket) and
    sends messages to RabbitMQ.

    :todo: receive Linux kernel messages
    """

    def __init__(self, rabbitmq_config, relp_server_config, task_id):
        TCPServer.__init__(self)
        NotificationProducer.__init__(self)

        self.rabbitmq_config = rabbitmq_config
        self.syslog_conf = relp_server_config
        self.task_id = task_id
        Clients.set_task_id(task_id)

        self.rabbitmq_connection = None
        self._connect_rabbitmq_later = None
        self.running = False

        self._reset()

    def _reset(self):
        self.running = False
        self.periodic_queue_saving = None
        self.rabbitmq_connection = None
        self._connect_rabbitmq_later = None

    def _cancel_connect_later(self):
        """
        Used in normak shutdown process (dont try to reconnect when we have decided to shutdown)
        """
        if self._connect_rabbitmq_later is not None:
            IOLoop.instance().remove_timeout(self._connect_rabbitmq_later)

    @coroutine
    def launch(self):
        self.rabbitmq_connection = Publisher(self.rabbitmq_config)
        try:
            rabbit_close_ev = yield self.rabbitmq_connection.start()
        except RabbitMQConnectionError:
            logger.error("Can't connect to RabbitMQ")
            self.rabbitmq_connection = None
            self.shutdown()
            logger.info("We will try to reconnect to RabbitMQ in {} seconds".format(SLEEP_TIME))
            self._connect_rabbitmq_later = IOLoop.instance().call_later(SLEEP_TIME, self.launch)
        else:
            if not self.running:
                self._start_syslog()
            clients.register_queue(self.rabbitmq_connection)
            self.register_queue(self.rabbitmq_connection)
            # the next yield only returns if rabbitmq connection has been closed
            yield rabbit_close_ev.wait()
            clients.unregister_queue()
            self.unregister_queue()
            if not self.rabbitmq_connection.shutting_down:
                self.rabbitmq_connection = None
                self.shutdown()
                logger.info("We will try to reconnect to RabbitMQ in {} seconds".format(SLEEP_TIME))
                self._connect_rabbitmq_later = IOLoop.instance().call_later(SLEEP_TIME, self.launch)

    @classmethod
    def get_ports(cls):
        """
        Returns the list of listening ports.
        :returns: list of ports
        :rtype: list
        """
        ports = list()
        if SYSLOG_CONF['relp_port'] is not None:
            ports.append(SYSLOG_CONF['relp_port'])
        if SYSLOG_CONF['tcp_port'] is not None:
            ports.append(SYSLOG_CONF['tcp_port'])
        if SYSLOG_CONF['ssl'] is not None:
            if SYSLOG_CONF['tcpssl_port'] is not None:
                ports.append(SYSLOG_CONF['tcpssl_port'])
            if SYSLOG_CONF['relpssl_port'] is not None:
                ports.append(SYSLOG_CONF['relpssl_port'])
        return ports

    @classmethod
    def bind_all_sockets(cls):
        """
        Bind the sockets to the current server
        :rtype: list

        """
        address = ''
        if SYSLOG_CONF['localhost_only']:
            address = '127.0.0.1'

        ports = cls.get_ports()

        cls.list_of_sockets = [bind_sockets(port, address) for port in ports]
        logger.info("Syslog Pyloggr listening on {}".format(str(ports)))

        if SYSLOG_CONF['unix_socket'] is not None:
            cls.list_of_sockets.append([bind_unix_socket(SYSLOG_CONF['unix_socket'], mode=0o666)])
            logger.info("Syslog Pyloggr listening on socket '{}'".format(SYSLOG_CONF['unix_socket']))

        return cls.list_of_sockets

    def _start_syslog(self):
        """
        Start to listen for syslog connections
        """
        for sockets in self.list_of_sockets:
            self.add_sockets(sockets)

        self.running = True
        cache.syslog[self.task_id].status = True

        self._start_periodic()
        IOLoop.instance().add_callback(
            self.notify_observers, {
                'action': 'add_server', 'subject': 'pyloggr.syslog_servers', 'ports': self.get_ports(),
                'id': self.task_id
            }
        )

    def _start_periodic(self):
        """
        Every 2 minutes we check if there are some events in the rescue queue
        """
        if self.periodic_queue_saving is None:
            self.periodic_queue_saving = PeriodicCallback(
                callback=self._try_publish_again,
                callback_time=1000 * 60 * 2
            )
            self.periodic_queue_saving.start()

    def _stop_periodic(self):
        """
        Stop the periodic check
        """
        if self.periodic_queue_saving:
            if self.periodic_queue_saving.is_running:
                self.periodic_queue_saving.stop()
            self.periodic_queue_saving = None

    @coroutine
    def _try_publish_again(self):
        """
        _try_publish_again()
        Check the rescue queue, try to publish events in RabbitMQ if we find some of them

        .. note:: Tornado coroutine
        """
        nb_events = cache.rescue_queue_length
        if nb_events is None:
            logger.info("Rescue queue: can't connect to Redis")
            return

        logger.info("{} elements in the rescue queue".format(nb_events))
        if nb_events == 0:
            return

        publish_futures = dict()
        for bytes_event in cache.rescue_queue_generator():
            try:
                event = bytes_to_event(bytes_event)
            except ParsingError:
                # we silently drop the unparsable event
                logger.debug("Rescue queue: dropping one unparsable event")
                continue

            publish_futures[bytes_event] = publish_syslog_event(
                self.rabbitmq_connection, self.rabbitmq_config['exchange'], event
            )

        results = yield publish_futures
        failed_events = [bytes_event for (bytes_event, ack) in results.items() if not ack]
        map(cache.save_in_rescue, failed_events)

    @coroutine
    def handle_stream(self, stream, address):
        """
        handle_stream(stream, address)
        Called by tornado when we have a new client.

        :param stream: IOStream for the new connection
        :param address: ???

        .. note:: Tornado coroutine
        """
        connection = SyslogClientConnection(
            stream=stream,
            address=address,
            relp_server_config=self.syslog_conf,
            rabbitmq_config=self.rabbitmq_config,
            rabbitmq_connection=self.rabbitmq_connection,
        )
        yield connection.on_connect()

    def _stop_syslog(self):
        """
        Stop listening for syslog connections
        """
        if self.running:
            self.running = False
            cache.syslog[self.task_id].status = False
            logger.info("Closing RELP clients connections")
            for client in clients.values():
                client.stream.close()
            logger.info("Stopping the RELP server")
            self.stop()
            IOLoop.instance().add_callback(
                self.notify_observers, {
                    'action': 'remove_server', 'subject': 'pyloggr.syslog_servers', 'id': self.task_id
                }
            )

        self._stop_periodic()

    @coroutine
    def shutdown(self):
        """
        Shutdowns completely the server. Stop listening for syslog clients. Close connection to RabbitMQ.
        """
        clients.unregister_queue()
        self._stop_syslog()
        self._cancel_connect_later()
        if self.rabbitmq_connection:
            yield sleep(2)
            self.rabbitmq_connection.stop()
            self.rabbitmq_connection = None
        del cache.syslog[self.task_id]

        self._reset()

    def _handle_connection(self, connection, address):
        """
        Inherits _handle_connection from vanilla TCPServer to manage SSL connections
        Called by Tornado when a client connects
        """

        if (self.syslog_conf['ssl']) is None:
            return super(SyslogServer, self)._handle_connection(connection, address)

        port = connection.getsockname()[1]

        if port != self.syslog_conf['tcpssl_port'] and port != self.syslog_conf['relpssl_port']:
            return super(SyslogServer, self)._handle_connection(connection, address)

        try:
            connection = ssl_wrap_socket(
                connection,
                self.syslog_conf['ssl'],
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
