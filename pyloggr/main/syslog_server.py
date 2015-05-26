# encoding: utf-8

"""
This module provides stuff to implement a the main syslog/RELP server with Tornado
"""
from __future__ import absolute_import, division, print_function
__author__ = 'stef'

import logging
import threading
from collections import namedtuple
from datetime import timedelta

from tornado.gen import coroutine, Return, TimeoutError
from tornado.ioloop import IOLoop
from tornado.concurrent import Future
# noinspection PyCompatibility
from concurrent.futures import Future as RealFuture
import ujson

from pyloggr.syslog.server import BaseSyslogServer, SyslogParameters, BaseSyslogClientConnection
from pyloggr.event import Event, ParsingError
from pyloggr.rabbitmq.publisher import Publisher, RabbitMQConnectionError
from pyloggr.config import Config
from pyloggr.utils import sleep
from pyloggr.utils.simple_queue import ThreadSafeQueue
from pyloggr.cache import cache

logger = logging.getLogger(__name__)
security_logger = logging.getLogger('security')


class ListOfClients(object):
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

        if publications:
            d = {'action': 'add_client'}
            d.update(client.props)
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

    # noinspection PyDocstring
    def values(self):
        return self._clients.values()

    # noinspection PyDocstring
    def keys(self):
        return self._clients.keys()


list_of_clients = ListOfClients()

Notification = namedtuple('Notification', ['dictionnary', 'routing_key'])
SyslogMessage = namedtuple(
    'SyslogMessage',
    ['protocol', 'server_port', 'client_host', 'bytes_event', 'client_id', 'relp_id']
)


class Publicator(object):
    """
    `Publicator` manages the RabbitMQ connection and actually makes the publish calls.

    Publicator runs in its own thread, and has its own IOLoop.

    Parameters
    ==========
    syslog_servers_conf:
        Syslog configuration (used to initialize packers)
    rabbitmq_config:
        RabbitMQ connection parameters
    """
    def __init__(self, syslog_servers_conf, rabbitmq_config):
        self.rabbitmq_config = rabbitmq_config
        self.publisher = None
        self.syslog_servers_conf = syslog_servers_conf
        self._publications_queue = ThreadSafeQueue()
        self._publication_thread = threading.Thread(target=self.init_thread)
        self._shutting_down = threading.Event()
        self._rabbitmq_status = threading.Event()
        self.rabbit_is_lost_future = None
        self.packer_groups = {}

    def start(self):
        """
        Start `publications` own thread
        """
        self._publication_thread.start()

    def rabbitmq_status(self):
        """
        Return True if we have an established connection to RabbitMQ
        """
        return self._rabbitmq_status.is_set()

    def init_thread(self):
        """
        `Publicator` thread: start a new IOLoop, make it current, call `_do_start` as a callback
        """
        # make a specific IOLoop in this thread for RabbitMQ publishing
        # noinspection PyAttributeOutsideInit
        self.publication_ioloop = IOLoop()
        self.publication_ioloop.make_current()
        self.publication_ioloop.add_callback(self._do_start)
        self.publication_ioloop.start()
        # when the publication_ioloop will be stopped by 'shutdown', previous start() will return, and the
        # thread will terminate

    @coroutine
    def _do_start(self):
        lost_rabbit_connection = yield self._connect_to_rabbit()
        if lost_rabbit_connection is None:
            self.publication_ioloop.stop()
            return
        IOLoop.current().add_callback(self._wait_for_messages)
        yield lost_rabbit_connection.wait()
        # if we get here, it means we lost rabbitmq
        logger.debug("Syslog publications: lost rabbit!")
        self._rabbitmq_status.clear()
        self.rabbit_is_lost_future.set_result(True)

    def shutdown(self):
        """
        Ask Publicator to shutdown. Can be called by any thread.
        """
        if not self._shutting_down.is_set():
            self._shutting_down.set()

    def notify_observers(self, d, routing_key=''):
        """
        Send a notification via RabbitMQ

        :param d: dictionnary to send as a notification
        :param routing_key: RabbitMQ routing key
        :type d: dict
        :type routing_key: str
        """
        if self._rabbitmq_status.is_set() and not self._shutting_down.is_set():
            self._publications_queue.put(Notification(d, routing_key))
            return True
        else:
            return False

    # noinspection PyDocstring
    def publish_syslog_message(self, protocol, server_port, client_host, bytes_event, client_id, relp_id=None):
        """
        Ask `publications` to publish a syslog event to RabbitMQ. Can be called by any thread

        Parameters
        ==========
        protocol: str
            'tcp' or 'relp'
        server_port: int
            which syslog server port was used to transmit the event
        client_host: str
            client hostname that sent the event
        bytes_event: bytes
            the event as bytes
        client_id: str
            SyslogClientConnection client_id
        relp_id: int
            event RELP id
        """
        if self._rabbitmq_status.is_set() and not self._shutting_down.is_set():
            self._publications_queue.put(
                SyslogMessage(protocol, server_port, client_host, bytes_event, client_id, relp_id)
            )
            return True
        else:
            return False

    @coroutine
    def _connect_to_rabbit(self):
        rabbit_close_ev = None
        self.publisher = Publisher(self.rabbitmq_config)
        while True:
            try:
                rabbit_close_ev = yield self.publisher.start()
            except (RabbitMQConnectionError, TimeoutError):
                logger.error("local thread: can't connect to RabbitMQ")
                # _shutting_down could be set when we are sleeping
                # if we don't listen to it, the thread will not be stopped... and pyloggr will stay alive
                yield sleep(Config.SLEEP_TIME, self._shutting_down)
                if self._shutting_down.is_set():
                    return
            else:
                break
        self.rabbit_is_lost_future = RealFuture()
        self._rabbitmq_status.set()
        raise Return(rabbit_close_ev)

    def _initialize_packers(self):
        for server in self.syslog_servers_conf:
            new_packer_groups = []
            for packer_group in server.packer_groups:
                current_publisher = self.publisher
                for packer_partial in reversed(packer_group.packers):
                    current_publisher = packer_partial(current_publisher)
                new_packer_groups.append((packer_group.condition, current_publisher))
            for port in server.ports:
                self.packer_groups[str(port)] = new_packer_groups

    def _do_publish_syslog(self, message):
        """
        :type message: SyslogMessage
        """
        # protocol, server_port, client_host, bytes_event, client_id, relp_id  = message
        try:
            event = Event.parse_bytes_to_event(message.bytes_event, hmac=True)
        except ParsingError as ex:
            if ex.json:
                logger.warning("JSON decoding failed. We log the event, drop it and continue")
                logger.warning(message.bytes_event)
                return
            else:
                IOLoop.instance().add_callback(list_of_clients[message.client_id].disconnect)
                return

        event.relp_id = message.relp_id
        event['syslog_server_port'] = message.server_port
        event['syslog_client_host'] = message.client_host
        event['syslog_protocol'] = message.protocol

        # pick the right publisher/packer
        early_fails = False
        if self.publisher is None:
            early_fails = True
        else:
            chosen_publisher = self.publisher
            for condition, publisher in self.packer_groups[str(message.server_port)]:
                if condition is None:
                    chosen_publisher = publisher
                    break
                elif condition.eval(event):
                    chosen_publisher = publisher
                    break

            if self._rabbitmq_status.is_set():
                future = chosen_publisher.publish_event(
                    event,
                    routing_key='pyloggr.syslog.{}'.format(message.server_port)
                )
                future.protocol = message.protocol
                future.client_id = message.client_id
                IOLoop.current().add_future(future, self._after_published)
            else:
                early_fails = True

        if early_fails:
            # don't even try to publish to rabbit, but notify the syslog client
            future = Future()
            future.protocol = message.protocol
            future.client_id = message.client_id
            future.set_result((False, event))
            self._after_published(future)

    def _do_publish_notification(self, message):
        """
        :type message: Notification
        """
        json_message = ujson.dumps(message.dictionnary)
        if self.publisher is not None and self._rabbitmq_status.is_set():
            IOLoop.current().add_callback(
                self.publisher.publish,
                exchange=Config.NOTIFICATIONS.exchange,
                body=json_message,
                routing_key=message.routing_key,
                persistent=False,
                event_type=Config.NOTIFICATIONS.event_type,
                application_id=Config.NOTIFICATIONS.application_id
            )

    @coroutine
    def _wait_for_messages(self):
        self._initialize_packers()
        while not self._shutting_down.is_set():
            try:
                message = yield self._publications_queue.get_wait(deadline=timedelta(seconds=1))
            except TimeoutError:
                pass
            else:
                if isinstance(message, Notification):
                    self._do_publish_notification(message)
                elif isinstance(message, SyslogMessage):
                    self._do_publish_syslog(message)
                else:
                    logger.warning("Ignoring strange message type '{}'".format(type(message)))

        yield self._do_shutdown()

    @coroutine
    def _do_shutdown(self):
        # notify the packers that they should not accept new stuff
        for packer_groups in self.packer_groups.values():
            for _, packer in packer_groups:
                packer.shutdown()
        # shutdown the rabbitmq publisher
        if self.publisher:
            yield self.publisher.stop()     # yield returns when the rabbit connection is actually closed
            self.publisher = None
        # wait a bit so that 'publications' can finish its stuff
        # noinspection PyUnresolvedReferences,PyProtectedMember
        if self.publication_ioloop._callbacks or self.publication_ioloop._timeouts:
            yield sleep(1)
        self.publication_ioloop.stop()

    @staticmethod
    def _after_published(f):
        """
        :type f: tornado.concurrent.Future
        """
        status, event = f.result()
        if event is None or status is None:
            return
        # give control back to main thread
        if hasattr(f, 'protocol'):
            protocol = f.protocol.lower()
            if protocol == "tcp":
                IOLoop.instance().add_callback(after_published_tcp, status, event=event)
            elif protocol == "relp":
                if hasattr(f, 'client_id'):
                    if f.client_id in list_of_clients:
                        client = list_of_clients[f.client_id]
                        IOLoop.instance().add_callback(client.after_published_relp, status, event=event)
                    else:
                        # client is already gone...
                        logger.info("Couldn't send RELP response to already gone client")


@coroutine
def after_published_tcp(status, event=None, bytes_event=None):
    """
    Called after an event received by TCP has been published in RabbitMQ

    :param status: True if the event was successfully sent
    :param event: the Event object
    :param bytes_event: the event as bytes
    """
    if event is None and bytes_event is None:
        return
    if not status:
        logger.warning("RabbitMQ publisher said NACK :(")
        if cache.rescue.append(event, bytes_event):
            logger.info("Published in RabbitMQ failed, but we pushed the event in the rescue queue")
        else:
            logger.error("Failed to save event in redis. Event has been lost")


publications = None


class SyslogClientConnection(BaseSyslogClientConnection):
    """
    Encapsulates a connection with a syslog client
    """

    def __init__(self, stream, address, syslog_parameters, rabbitmq_config):
        super(SyslogClientConnection, self).__init__(stream, address, syslog_parameters)
        self.rabbitmq_config = rabbitmq_config

    @property
    def props(self):
        """
        Return a few properties for this client

        :rtype: dict
        """
        return {
            'host': self.client_host,
            'client_port': self.client_port,
            'server_port': self.server_port,
            'id': self.client_id,
            'server_id': ListOfClients.server_id
        }

    @coroutine
    def after_published_relp(self, status, event=None, relp_id=None):
        """
        Called after an event received by RELP has been published in RabbitMQ

        :param status: True if the event was successfully sent
        :param event: the Event object
        :param relp_id: event RELP id
        """
        if event is None and relp_id is None:
            return
        relp_event_id = event.relp_id if relp_id is None else relp_id
        if relp_event_id is None:
            return
        if status:
            yield self.stream.write('{} rsp 6 200 OK\n'.format(relp_event_id))
        else:
            logger.warning(
                "RabbitMQ publisher said NACK, sending 500 to RELP client. Event ID: {}".format(relp_event_id)
            )
            yield self.stream.write('{} rsp 6 500 KO\n'.format(relp_event_id))

    def _process_event(self, bytes_event, protocol, relp_event_id=None):
        """
        _process_relp_event(bytes_event, relp_event_id)
        Process a TCP syslog or RELP event.

        :param bytes_event: the event as `bytes`
        :param protocol: relp or tcp
        :param relp_event_id: event RELP ID, given by the RELP client

        Note
        ====
        Tornado coroutine
        """
        if bytes_event:
            logger.debug("Got an event from {} client {}:{}".format(protocol, self.client_host, self.client_port))
            self.nb_messages_received += 1
            accepted = publications.publish_syslog_message(
                protocol, self.server_port, self.client_host, bytes_event, self.client_id, relp_event_id
            )
            if not accepted:
                protocol = protocol.lower()
                # message was refused by 'publications' because rabbitmq is not available
                if protocol == 'tcp':
                    IOLoop.instance().add_callback(after_published_tcp, False, bytes_event=bytes_event)
                elif protocol == 'relp':
                    IOLoop.instance().add_callback(self.after_published_relp, False, relp_id=relp_event_id)

    def _set_socket(self):
        super(SyslogClientConnection, self)._set_socket()
        list_of_clients.add(self.client_id, self)

    def disconnect(self):
        """
        Disconnects the client
        """
        if not self.disconnecting.is_set():
            super(SyslogClientConnection, self).disconnect()
            list_of_clients.remove(self.client_id)

    def on_stream_closed(self):
        """
        on_stream_closed()
        Called when a client has been disconnected
        """
        list_of_clients.remove(self.client_id)
        super(SyslogClientConnection, self).on_stream_closed()


class MainSyslogServer(BaseSyslogServer):
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
        super(MainSyslogServer, self).__init__(syslog_parameters)

        self.rabbitmq_config = rabbitmq_config
        self.server_id = server_id
        ListOfClients.set_server_id(server_id)

    @coroutine
    def launch(self):
        """
        launch()
        Starts the server

        - First we try to connect to RabbitMQ
        - If successfull, we start to listen for syslog clients

        Note
        ====
        Tornado coroutine
        """
        global publications
        publications = Publicator(self.syslog_parameters.conf.values(), self.rabbitmq_config)
        publications.start()

        while not publications.rabbitmq_status():
            yield sleep(1)

        yield super(MainSyslogServer, self).launch()

        yield publications.rabbit_is_lost_future
        logger.debug("Syslog server: lost Rabbit!")

        yield self.stop_all()
        yield sleep(Config.SLEEP_TIME)
        if not self.shutting_down:
            IOLoop.instance().add_callback(self.launch)

    @coroutine
    def _start_syslog(self):
        """
        _start_syslog()
        Start to listen for syslog clients

        Note
        ====
        Tornado coroutine
        """
        if not self.listening:
            yield super(MainSyslogServer, self)._start_syslog()

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
    def _stop_syslog(self):
        """
        _stop_syslog()
        Stop listening for syslog connections

        Note
        ====
        Tornado coroutine
        """
        if self.listening:
            yield super(MainSyslogServer, self)._stop_syslog()
            cache.syslog_list[self.server_id].status = False
            if publications:
                publications.notify_observers(
                    {'action': 'remove_server', 'server_id': self.server_id},
                    'pyloggr.syslog.servers'
                )
                # buy a bit of time so that notifications actually reach rabbit
                yield sleep(2)

    @coroutine
    def stop_all(self):
        """
        stop_all()
        Stops completely the server. Stop listening for syslog clients. Close connection to RabbitMQ.

        Note
        ====
        Tornado coroutine
        """

        yield super(MainSyslogServer, self).stop_all()
        del cache.syslog_list[self.server_id]
        if publications:
            publications.shutdown()

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
        self.list_of_clients.append(connection)
        yield connection.on_connect()

    def _stop_listen_sockets(self):
        """
        Stop listening the bound sockets
        """
        for fd, sock in self._sockets.items():
            self.io_loop.remove_handler(fd)
