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
from itertools import ifilter

from tornado.gen import coroutine, Return, TimeoutError
from tornado.ioloop import IOLoop
from tornado.concurrent import Future
# noinspection PyCompatibility
from concurrent.futures import Future as RealFuture
# noinspection PyCompatibility
from concurrent.futures import ThreadPoolExecutor
import ujson

from pyloggr.syslog.server import BaseSyslogServer, SyslogParameters, BaseSyslogClientConnection
from pyloggr.event import Event, ParsingError, InvalidSignature
from pyloggr.rabbitmq.publisher import Publisher, RabbitMQConnectionError
from pyloggr.config import Config
from pyloggr.utils import sleep
from pyloggr.utils.parsing_classes import always_true_singleton
from pyloggr.utils.lmdb_wrapper import LmdbWrapper
from pyloggr.utils.simple_queue import ThreadSafeQueue
from pyloggr.cache import Cache


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
        Cache.syslog_list[self.server_id].clients = self

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
            Cache.syslog_list[self.server_id].clients = self
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
    ['protocol', 'server_port', 'client_host', 'bytes_event', 'client_id', 'relp_id', 'total_messages']
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
        self.logger = None

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
        self.logger = logging.getLogger(__name__)
        lost_rabbit_connection = yield self._connect_to_rabbit()
        if lost_rabbit_connection is None:
            self.publication_ioloop.stop()
            return
        IOLoop.current().add_callback(self._wait_for_messages)
        yield lost_rabbit_connection.wait()
        # if we get here, it means we lost rabbitmq
        logging.getLogger(__name__).debug("syslog_server.Publication: lost rabbit!")
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

    def publish_syslog_messages(self, protocol, server_port, client_host, bytes_events, client_id, relp_id):
        if self._rabbitmq_status.is_set() and not self._shutting_down.is_set():
            total = len(bytes_events)
            [
                self._publications_queue.put(
                    SyslogMessage(protocol, server_port, client_host, bytes_event, client_id, relp_id, total)
                )
                for bytes_event in bytes_events
            ]
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
                SyslogMessage(protocol, server_port, client_host, bytes_event, client_id, relp_id, 1)
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
                logging.getLogger(__name__).error("local thread: can't connect to RabbitMQ")
                # _shutting_down could be set when we are sleeping
                # if we don't listen to it, the thread will not be stopped... and pyloggr will stay alive
                yield sleep(Config.SLEEP_TIME, threading_event=self._shutting_down)
                if self._shutting_down.is_set():
                    return
            else:
                break
        self.rabbit_is_lost_future = RealFuture()
        self._rabbitmq_status.set()
        raise Return(rabbit_close_ev)

    def _initialize_packers(self):
        # chain packers: packers A publishes the event in packer A+1, which publishes the event in A+2...
        for server in self.syslog_servers_conf:
            new_packer_groups = []
            for packer_group in server.packer_groups:
                current_publisher = self.publisher
                for packer_partial in reversed(packer_group.packers):
                    current_publisher = packer_partial(current_publisher)
                new_packer_groups.append((packer_group.condition, current_publisher))
            for port in server.ports:
                # str because port can be a unix socket name
                self.packer_groups[str(port)] = new_packer_groups

    def _do_publish_syslog(self, message):
        """
        :type message: SyslogMessage
        """
        try:
            event = Event.parse_bytes_to_event(message.bytes_event, hmac=True)
        except ParsingError as ex:
            if ex.json:
                self.logger.warning("JSON decoding failed. We log the event, drop it and continue")
                self.logger.warning(message.bytes_event)
                return
            else:
                # kick out the misleading client
                if message.client_id in list_of_clients:
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
            # pick the first packer which condition gives True
            # if no packer is suited, just use the traditional publisher
            port = str(message.server_port)
            if port in self.packer_groups:
                packers = self.packer_groups[str(message.server_port)] + [(always_true_singleton, self.publisher)]
                chosen_publisher = ifilter(
                    lambda (condition, publisher): condition.eval(event), packers
                ).next()[1]
            else:
                chosen_publisher = self.publisher

            if self._rabbitmq_status.is_set():
                future = chosen_publisher.publish_event(
                    event, routing_key='pyloggr.syslog.{}'.format(message.server_port)
                )
                future.protocol = message.protocol
                future.client_id = message.client_id
                future.total_messages = message.total_messages
                # after the event has been published (or not), we need to inform the sender
                IOLoop.current().add_future(future, Publicator._after_published)
            else:
                early_fails = True

        if early_fails:
            # don't even try to publish to rabbit, but notify the syslog client
            future = Future()
            future.protocol = message.protocol
            future.client_id = message.client_id
            future.total_messages = message.total_messages
            future.set_result((False, event))
            Publicator._after_published(future)

    def _do_publish_notification(self, message):
        """
        :type message: Notification
        """
        json_message = ujson.dumps(message.dictionnary)
        if (self.publisher is not None) and self._rabbitmq_status.is_set():
            IOLoop.current().add_callback(
                self.publisher.publish,
                exchange=Config.NOTIFICATIONS.exchange,
                body=json_message,
                routing_key=message.routing_key,
                persistent=False,
                event_type=Config.NOTIFICATIONS.event_type,
                application_id=Config.NOTIFICATIONS.application_id
            )
        else:
            self.logger.debug(
                "Some notification was not sent cause connection with RabbitMQ was not available"
            )

    @coroutine
    def _wait_for_messages(self):
        self.count_lz4_messages = {}
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
                    self.logger.warning("Ignoring strange message type '{}'".format(type(message)))

        yield self._do_shutdown()

    @coroutine
    def _do_shutdown(self):
        # notify the packers that they should not accept new stuff
        [packer.shutdown() for packer_groups in self.packer_groups.values() for _, packer in packer_groups]
        # shutdown the rabbitmq publisher
        if self.publisher:
            yield self.publisher.stop()     # yield returns when the rabbit connection is actually closed
            self.publisher = None
        # wait a bit so that 'publications' can finish its stuff
        # noinspection PyUnresolvedReferences,PyProtectedMember
        if self.publication_ioloop._callbacks or self.publication_ioloop._timeouts:
            yield sleep(1)
        self.publication_ioloop.stop()

    @classmethod
    def _after_published(cls, f):
        """
        :type f: tornado.concurrent.Future
        """
        status, event = f.result()
        if event is None or status is None:
            return
        # give control back to main thread
        if hasattr(f, 'protocol'):
            protocol = f.protocol.lower()
            if protocol in ("tcp", "udp"):
                # if TCP protocol, no need to answer the client
                # but we must save the event when the RabbitMQ publication has failed
                IOLoop.instance().add_callback(after_published_tcp, status, event=event)
            elif protocol == "relp":
                if hasattr(f, 'client_id'):
                    if f.client_id in list_of_clients:
                        client = list_of_clients[f.client_id]
                        # if RELP protocol, we have to send a response to the RELP client
                        IOLoop.instance().add_callback(client.after_published_relp, status, event=event)
                    else:
                        # client is already gone...
                        logging.getLogger(__name__).info(
                            "Couldn't send RELP response to already gone client '%s'", f.client_id
                        )
            elif protocol == "lz4":
                if hasattr(f, 'client_id'):
                    if f.client_id in list_of_clients:
                        if hasattr(f, 'total_messages'):
                            client = list_of_clients[f.client_id]
                            IOLoop.instance().add_callback(client.after_published_lz4, status, event, f.total_messages)
                    else:
                        logging.getLogger(__name__).info(
                            "Couldn't send LZ4 response to already gone client '%s'", f.client_id
                        )


@coroutine
def after_published_tcp(status, event=None, bytes_event=None):
    """
    Called after an event received by TCP has been tried to be published in RabbitMQ

    :param status: True if the event was successfully sent
    :param event: the Event object
    :param bytes_event: the event as bytes
    """
    if event is None and bytes_event is None:
        return
    if not status:
        logger = logging.getLogger(__name__)
        logger.warning("RabbitMQ could not store an event :( We shall try tu put in rescue queue")
        if event is None:
            try:
                event = Event.parse_bytes_to_event(bytes_event, hmac=True)
            except (ParsingError, InvalidSignature):
                logger.warning("... but event was unparsable: we drop it")
                return

        # LMDB can block, use a thread
        with ThreadPoolExecutor(1) as exe:
            yield exe.submit(_store_lmdb(event))


def _store_lmdb(event):
    logger = logging.getLogger(__name__)
    lmdb = LmdbWrapper.get_instance(Config.RESCUE_QUEUE_DIRNAME)

    if lmdb.queue('pyloggr.rescue').push(event):
        logger.info("Published in RabbitMQ failed, but we pushed the event in the rescue queue")
    else:
        logger.error("Failed to save event in redis. Event has been lost")


publications = None


class SyslogClientConnection(BaseSyslogClientConnection):
    """
    Encapsulates a connection with a syslog client
    """

    def __init__(self, stream, address, syslog_parameters):
        super(SyslogClientConnection, self).__init__(stream, address, syslog_parameters)
        self.count_lz4_messages = {}

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
            logging.getLogger(__name__).info("RabbitMQ publisher said NACK, sending 500 to RELP client. Event ID: {}".format(
                relp_event_id
            ))
            yield self.stream.write('{} rsp 6 500 KO\n'.format(relp_event_id))

    @coroutine
    def after_published_lz4(self, status, event, total_messages):
        relp_id = event.relp_id
        already_received_count = self.count_lz4_messages.get(relp_id)

        # noinspection PySimplifyBooleanCheck
        if already_received_count is False:
            # we already answered this group of messages
            return
        elif not status:
            # an event of the group was rejected. we have to answer the client and refuse all the group.
            self.count_lz4_messages[relp_id] = False
            yield self.stream.write('{} rsp 6 500 KO\n'.format(relp_id))
        elif already_received_count is None:
            self.count_lz4_messages[relp_id] = 1
        else:
            self.count_lz4_messages[relp_id] += 1

        if self.count_lz4_messages[relp_id] == total_messages:
            # all events from this group have been published, confirm it to the client
            yield self.stream.write('{} rsp 6 200 OK\n'.format(relp_id))
            del self.count_lz4_messages[relp_id]

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

    def _process_event(self, bytes_event, protocol, relp_event_id=None):
        """
        _process_relp_event(bytes_event, relp_event_id)
        Process a TCP syslog or RELP event.

        :param bytes_event: the event as `bytes`
        :param protocol: relp or tcp
        :param relp_event_id: event RELP ID, given by the RELP client
        """
        if bytes_event:
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

    def _process_group_events(self, bytes_events, relp_event_id):
        nb_events = len(bytes_events)
        self.nb_messages_received += nb_events
        accepted = publications.publish_syslog_messages(
            'lz4', self.server_port, self.client_host, bytes_events, self.client_id, relp_event_id
        )
        if not accepted:
            # only send one response to client for all the LZ4 transmitted messages
            IOLoop.instance().add_callback(self.after_published_relp, False, relp_id=relp_event_id)


class MainSyslogServer(BaseSyslogServer):
    """
    Tornado syslog server

    `SyslogServer` listens for syslog messages (RELP, RELP/TLS, TCP, TCP/TLS, Unix socket) and
    sends messages to RabbitMQ.
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
        LmdbWrapper(Config.RESCUE_QUEUE_DIRNAME, size=52428800).open(
            sync=True, metasync=True, max_dbs=2
        )
        global publications
        publications = Publicator(self.syslog_parameters.conf.values(), self.rabbitmq_config)
        publications.start()

        # wait that publications object has successfully connected to rabbitmq
        while not publications.rabbitmq_status():
            yield sleep(1)

        yield super(MainSyslogServer, self).launch()

        yield publications.rabbit_is_lost_future
        logging.getLogger(__name__).debug("Syslog server: lost Rabbit!")

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

            Cache.syslog_list[self.server_id].status = True
            Cache.syslog_list[self.server_id].ports = self.syslog_parameters.port_to_protocol.keys()
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
            Cache.syslog_list[self.server_id].status = False
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
        del Cache.syslog_list[self.server_id]
        if publications:
            publications.shutdown()

    def handle_data(self, data, sockname, peername):
        """
        Handle UDP syslog

        :param data: data sent
        :param sockname: the server socket info
        :param peername: the client socket info
        """
        data = data.strip('\r\n ')
        if not data:
            return
        if not peername:
            client = "unix socket"
            server_port = sockname
        else:
            client = peername[0]
            server_port = sockname[0]
        accepted = publications.publish_syslog_message(
            'udp', server_port, client, data, None, None
        )
        if not accepted:
            IOLoop.instance().add_callback(after_published_tcp, False, bytes_event=data)

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
            syslog_parameters=self.syslog_parameters
        )
        self.list_of_clients.append(connection)
        yield connection.on_connect()

    @coroutine
    def shutdown(self):
        """
        Authoritarian shutdown
        """
        yield super(MainSyslogServer, self).shutdown()
        LmdbWrapper.get_instance(Config.RESCUE_QUEUE_DIRNAME).close()
