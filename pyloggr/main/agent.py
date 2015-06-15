# encoding: utf-8

"""
Local syslog agent
"""
from __future__ import absolute_import, division, print_function
__author__ = 'stef'

from threading import Thread
import threading
import socket
import arrow
import logging
import time
# noinspection PyCompatibility
from queue import Queue
from copy import copy
from datetime import timedelta
# noinspection PyCompatibility
from queue import Empty as QueueEmpty
from tornado.gen import coroutine, TimeoutError
from tornado.ioloop import IOLoop
from pyloggr.event import Event, ParsingError, InvalidSignature
from pyloggr.utils.lmdb_wrapper import LmdbWrapper
from pyloggr.utils import sleep
from pyloggr.utils.simple_queue import ThreadSafeQueue
from pyloggr.syslog.relp_client import ServerClose
from pyloggr.syslog import client_factory
from pyloggr.syslog.server import BaseSyslogServer, SyslogParameters, BaseSyslogClientConnection
from pyloggr.config import SyslogServerConfig, SyslogAgentConfig


class SyslogAgent(BaseSyslogServer):
    """
    Syslog agent

    `SyslogServer` listens for syslog messages (RELP, RELP/TLS, TCP, TCP/TLS, Unix socket) and
    sends messages to a remote TCP/Syslog or RELP server.
    """
    # todo: retrieve kernel log messages on linux

    def __init__(self, syslog_agent_config):
        """
        :type syslog_agent_config: SyslogAgentConfig
        """
        syslog_conf = {}
        if syslog_agent_config.tcp_ports:
            syslog_conf['tcp_agent'] = SyslogServerConfig(
                name="tcp_agent",
                ports=syslog_agent_config.tcp_ports,
                stype="tcp",
                localhost_only=syslog_agent_config.localhost_only
            )
        if syslog_agent_config.udp_ports:
            syslog_conf['udp_agent'] = SyslogServerConfig(
                name="udp_agent",
                ports=syslog_agent_config.udp_ports,
                stype="udp",
                localhost_only=syslog_agent_config.localhost_only
            )
        if syslog_agent_config.relp_ports:
            syslog_conf['relp_agent'] = SyslogServerConfig(
                name="relp_agent",
                ports=syslog_agent_config.relp_ports,
                stype="relp",
                localhost_only=syslog_agent_config.localhost_only
            )
        if syslog_agent_config.socket_names:
            syslog_conf['sockets'] = SyslogServerConfig(
                name="sockets",
                socket_names=syslog_agent_config.socket_names,
                stype="unix"
            )
        syslog_parameters = SyslogParameters(syslog_conf)
        self.syslog_agent_config = syslog_agent_config
        super(SyslogAgent, self).__init__(syslog_parameters)

        self._put_messages_in_lmdb_thread = None
        self._retrieve_messages_from_lmdb_thread = None
        self._publication_thread = None

        self.publication_queue = None
        self.published_messages_queue = None
        self.failed_messages_queue = None
        self.received_messages_queue = None

        self.syslog_server_is_available = None

    @coroutine
    def launch(self):
        """
        launch()
        Starts the agent

        Note
        ====
        Tornado coroutine
        """
        LmdbWrapper(self.syslog_agent_config.lmdb_db_name, size=52428800).open(
            sync=False, metasync=False, max_dbs=3
        )
        self.received_messages_queue = Queue()
        self.publication_queue = ThreadSafeQueue()
        self.published_messages_queue = Queue()
        self.failed_messages_queue = Queue()

        self.syslog_server_is_available = threading.Event()

        self._start_publication_thread()
        self._start_thread_retrieve_messages_from_lmdb()
        self._start_thread_store_messages_in_lmdb()
        yield self._start_syslog()

    def _start_publication_thread(self):
        if self._publication_thread is None:
            self._publication_thread = Publications(
                syslog_agent_config=self.syslog_agent_config,
                publication_queue=self.publication_queue,
                published_messages_queue=self.published_messages_queue,
                failed_messages_queue=self.failed_messages_queue,
                syslog_server_is_available=self.syslog_server_is_available
            )
            logger = logging.getLogger(__name__)
            logger.debug("Starting 'Publication' thread")
            self._publication_thread.start()

    def _start_thread_retrieve_messages_from_lmdb(self):
        if self._retrieve_messages_from_lmdb_thread is None:
            self._retrieve_messages_from_lmdb_thread = RetrieveMessagesFromLMDB(
                lmdb_db_name=self.syslog_agent_config.lmdb_db_name,
                publication_queue=self.publication_queue,
                published_messages_queue=self.published_messages_queue,
                failed_messages_queue=self.failed_messages_queue,
                syslog_server_is_available=self.syslog_server_is_available,
                pause=self.syslog_agent_config.pause
            )
            logger = logging.getLogger(__name__)
            logger.debug("Starting 'Retrieve messages from LMDB' thread")
            self._retrieve_messages_from_lmdb_thread.start()

    def _start_thread_store_messages_in_lmdb(self):
        if self._put_messages_in_lmdb_thread is None:
            self._put_messages_in_lmdb_thread = StoreMessagesInLMDB(
                received_messages_queue=self.received_messages_queue,
                lmdb_db_name=self.syslog_agent_config.lmdb_db_name
            )
            logger = logging.getLogger(__name__)
            logger.debug("Starting 'Store messages in LMDB' thread")
            self._put_messages_in_lmdb_thread.start()

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
            yield super(SyslogAgent, self)._start_syslog()

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
            yield super(SyslogAgent, self)._stop_syslog()
            # close the sockets
            self.stop()
            self.syslog_parameters.delete_unix_sockets()

    def _stop_publication_thread(self):
        logger = logging.getLogger(__name__)
        if self._publication_thread is not None:
            logger.debug("Asking thread 'Publication' to stop")
            self._publication_thread.stopping.set()
            self._publication_thread.publication_ioloop.stop()
            self._publication_thread.join()
            self._publication_thread = None

    def _stop_thread_put_messages_in_lmdb(self):
        logger = logging.getLogger(__name__)
        if self._put_messages_in_lmdb_thread is not None:
            logger.debug("Asking thread 'Store messages in LMDB' to stop")
            self._put_messages_in_lmdb_thread.stopping.set()
            # wait until it is actually stopped
            self._put_messages_in_lmdb_thread.join()
            self._put_messages_in_lmdb_thread = None

    def _stop_thread_retrieve_messages_from_lmdb(self):
        logger = logging.getLogger(__name__)
        if self._retrieve_messages_from_lmdb_thread is not None:
            logger.debug("Asking thread 'Retrieve messages from LMDB' to stop")
            self._retrieve_messages_from_lmdb_thread.stopping.set()
            # wait until it is actually stopped
            self._retrieve_messages_from_lmdb_thread.join()
            self._retrieve_messages_from_lmdb_thread = None

    @coroutine
    def stop_all(self):
        """
        stop_all()
        Stops completely the server. Stop listening for syslog clients. Close connection to remote server.

        Note
        ====
        Tornado coroutine
        """
        # stop the syslog server
        yield self._stop_syslog()
        # stop the "put things in LMDB thread"
        self._stop_thread_put_messages_in_lmdb()
        # stop the "retrieve from LMDB thread"
        self._stop_thread_retrieve_messages_from_lmdb()
        # stop the publication thread
        self._stop_publication_thread()

    @coroutine
    def shutdown(self):
        """
        Authoritarian shutdown
        """
        # call stop_all
        yield super(SyslogAgent, self).shutdown()
        # cleanly close LMDB
        LmdbWrapper.get_instance(self.syslog_agent_config.lmdb_db_name).close()

    # noinspection PyDocstring
    def handle_data(self, data, sockname, peername):
        """
        Handle UDP connections
        """
        data = data.strip('\r\n ')
        if data:
            self.received_messages_queue.put_nowait(data)

    # noinspection PyDocstring
    @coroutine
    def handle_stream(self, stream, address):
        """
        Handle TCP and RELP clients
        """
        connection = SyslogAgentClient(
            stream=stream,
            address=address,
            syslog_parameters=self.syslog_parameters,
            received_messages=self.received_messages_queue
        )
        yield connection.on_connect()


class Publications(Thread):
    """
    The Publications thread handles the connection to the remote syslog server to publish the messages
    """
    def __init__(self, syslog_agent_config, publication_queue, published_messages_queue, failed_messages_queue,
                 syslog_server_is_available):
        """
        :type syslog_agent_config: SyslogAgentConfig
        """
        super(Publications, self).__init__(name="Send messages to remote syslog")
        self.syslog_agent_config = syslog_agent_config
        self.publication_queue = publication_queue
        self.published_messages_queue = published_messages_queue
        self.failed_messages_queue = failed_messages_queue
        self.stopping = threading.Event()
        self.syslog_server_is_available = syslog_server_is_available
        self.publication_ioloop = None

    # noinspection PyDocstring
    def run(self):
        # start a second IOLoop for publications to remote syslog
        self.publication_ioloop = IOLoop()
        self.publication_ioloop.make_current()
        self.publication_ioloop.add_callback(self._do_start)
        self.publication_ioloop.start()
        # will not return until the ioloop is stopped
        logging.getLogger(__name__).debug("End of 'Publications' thread")

    @coroutine
    def _do_start(self):
        # todo: implement failover
        # try to connect to the remote syslog
        logger = logging.getLogger(__name__)
        self.syslog_or_relp_client = client_factory(
            protocol=self.syslog_agent_config.protocol,
            servr=self.syslog_agent_config.host,
            port=self.syslog_agent_config.port,
            use_ssl=self.syslog_agent_config.tls,
            verify_cert=self.syslog_agent_config.verify_server_cert,
            hostname=self.syslog_agent_config.tls_hostname,
            server_deadline=self.syslog_agent_config.server_deadline
        )
        if self.stopping.is_set():
            return
        try:
            self.closed_connection_event = yield self.syslog_or_relp_client.start()
        except (socket.error, TimeoutError):
            logger.error("Syslog agent: can't connect to remote syslog server")
            yield sleep(60, threading_event=self.stopping)
            if not self.stopping.is_set():
                yield sleep(60, self.stopping)
                if not self.stopping.is_set():
                    IOLoop.current().add_callback(self._do_start)
            return
        except ServerClose:
            logger.critical("Syslog agent: remote syslog unexpectedly closed the connection")
            if not self.stopping.is_set():
                yield sleep(60, self.stopping)
                if not self.stopping.is_set():
                    IOLoop.current().add_callback(self._do_start)
            return
        else:
            self.syslog_server_is_available.set()
            IOLoop.current().add_callback(self._wait_for_messages)
            # the next wait will return if self.stopping is set thanks to the end of _wait_for_messages
            yield self.closed_connection_event.wait()
            self.syslog_server_is_available.clear()
            if self.stopping.is_set():
                # shutdown
                IOLoop.current().stop()
                # the run_method will then return, terminating the publication thread
            else:
                # lost connection to the remote syslog server
                # we wait 1 minute before trying to reconnect
                yield sleep(60, threading_event=self.stopping)
                if not self.stopping.is_set():
                    IOLoop.current().add_callback(self._do_start)

    def _consume_relp_client_response(self, f):
        logger = logging.getLogger(__name__)
        status, event = f.result()
        if status:
            logger.debug("Syslog agent: Pushing event '%s' to published queue", event.uuid)
            self.published_messages_queue.put_nowait(event.lmdb_idx())
        else:
            logger.debug("Syslog agent: Pushing event '%s' to failed queue", event.uuid)
            self.failed_messages_queue.put_nowait(event.lmdb_idx())

    @coroutine
    def _wait_for_messages(self):
        logger = logging.getLogger(__name__)
        # wait for LMDB messages. we stop the loop if we are asked to stop, or if we lost the connection to
        # the remote syslog server
        compress = self.syslog_agent_config.compress
        while (not self.stopping.is_set()) and (not self.closed_connection_event.is_set()):
            try:
                idx, obj = yield self.publication_queue.get_wait(deadline=timedelta(seconds=1))
            except TimeoutError:
                continue
            try:
                ev = Event.load(obj)
            except ParsingError:
                logger.info("agent _wait_for_messages: parsing error, an event has been dropped")
                continue
            try:
                ev.verify_hmac()
            except InvalidSignature:
                logger.info("agent _wait_for_messages: invalid HMAC, an event has been dropped")
                continue
            logger.debug("Sending one event to remote syslog: {}".format(ev.uuid))
            future = self.syslog_or_relp_client.publish_event(
                event=ev,
                frmt=self.syslog_agent_config.frmt,
                compress=compress
            )
            IOLoop.current().add_future(future, self._consume_relp_client_response)

        if self.stopping.is_set():
            # we were asked to stop (shutdown) ==> close connection to remote server
            yield self.syslog_or_relp_client.stop()


class StoreMessagesInLMDB(Thread):
    """
    The `StoreMessagesInLMDB` thread gets messages from the TCP, UDP and unix sockets, via a queue, and pushes
    them to LMDB
    """
    def __init__(self, received_messages_queue, lmdb_db_name):
        super(StoreMessagesInLMDB, self).__init__(name="Store messages in LMDB")
        self.received_messages_queue = received_messages_queue
        self.lmdb_db_name = lmdb_db_name
        self.stopping = threading.Event()

    # noinspection PyDocstring
    def run(self):
        lmdb = LmdbWrapper.get_instance(self.lmdb_db_name)
        lmdb_queue = lmdb.queue("received_messages")
        logger = logging.getLogger(__name__)

        # we loop until we have been told to stop, and we have no more messages to handle
        while (not self.stopping.is_set()) or (not self.received_messages_queue.empty()):
            try:
                data = self.received_messages_queue.get(block=True, timeout=1)
            except QueueEmpty:
                continue
            try:
                event = Event.parse_bytes_to_event(data, hmac=True)
            except ParsingError:
                logger.exception("Syslog agent: can't parse received message")
                continue
            except InvalidSignature:
                logger.exception("Syslog agent: message had an invalid signature")
                continue
            # actually store message in LMDB
            logger.debug("Pushing event '%s' to LMDB", event.uid)
            lmdb_queue.push(event)
        logger = logging.getLogger(__name__)
        logger.debug("End of 'Store messages in LMDB' thread")


class RetrieveMessagesFromLMDB(Thread):
    """
    The `RetrieveMessagesFromLMDB` thread gets messages from LMDB and pushes them to the Publications thread, via a
    queue
    """
    def __init__(self, lmdb_db_name, publication_queue, published_messages_queue, failed_messages_queue,
                 syslog_server_is_available, pause):
        super(RetrieveMessagesFromLMDB, self).__init__(name="Retrieve messages from LMDB")
        self.lmdb_db_name = lmdb_db_name
        self.stopping = threading.Event()
        self.pending_idx = set()
        self.published_messages_queue = published_messages_queue
        self.failed_messages_queue = failed_messages_queue
        self.publication_queue = publication_queue
        self.syslog_server_is_available = syslog_server_is_available
        self.pause = pause * 60

    # noinspection PyDocstring
    def run(self):
        lmdb = LmdbWrapper.get_instance(self.lmdb_db_name)
        logger = logging.getLogger(__name__)
        with lmdb.queue("received_messages") as lmdb_queue:
            with lmdb.queue("failed_messages") as lmdb_failed_queue:
                while not self.stopping.is_set():

                    # push back messages from the failed queue
                    for idx, obj in lmdb_failed_queue.generator():
                        age = arrow.get(obj['time'])
                        if (arrow.utcnow() - age).total_seconds() > self.pause:
                            lmdb_queue.push(obj['obj'], idx=idx)
                            lmdb_failed_queue.delete(idx)

                    # only add messages to the publication queue if we actually have a working connection
                    # to the remote syslog server; this way we prevent publication_queue to grow indefinitely
                    if self.syslog_server_is_available.is_set():
                        # wait 2 seconds maximum for new messages to publish
                        if lmdb_queue.wait_not_empty(tick=1, timeout=4, exclude=self.pending_idx):
                            for idx, obj in lmdb_queue.generator(exclude=copy(self.pending_idx)):
                                self.pending_idx.add(idx)
                                # try to publish the message to the remote syslog server
                                logger.debug("One message in LMDB: {}".format(idx))
                                self.publication_queue.put((idx, obj))
                        else:
                            logger.debug("Syslog agent: no more events in LMDB")
                    else:
                        logger.debug("Syslog agent: syslog server not available, not pushing events")
                        time.sleep(2)

                    # get "published" notifications from the publication thread
                    while True:
                        try:
                            idx = self.published_messages_queue.get(block=False)
                        except QueueEmpty:
                            break
                        logger.debug("Got confirmation for '{}'".format(idx))
                        # now we can safely delete the published event from LMDB
                        lmdb_queue.delete(idx=idx)
                        self.pending_idx.remove(idx)

                    # get "failed" notifications from the publication thread
                    while True:
                        try:
                            idx = self.failed_messages_queue.get(block=False)
                        except QueueEmpty:
                            break
                        # we push the failed message in the waiting list
                        obj = lmdb_queue.pop(idx)
                        if obj:
                            lmdb_failed_queue.push(
                                {
                                    'time': str(arrow.utcnow()),
                                    'obj': obj
                                },
                                idx=idx
                            )
                        self.pending_idx.remove(idx)

        logger.debug("End of 'Retrieve messages from LMDB' thread")


class SyslogAgentClient(BaseSyslogClientConnection):
    """
    Handles TCP connections
    """
    def __init__(self, stream, address, syslog_parameters, received_messages):
        super(SyslogAgentClient, self).__init__(stream, address, syslog_parameters)
        self.received_messages = received_messages

    def _process_event(self, bytes_event, protocol, relp_event_id=None):
        """
        Handle TCP and RELP connections
        """
        logger = logging.getLogger(__name__)
        data = bytes_event.strip('\r\n ')
        if data:
            logger.debug("Syslog agent: got one event via %s", protocol)
            self.received_messages.put_nowait(data)

    def _process_group_events(self, bytes_events, relp_event_id):
        pass
