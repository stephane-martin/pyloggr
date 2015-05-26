# encoding: utf-8

"""
This module defines the Cache class and the `cache` singleton. They are used to store and retrieve
data from Redis. For example, the syslog server process uses Cache to stores information about
currently connected syslog clients, so that the web frontend is able to display that information.

Clients should typically use the `cache` singleton, instead of the `Cache` class.

`Cache` initialization is done by `initialize` class method. The `initialize` method should be called
by launchers, at startup time.

Note
====

In a development environment, if Redis has not been started by the OS, Redis can be started directly by
pyloggr using configuration item ``REDIS['try_spawn_redis'] = True``



.. py:data:: cache

    Cache singleton
"""

__author__ = 'stef'

from tempfile import TemporaryFile
import logging
import time
import os
from os.path import exists, join, isfile, basename, dirname
import shutil

from subprocess32 import Popen
from redis import StrictRedis, RedisError
from ujson import dumps, loads

from pyloggr.config import Config
from pyloggr.event import Event, InvalidSignature, ParsingError

logger = logging.getLogger(__name__)
security_logger = logging.getLogger('security')

syslog_key = 'pyloggr.syslog.server.'
rescue_key = 'pyloggr.rescue_queue'


class CacheError(Exception):
    """
    Raised when some cache operation fails. Typically on Redis connection error.
    """
    pass


class SyslogCache(object):
    """
    Stores information about the running pyloggr's syslog processes in a Redis cache

    Parameters
    ----------
    redis_conn: :py:class:`StrictRedis`
        the Redis connection
    server_id: int
        The syslog process server_id
    """

    def __init__(self, redis_conn, server_id):
        assert(isinstance(redis_conn, StrictRedis))
        self.redis_conn = redis_conn
        self.server_id = server_id
        self.hash_name = syslog_key + '_' + str(server_id)

    @property
    def status(self):
        """
        Returns syslog process status

        :returns: Boolean or None (if Redis not available)
        """
        try:
            return self.redis_conn.hget(self.hash_name, 'status') == "True"
        except RedisError:
            return None

    @status.setter
    def status(self, new_status):
        """
        Set one syslog process's status

        :param new_status: True (running) or False (stopped)
        :type new_status: bool
        """
        try:
            if new_status:
                self.redis_conn.sadd(syslog_key, self.server_id)
            else:
                # syslog server has been shut down
                self.clients = []
                self.redis_conn.srem(syslog_key, self.server_id)
            self.redis_conn.hset(self.hash_name, 'status', str(new_status))
        except RedisError:
            pass

    @property
    def clients(self):
        """
        Return the list of clients for this syslog process

        :return: list of clients or None (Redis not available)
        """
        if not self.status:
            return []
        try:
            buf = self.redis_conn.hget(self.hash_name, 'clients')
            clients = loads(buf) if buf else []
            if clients:
                clients.sort(key=lambda client: client['id'])
                return clients
            return []
        except RedisError:
            return None

    @clients.setter
    def clients(self, new_clients):
        """
        Sets the list of clients for this syslog process

        :param new_clients: The list of clients
        :type new_clients: list of :py:class:`pyloggr.main.syslog_server.SyslogClientConnection`
        """
        try:
            if self.status:
                if hasattr(new_clients, 'values'):
                    self.redis_conn.hset(self.hash_name, 'clients', dumps(
                        [client.props for client in new_clients.values()]
                    ))
                else:
                    self.redis_conn.hset(self.hash_name, 'clients', dumps(
                        [client.props for client in new_clients]
                    ))
        except RedisError:
            pass

    @property
    def ports(self):
        """
        Return the list of ports that the syslog process listens on

        :returns: list of ports (numeric and socket name)
        :rtype: list
        """
        if not self.status:
            return []
        try:
            buf = self.redis_conn.hget(self.hash_name, 'ports')
            ports = loads(buf) if buf else []
            return ports
        except RedisError:
            return None

    @ports.setter
    def ports(self, ports):
        """
        Sets the list of ports that the syslog process listens on

        :param ports: list of ports
        :type ports: list
        """
        try:
            if self.status:
                self.redis_conn.hset(self.hash_name, 'ports', dumps(ports))
        except RedisError:
            pass


class SyslogServerList(object):
    """
    Encapsulates the list of syslog processes

    Arguments
    ---------
    redis_conn: :py:class:`StrictRedis`
        the Redis connection


    """
    def __init__(self, redis_conn):
        """
        :type redis_conn: StrictRedis
        """
        self.redis_conn = redis_conn
        self._syslog_servers_dict = dict()

    def __getitem__(self, server_id):
        """
        Return a SyslogCache object based on process id

        :param server_id: process id
        :rtype: SyslogCache
        """
        if server_id not in self._syslog_servers_dict:
            self._syslog_servers_dict[server_id] = SyslogCache(self.redis_conn, server_id)
        return self._syslog_servers_dict[server_id]

    def __setitem__(self, key, value):
        raise NotImplementedError

    def __delitem__(self, server_id):
        """
        Deletes a SyslogCache object (used when the syslog server shuts down)

        :param server_id: process id
        """
        hash_name = syslog_key + '_' + str(server_id)
        try:
            self.redis_conn.srem(syslog_key, server_id)
            self.redis_conn.delete(hash_name)
        except RedisError:
            pass

    def __len__(self):
        """
        Returns the number of syslog processes

        :returns: how many syslog processes, or None (Redis not available)
        :rtype: int
        """
        try:
            return self.redis_conn.scard(syslog_key)
        except RedisError:
            return None

    # noinspection PyDocstring
    def keys(self):
        try:
            keys = list(int(key) for key in self.redis_conn.smembers(syslog_key))
            keys.sort()
            return keys
        except RedisError:
            return None

    # noinspection PyDocstring
    def values(self):
        return [self[key] for key in self.keys()]

    def __iter__(self):
        k = self.keys()
        return iter(k) if k else iter([])


class Cache(object):
    """
    `Cache` class abstracts storage and retrieval from Redis

    Attributes
    ----------
    redis_conn: :py:class:`StrictRedis`
        underlying `StrictRedis` connection object (class variable)
    """
    redis_conn = None
    redis_child = None
    _temp_redis_output_file = None
    _syslog = None
    _rescue = None

    def __init__(self):
        pass

    @classmethod
    def _connect_to_redis(cls):
        cls.redis_conn = StrictRedis(
            host=Config.REDIS.host,
            port=Config.REDIS.port,
            password=Config.REDIS.password,
            decode_responses=False
        )
        try:
            cls.redis_conn.ping()
        except RedisError:
            if Config.REDIS.try_spawn_redis:
                # spawn a Redis server
                cls._temp_redis_output_file = TemporaryFile()
                try:
                    logger.info("Try to launch Redis instance")
                    cls.redis_child = Popen(
                        args=[Config.REDIS.path, Config.REDIS.config_file], close_fds=True,
                        stdout=cls._temp_redis_output_file, stderr=cls._temp_redis_output_file,
                        start_new_session=True
                    )
                    time.sleep(1)

                except OSError:
                    raise CacheError("Spawning Redis failed, please check REDIS.path")
                except ValueError:
                    raise CacheError("Spawning Redis failed because of invalid configuration")
                except:
                    raise CacheError("Connection to redis failed, and Redis spawning failed too")
            else:
                raise CacheError("Connection to redis failed. Maybe Redis is not running ?")

    @classmethod
    def initialize(cls):
        """
        Cache initialization.

        `initialize` tries to connect to redis and sets `redis_conn` class variable. If connection fails and
        ``REDIS['try_spawn_redis']`` is set, it also tries to spawn the Redis process.

        :raise CacheError: when redis initialization fails
        """
        if cls.redis_conn is None:
            cls._connect_to_redis()
        cls._syslog = SyslogServerList(cls.redis_conn)
        cls._rescue = RescueQueue(cls.redis_conn)

    @classmethod
    def shutdown(cls):
        """
        Shutdowns the Redis cache. If a Redis server was spawned, shutdowns it.
        """
        if cls.redis_child is None:
            return
        try:
            cls.redis_child.terminate()
        except OSError:
            pass
        cls.redis_child = None
        cls.redis_conn = None

    # noinspection PyDocstring
    @property
    def syslog_list(self):
        return self._syslog

    # noinspection PyDocstring
    @property
    def rescue(self):
        return self._rescue

    @property
    def available(self):
        """
        Tests if Redis is available

        :return: True if available
        :rtype: bool
        """
        try:
            self.redis_conn.ping()
        except (RedisError, AttributeError):
            return False
        return True


class RescueQueue(object):
    """
    Encapsulate a queue that can store events in Redis or on the filesystem
    """
    def __init__(self, redis_conn):
        """
        :type redis_conn: StrictRedis
        """
        self.redis_conn = redis_conn

    @staticmethod
    def _store_event_on_fs(event):
        """
        Store the given event in a file. Either parameter `event` or `bytes_event` should be given

        :param event: the event
        :type event: Event
        :raise OSError: when some problem happens with FS operations
        """

        fname = join(Config.RESCUE_QUEUE_DIRNAME, "event_" + event.uuid)
        if exists(fname):
            # an event with same UUID has already been stored on FS
            try:
                event_on_fs = Event.from_file(fname)
            except ParsingError:
                logger.error("_store_event_on_fs: an existing event on FS is not parsable. We delete it")
                try:
                    os.remove(fname)
                except OSError:
                    logger.exception("_store_event_on_fs: Error while deleting existing event on FS. Abort.")
                    raise
            except OSError:
                logger.exception("_store_event_on_fs: error while reading an existing event from FS. Abort.")
                raise
            except InvalidSignature:
                moved_fname = join(dirname(fname), '_invalid_signature_' + basename(fname) + '_invalid_signature_')
                security_logger.error(
                    "_store_event_on_fs: an existing event on FS has an invalid signature. We move it to '{}'.".format(
                        moved_fname
                    )
                )
                try:
                    shutil.move(fname, moved_fname)
                except OSError:
                    security_logger.exception("_store_event_on_fs: error while moving event. Abort.")
                    raise
            else:
                if event_on_fs == event:
                    return
                if exists(fname):
                    moved_fname = join(dirname(fname), '_duplicate_' + basename(fname) + '_duplicate_')
                    logger.error(
                        "_store_event_on_fs: a different event with same filename exists. We move it to '{}'".format(
                            moved_fname
                        )
                    )
                    try:
                        shutil.move(fname, moved_fname)
                    except OSError:
                        logger.exception("_store_event_on_fs: error while moving event. Abort.")
                        raise
        try:
            event.dump(frmt="JSON", fname=fname)
            return True
        except OSError:
            logger.exception("_store_event_on_fs: error while dumping the event to '{}'".format(fname))
            return False

    def append(self, event=None, bytes_event=None):
        """
        Append an event to the rescue queue

        :type event: pyloggr.event.Event
        :type bytes_event: bytes
        :return: True if the event has been successfully added to the rescue queue
        """
        if event is None and bytes_event is None:
            return True
        if bytes_event is None:
            try:
                event.verify_hmac()
            except InvalidSignature:
                security_logger.error("Append to cache: the given event has an invalid HMAC. Abort.")
                return False
            bytes_event = event.dump_json()
        if event is None:
            try:
                event = Event.parse_bytes_to_event(bytes_event, hmac=True)
            except InvalidSignature:
                security_logger.error("Append to cache: the given event has an invalid HMAC. Abort.")
                return False
            except ParsingError:
                logger.error("Append to cache: the given event is not parsable. Abort.")
                return False
        try:
            self.redis_conn.rpush(rescue_key, bytes_event)
        except RedisError:
            try:
                return self._store_event_on_fs(event)
            except OSError:
                return False
        return True

    def __len__(self):
        """
        Return the number of events in rescue queue (Redis + FS)

        :return: number of events
        :rtype: int
        """
        try:
            redis_len = self.redis_conn.llen(rescue_key)
        except RedisError:
            logger.exception("Redis not available when reading RescueQueue length")
            redis_len = 0
        try:
            fnames = [join(Config.RESCUE_QUEUE_DIRNAME, fname) for fname in os.listdir(Config.RESCUE_QUEUE_DIRNAME)]
            fnames = [fname for fname in fnames if isfile(fname) and basename(fname).startswith('event_')]
        except OSError:
            logger.exception("Exception happened when reading RescueQueue length")
            fnames = []

        return redis_len + len(fnames)

    @staticmethod
    def _read_all_events_from_fs():
        fnames = [join(Config.RESCUE_QUEUE_DIRNAME, fname) for fname in os.listdir(Config.RESCUE_QUEUE_DIRNAME)]
        fnames = [fname for fname in fnames if isfile(fname) and basename(fname).startswith('event_')]
        events = []
        for fname in fnames:
            try:
                event = Event.from_file(fname)
            except InvalidSignature:
                security_logger.error("read_all_events_from_fs: dropping one FS event with invalid signature")
            except ParsingError:
                logger.error("read_all_events_from_fs: dropping one unparsable FS event")
            except OSError:
                logger.error("read_all_events_from_fs: error while reading event from FS")
            else:
                try:
                    os.remove(fname)
                except OSError:
                    logger.error("read_all_events_from_fs: failed to delete '{}'".format(fname))
                else:
                    events.append(event)
        return events

    def event_generator(self):
        """
        Return a generator that yields events Redis and FS

        :return: generator
        """
        while True:
            try:
                bytes_event = self.redis_conn.lpop(rescue_key)
            except RedisError:
                logger.warning("RescueQueue.get_generator: Redis not available")
                # redis not available, next we check events on FS
                break
            else:
                if bytes_event is None:
                    # no more events in redis, next we check events on FS
                    break
                try:
                    yield Event.parse_bytes_to_event(bytes_event, hmac=True, json=True)
                except InvalidSignature:
                    security_logger.error("get_generator: Dropping one event from Redis with invalid signature")
                    continue
                except ParsingError:
                    logger.error("get_generator: Dropping one unparsable event")
                    continue

        # now let's get events from FS
        for event in self._read_all_events_from_fs():
            yield event

cache = Cache()
