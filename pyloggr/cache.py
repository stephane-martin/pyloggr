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

from subprocess32 import Popen
from redis import StrictRedis, RedisError
from ujson import dumps, loads

from pyloggr.config import Config

logger = logging.getLogger(__name__)

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
        try:
            return self.redis_conn.hget(self.hash_name, 'status') == "True"
        except RedisError:
            return None

    @status.setter
    def status(self, new_status):
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
        :type ports: list
        """
        try:
            if self.status:
                self.redis_conn.hset(self.hash_name, 'ports', dumps(ports))
        except RedisError:
            pass


class SyslogServerList(object):
    def __init__(self, redis_conn):
        """
        :type redis_conn: StrictRedis
        """
        self.redis_conn = redis_conn
        self._syslog_servers_dict = dict()

    def __getitem__(self, server_id):
        """
        :rtype: SyslogCache
        """
        if server_id not in self._syslog_servers_dict:
            self._syslog_servers_dict[server_id] = SyslogCache(self.redis_conn, server_id)
        return self._syslog_servers_dict[server_id]

    def __setitem__(self, key, value):
        raise NotImplementedError

    def __delitem__(self, server_id):
        hash_name = syslog_key + '_' + str(server_id)
        try:
            self.redis_conn.srem(syslog_key, server_id)
            self.redis_conn.delete(hash_name)
        except RedisError:
            pass

    def __len__(self):
        try:
            return self.redis_conn.scard(syslog_key)
        except RedisError:
            return None

    def keys(self):
        try:
            keys = list(int(key) for key in self.redis_conn.smembers(syslog_key))
            keys.sort()
            return keys
        except RedisError:
            return None

    def values(self):
        return [self[key] for key in self.keys()]

    def __iter__(self):
        k = self.keys()
        return iter(k) if k else iter([])


class Cache(object):
    """
    Cache class abstracts storage and retrieval from redis.

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
        if cls.redis_child is None:
            return
        try:
            cls.redis_child.terminate()
        except OSError:
            pass
        cls.redis_child = None
        cls.redis_conn = None

    @property
    def syslog_list(self):
        return self._syslog

    @property
    def rescue(self):
        return self._rescue

    @property
    def available(self):
        try:
            self.redis_conn.ping()
        except (RedisError, AttributeError):
            return False
        return True


class RescueQueue(object):
    def __init__(self, redis_conn):
        """
        :type redis_conn: StrictRedis
        """
        self.redis_conn = redis_conn

    def append(self, bytes_ev):
        # todo: refactor so that we can accept an Event instead of bytes
        try:
            self.redis_conn.rpush(rescue_key, bytes_ev)
        except RedisError:
            return False
        return True

    def __len__(self):
        try:
            return self.redis_conn.llen(rescue_key)
        except RedisError:
            return None

    def get_generator(self):
        try:
            next_ev = self.redis_conn.lpop(rescue_key)
            if next_ev is None:
                raise StopIteration
            yield next_ev
        except RedisError:
            raise StopIteration


cache = Cache()
