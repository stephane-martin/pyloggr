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
pyloggr using configuration item ``REDIS_CONFIG['try_spawn_redis'] = True``



.. py:data:: cache

    Cache singleton

"""

# todo: handle redis exceptions, so that redis is not mandatory for syslog server

__author__ = 'stef'

from tempfile import TemporaryFile
import logging
logger = logging.getLogger(__name__)

from subprocess32 import Popen
from redis import StrictRedis, RedisError
from ujson import dumps, loads

from .config import REDIS_CONFIG

syslog_key = 'pyloggr.syslog.server.'
rescue_key = 'pyloggr.rescue_queue'


class CacheError(Exception):
    """
    Raised when some cache operation fails. Typically on Redis connection error.
    """
    pass


class SyslogCache(object):
    """
    Stores information about the running pyloggr's syslog services in a Redis cache

    Parameters
    ----------
    redis_conn: :py:class:`StrictRedis`
        the Redis connection
    task_id: int
        The syslog process task_id
    """

    def __init__(self, redis_conn, task_id):
        assert(isinstance(redis_conn, StrictRedis))
        self.redis_conn = redis_conn
        self.task_id = task_id
        self.hash_name = syslog_key + str(task_id)

    @property
    def status(self):
        return self.redis_conn.hget(self.hash_name, 'status') == "True"

    @status.setter
    def status(self, new_status):
        if not new_status:
            # syslog server has been shut down
            self.clients = []

        self.redis_conn.hset(self.hash_name, 'status', str(new_status))

    @property
    def clients(self):
        buf = self.redis_conn.hget(self.hash_name, 'clients')
        return loads(buf) if buf else []

    @clients.setter
    def clients(self, new_clients):
        if self.status:
            if hasattr(new_clients, 'values'):
                self.redis_conn.hset(self.hash_name, 'clients', dumps(
                    [client.props for client in new_clients.values()]
                ))
            else:
                self.redis_conn.hset(self.hash_name, 'clients', dumps(
                    [client.props for client in new_clients]
                ))


class SyslogServerList(object):
    def __init__(self, redis_conn):
        """
        :type redis_conn: StrictRedis
        """
        self.redis_conn = redis_conn

    def __getitem__(self, task_id):
        return SyslogCache(self.redis_conn, task_id)

    def __setitem__(self, key, value):
        raise NotImplementedError

    def __delitem__(self, task_id):
        hash_name = syslog_key + str(task_id)
        self.redis_conn.delete(hash_name)

    def __len__(self):
        i = 0
        while True:
            if self.redis_conn.exists(syslog_key + str(i)):
                i += 1
            else:
                break
        return i

    def keys(self):
        return range(self.__len__())

    def __iter__(self):
        res = [self[task_id] for task_id in self.keys()]
        return iter(res)


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

    def __init__(self):
        self._rescue = None

    @classmethod
    def _connect_to_redis(cls):
        cls.redis_conn = StrictRedis(
            host=REDIS_CONFIG['host'],
            port=REDIS_CONFIG['port'],
            password=REDIS_CONFIG['password'],
            decode_responses=False
        )
        try:
            cls.redis_conn.ping()
        except RedisError:
            if REDIS_CONFIG['try_spawn_redis']:
                cls._temp_redis_output_file = TemporaryFile()
                try:
                    logger.info("Try to launch Redis instance")
                    cls.redis_child = Popen(
                        args=[REDIS_CONFIG['path'], REDIS_CONFIG['config_file']], close_fds=True,
                        stdout=cls._temp_redis_output_file, stderr=cls._temp_redis_output_file,
                        start_new_session=True
                    )
                except OSError:
                    raise CacheError("Spawning Redis failed, please check REDIS_CONFIG['path']")
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
        ``REDIS_CONFIG['try_spawn_redis']`` is set, it also tries to spawn the Redis process.

        :raise CacheError: when redis initialization fails
        """
        if cls.redis_conn is None:
            cls._connect_to_redis()

    @classmethod
    def shutdown(cls):
        if cls.redis_conn is None:
            return
        cls.redis_conn = None
        if cls.redis_child is None:
            return
        cls.redis_child.terminate()
        cls.redis_child = None

    @property
    def syslog(self):
        return SyslogServerList(self.redis_conn)

    @property
    def rescue(self):
        if self._rescue is None:
            self._rescue = RescueQueue(self.redis_conn)
        return self._rescue

    @property
    def available(self):
        try:
            self.redis_conn.ping()
        except RedisError:
            return False
        return True


class RescueQueue(object):
    def __init__(self, redis_conn):
        """
        :type redis_conn: StrictRedis
        """
        self.redis_conn = redis_conn

    def append(self, bytes_ev):
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
