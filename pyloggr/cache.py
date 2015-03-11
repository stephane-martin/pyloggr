# encoding: utf-8

"""
This module defines the Cache class and the 'cache' singleton. They are used to store and retrieve data from Redis.

.. py:data:: cache

    Cache singleton

"""

# todo: handle redis exceptions

import logging

from tornado.process import Subprocess
from redis import StrictRedis, RedisError
from ujson import dumps, loads

from .config import REDIS_CONFIG

__author__ = 'stef'
logger = logging.getLogger(__name__)


class SyslogCache(object):
    """
    Stores information about the running pyloggr's syslog services in a Redis cache

    Parameters
    ----------
    redis_conn: StrictRedis
        the Redis connection
    task_id: int
        The syslog process task_id
    """

    def __init__(self, redis_conn, task_id):
        assert(isinstance(redis_conn, StrictRedis))
        self.redis_conn = redis_conn
        self.task_id = task_id
        self.hash_name = "pyloggr.syslog" + str(task_id)

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

class SyslogList(object):
    def __init__(self, redis_conn):
        assert(isinstance(redis_conn, StrictRedis))
        self.redis_conn = redis_conn

    def __getitem__(self, task_id):
        return SyslogCache(self.redis_conn, task_id)

    def __setitem__(self, key, value):
        raise NotImplementedError

    def __delitem__(self, task_id):
        hash_name = "pyloggr.syslog" + str(task_id)
        self.redis_conn.delete(hash_name)

    def __len__(self):
        i = 0
        while True:
            if self.redis_conn.exists("pyloggr.syslog" + str(i)):
                i += 1
            else:
                break
        return i

    def keys(self):
        return range(self.__len__())

    def __iter__(self):
        res = [self[task_id] for task_id in self.keys()]
        return iter(res)

def launch_redis():
    pass


def connect_to_redis():
    redis_conn = StrictRedis(
        host=REDIS_CONFIG['host'],
        port=REDIS_CONFIG['port'],
        password=REDIS_CONFIG['password'],
        decode_responses=False
    )
    try:
        redis_conn.ping()
    except RedisError:
        if REDIS_CONFIG['try_spawn_redis']:
            try:
                launch_redis()
            except:
                return None
            else:
                return connect_to_redis()
    else:
        return redis_conn


class Cache(object):
    """
    Cache class abstracts storage and retrieve from redis.

    Attributes
    ----------
    redis_conn: StrictRedis
        underlying StrictRedis connection object (class variable)
    """
    redis_conn = None

    def __init__(self):
        self.syslog_status = dict()

    @classmethod
    def initialize(cls):
        if cls.redis_conn is None:
            cls.redis_conn = connect_to_redis()

    @property
    def syslog(self):
        return SyslogList(self.redis_conn)

    def save_in_rescue(self, bytes_event):
        try:
            self.redis_conn.rpush(REDIS_CONFIG['rescue_queue_name'], bytes_event)
        except RedisError:
            return None
        return True

    @property
    def rescue_queue_length(self):
        try:
            return self.redis_conn.llen(REDIS_CONFIG['rescue_queue_name'])
        except RedisError:
            return None

    @property
    def available(self):
        try:
            self.redis_conn.ping()
        except RedisError:
            return False
        return True

    def rescue_queue_generator(self):
        try:
            next_ev = self.redis_conn.lpop(REDIS_CONFIG['rescue_queue_name'])
            if next_ev is None:
                raise StopIteration
            yield next_ev
        except RedisError:
            raise StopIteration

cache = Cache()
