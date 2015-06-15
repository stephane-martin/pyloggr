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

import logging

from pyloggr.utils.lmdb_wrapper import LmdbWrapper
from pyloggr.config import Config

logger = logging.getLogger(__name__)
security_logger = logging.getLogger('security')

syslog_key = 'pyloggr.syslog.server.'
rescue_key = 'pyloggr.rescue_queue'


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

    def __init__(self, server_id):
        self.server_id = server_id
        self.hash_name = syslog_key + '.' + str(server_id)

    @property
    def status(self):
        """
        Returns syslog process status

        :returns: Boolean or None (if Redis not available)
        """
        lmdb = LmdbWrapper.get_instance(Config.EXCHANGE_SPACE)
        return lmdb.hash(self.hash_name)['status'] == "True"

    @status.setter
    def status(self, new_status):
        """
        Set one syslog process's status

        :param new_status: True (running) or False (stopped)
        :type new_status: bool
        """
        lmdb = LmdbWrapper.get_instance(Config.EXCHANGE_SPACE)
        if new_status:
            lmdb.set(syslog_key).add(self.server_id)
        else:
            # syslog server has been shut down
            self.clients = []
            lmdb.set(syslog_key).remove(self.server_id)
        lmdb.hash(self.hash_name)['status'] = str(new_status)

    @property
    def clients(self):
        """
        Return the list of clients for this syslog process

        :return: list of clients or None (Redis not available)
        """
        if not self.status:
            return []
        lmdb = LmdbWrapper.get_instance(Config.EXCHANGE_SPACE)
        clients = lmdb.hash(self.hash_name)['clients']
        clients = clients if clients else []
        if clients:
            clients.sort(key=lambda client: client['id'])
            return clients
        return []

    @clients.setter
    def clients(self, new_clients):
        """
        Sets the list of clients for this syslog process

        :param new_clients: The list of clients
        :type new_clients: list of :py:class:`pyloggr.main.syslog_server.SyslogClientConnection`
        """
        if self.status:
            lmdb = LmdbWrapper.get_instance(Config.EXCHANGE_SPACE)
            if hasattr(new_clients, 'values'):
                lmdb.hash(self.hash_name)['clients'] = [client.props for client in new_clients.values()]
            else:
                lmdb.hash(self.hash_name)['clients'] = [client.props for client in new_clients]

    @property
    def ports(self):
        """
        Return the list of ports that the syslog process listens on

        :returns: list of ports (numeric and socket name)
        :rtype: list
        """
        if not self.status:
            return []
        lmdb = LmdbWrapper.get_instance(Config.EXCHANGE_SPACE)
        ports = lmdb.hash(self.hash_name)['ports']
        return ports if ports else []

    @ports.setter
    def ports(self, ports):
        """
        Sets the list of ports that the syslog process listens on

        :param ports: list of ports
        :type ports: list
        """
        if self.status:
            lmdb = LmdbWrapper.get_instance(Config.EXCHANGE_SPACE)
            lmdb.hash(self.hash_name)['ports'] = ports


class SyslogServerList(object):
    """
    Encapsulates the list of syslog processes
    """
    def __init__(self):
        self._syslog_servers_dict = dict()

    def __getitem__(self, server_id):
        """
        Return a SyslogCache object based on process id

        :param server_id: process id
        :rtype: SyslogCache
        """
        if server_id not in self._syslog_servers_dict:
            self._syslog_servers_dict[server_id] = SyslogCache(server_id)
        return self._syslog_servers_dict[server_id]

    def __setitem__(self, key, value):
        raise NotImplementedError

    def __delitem__(self, server_id):
        """
        Deletes a SyslogCache object (used when the syslog server shuts down)

        :param server_id: process id
        """
        lmdb = LmdbWrapper.get_instance(Config.EXCHANGE_SPACE)
        hash_name = syslog_key + '_' + str(server_id)
        lmdb.set(syslog_key).remove(server_id)
        # self.redis_conn.delete(hash_name)

    def __len__(self):
        """
        Returns the number of syslog processes

        :returns: how many syslog processes, or None (Redis not available)
        :rtype: int
        """
        lmdb = LmdbWrapper.get_instance(Config.EXCHANGE_SPACE)
        return len(lmdb.set(syslog_key))

    # noinspection PyDocstring
    def keys(self):
        lmdb = LmdbWrapper.get_instance(Config.EXCHANGE_SPACE)
        keys = list(int(key) for key in lmdb.set(syslog_key).members())
        keys.sort()
        return keys

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
    syslog_list = None

    def __init__(self):
        pass

    @classmethod
    def initialize(cls):
        """
        Cache initialization.

        `initialize` tries to connect to redis and sets `redis_conn` class variable. If connection fails and
        ``REDIS['try_spawn_redis']`` is set, it also tries to spawn the Redis process.

        :raise CacheError: when redis initialization fails
        """
        cls.syslog_list = SyslogServerList()
        LmdbWrapper(Config.EXCHANGE_SPACE, size=52428800).open(
            sync=False, metasync=False, max_dbs=0
        )

    @classmethod
    def shutdown(cls):
        LmdbWrapper.get_instance(Config.EXCHANGE_SPACE).close()

