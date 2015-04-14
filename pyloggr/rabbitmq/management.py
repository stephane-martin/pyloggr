# encoding: utf-8

"""
Pure python client to RabbitMQ management API
forked from https://github.com/bkjones/pyrabbit

Copyright (c) 2011, Brian K. Jones (bkjones@gmail.com)
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

* Neither the name of the author nor the names of its
contributors may be used to endorse or promote products derived from this
software without specific prior written permission
"""

import logging

from future import standard_library

standard_library.install_aliases()
# noinspection PyUnresolvedReferences
from urllib.parse import urljoin, quote
from requests_futures.sessions import FuturesSession
from requests.exceptions import ConnectionError, Timeout, RequestException
from tornado.gen import coroutine, Return
import ujson as json


class HTTPError(Exception):
    """
    An error response from the API server. This should be an
    HTTP error of some kind (404, 500, etc).
    """

    def __init__(self, content, status=None, reason=None, path=None, body=None):
        self.status = status  # HTTP status code
        self.reason = reason  # human readable HTTP status
        self.path = path
        self.body = body
        self.detail = None

        # Actual, useful reason for failure returned by RabbitMQ
        if content and content.get('reason'):
            self.detail = content['reason']

        self.output = "%s - %s (%s) (%s) (%s)" % (self.status,
                                                  self.reason,
                                                  self.detail,
                                                  self.path,
                                                  repr(self.body))

    def __str__(self):
        return self.output


class NetworkError(Exception):
    """
    Denotes a failure to communicate with the REST API
    """
    pass


class HTTPClient(object):
    def __init__(self, server, uname, passwd, timeout=10, threads=5, https=False):
        """
        Constructor

        :param server: `host:port` string denoting the location of the broker and the port for interfacing
        with its REST API.
        :type server: str
        :param uname: Username credential used to authenticate.
        :type uname: str
        :param passwd: Password used to authenticate with RabbitMQ API
        :type passwd: str
        :param timeout: number of seconds to wait for each call
        :type timeout: int
        :param threads: how many background threads to use
        :type threads: int
        """

        self.base_url = 'https://{}/api/'.format(server) if https else 'http://{}/api/'.format(server)
        self.client = FuturesSession(max_workers=threads)
        self.client.auth = (uname, passwd)
        self.timeout = timeout

    @coroutine
    def do_call(self, url, method, data=None, headers=None):
        """
        Send an HTTP request to the REST API.

        :param string url: A URL
        :param string method: The HTTP method (GET, POST, etc.) to use in the request.
        :param string data: A string representing any data to be sent in the body of the HTTP request.
        :param dictionary headers: `{header-name: header-value}` dictionary.

        """
        url = urljoin(self.base_url, url)
        logging.getLogger(__name__).debug("Call to RabbitMQ management API: {}".format(url))

        try:
            response = yield self.client.request(method=method, url=url, data=data, headers=headers)
        except Timeout:
            raise NetworkError("Timeout while trying to connect to RabbitMQ")
        except ConnectionError:
            raise NetworkError("Network problem: is RabbitMQ available ?")
        except RequestException as ex:
            raise NetworkError("Error: %s %s" % (type(ex), ex))

        try:
            content = response.json()
        except ValueError:
            content = None

        # 'success' HTTP status codes are 200-206
        if 200 <= response.status_code <= 206:
            if content:
                raise Return(content)
        else:
            raise HTTPError(content, response.status_code, response.content, url, data)


class APIError(Exception):
    """Denotes a failure due to unexpected or invalid
    input/output between the client and the API

    """
    pass


class PermissionError(Exception):
    """
    Raised if the operation requires admin permissions, and the user used to
    instantiate the Client class does not have admin privileges.
    """
    pass


class Client(object):
    """
    Abstraction of the RabbitMQ Management HTTP API.

    HTTP calls are delegated to the  HTTPClient class for ease of testing,
    cleanliness, separation of duty, flexibility, etc.
    """
    urls = {'overview': 'overview',
            'all_queues': 'queues',
            'all_exchanges': 'exchanges',
            'all_channels': 'channels',
            'all_connections': 'connections',
            'all_nodes': 'nodes',
            'all_vhosts': 'vhosts',
            'all_users': 'users',
            'all_permissions': 'permissions',
            'all_bindings': 'bindings',
            'whoami': 'whoami',
            'queues_by_vhost': 'queues/%s',
            'queues_by_name': 'queues/%s/%s',
            'exchanges_by_vhost': 'exchanges/%s',
            'exchange_by_name': 'exchanges/%s/%s',
            'live_test': 'aliveness-test/%s',
            'purge_queue': 'queues/%s/%s/contents',
            'channels_by_name': 'channels/%s',
            'connections_by_name': 'connections/%s',
            'bindings_by_source_exch': 'exchanges/%s/%s/bindings/source',
            'bindings_by_dest_exch': 'exchanges/%s/%s/bindings/destination',
            'bindings_on_queue': 'queues/%s/%s/bindings',
            'bindings_between_exch_queue': 'bindings/%s/e/%s/q/%s',
            'rt_bindings_between_exch_queue': 'bindings/%s/e/%s/q/%s/%s',
            'get_from_queue': 'queues/%s/%s/get',
            'publish_to_exchange': 'exchanges/%s/%s/publish',
            'vhosts_by_name': 'vhosts/%s',
            'vhost_permissions': 'permissions/%s/%s',
            'users_by_name': 'users/%s',
            'user_permissions': 'users/%s/permissions',
            'vhost_permissions_get': 'vhosts/%s/permissions'
            }

    json_headers = {"content-type": "application/json"}

    def __init__(self, host, user, passwd, timeout=5):
        """
        :param string host: string of the form 'host:port'
        :param string user: username used to authenticate to the API.
        :param string passwd: password used to authenticate to the API.

        Populates server attributes using passed-in parameters and
        the HTTP API's 'overview' information. It also instantiates
        an httplib2 HTTP client and adds credentia    ls

        """
        self.host = host
        self.user = user
        self.passwd = passwd
        self.timeout = timeout
        self.http = HTTPClient(
            self.host,
            self.user,
            self.passwd,
            self.timeout
        )

    @coroutine
    def is_alive(self, vhost='%2F'):
        """
        Uses the aliveness-test API call to determine if the
        server is alive and the vhost is active. The broker (not this code)
        creates a queue and then sends/consumes a message from it.

        :param string vhost: There should be no real reason to ever change
            this from the default value, but it's there if you need to.
        :returns bool: True if alive, False otherwise
        :raises: HTTPError if *vhost* doesn't exist on the broker.

        """
        uri = Client.urls['live_test'] % vhost

        try:
            resp = yield self.http.do_call(uri, 'GET')
        except HTTPError as err:
            if err.status == 404:
                raise APIError("No vhost named '%s'" % vhost)
            raise

        raise Return(resp['status'] == 'ok')

    @coroutine
    def get_whoami(self):
        """
        A convenience function used in the event that you need to confirm that
        the broker thinks you are who you think you are.

        :returns dict whoami: Dict structure contains:
            * administrator: whether the user is has admin privileges
            * name: user name
            * auth_backend: backend used to determine admin rights
        """
        path = Client.urls['whoami']
        whoami = yield self.http.do_call(path, 'GET')
        raise Return(whoami)

    @coroutine
    def get_overview(self):
        """
        :rtype: dict

        Data in the 'overview' depends on the privileges of the creds used,
        but typically contains information about the management plugin version,
        some high-level message stats, and aggregate queue totals. Admin-level
        creds gets you information about the cluster node, listeners, etc.


        """
        overview = yield self.http.do_call(Client.urls['overview'], 'GET')
        raise Return(overview)

    @coroutine
    def get_nodes(self):
        """
        :rtype: dict

        Returns a list of dictionaries, each containing the details of each
        node of the cluster.


        """
        nodes = yield self.http.do_call(Client.urls['all_nodes'], 'GET')
        raise Return(nodes)

    @coroutine
    def get_users(self):
        """
        Returns a list of dictionaries, each containing the attributes of a
        different RabbitMQ user.

        :returns: a list of dictionaries, each representing a user. This
              method is decorated with '@needs_admin_privs', and will raise
              an error if the credentials used to set up the broker connection
              do not have admin privileges.

        """

        users = yield self.http.do_call(Client.urls['all_users'], 'GET')
        raise Return(users)

    # VHOSTS

    @coroutine
    def get_all_vhosts(self):
        """
        Lists the names of all RabbitMQ vhosts.

        :returns: a list of dicts, each dict representing a vhost
                on the broker.

        """
        vhosts = yield self.http.do_call(Client.urls['all_vhosts'], 'GET')
        raise Return(vhosts)

    @coroutine
    def get_vhost_names(self):
        """
        A convenience function for getting back only the vhost names instead of
        the larger vhost dicts.

        :returns list vhost_names: A list of just the vhost names.
        """
        vhosts = yield self.get_all_vhosts()
        vhost_names = [i['name'] for i in vhosts]
        raise Return(vhost_names)

    @coroutine
    def get_vhost(self, vname):
        """
        Returns the attributes of a single named vhost in a dict.

        :param string vname: Name of the vhost to get.
        :returns dict vhost: Attribute dict for the named vhost

        """

        vname = quote(vname, '')
        path = Client.urls['vhosts_by_name'] % vname
        vhost = yield self.http.do_call(path, 'GET', headers=Client.json_headers)
        raise Return(vhost)

    @coroutine
    def create_vhost(self, vname):
        """
        Creates a vhost on the server to house exchanges.

        :param string vname: The name to give to the vhost on the server
        :returns: boolean
        """
        vname = quote(vname, '')
        path = Client.urls['vhosts_by_name'] % vname
        yield self.http.do_call(path, 'PUT', headers=Client.json_headers)

    @coroutine
    def delete_vhost(self, vname):
        """
        Deletes a vhost from the server. Note that this also deletes any
        exchanges or queues that belong to this vhost.

        :param string vname: Name of the vhost to delete from the server.
        """
        vname = quote(vname, '')
        path = Client.urls['vhosts_by_name'] % vname
        yield self.http.do_call(path, 'DELETE')

    # PERMISSIONS

    @coroutine
    def get_permissions(self):
        """
        :returns: list of dicts, or an empty list if there are no permissions.
        """
        path = Client.urls['all_permissions']
        conns = yield self.http.do_call(path, 'GET')
        raise Return(conns)

    @coroutine
    def get_vhost_permissions(self, vname):
        """
        :returns: list of dicts, or an empty list if there are no permissions.

        :param string vname: Name of the vhost to set perms on.
        """
        vname = quote(vname, '')
        path = Client.urls['vhost_permissions_get'] % (vname,)
        conns = yield self.http.do_call(path, 'GET')
        raise Return(conns)

    @coroutine
    def get_user_permissions(self, username):
        """
        :returns: list of dicts, or an empty list if there are no permissions.

        :param string username: User to set permissions for.
        """

        path = Client.urls['user_permissions'] % (username,)
        conns = yield self.http.do_call(path, 'GET')
        raise Return(conns)

    @coroutine
    def set_vhost_permissions(self, vname, username, config, rd, wr):
        """
        Set permissions for a given username on a given vhost. Both
        must already exist.

        :param string vname: Name of the vhost to set perms on.
        :param string username: User to set permissions for.
        :param string config: Permission pattern for configuration operations
            for this user in this vhost.
        :param string rd: Permission pattern for read operations for this user
            in this vhost
        :param string wr: Permission pattern for write operations for this user
            in this vhost.

        Permission patterns are regex strings. If you're unfamiliar with this,
        you should definitely check out this section of the RabbitMQ docs:

        http://www.rabbitmq.com/admin-guide.html#access-control
        """
        vname = quote(vname, '')
        body = json.dumps({"configure": config, "read": rd, "write": wr})
        path = Client.urls['vhost_permissions'] % (vname, username)
        res = yield self.http.do_call(path, 'PUT', body, headers=Client.json_headers)
        raise Return(res)

    @coroutine
    def delete_permission(self, vname, username):
        """
        Delete permission for a given username on a given vhost. Both
        must already exist.

        :param string vname: Name of the vhost to set perms on.
        :param string username: User to set permissions for.
        """
        vname = quote(vname, '')
        path = Client.urls['vhost_permissions'] % (vname, username)
        res = yield self.http.do_call(path, 'DELETE')
        raise Return(res)

    @coroutine
    def get_permission(self, vname, username):
        """
        :returns: dicts of permissions.

        :param string vname: Name of the vhost to set perms on.
        :param string username: User to set permissions for.
        """
        vname = quote(vname, '')
        path = Client.urls['vhost_permissions'] % (vname, username)
        res = yield self.http.do_call(path, 'GET')
        raise Return(res)

    # EXCHANGES

    @coroutine
    def get_exchanges(self, vhost=None):
        """
        :returns: A list of dicts
        :param string vhost: A vhost to query for exchanges, or None (default),
            which triggers a query for all exchanges in all vhosts.

        """
        if vhost:
            vhost = quote(vhost, '')
            path = Client.urls['exchanges_by_vhost'] % vhost
        else:
            path = Client.urls['all_exchanges']

        exchanges = yield self.http.do_call(path, 'GET')
        raise Return(exchanges)

    @coroutine
    def get_exchange(self, vhost, name):
        """
        Gets a single exchange which requires a vhost and name.

        :param string vhost: The vhost containing the target exchange
        :param string name: The name of the exchange
        :returns: dict

        """
        vhost = quote(vhost, '')
        name = quote(name, '')
        path = Client.urls['exchange_by_name'] % (vhost, name)
        exch = yield self.http.do_call(path, 'GET')
        raise Return(exch)

    @coroutine
    def create_exchange(self,
                        vhost,
                        name,
                        xtype,
                        auto_delete=False,
                        durable=True,
                        internal=False,
                        arguments=None):
        """
        Creates an exchange in the given vhost with the given name. As per the
        RabbitMQ API documentation, a JSON body also needs to be included that
        "looks something like this":

        {"type":"direct",
        "auto_delete":false,
        "durable":true,
        "internal":false,
        "arguments":[]}

        On success, the API returns a 204 with no content, in which case this
        function returns True. If any other response is received, it's raised.

        :param string vhost: Vhost to create the exchange in.
        :param string name: Name of the proposed exchange.
        :param string xtype: The AMQP exchange type.
        :param bool auto_delete: Whether or not the exchange should be
            dropped when the no. of consumers drops to zero.
        :param bool durable: Whether you want this exchange to persist a
            broker restart.
        :param bool internal: Whether or not this is a queue for use by the
            broker only.
        :param list arguments: If given, should be a list. If not given, an
            empty list is sent.

        """

        vhost = quote(vhost, '')
        name = quote(name, '')
        path = Client.urls['exchange_by_name'] % (vhost, name)
        base_body = {"type": xtype, "auto_delete": auto_delete,
                     "durable": durable, "internal": internal,
                     "arguments": arguments or list()}

        body = json.dumps(base_body)
        res = yield self.http.do_call(path, 'PUT', body, headers=Client.json_headers)
        raise Return(res)

    @coroutine
    def delete_exchange(self, vhost, name):
        """
        Delete the named exchange from the named vhost. The API returns a 204
        on success, in which case this method returns True, otherwise the
        error is raised.

        :param string vhost: Vhost where target exchange was created
        :param string name: The name of the exchange to delete.
        :returns bool: True on success.
        """
        vhost = quote(vhost, '')
        name = quote(name, '')
        path = Client.urls['exchange_by_name'] % (vhost, name)
        res = yield self.http.do_call(path, 'DELETE')
        raise Return(res)

    # QUEUES

    @coroutine
    def get_queues(self, vhost=None):
        """
        Get all queues, or all queues in a vhost if vhost is not None.
        Returns a list.

        :param string vhost: The virtual host to list queues for. If This is
                    None (the default), all queues for the broker instance
                    are returned.
        :returns: A list of dicts, each representing a queue.
        :rtype: list of dicts

        """
        if vhost:
            vhost = quote(vhost, '')
            path = Client.urls['queues_by_vhost'] % vhost
        else:
            path = Client.urls['all_queues']

        queues = yield self.http.do_call(path, 'GET')
        raise Return(queues if queues else list())

    @coroutine
    def get_queue(self, vhost, name):
        """
        Get a single queue, which requires both vhost and name.

        :param string vhost: The virtual host for the queue being requested.
            If the vhost is '/', note that it will be translated to '%2F' to
            conform to URL encoding requirements.
        :param string name: The name of the queue being requested.
        :returns: A dictionary of queue properties.
        :rtype: dict

        """
        vhost = quote(vhost, '')
        name = quote(name, '')
        path = Client.urls['queues_by_name'] % (vhost, name)
        queue = yield self.http.do_call(path, 'GET')
        raise Return(queue)

    @coroutine
    def get_queue_depth(self, vhost, name):
        """
        Get the number of messages currently in a queue. This is a convenience
         function that just calls :meth:`Client.get_queue` and pulls
         out/returns the 'messages' field from the dictionary it returns.

        :param string vhost: The vhost of the queue being queried.
        :param string name: The name of the queue to query.
        :returns: Number of messages in the queue
        :rtype: integer

        """
        vhost = quote(vhost, '')
        name = quote(name, '')
        path = Client.urls['queues_by_name'] % (vhost, name)
        queue = yield self.http.do_call(path, 'GET')
        depth = queue['messages']
        raise Return(depth)

    @coroutine
    def purge_queues(self, queues):
        """
        Purge all messages from one or more queues.

        :param list queues: A list of ('qname', 'vhost') tuples.
        :returns: True on success

        """
        for name, vhost in queues:
            vhost = quote(vhost, '')
            name = quote(name, '')
            path = Client.urls['purge_queue'] % (vhost, name)
            yield self.http.do_call(path, 'DELETE')

    @coroutine
    def purge_queue(self, vhost, name):
        """
        Purge all messages from a single queue. This is a convenience method
        so you aren't forced to supply a list containing a single tuple to
        the purge_queues method.

        :param string vhost: The vhost of the queue being purged.
        :param string name: The name of the queue being purged.
        :rtype: None

        """
        vhost = quote(vhost, '')
        name = quote(name, '')
        path = Client.urls['purge_queue'] % (vhost, name)
        res = yield self.http.do_call(path, 'DELETE')
        raise Return(res)

    @coroutine
    def create_queue(self, vhost, name, **kwargs):
        """
        Create a queue. The API documentation specifies that all of the body
        elements are optional, so this method only requires arguments needed
        to form the URI

        :param string vhost: The vhost to create the queue in.
        :param string name: The name of the queue

        More on these operations can be found at:
        http://www.rabbitmq.com/amqp-0-9-1-reference.html

        """

        vhost = quote(vhost, '')
        name = quote(name, '')
        path = Client.urls['queues_by_name'] % (vhost, name)

        body = json.dumps(kwargs)

        res = yield self.http.do_call(path, 'PUT', body, headers=Client.json_headers)
        raise Return(res)

    @coroutine
    def delete_queue(self, vhost, qname):
        """
        Deletes the named queue from the named vhost.

        :param string vhost: Vhost housing the queue to be deleted.
        :param string qname: Name of the queue to delete.

        Note that if you just want to delete the messages from a queue, you
        should use purge_queue instead of deleting/recreating a queue.
        """
        vhost = quote(vhost, '')
        qname = quote(qname, '')
        path = Client.urls['queues_by_name'] % (vhost, qname)
        res = yield self.http.do_call(path, 'DELETE', headers=Client.json_headers)
        raise Return(res)

    # CONNS/CHANS & BINDINGS

    @coroutine
    def get_connections(self):
        """
        :returns: list of dicts, or an empty list if there are no connections.
        """
        path = Client.urls['all_connections']
        conns = yield self.http.do_call(path, 'GET')
        raise Return(conns)

    @coroutine
    def get_connection(self, name):
        """
        Get a connection by name. To get the names, use get_connections.

        :param string name: Name of connection to get
        :returns dict conn: A connection attribute dictionary.

        """
        name = quote(name, '')
        path = Client.urls['connections_by_name'] % name
        conn = yield self.http.do_call(path, 'GET')
        raise Return(conn)

    @coroutine
    def delete_connection(self, name):
        """
        Close the named connection. The API returns a 204 on success,
        in which case this method returns True, otherwise the
        error is raised.

        :param string name: The name of the connection to delete.
        :returns bool: True on success.
        """
        name = quote(name, '')
        path = Client.urls['connections_by_name'] % name
        yield self.http.do_call(path, 'DELETE')

    @coroutine
    def get_channels(self):
        """
        Return a list of dicts containing details about broker connections.
        :returns: list of dicts
        """
        path = Client.urls['all_channels']
        chans = yield self.http.do_call(path, 'GET')
        raise Return(chans)

    @coroutine
    def get_channel(self, name):
        """
        Get a channel by name. To get the names, use get_channels.

        :param string name: Name of channel to get
        :returns dict conn: A channel attribute dictionary.

        """
        name = quote(name, '')
        path = Client.urls['channels_by_name'] % name
        chan = yield self.http.do_call(path, 'GET')
        raise Return(chan)

    @coroutine
    def get_bindings(self):
        """
        :returns: list of dicts

        """
        path = Client.urls['all_bindings']
        bindings = yield self.http.do_call(path, 'GET')
        raise Return(bindings)

    @coroutine
    def get_queue_bindings(self, vhost, qname):
        """
        Return a list of dicts, one dict per binding. The dict format coming
        from RabbitMQ for queue named 'testq' is:

        {"source":"sourceExch","vhost":"/","destination":"testq",
         "destination_type":"queue","routing_key":"*.*","arguments":{},
         "properties_key":"%2A.%2A"}
        """
        vhost = quote(vhost, '')
        qname = quote(qname, '')
        path = Client.urls['bindings_on_queue'] % (vhost, qname)
        bindings = yield self.http.do_call(path, 'GET')
        raise Return(bindings)

    @coroutine
    def create_binding(self, vhost, exchange, queue, rt_key=None, args=None):
        """
        Creates a binding between an exchange and a queue on a given vhost.

        :param string vhost: vhost housing the exchange/queue to bind
        :param string exchange: the target exchange of the binding
        :param string queue: the queue to bind to the exchange
        :param string rt_key: the routing key to use for the binding
        :param list args: extra arguments to associate w/ the binding.
        :returns: boolean
        """

        vhost = quote(vhost, '')
        exchange = quote(exchange, '')
        queue = quote(queue, '')
        body = json.dumps({'routing_key': rt_key, 'arguments': args or []})
        path = Client.urls['bindings_between_exch_queue'] % (vhost, exchange, queue)
        binding = yield self.http.do_call(path, 'POST', data=body, headers=Client.json_headers)
        raise Return(binding)

    @coroutine
    def delete_binding(self, vhost, exchange, queue, rt_key):
        """
        Deletes a binding between an exchange and a queue on a given vhost.

        :param string vhost: vhost housing the exchange/queue to bind
        :param string exchange: the target exchange of the binding
        :param string queue: the queue to bind to the exchange
        :param string rt_key: the routing key to use for the binding
        """

        vhost = quote(vhost, '')
        exchange = quote(exchange, '')
        queue = quote(queue, '')
        path = Client.urls['rt_bindings_between_exch_queue'] % (vhost, exchange, queue, rt_key)
        res = yield self.http.do_call(path, 'DELETE', headers=Client.json_headers)
        raise Return(res)
