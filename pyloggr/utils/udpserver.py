# encoding: utf-8

"""
UDP server for tornado
"""
from __future__ import absolute_import, division, print_function
__author__ = 'stef'

import socket
import errno
from tornado.ioloop import IOLoop
from tornado.netutil import set_close_exec, _DEFAULT_BACKLOG
from tornado.util import errno_from_exception
from tornado.iostream import IOStream
import sys
import os
import stat
import logging

from future.builtins import range

_ERRNO_WOULDBLOCK = (errno.EWOULDBLOCK, errno.EAGAIN)

class UDPServer(object):
    def add_udp_sockets(self, sockets):
        if self.io_loop is None:
            self.io_loop = IOLoop.current()

        for sock in sockets:
            self._sockets[sock.fileno()] = sock
            add_udp_accept_handler(sock, self._handle_data, io_loop=self.io_loop)

    def _handle_data(self, data, sockname, peername):
        try:
            self.handle_data(data, sockname, peername)
        except Exception:
            logging.getLogger(__name__).exception("Error dealing with UDP data")

    def handle_data(self, data, address, peername):
        raise NotImplementedError

def bind_udp_sockets(port, address=None, family=socket.AF_UNSPEC, flags=None):

    sockets = []
    if address == "":
        address = None
    if not socket.has_ipv6 and family == socket.AF_UNSPEC:
        family = socket.AF_INET
    if flags is None:
        flags = socket.AI_PASSIVE
    for res in set(socket.getaddrinfo(address, port, family, socket.SOCK_DGRAM,
                                      0, flags)):
        af, socktype, proto, canonname, sockaddr = res
        if (sys.platform == 'darwin' and address == 'localhost' and
                af == socket.AF_INET6 and sockaddr[3] != 0):
            continue
        try:
            sock = socket.socket(af, socktype, proto)
        except socket.error as e:
            if errno_from_exception(e) == errno.EAFNOSUPPORT:
                continue
            raise
        set_close_exec(sock.fileno())
        if os.name != 'nt':
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if af == socket.AF_INET6:
            if hasattr(socket, "IPPROTO_IPV6"):
                sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)

        sock.setblocking(0)
        sock.bind(sockaddr)
        #sock.listen(backlog)
        sockets.append(sock)
    return sockets


def bind_udp_unix_socket(file, mode=0o600):
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    set_close_exec(sock.fileno())
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.setblocking(0)
    try:
        st = os.stat(file)
    except OSError as err:
        if errno_from_exception(err) != errno.ENOENT:
            raise
    else:
        if stat.S_ISSOCK(st.st_mode):
            os.remove(file)
        else:
            raise ValueError("File %s exists and is not a socket", file)
    sock.bind(file)
    os.chmod(file, mode)
    #sock.listen(backlog)
    return sock


def add_udp_accept_handler(sock, callback, io_loop=None):
    if io_loop is None:
        io_loop = IOLoop.current()

    def accept_handler(fd, events):
        while True:
            try:
                data, address = sock.recvfrom(65536)
            except socket.error as e:
                if errno_from_exception(e) in _ERRNO_WOULDBLOCK:
                    return
                if errno_from_exception(e) == errno.ECONNABORTED:
                    continue
                #raise
                return
            callback(data, sock.getsockname(), address)
    io_loop.add_handler(sock, accept_handler, IOLoop.READ)
