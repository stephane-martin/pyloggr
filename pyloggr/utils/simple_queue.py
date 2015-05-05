# encoding: utf-8

"""
Blabla
"""

__author__ = 'stef'

import threading
from collections import deque
from functools import partial

from queue import Empty
from tornado.ioloop import IOLoop
from toro import _TimeoutFuture, _consume_expired_waiters


class ThreadSafeQueue11(object):
    def __init__(self):
        self.lock = threading.Lock()
        self.queue = deque()

    def put(self, item):
        with self.lock:
            self.queue.append(item)

    def put_many(self, items):
        with self.lock:
            self.queue.extend(items)

    def get_all(self):
        with self.lock:
            if not len(self.queue):
                return []
            l, self.queue = self.queue, deque()
            return l

    def get(self):
        with self.lock:
            if not len(self.queue):
                return None
            return self.queue.popleft()


class SimpleToroQueue(object):

    def __init__(self, io_loop=None):
        self.io_loop = io_loop or IOLoop.current()
        self.getters = deque()
        self.queue = deque()

    def qsize(self):
        """Number of items in the queue"""
        return len(self.queue)

    def empty(self):
        """Return ``True`` if the queue is empty, ``False`` otherwise."""
        return not self.queue

    def put_nowait(self, item):
        """
        Put an item into the queue.
        """
        _consume_expired_waiters(self.getters)
        if self.getters:
            assert not self.queue, "queue non-empty, why are getters waiting?"
            getter = self.getters.popleft()
            self.queue.append(item)
            getter.set_result(self.queue.popleft())
        else:
            self.queue.append(item)

    def get(self, deadline=None):
        """Remove and return an item from the queue. Returns a Future.

        The Future blocks until an item is available, or raises
        :exc:`toro.Timeout`.

        :Parameters:
          - `deadline`: Optional timeout, either an absolute timestamp
            (as returned by ``io_loop.time()``) or a ``datetime.timedelta`` for a
            deadline relative to the current time.
        """
        future = _TimeoutFuture(deadline, self.io_loop)
        if self.qsize():
            future.set_result(self.queue.popleft())
        else:
            self.getters.append(future)

        return future

    def get_nowait(self):
        """Remove and return an item from the queue without blocking.

        Return an item if one is immediately available, else raise
        :exc:`queue.Empty`.
        """
        if self.qsize():
            return self.queue.popleft()
        else:
            raise Empty

    def get_all_nowait(self):
        if self.qsize():
            l, self.queue = self.queue, deque()
            return l
        else:
            raise Empty
