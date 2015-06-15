# encoding: utf-8

"""
Simplified queues
"""

__author__ = 'stef'

import threading
from collections import deque

from cytoolz import remove
# noinspection PyCompatibility
from concurrent.futures import Future as RealFuture
from tornado.ioloop import IOLoop
from tornado.gen import Task, TimeoutError
from tornado.concurrent import Future


class ThreadSafeQueue(object):
    """
    Simplified thread-safe/coroutine queue, without size limit
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.queue = deque()
        self._waiting = deque()

    def put(self, item):
        """
        Put an item on the queue

        :param item: item
        """
        with self.lock:
            # remove waiters that have expired
            self._waiting = deque(remove(lambda w: w.done(), self._waiting))
            if len(self._waiting) > 0:
                waiter = self._waiting.popleft()
                self.queue.append(item)
                waiter.set_result(self.queue.popleft())
            else:
                self.queue.append(item)

    def get_wait(self, deadline=None):
        """
        Wait for an available item and pop it from the queue

        :param deadline: optional deadline
        """
        f = RealFuture()

        def _expired():
            if not f.done():
                f.set_exception(TimeoutError())

        if deadline:
            IOLoop.current().add_timeout(deadline, _expired)

        with self.lock:
            if len(self.queue) > 0:
                f.set_result(self.queue.popleft())
            else:
                self._waiting.append(f)
            return f

    def get_all(self):
        """
        Pop all items from the queue, without waiting
        """
        with self.lock:
            if len(self.queue) == 0:
                return deque()
            l, self.queue = self.queue, deque()
            return l

    def get(self):
        """
        Pop one item from the queue, without waiting
        """
        with self.lock:
            if len(self.queue) == 0:
                return None
            return self.queue.popleft()


class SimpleToroQueue(object):
    """
    Simplified Toro queue without size limit
    """

    def __init__(self, io_loop=None):
        self.io_loop = io_loop or IOLoop.current()
        self.getters = deque()
        self.queue = deque()

    def qsize(self):
        """
        Return number of items in the queue
        """
        return len(self.queue)

    def empty(self):
        """
        Return ``True`` if the queue is empty, ``False`` otherwise
        """
        return not self.queue

    def put(self, item):
        """
        Put an item into the queue (without waiting)

        :param item: item to add
        """
        self.getters = deque(remove(lambda gettr: gettr.done(), self.getters))
        if self.getters:
            getter = self.getters.popleft()
            self.queue.append(item)
            getter.set_result(self.queue.popleft())
        else:
            self.queue.append(item)

    def get_wait(self, deadline=None):
        """
        Remove and return an item from the queue. Returns a Future.

        The Future blocks until an item is available, or raises :exc:`toro.Timeout`.

        :param deadline: Optional timeout, either an absolute timestamp (as returned by ``io_loop.time()``) or a
        ``datetime.timedelta`` for a deadline relative to the current time.
        """
        f = Future()

        def _expired():
            if not f.done():
                f.set_exception(TimeoutError())

        if deadline:
            IOLoop.current().add_timeout(deadline, _expired)

        if len(self.queue) > 0:
            f.set_result(self.queue.popleft())
        else:
            self.getters.append(f)
        return f

    def get_nowait(self):
        """
        Remove and return an item from the queue without blocking.

        Return an item if one is immediately available, else raise :exc:`queue.Empty`.
        """
        if self.qsize():
            return self.queue.popleft()
        else:
            return None

    def get_all(self):
        """
        Remove ans return all items from the queue, without blocking
        """
        if self.qsize():
            l, self.queue = self.queue, deque()
            return l
        else:
            return deque()


def TimeoutTask(func, deadline=None, *args, **kwargs):
    """
    Encapsulate a Tornado Task with a deadline
    """
    f = Future()

    def _expired():
        if not f.done():
            f.set_exception(TimeoutError())

    def _done(task):
        res = task.result()
        if not f.done():
            f.set_result(res)

    future_task = Task(func, *args, **kwargs)
    future_task.add_done_callback(_done)
    if deadline:
        IOLoop.current().add_timeout(deadline, _expired)
    return f
