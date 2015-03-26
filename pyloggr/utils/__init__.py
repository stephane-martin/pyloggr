# encoding: utf-8

"""
The `utils` subpackage provides various tools used by other packages.

--------------------

"""

__author__ = 'stef'
from tornado.concurrent import Future
from tornado.ioloop import IOLoop


def sleep(duration):
    f = Future()

    def nothing():
        f.set_result(None)

    timeout = IOLoop.current().call_later(duration, nothing)
    timeout.callback.sleeper = True
    return f
