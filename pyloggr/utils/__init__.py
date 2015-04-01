# encoding: utf-8

"""
The `utils` subpackage provides various tools used by other packages.

--------------------

"""

__author__ = 'stef'

from os.path import join, exists
import os
from distutils.util import strtobool

from tornado.concurrent import Future
from tornado.ioloop import IOLoop


def sleep(duration):
    f = Future()

    def nothing():
        f.set_result(None)

    timeout = IOLoop.current().call_later(duration, nothing)
    timeout.callback.sleeper = True
    return f


def remove_pid_file(name):
    from pyloggr.config import PIDS_DIRECTORY
    pid_file = join(PIDS_DIRECTORY, name + u".pid")
    if exists(pid_file):
        try:
            os.remove(pid_file)
        except OSError:
            pass

def ask_question(question):
    answer = raw_input(question + ' ').strip().lower()
    try:
        answer = strtobool(answer)
    except ValueError:
        answer = False
    return answer
