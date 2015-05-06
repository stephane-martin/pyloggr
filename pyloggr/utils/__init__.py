# encoding: utf-8

"""
The `utils` subpackage provides various tools used by other packages.

--------------------

"""

__author__ = 'stef'

from os.path import join, exists, isdir, expanduser, abspath
import os
from distutils.util import strtobool

from future.builtins import input
from tornado.concurrent import Future
from tornado.ioloop import IOLoop

from .fix_unicode import to_unicode


def sleep(duration):
    f = Future()

    def nothing():
        f.set_result(None)

    timeout = IOLoop.current().call_later(duration, nothing)
    timeout.callback.sleeper = True
    return f


def remove_pid_file(name):
    from pyloggr.config import Config
    pid_file = join(Config.PIDS_DIRECTORY, name + u".pid")
    if exists(pid_file):
        try:
            os.remove(pid_file)
        except OSError:
            pass


def ask_question(question):
    answer = input(question + ' ').strip().lower()
    try:
        answer = strtobool(answer)
    except ValueError:
        answer = False
    return answer


def check_directory(dname):
    """
    Checks that directory `dname` exists (we create it if needed), is really a directory, and is writeable

    :param dname: directory name
    :return: absolute directory name
    :rtype: str
    :raise OSError: when tests fail
    """
    dname = abspath(expanduser(dname))
    if exists(dname):
        if not isdir(dname):
            raise OSError("'{}' exists but is not a directory".format(dname))
        if not os.access(dname, os.W_OK | os.X_OK):
            raise OSError("'{}' is not writeable".format(dname))
    else:
        os.makedirs(dname)
    return dname

