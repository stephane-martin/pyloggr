# encoding: utf-8

"""
The `utils` subpackage provides various tools used by other packages.

--------------------

"""

__author__ = 'stef'

from os.path import join, exists, isdir, expanduser, abspath
import os
import re
from distutils.util import strtobool

from future.builtins import input
from tornado.concurrent import Future
from tornado.ioloop import IOLoop
from unidecode import unidecode

from .fix_unicode import to_unicode, to_bytes


def sleep(duration, shutting_down_event=None):
    """
    Return a Future that just 'sleeps'. The Future can be interrupted in case of process shutdown.

    :param duration: sleep time in seconds
    :rtype: Future
    """
    f = Future()

    def _nothing():
        if not f.done():
            f.set_result(None)

    def _check_event():
        if not f.done():
            if shutting_down_event.is_set():
                f.set_result(None)
            else:
                IOLoop.current().call_later(1, _check_event)

    if shutting_down_event is not None:
        _check_event()
    timeout = IOLoop.current().call_later(duration, _nothing)
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


_not_allowed_chars_in_key = re.compile(r'[=\s\]"]')
_not_allowed_chars_in_tag = re.compile('[=&\\(\\)!^¨*/;,\\?\\.:\\[\\]"\']')
_escape_re = re.compile(r'([="\]])')

def sanitize_key(key):
    return (_not_allowed_chars_in_key.sub(u'', unidecode(to_unicode(key)))).decode('ascii')

def sanitize_tag(tag):
    return _not_allowed_chars_in_tag.sub(u'', to_unicode(tag))

