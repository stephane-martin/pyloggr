# encoding: utf-8

"""
The `utils` subpackage provides various tools used by other packages.

--------------------

"""

__author__ = 'stef'

import logging
from os.path import join, exists, isdir, expanduser, abspath, dirname
import os
import re
import platform
from io import BytesIO

from distutils.util import strtobool
from future.builtins import input
from tornado.concurrent import Future
from tornado.ioloop import IOLoop
from unidecode import unidecode

from .fix_unicode import to_unicode, to_bytes


def sleep(duration, wake_event=None, threading_event=None):
    """
    Return a Future that just 'sleeps'. The Future can be interrupted in case of process shutdown.

    :param duration: sleep time in seconds
    :param wake_event: optional event to wake the sleeper
    :param threading_event optional threading.Event to wake up the sleeper
    :type duration: int
    :type wake_event: `toro.Event`
    :rtype: Future
    """
    f = Future()

    def _nothing():
        if not f.done():
            f.set_result(None)

    def _wakeup(ringthebell=None):
        if not f.done():
            f.set_result(None)

    def _check_event():
        if not f.done():
            if threading_event.is_set():
                f.set_result(None)
            else:
                IOLoop.current().call_later(1, _check_event)

    if wake_event is not None:
        IOLoop.current().add_future(wake_event.wait(), _wakeup)
    if threading_event is not None:
        IOLoop.current().call_later(1, _check_event)
    timeout = IOLoop.current().call_later(duration, _nothing)
    timeout.callback.sleeper = True
    return f


def remove_pid_file(name):
    """
    Try to remove PID file

    :param name: PID name
    """
    from pyloggr.config import Config
    pid_file = join(Config.PIDS_DIRECTORY, name + u".pid")
    if exists(pid_file):
        try:
            os.remove(pid_file)
        except OSError as ex:
            if ex.errno == 2:
                logging.info("PID file '%s' not removed cause it does not exist", pid_file)
            else:
                logging.warning("Impossible to remove PID file '%s'", pid_file)

def write_pid_file(directory, process, uid=None, gid=None):
    pid_file = join(directory, process + u".pid")
    try:
        with open(pid_file, 'w') as f:
            f.write(str(os.getpid()))
    except OSError:
        raise OSError("Error trying to write PID file '%s'", pid_file)
    if uid is not None:
        try:
            os.chown(pid_file, uid, -1)
        except OSError:
            logging.warning("Impossible to change UID of file '%s' to '%s'", pid_file, uid)
    if gid is not None:
        try:
            os.chown(pid_file, -1, gid)
        except OSError:
            logging.warning("Impossible to change GID of file '%s' to '%s'", pid_file, gid)

def ask_question(question):
    """
    Ask a Y/N question on command line

    :param question: question text
    :return: user answer
    :rtype: bool
    """
    answer = input(question + ' ').strip().lower()
    try:
        answer = strtobool(answer)
    except ValueError:
        answer = False
    return answer


def chown_r(path, uid, gid):
    """
    Recursively chown a directory

    :param path: directory path
    :param uid: numeric UID
    :param gid: numeric GID
    """
    try:
        os.chown(path, uid, gid)
    except OSError:
        pass
    for root, dirs, files in os.walk(path):
        for d in dirs:
            try:
                os.chown(os.path.join(root, d), uid, gid)
            except OSError:
                pass
        for f in files:
            try:
                os.chown(os.path.join(root, f), uid, gid)
            except OSError:
                pass


def make_dir_r(path):
    """
    Recursively create directories

    :param path: directory to create
    :return: list of created directories
    """
    created_paths = []

    def _make(p):
        if exists(p):
            return
        _make(dirname(p))
        os.mkdir(p)
        created_paths.append(p)

    _make(path)
    return created_paths


def check_directory(dname, uid=None, gid=None, create=True):
    """
    Checks that directory `dname` exists (we create it if needed), is really a directory, and is writeable

    :param dname: directory name
    :return: absolute directory name
    :rtype: str
    :raise OSError: when tests fail
    """
    dname = abspath(expanduser(dname))
    created = []
    if exists(dname):
        if not isdir(dname):
            raise ValueError("'{}' exists but is not a directory".format(dname))
        if not os.access(dname, os.W_OK | os.X_OK):
            raise ValueError("'{}' is not writeable".format(dname))
    elif create:
        created = make_dir_r(dname)
    else:
        raise ValueError("'{}' does not exist".format(dname))

    if uid is not None:
        chown_r(dname, uid, -1)
        for p in created:
            chown_r(p, uid, -1)
    if gid is not None:
        chown_r(dname, -1, gid)
        for p in created:
            chown_r(p, -1, gid)
    return dname


_not_allowed_chars_in_key = re.compile('[=\\s\\[\\]\\(\\)\\?,\'"]')
_not_allowed_chars_in_tag = re.compile('[=\\(\\)!^¨*,"\']')
_escape_re = re.compile(r'([="\]])')


def sanitize_key(key):
    """
    Remove unwanted chars (=, spaces, quote marks, [], (), commas) from key. Convert to pure ASCII.

    :param key: key
    :return: sanitized key
    :rtype: unicode
    """
    return (_not_allowed_chars_in_key.sub(u'', unidecode(to_unicode(key)))).decode('ascii')


def sanitize_tag(tag):
    """
    Remove unwanted chars from tag

    :param tag: tag
    :return: sanitized tag
    :rtype: unicode
    """
    return _not_allowed_chars_in_tag.sub(u'', to_unicode(tag))


def change_user(uid=None, gid=None):
    security = logging.getLogger('security')
    try:
        if gid is not None:
            os.setregid(gid, gid)
            security.info("Changed GID to %s", gid)
        if uid is not None:
            os.setreuid(uid, uid)
            security.info("Changed UID to %s", uid)
    except OSError:
        security.error("Impossible to change UID, GID to '%s', '%s'", uid, gid)


def drop_capabilities(all=True):
    """
    Drop capabilities on Linux in case pyloggr runs as root. Only keep "net_bind_service" and "syslog".
    """
    if platform.system().lower().strip() == 'linux':
        try:
            # noinspection PyPackageRequirements,PyUnresolvedReferences
            import prctl
        except ImportError:
            print("Warning: prctl module not found. Can't drop capabilities")
        else:
            l = ['net_bind_service', 'syslog']
            if not all:
                l.extend(['setuid', 'setgid'])
            l = ','.join(l)
            try:
                #prctl.cap_permitted.limit(l)
                prctl.cap_inheritable.limit(l)
                prctl.cap_effective.limit(l)
            except ValueError:
                print("Warning: exception occured while dropping capabilities")
            else:
                print("Only keeping capabilities: " + ",".join(l))
                return True


def drop_caps_or_change_user(uid=None, gid=None):
    if uid is not None and gid is not None:
        change_user(uid, gid)
    try:
        drop_capabilities(all=True)
    except:
        pass


def read_next_token_in_stream(stream):
    """
    Reads the stream until we get a space delimiter
    """
    spaces = set(' \r\n')
    tok = BytesIO()
    while True:
        c = stream.read(1)
        if not c:
            break
        if c in spaces:
            v = tok.getvalue().strip(' \r\n')
            if v:
                tok.close()
                tok = BytesIO()
                yield v
        else:
            tok.write(c)
