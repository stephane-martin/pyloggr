# encoding: utf-8

"""
Utility script to start the pyloggr's processes.
"""

__author__ = 'stef'

import os
import sys
from os.path import exists, expanduser, join
from signal import SIGTERM

import psutil
from argh.helpers import ArghParser
from argh.exceptions import CommandError


def set_config_env(config_dir):
    """
    Set up configuration

    :param config_dir: configuration directory
    :type config_dir: str
    :raise RuntimeError: when configuration failed
    """
    config_env = os.environ.get('PYLOGGR_CONFIG_DIR')
    if not config_env:
        if config_dir:
            os.environ['PYLOGGR_CONFIG_DIR'] = config_dir
        else:
            os.environ['PYLOGGR_CONFIG_DIR'] = expanduser('~/.pyloggr')

    config_env = os.environ.get('PYLOGGR_CONFIG_DIR')
    if not exists(config_env):
        raise CommandError("Config directory '{}' doesn't exists".format(config_env))


def check_pid(name):
    from pyloggr.config import PIDS_DIRECTORY
    if not exists(PIDS_DIRECTORY):
        try:
            os.makedirs(PIDS_DIRECTORY)
        except OSError:
            raise CommandError("PID directory '{}' is not writable".format(PIDS_DIRECTORY))
    pid_file = join(PIDS_DIRECTORY, name + u".pid")
    if exists(pid_file):
        try:
            with open(pid_file) as f:
                pid = f.read()
        except OSError:
            raise CommandError("PID file '{}' is not readable".format(pid_file))
        try:
            pid = int(pid)
        except ValueError:
            raise CommandError("PID file '{}' doesn't contain a PID".format(pid_file))
        try:
            process = psutil.Process(pid)
        except psutil.NoSuchProcess:
            os.remove(pid_file)
            return None
        else:
            return process


def store_pid(name):
    from pyloggr.config import PIDS_DIRECTORY
    pid_file = join(PIDS_DIRECTORY, name + u".pid")
    try:
        with open(pid_file, 'w') as f:
            f.write(str(os.getpid()))
    except OSError:
        raise CommandError("Error trying to write PID file '{}'".format(pid_file))


def setup(process, config_dir):
    # after set_config_env we can start to import configuration
    set_config_env(config_dir)
    if process not in pyloggr_process:
        raise CommandError("Unknown process. Please choose in {}".format(', '.join(pyloggr_process)))


def get_process_object(process):
    from pyloggr.scripts.processes import SyslogProcess, FrontendProcess, ParserProcess, PgSQLShipperProcess
    dispatcher = {
        'frontend': FrontendProcess,
        'syslog': SyslogProcess,
        'pgsql_shipper': PgSQLShipperProcess,
        'parser': ParserProcess
    }
    return dispatcher[process]


def _run(process_object, name):
    from pyloggr.config import set_logging, LOGGING_FILES
    set_logging(getattr(LOGGING_FILES, name))
    process_object(name).main()


def run(process, config_dir=None):
    """
    Starts a pyloggr process in foreground

    :param process: 'frontend', 'syslog', 'pgsql_shipper', 'parser
    :param config_dir: optional configuration directory
    """
    setup(process, config_dir)

    if check_pid(process) is not None:
        raise CommandError("'{}' is already running".format(process))
    store_pid(process)
    process_object = get_process_object(process)
    try:
        _run(process_object, process)
    finally:
        from pyloggr.utils import remove_pid_file
        remove_pid_file(process)


def run_daemon(process, config_dir=None):
    """
    Starts a pyloggr process as a Unix daemon

    Note
    ====
    This is not recommended. You should use supervisord and the `run` command.

    :param process: 'frontend', 'syslog', 'pgsql_shipper', 'parser'
    :param config_dir: optional configuration directory
    """
    import daemon
    from os.path import expanduser
    context = daemon.DaemonContext()
    context.umask = 0o022
    context.working_directory = expanduser('~')
    context.prevent_core = True
    context.files_preserve = None
    context.detach_process = True
    setup(process, config_dir)
    if check_pid(process) is not None:
        raise CommandError("'{}' is already running".format(process))

    with context:
        # we store the PID after the double-fork has been done
        store_pid(process)
        process_object = get_process_object(process)
        _run(process_object, process)


def stop_daemon(process, config_dir=None):
    """
    Stops a daemonized pyloggr process
    :param process: 'frontend', 'syslog', 'pgsql_shipper', 'parser'
    """
    setup(process, config_dir)
    p = check_pid(process)
    if p is None:
        raise CommandError("'{}' is not running".format(process))
    p.send_signal(SIGTERM)
    p.wait()
    sys.stdout.write("{} has been stopped".format(process))


def init_db():
    pass


def init_rabbitmq():
    pass


def status():
    pass

pyloggr_process = ['frontend', 'syslog', 'pgsql_shipper', 'parser']


def main():
    p = ArghParser()
    p.add_commands([run, run_daemon, stop_daemon, status, init_db, init_rabbitmq])
    try:
        p.dispatch()
    except RuntimeError as ex:
        sys.stderr.write(str(ex) + '\n')
        sys.exit(1)


if __name__ == '__main__':
    main()
