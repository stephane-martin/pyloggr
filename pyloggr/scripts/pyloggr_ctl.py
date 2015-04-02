# encoding: utf-8

"""
Utility script to manage the pyloggr's processes.
"""

from __future__ import print_function

__author__ = 'stef'

from io import open
import os
import sys
from os.path import exists, expanduser, join
from signal import SIGTERM

import psutil
from tornado.ioloop import IOLoop
from tornado.gen import coroutine
from argh.helpers import ArghParser
from argh.exceptions import CommandError
from pyloggr.utils import ask_question


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


def _run(process):
    from pyloggr.config import PIDS_DIRECTORY
    from pyloggr.scripts.processes import SyslogProcess, FrontendProcess, ParserProcess, PgSQLShipperProcess
    from pyloggr.config import set_logging, LOGGING_FILES
    from pyloggr.utils import remove_pid_file
    pid_file = join(PIDS_DIRECTORY, process + u".pid")
    try:
        with open(pid_file, 'w') as f:
            f.write(str(os.getpid()))
    except OSError:
        raise CommandError("Error trying to write PID file '{}'".format(pid_file))
    set_logging(getattr(LOGGING_FILES, process))
    dispatcher = {
        'frontend': FrontendProcess,
        'syslog': SyslogProcess,
        'pgsql_shipper': PgSQLShipperProcess,
        'parser': ParserProcess
    }
    try:
        dispatcher['name'](process).main()
    finally:
        remove_pid_file(process)


def run(process, config_dir=None):
    """
    Starts a pyloggr process in foreground.

    :param process: 'frontend', 'syslog', 'pgsql_shipper', 'parser
    :param config_dir: optional configuration directory
    """
    set_config_env(config_dir)
    if process not in pyloggr_process:
        raise CommandError("Unknown process. Please choose in {}".format(', '.join(pyloggr_process)))

    if check_pid(process) is not None:
        raise CommandError("'{}' is already running".format(process))

    _run(process)


def run_daemon(process, config_dir=None):
    """
    Starts a pyloggr process as a Unix daemon

    Note
    ====
    This is not recommended. You should use supervisord and the `run` command.

    :param process: 'frontend', 'syslog', 'pgsql_shipper', 'parser'
    :param config_dir: optional configuration directory
    """
    set_config_env(config_dir)
    if process not in pyloggr_process:
        raise CommandError("Unknown process. Please choose in {}".format(', '.join(pyloggr_process)))

    import daemon
    from os.path import expanduser
    context = daemon.DaemonContext()
    context.umask = 0o022
    context.working_directory = expanduser('~')
    context.prevent_core = True
    context.files_preserve = None
    context.detach_process = True

    if check_pid(process) is not None:
        raise CommandError("'{}' is already running".format(process))

    with context:
        # we store the PID after the double-fork has been done
        _run(process)


def stop(process, config_dir=None):
    """
    Stops a pyloggr process

    :param process: 'frontend', 'syslog', 'pgsql_shipper', 'parser'
    """
    set_config_env(config_dir)
    if process not in pyloggr_process:
        raise CommandError("Unknown process. Please choose in {}".format(', '.join(pyloggr_process)))

    p = check_pid(process)
    if p is None:
        raise CommandError("'{}' is not running".format(process))
    p.send_signal(SIGTERM)
    p.wait()
    sys.stdout.write("{} has been stopped".format(process))


def run_all(config_dir=None):
    """
    Starts all pyloggr processes

    Each pyloggr process is run inside a subprocess.

    :param config_dir: optionnal configuration directory
    """
    set_config_env(config_dir)
    from concurrent.futures import ProcessPoolExecutor
    # execute all the pyloggr process in children subprocess
    with ProcessPoolExecutor(max_workers=len(pyloggr_process)) as executor:
        list(executor.map(_run, pyloggr_process))
    # exit of 'with' clause 'waits' on process executor


def run_daemon_all(config_dir=None):
    """
    Runs all pyloggr processes as daemons

    Note
    ====
    Not recommended, use supervisord instead.

    :param config_dir: optionnal configuration directory

    """
    for process in pyloggr_process:
        run_daemon(process, config_dir)


def stop_all(config_dir=None):
    """
    Stop all pyloggr processes

    :param config_dir: optionnal configuration directory
    """
    set_config_env(config_dir)


def init_db(config_dir=None):
    """
    Creates the needed tables and indices in PGSQL

    Note
    ====
    The PostgreSQL user and database should have been manually created by the admin before
    """
    set_config_env(config_dir)
    from pyloggr.config import POSTGRESQL
    import psycopg2
    try:
        psycopg2.connect(POSTGRESQL.DSN)
    except psycopg2.Error:
        raise CommandError(
            "Impossible to connect to PGSQL. Check that you have created the user and the database"
        )

    with psycopg2.connect(POSTGRESQL.DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT schemaname, tablename FROM pg_tables WHERE schemaname='public';")
            tables = [c[0] + '.' + c[1] for c in cur.fetchall()]

        if POSTGRESQL.tablename in tables:
            answer = ask_question("Table '{}' already exists. Do you want to delete and create it "
                                  "again ? (y/N)".format(POSTGRESQL.tablename))
            if not answer:
                return
            with conn.cursor() as cur:
                cur.execute("DROP TABLE {};".format(POSTGRESQL.tablename))

    sql_fname = join(os.environ['PYLOGGR_CONFIG_DIR'], '3rd_party', 'postgresql', 'table.sql')
    with open(sql_fname, encoding='utf-8') as f:
        sql = f.read().replace('XXXXXXXX', POSTGRESQL.tablename)
    try:
        with psycopg2.connect(POSTGRESQL.DSN) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
    except psycopg2.Error:
        print("Error happened when creating '{}'".format(POSTGRESQL.tablename), file=sys.stderr)
    else:
        print("Table '{}' was created".format(POSTGRESQL.tablename))


def purge_db(config_dir=None):
    """
    Purge the content of pyloggr's PGSQL db (not deleting the table).
    """
    set_config_env(config_dir)
    answer = ask_question("Are you sure you want to empty the PGSQL db ? (y/N)")
    if not answer:
        return
    from pyloggr.config import POSTGRESQL
    import psycopg2
    with psycopg2.connect(POSTGRESQL.DSN) as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("DELETE FROM {};".format(POSTGRESQL.tablename))
            except psycopg2.Error as ex:
                print("Error happened during purge:", str(ex), file=sys.stderr)
            else:
                print("Database has been purged")


def purge_queues(config_dir=None):
    """
    Purge the content of Pyloggr's RabbitMQ queues (no delete)
    """
    set_config_env(config_dir)
    answer = ask_question("Are you sure you want to empty the RabbitMQ queues ? (y/N)")
    if not answer:
        return
    from pyloggr.config import NOTIFICATIONS, PARSER_CONSUMER, PGSQL_CONSUMER
    from pyloggr.rabbitmq.management import Client

    @coroutine
    def purge():
        client = Client(NOTIFICATIONS.host, NOTIFICATIONS.user, NOTIFICATIONS.password)
        yield client.purge_queue(NOTIFICATIONS.vhost, PARSER_CONSUMER.queue)
        yield client.purge_queue(NOTIFICATIONS.vhost, PGSQL_CONSUMER.queue)

    try:
        IOLoop.instance().run_sync(purge)
    except Exception as ex:
        print("Purge of RabbitMQ queues failed:", str(ex), file=sys.stderr)
    else:
        print("RabbitMQ queues have been purged")


def init_rabbitmq(config_dir=None):
    """
    Creates the exchanges and queues needed for pyloggr in RabbitMQ.

    Note
    ====
    If an exchange or queue already exists, we silently pass
    """
    set_config_env(config_dir)
    from pyloggr.config import NOTIFICATIONS, PARSER_CONSUMER, PGSQL_CONSUMER
    from pyloggr.config import PARSER_PUBLISHER, SYSLOG_PUBLISHER
    from pyloggr.rabbitmq.management import Client, HTTPError



    @coroutine
    def init_rabbit():
        client = Client(NOTIFICATIONS.host, NOTIFICATIONS.user, NOTIFICATIONS.password)
        create_parser_queue = False
        create_pgsql_queue = False
        create_syslog_exchange = False
        create_parser_exchange = False
        create_notifications_exchange = False


        try:
            yield client.get_queue(PARSER_CONSUMER.vhost, PARSER_CONSUMER.queue)
        except HTTPError as ex:
            if ex.status != 404:
                print("Error happened when querying RabbitMQ management API", file=sys.stderr)
                return
            else:
                create_parser_queue = True
        else:
            print("PARSER_CONSUMER queue already exists", file=sys.stderr)

        try:
            yield client.get_queue(PGSQL_CONSUMER.vhost, PGSQL_CONSUMER.queue)
        except HTTPError as ex:
            if ex.status != 404:
                print("Error happened when querying RabbitMQ management API", file=sys.stderr)
                return
            else:
                create_pgsql_queue = True
        else:
            print("PGSQL_CONSUMER queue already exists", file=sys.stderr)


        try:
            yield client.get_exchange(SYSLOG_PUBLISHER.vhost, SYSLOG_PUBLISHER.exchange)
        except HTTPError as ex:
            if ex.status != 404:
                print("Error happened when querying RabbitMQ management API", file=sys.stderr)
                return
            else:
                create_syslog_exchange = True
        else:
            print("SYSLOG_PUBLISHER exchange already exists", file=sys.stderr)

        try:
            yield client.get_exchange(PARSER_PUBLISHER.vhost, PARSER_PUBLISHER.exchange)
        except HTTPError as ex:
            if ex.status != 404:
                print("Error happened when querying RabbitMQ management API", file=sys.stderr)
                return
            else:
                create_parser_exchange = True
        else:
            print("PARSER_PUBLISHER exchange already exists", file=sys.stderr)

        try:
            yield client.get_exchange(NOTIFICATIONS.vhost, NOTIFICATIONS.exchange)
        except HTTPError as ex:
            if ex.status != 404:
                print("Error happened when querying RabbitMQ management API", file=sys.stderr)
                return
            else:
                create_notifications_exchange = True
        else:
            print("NOTIFICATIONS exchange already exists", file=sys.stderr)

        if create_parser_queue:
            try:
                yield client.create_queue(PARSER_CONSUMER.vhost, PARSER_CONSUMER.queue,
                                          durable=True, auto_delete=False)
            except Exception:
                print("Error while creating queue '{}'".format(PARSER_CONSUMER.queue), file=sys.stderr)
                raise
            else:
                print("Queue '{}' was created".format(PARSER_CONSUMER.queue))

        if create_pgsql_queue:
            try:
                yield client.create_queue(PGSQL_CONSUMER.vhost, PGSQL_CONSUMER.queue,
                                          durable=True, auto_delete=False)
            except Exception:
                print("Error while creating queue '{}'".format(PGSQL_CONSUMER.queue), file=sys.stderr)
                raise
            else:
                print("Queue '{}' was created".format(PGSQL_CONSUMER.queue))

        if create_syslog_exchange:
            try:
                yield client.create_exchange(
                    SYSLOG_PUBLISHER.vhost, SYSLOG_PUBLISHER.exchange, 'topic', durable=True, auto_delete=False
                )
            except Exception:
                print("Error while creating exchange '{}'".format(SYSLOG_PUBLISHER.exchange), file=sys.stderr)
                raise
            else:
                print("Exchange '{}' was created".format(SYSLOG_PUBLISHER.exchange))

        if create_parser_exchange:
            try:
                yield client.create_exchange(
                    PARSER_PUBLISHER.vhost, PARSER_PUBLISHER.exchange, 'fanout', durable=True, auto_delete=False
                )
            except Exception:
                print("Error while creating exchange '{}'".format(PARSER_PUBLISHER.exchange), file=sys.stderr)
                raise
            else:
                print("Exchange '{}' was created".format(PARSER_PUBLISHER.exchange))

        if create_notifications_exchange:
            try:
                yield client.create_exchange(
                    NOTIFICATIONS.vhost, NOTIFICATIONS.exchange, 'topic', durable=True, auto_delete=False
                )
            except Exception:
                print("Error while creating exchange '{}'".format(NOTIFICATIONS.exchange), file=sys.stderr)
                raise
            else:
                print("Exchange '{}' was created".format(NOTIFICATIONS.exchange))

        if create_syslog_exchange or create_parser_queue:
            try:
                yield client.create_binding(
                    SYSLOG_PUBLISHER.vhost, SYSLOG_PUBLISHER.exchange, PARSER_CONSUMER.queue, 'pyloggr.syslog.*'
                )
            except Exception:
                print("Error while creating binding '{} -> {}'".format(
                    SYSLOG_PUBLISHER.exchange, PARSER_CONSUMER.queue), file=sys.stderr)
                raise
            else:
                print("Created binding '{} -> {}'".format(SYSLOG_PUBLISHER.exchange, PARSER_CONSUMER.queue))

        if create_parser_exchange or create_pgsql_queue:
            try:
                yield client.create_binding(
                    PARSER_PUBLISHER.vhost, PARSER_PUBLISHER.exchange, PGSQL_CONSUMER.queue
                )
            except Exception:
                print("Error while creating binding '{} -> {}'".format(
                    PARSER_PUBLISHER.exchange, PGSQL_CONSUMER.queue), file=sys.stderr)
                raise
            else:
                print("Created binding '{} -> {}'".format(PARSER_PUBLISHER.exchange, PGSQL_CONSUMER.queue))

    IOLoop.instance().run_sync(init_rabbit)


def status(config_dir=None):
    set_config_env(config_dir)
    print()
    for p_name in pyloggr_process:
        if check_pid(p_name):
            print(p_name + " is running")
        else:
            print(p_name + " is not running")
    print()
    from pyloggr.cache import cache
    print("Redis is running" if cache.available else "Redis is not running")
    from pyloggr.config import NOTIFICATIONS
    from pyloggr.rabbitmq.publisher import Publisher, RabbitMQConnectionError
    publisher = Publisher(NOTIFICATIONS)
    try:
        IOLoop.instance().run_sync(publisher.start)
    except RabbitMQConnectionError as ex:
        print("Can't connect to RabbitMQ: " + str(ex))
    else:
        print("Connected to RabbitMQ")
    from pyloggr.config import POSTGRESQL
    from psycopg2 import connect, DatabaseError
    try:
        connect(POSTGRESQL.DSN).cursor().execute("SELECT 1;")
    except DatabaseError as ex:
        print("Can't connect to PGSQL: {}".format(str(ex)))
    else:
        print("Connected to PGSQL")

pyloggr_process = ['frontend', 'syslog', 'pgsql_shipper', 'parser']


def main():
    p = ArghParser()
    p.add_commands([
        run, run_all, run_daemon, run_daemon_all, stop, stop_all, status, init_db, init_rabbitmq, purge_db,
        purge_queues
    ])
    try:
        p.dispatch()
    except RuntimeError as ex:
        sys.stderr.write(str(ex) + '\n')
        sys.exit(1)


if __name__ == '__main__':
    main()
