# encoding: utf-8

"""
Utility script to manage the pyloggr's processes.
"""

from __future__ import print_function

__author__ = 'stef'

from io import open
import os
import sys
from os.path import exists, expanduser, join, isfile
from signal import SIGTERM
import socket

import psutil
# noinspection PyCompatibility
import psycopg2
# noinspection PyCompatibility
from concurrent.futures import ProcessPoolExecutor
from future.builtins import str as text
from tornado.ioloop import IOLoop
from tornado.gen import coroutine, TimeoutError
from argh.helpers import ArghParser
from argh.exceptions import CommandError


from pyloggr.utils import ask_question
from pyloggr.config import Config, set_configuration, set_logging


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
    try:
        set_configuration(config_env)
    except ValueError as ex:
        raise CommandError(ex)


def _check_pid(name):
    if not exists(Config.PIDS_DIRECTORY):
        try:
            os.makedirs(Config.PIDS_DIRECTORY)
        except OSError:
            raise CommandError("PID directory '{}' is not writable".format(Config.PIDS_DIRECTORY))
    pid_file = join(Config.PIDS_DIRECTORY, name + u".pid")
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
    from pyloggr.scripts.processes import SyslogProcess, FrontendProcess, FilterMachineProcess, PgSQLShipperProcess
    from pyloggr.scripts.processes import HarvestProcess, CollectorProcess, FSShipperProcess, SyslogShipperProcess
    from pyloggr.utils import remove_pid_file
    pid_file = join(Config.PIDS_DIRECTORY, process + u".pid")
    try:
        with open(pid_file, 'w') as f:
            f.write(text(os.getpid()))
    except OSError:
        raise CommandError("Error trying to write PID file '{}'".format(pid_file))
    try:
        set_logging(
            filename=getattr(Config.LOGGING_FILES, process),
            level=Config.LOGGING_FILES.level
        )
        dispatcher = {
            'frontend': FrontendProcess,
            'syslog': SyslogProcess,
            'shipper2pgsql': PgSQLShipperProcess,
            'filtermachine': FilterMachineProcess,
            'harvest': HarvestProcess,
            'collector': CollectorProcess,
            'shipper2fs': FSShipperProcess,
            'shipper2syslog': SyslogShipperProcess
        }
        process_class = dispatcher[process]
        try:
            process_obj = process_class()
        except RuntimeError as ex:
            raise CommandError("Process '{}' initialization error: {}".format(process, str(ex)))
        else:
            process_obj.main()
    finally:
        remove_pid_file(process)


def run(process, config_dir=None):
    """
    Starts a pyloggr process in foreground.

    :param process: name of process
    :param config_dir: optional configuration directory
    """
    set_config_env(config_dir)
    if process not in pyloggr_process:
        raise CommandError("Unknown process. Please choose in {}".format(', '.join(pyloggr_process)))

    if _check_pid(process) is not None:
        raise CommandError("'{}' is already running".format(process))

    _run(process)


def run_daemon(process, config_dir=None):
    """
    Starts a pyloggr process as a Unix daemon

    Note
    ====
    This is not recommended. You should use supervisord and the `run` command.

    :param process: name of process
    :param config_dir: optional configuration directory
    """
    set_config_env(config_dir)
    if process not in pyloggr_process:
        raise CommandError("Unknown process. Please choose in {}".format(', '.join(pyloggr_process)))

    import daemon
    context = daemon.DaemonContext()
    context.umask = 0o022
    context.working_directory = expanduser('~')
    context.prevent_core = True
    context.files_preserve = None
    context.detach_process = True

    if _check_pid(process) is not None:
        raise CommandError("'{}' is already running".format(process))

    with context:
        # we store the PID after the double-fork has been done
        _run(process)


def stop(process, config_dir=None):
    """
    Stops a pyloggr process

    :param process: name of process
    :param config_dir: optional configuration directory
    """
    set_config_env(config_dir)
    if process not in pyloggr_process:
        raise CommandError("Unknown process. Please choose in {}".format(', '.join(pyloggr_process)))

    p = _check_pid(process)
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

    :param config_dir: optional configuration directory

    Note
    ====
    The PostgreSQL user and database should have been manually created by the admin before
    """
    set_config_env(config_dir)
    try:
        psycopg2.connect(Config.POSTGRESQL.DSN)
    except psycopg2.Error:
        raise CommandError(
            "Impossible to connect to PGSQL. Check that you have created the user and the database"
        )

    with psycopg2.connect(Config.POSTGRESQL.DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT schemaname, tablename FROM pg_tables WHERE schemaname='public';")
            tables = [c[0] + '.' + c[1] for c in cur.fetchall()]

        if Config.POSTGRESQL.tablename in tables:
            answer = ask_question("Table '{}' already exists. Do you want to delete and create it "
                                  "again ? (y/N)".format(Config.POSTGRESQL.tablename))
            if not answer:
                return
            with conn.cursor() as cur:
                cur.execute("DROP TABLE {};".format(Config.POSTGRESQL.tablename))

    sql_fname = join(os.environ['PYLOGGR_CONFIG_DIR'], '3rd_party', 'postgresql', 'table.sql')
    with open(sql_fname, encoding='utf-8') as f:
        sql = f.read().replace('XXXXXXXX', Config.POSTGRESQL.tablename)
    try:
        with psycopg2.connect(Config.POSTGRESQL.DSN) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
    except psycopg2.Error:
        print("Error happened when creating '{}'".format(Config.POSTGRESQL.tablename), file=sys.stderr)
    else:
        print("Table '{}' was created".format(Config.POSTGRESQL.tablename))


def purge_db(config_dir=None):
    """
    Purge the content of pyloggr's PGSQL db (not deleting the table).

    :param config_dir: optional configuration directory
    """
    set_config_env(config_dir)
    answer = ask_question("Are you sure you want to empty the PGSQL db ? (y/N)")
    if not answer:
        return

    with psycopg2.connect(Config.POSTGRESQL.DSN) as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("DELETE FROM {};".format(Config.POSTGRESQL.tablename))
            except psycopg2.Error as ex:
                print("Error happened during purge:", str(ex), file=sys.stderr)
            else:
                print("Database has been purged")


def purge_queues(config_dir=None):
    """
    Purge the content of Pyloggr's RabbitMQ queues (no delete)

    :param config_dir: optional configuration directory
    """
    set_config_env(config_dir)
    answer = ask_question("Are you sure you want to empty the RabbitMQ queues ? (y/N)")
    if not answer:
        return
    from pyloggr.rabbitmq.management import Client

    @coroutine
    def _purge():
        client = Client(Config.NOTIFICATIONS.host, Config.NOTIFICATIONS.user, Config.NOTIFICATIONS.password)
        yield client.purge_queue(Config.NOTIFICATIONS.vhost, Config.PARSER_CONSUMER.queue)
        yield client.purge_queue(Config.NOTIFICATIONS.vhost, Config.PGSQL_CONSUMER.queue)

    try:
        IOLoop.instance().run_sync(_purge)
    except Exception as ex:
        print("Purge of RabbitMQ queues failed:", str(ex), file=sys.stderr)
    else:
        print("RabbitMQ queues have been purged")


def init_rabbitmq(config_dir=None):
    """
    Creates the exchanges and queues needed for pyloggr in RabbitMQ.

    :param config_dir: optional configuration directory

    Note
    ====
    If an exchange or queue already exists, we silently pass
    """
    set_config_env(config_dir)
    from pyloggr.rabbitmq.management import Client, HTTPError

    @coroutine
    def _init_rabbit():
        client = Client(Config.NOTIFICATIONS.host, Config.NOTIFICATIONS.user, Config.NOTIFICATIONS.password)
        create_parser_queue = False
        create_pgsql_queue = False
        create_syslog_exchange = False
        create_parser_exchange = False
        create_notifications_exchange = False

        try:
            yield client.get_queue(Config.PARSER_CONSUMER.vhost, Config.PARSER_CONSUMER.queue)
        except HTTPError as ex:
            if ex.status != 404:
                print("Error happened when querying RabbitMQ management API", file=sys.stderr)
                return
            else:
                create_parser_queue = True
        else:
            print("PARSER_CONSUMER queue already exists", file=sys.stderr)

        try:
            yield client.get_queue(Config.PGSQL_CONSUMER.vhost, Config.PGSQL_CONSUMER.queue)
        except HTTPError as ex:
            if ex.status != 404:
                print("Error happened when querying RabbitMQ management API", file=sys.stderr)
                return
            else:
                create_pgsql_queue = True
        else:
            print("PGSQL_CONSUMER queue already exists", file=sys.stderr)

        try:
            yield client.get_exchange(Config.SYSLOG_PUBLISHER.vhost, Config.SYSLOG_PUBLISHER.exchange)
        except HTTPError as ex:
            if ex.status != 404:
                print("Error happened when querying RabbitMQ management API", file=sys.stderr)
                return
            else:
                create_syslog_exchange = True
        else:
            print("SYSLOG_PUBLISHER exchange already exists", file=sys.stderr)

        try:
            yield client.get_exchange(Config.PARSER_PUBLISHER.vhost, Config.PARSER_PUBLISHER.exchange)
        except HTTPError as ex:
            if ex.status != 404:
                print("Error happened when querying RabbitMQ management API", file=sys.stderr)
                return
            else:
                create_parser_exchange = True
        else:
            print("PARSER_PUBLISHER exchange already exists", file=sys.stderr)

        try:
            yield client.get_exchange(Config.NOTIFICATIONS.vhost, Config.NOTIFICATIONS.exchange)
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
                yield client.create_queue(Config.PARSER_CONSUMER.vhost, Config.PARSER_CONSUMER.queue,
                                          durable=True, auto_delete=False)
            except Exception:
                print("Error while creating queue '{}'".format(Config.PARSER_CONSUMER.queue), file=sys.stderr)
                raise
            else:
                print("Queue '{}' was created".format(Config.PARSER_CONSUMER.queue))

        if create_pgsql_queue:
            try:
                yield client.create_queue(Config.PGSQL_CONSUMER.vhost, Config.PGSQL_CONSUMER.queue,
                                          durable=True, auto_delete=False)
            except Exception:
                print("Error while creating queue '{}'".format(Config.PGSQL_CONSUMER.queue), file=sys.stderr)
                raise
            else:
                print("Queue '{}' was created".format(Config.PGSQL_CONSUMER.queue))

        if create_syslog_exchange:
            try:
                yield client.create_exchange(
                    Config.SYSLOG_PUBLISHER.vhost, Config.SYSLOG_PUBLISHER.exchange, 'topic', durable=True,
                    auto_delete=False
                )
            except Exception:
                print(
                    "Error while creating exchange '{}'".format(Config.SYSLOG_PUBLISHER.exchange),
                    file=sys.stderr
                )
                raise
            else:
                print("Exchange '{}' was created".format(Config.SYSLOG_PUBLISHER.exchange))

        if create_parser_exchange:
            try:
                yield client.create_exchange(
                    Config.PARSER_PUBLISHER.vhost, Config.PARSER_PUBLISHER.exchange, 'fanout',
                    durable=True, auto_delete=False
                )
            except Exception:
                print("Error while creating exchange '{}'".format(Config.PARSER_PUBLISHER.exchange),
                      file=sys.stderr)
                raise
            else:
                print("Exchange '{}' was created".format(Config.PARSER_PUBLISHER.exchange))

        if create_notifications_exchange:
            try:
                yield client.create_exchange(
                    Config.NOTIFICATIONS.vhost, Config.NOTIFICATIONS.exchange, 'topic', durable=True,
                    auto_delete=False
                )
            except Exception:
                print("Error while creating exchange '{}'".format(Config.NOTIFICATIONS.exchange), file=sys.stderr)
                raise
            else:
                print("Exchange '{}' was created".format(Config.NOTIFICATIONS.exchange))

        if create_syslog_exchange or create_parser_queue:
            try:
                yield client.create_binding(
                    Config.SYSLOG_PUBLISHER.vhost, Config.SYSLOG_PUBLISHER.exchange,
                    Config.PARSER_CONSUMER.queue, 'pyloggr.syslog.*'
                )
            except Exception:
                print("Error while creating binding '{} -> {}'".format(
                    Config.SYSLOG_PUBLISHER.exchange, Config.PARSER_CONSUMER.queue), file=sys.stderr)
                raise
            else:
                print("Created binding '{} -> {}'".format(
                    Config.SYSLOG_PUBLISHER.exchange, Config.PARSER_CONSUMER.queue
                ))

        if create_parser_exchange or create_pgsql_queue:
            try:
                yield client.create_binding(
                    Config.PARSER_PUBLISHER.vhost, Config.PARSER_PUBLISHER.exchange, Config.PGSQL_CONSUMER.queue
                )
            except Exception:
                print("Error while creating binding '{} -> {}'".format(
                    Config.PARSER_PUBLISHER.exchange, Config.PGSQL_CONSUMER.queue), file=sys.stderr)
                raise
            else:
                print("Created binding '{} -> {}'".format(
                    Config.PARSER_PUBLISHER.exchange, Config.PGSQL_CONSUMER.queue
                ))

    IOLoop.instance().run_sync(_init_rabbit)


def status(config_dir=None):
    """
    Display status of pyloggr processes

    :param config_dir: optional configuration directory
    """
    set_config_env(config_dir)
    print()
    for p_name in pyloggr_process:
        if _check_pid(p_name):
            print(p_name + " is running")
        else:
            print(p_name + " is not running")
    print()
    from pyloggr.cache import cache
    cache.initialize()
    print("Redis is running" if cache.available else "Redis is not running")
    from pyloggr.rabbitmq.publisher import Publisher, RabbitMQConnectionError
    publisher = Publisher(Config.NOTIFICATIONS)
    try:
        IOLoop.instance().run_sync(publisher.start)
    except RabbitMQConnectionError as ex:
        print("Can't connect to RabbitMQ: " + str(ex))
    else:
        print("Connected to RabbitMQ")
    try:
        psycopg2.connect(Config.POSTGRESQL.DSN).cursor().execute("SELECT 1;")
    except psycopg2.DatabaseError as ex:
        print("Can't connect to PGSQL: {}".format(str(ex)))
    else:
        print("Connected to PGSQL")


def syslog_client(server, port, source=None, message=None, filename=None, severity='notice', facility='user',
                  app_name='pyloggr', usessl=False, verify=False, hostname='', cacerts=None):
    """
    Send a message to a syslog server, using TCP (rfc5424).

    Either `message` or `filename` should be given. If `message` is not None, the string is sent as a log message.
    If `filename` is not None, the file is read, and each line is sent as a log message.

    :param server: server hostname or IP
    :param port: server port
    :param source: log source host (if None, gethostname will be used)
    :param message: log message
    :param filename: if not None, each line of filename will be sent as a log message
    :param severity: log severity ('notice' by default)
    :param facility: log facility ('user' by default)
    :param app_name: log source application name ('pyloggr' by default)
    :type server: str
    :type port: int
    :type source: str
    :type message: str
    :type filename: str
    :type severity: str
    :type facility: str
    :type app_name: str
    :type usessl: bool
    """
    from pyloggr.syslog.tcp_syslog_client import SyslogClient

    if message is None and filename is None:
        raise CommandError("Either message or filename should be given")
    if message is not None and filename is not None:
        raise CommandError("Only fill message OR filename")
    try:
        port = int(port)
    except ValueError:
        raise CommandError("port must be an integer")

    if source is None:
        source = socket.gethostname()

    @coroutine
    def _run_client():
        client = SyslogClient(server, port, use_ssl=usessl, hostname=hostname, verify_cert=verify,
                              ca_certs=cacerts)
        try:
            closed_connection_ev = yield client.start()
        except (socket.error, TimeoutError) as ex:
            print("Connection error: " + str(ex))
            return
        try:
            if filename:
                if not exists(filename) or not isfile(filename):
                    raise CommandError("'{}' is not a file".format(filename))
                res = yield client.send_file(filename, source, severity, facility, app_name)
            else:
                res = yield client.send_message(message, source, severity, facility, app_name)
            print("Server said '{}'".format("OK" if res else "KO"))
        finally:
            yield client.stop()

    IOLoop.instance().run_sync(_run_client)


def relp_client(server, port, source=None, message=None, filename=None, severity='notice', facility='user',
                app_name='pyloggr', usessl=False, verify=False, hostname='', cacerts=None):
    """
    Send a message to a RELP server

    Either `message` or `filename` should be given. If `message` is not None, the string is sent as a log message.
    If `filename` is not None, the file is read, and each line is sent as a log message.

    :param server: server hostname or IP
    :param port: server port
    :param source: log source host (if None, gethostname will be used)
    :param message: log message
    :param filename: if not None, each line of filename will be sent as a log message
    :param severity: log severity ('notice' by default)
    :param facility: log facility ('user' by default)
    :param app_name: log source application name ('pyloggr' by default)
    :type server: str
    :type port: int
    :type source: str
    :type message: str
    :type filename: str
    :type severity: str
    :type facility: str
    :type app_name: str
    :type usessl: bool
    """
    from pyloggr.syslog.relp_client import RELPClient, ServerClose
    if message is None and filename is None:
        raise CommandError("either message or filename should be given")
    if message is not None and filename is not None:
        raise CommandError("Only fill message OR filename")
    try:
        port = int(port)
    except ValueError:
        raise CommandError("port must be an integer")

    if source is None:
        source = socket.gethostname()

    @coroutine
    def _send(sendfile):
        relp_clt = RELPClient(server, port, use_ssl=usessl, hostname=hostname, verify_cert=verify,
                              ca_certs=cacerts)
        try:
            yield relp_clt.start()
        except (socket.error, TimeoutError, ServerClose) as ex:
            print("Connection error: " + str(ex))
            return
        if sendfile:
            resp, acks = yield relp_clt.send_file(filename, source, severity, facility, app_name)
            positives = sum(acks.values())
            print("No error occured" if resp else "Error occured while sending events")
            print("{} events were sent".format(len(acks)))
            print("{} events were ACKed".format(positives))
        else:
            resp = yield relp_clt.send_message(message, source, severity, facility, app_name)
            print("RELP server ACK the message" if resp else "RELP server NACK the message")
        yield relp_clt.stop()

    if filename:
        if not exists(filename) or not isfile(filename):
            raise CommandError("'{}' is not a file".format(filename))
        IOLoop.instance().run_sync(_send(True))
    else:
        IOLoop.instance().run_sync(_send(False))


pyloggr_process = ['frontend', 'syslog', 'shipper2pgsql', 'shipper2fs', 'filtermachine', 'harvest', 'collector',
                   'shipper2syslog']


def _main():
    p = ArghParser()
    p.add_commands([
        run, run_all, run_daemon, run_daemon_all, stop, stop_all, status, init_db, init_rabbitmq, purge_db,
        purge_queues, syslog_client, relp_client
    ])
    try:
        p.dispatch()
    except RuntimeError as ex:
        sys.stderr.write(str(ex) + '\n')
        sys.exit(1)


if __name__ == '__main__':
    _main()
