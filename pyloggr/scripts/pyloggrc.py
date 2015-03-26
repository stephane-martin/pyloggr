# encoding: utf-8

"""
Utility script to start the pyloggr's processes.
"""

__author__ = 'stef'

import os
import sys
from os.path import exists, expanduser

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


def parser(config_dir):
    set_config_env(config_dir)
    from pyloggr.config import set_logging, LOGGING_FILES
    set_logging(LOGGING_FILES.parser)
    from pyloggr.scripts.processes import ParserProcess
    ParserProcess().main()


def syslog(config_dir):
    set_config_env(config_dir)
    from pyloggr.config import set_logging, LOGGING_FILES
    set_logging(LOGGING_FILES.syslog)
    from pyloggr.scripts.processes import SyslogProcess
    SyslogProcess().main()


def pgsql_shipper(config_dir):
    set_config_env(config_dir)
    from pyloggr.config import set_logging, LOGGING_FILES
    set_logging(LOGGING_FILES.pgsql_shipper)
    from pyloggr.scripts.processes import PgSQLShipperProcess
    PgSQLShipperProcess().main()


def frontend(config_dir):
    set_config_env(config_dir)
    from pyloggr.config import set_logging, LOGGING_FILES
    set_logging(LOGGING_FILES.frontend)
    from pyloggr.scripts.processes import FrontendProcess
    FrontendProcess().main()


def run(process, config_dir=None):
    disp = {
        'frontend': frontend,
        'syslog': syslog,
        'pgsql_shipper': pgsql_shipper,
        'parser': parser
    }
    if process not in disp:
        raise CommandError("Unknown process. Please choose in {}".format(','.join(disp.keys())))
    disp[process](config_dir)


def init_db():
    pass


def init_rabbitmq():
    pass


def status():
    pass


def main():
    p = ArghParser()
    p.add_commands([run, status, init_db, init_rabbitmq])
    try:
        p.dispatch()
    except RuntimeError as ex:
        sys.stderr.write(str(ex) + '\n')
        sys.exit(1)


if __name__ == '__main__':
    main()
