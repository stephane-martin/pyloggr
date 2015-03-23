# encoding: utf-8

"""
Utility script to start the pyloggr's processes.
"""

__author__ = 'stef'

import os
import sys
from os.path import exists, expanduser
import logging

from argh.helpers import ArghParser


def check_config_dir():
    config_env = os.environ.get('PYLOGGR_CONFIG_DIR')
    if not exists(config_env):
        raise RuntimeError("Config directory '{}' doesn't exists".format(config_env))


def set_config(config_dir, filename):
    config_env = os.environ.get('PYLOGGR_CONFIG_DIR')
    if not config_env:
        if config_dir:
            os.environ['PYLOGGR_CONFIG_DIR'] = config_dir
        else:
            os.environ['PYLOGGR_CONFIG_DIR'] = expanduser('~/.pyloggr')
    check_config_dir()
    from pyloggr.config import set_logging
    set_logging(filename)


def parser(config_dir=None):
    set_config(config_dir, "/tmp/pyloggr_parser.log")
    logging.debug("im the parser")
    from pyloggr.scripts.processes import ParserProcess
    ParserProcess().main()


def syslog(config_dir=None):
    set_config(config_dir, "/tmp/pyloggr_syslog_server.log")
    logging.debug("im the syslog server")
    from pyloggr.scripts.processes import SyslogProcess
    SyslogProcess().main()


def pgsql_shipper(config_dir=None):
    set_config(config_dir, "/tmp/pyloggr_pgsql_shipper.log")
    logging.debug("im the pgsql shipper")
    from pyloggr.scripts.processes import PgSQLShipperProcess
    PgSQLShipperProcess().main()


def frontend(config_dir=None):
    set_config(config_dir, "/tmp/pyloggr_frontend.log")
    logging.debug("im the frontend")
    from pyloggr.scripts.processes import FrontendProcess
    FrontendProcess().main()


def main():
    p = ArghParser()
    p.add_commands([parser, syslog, pgsql_shipper, frontend])
    try:
        p.dispatch()
    except RuntimeError as ex:
        sys.stderr.write(str(ex) + '\n')
        sys.exit(1)


if __name__ == '__main__':
    main()

