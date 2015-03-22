# encoding: utf-8
"""
Small hack to be able to import configuration from an environment variable.

PYLOGGR_CONFIG_DIR envvar must be defined to some directory that actually contains 'pyloggr_config.py'.
"""

__author__ = 'stef'

import os
import sys
from os.path import join, dirname
# TODO: refactor so that
# - the configuration is returned as an object
# - the configuration is read from a .ini file
# - run main script can provide the .ini path
# - if no .ini path as argument, look for the env variable
# - if no env variable, look in ~/.pyloggr directory

CONFIG_ENV = os.environ.get('PYLOGGR_CONFIG_DIR')

if CONFIG_ENV:
    CONFIG_DIR = CONFIG_ENV
else:
    current_dir = dirname(__file__)
    base_dir = dirname(current_dir)
    CONFIG_DIR = join(base_dir, 'config')

sys.path.insert(0, CONFIG_DIR)
try:
    from pyloggr_config import *
except ImportError:
    raise RuntimeError("PYLOGGR_CONFIG_DIR environment variable is not defined. Can't find configuration.")
finally:
    sys.path.pop(0)

