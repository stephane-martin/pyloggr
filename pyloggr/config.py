# encoding: utf-8
__author__ = 'stef'

"""
Small hack to be able to import configuration from an environment variable.

PYLOGGR_CONFIG_DIR envvar can be defined to some directory that actually contains 'pyloggr_config.py'.
If PYLOGGR_CONFIG_DIR is not defined, 'pyloggr_config.py' will be read from ./config, relative to package install
directory.
"""

import os
import sys
from os.path import join, dirname



CONFIG_ENV = os.environ.get('PYLOGGR_CONFIG_DIR')

if CONFIG_ENV:
    CONFIG_DIR = CONFIG_ENV
else:
    current_dir = dirname(__file__)
    base_dir = dirname(current_dir)
    CONFIG_DIR = join(base_dir, 'config')

sys.path.insert(0, CONFIG_DIR)
from pyloggr_config import *
sys.path.pop(0)

