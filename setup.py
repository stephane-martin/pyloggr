#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages
from itertools import chain
import os
on_rtd = os.environ.get('READTHEDOCS', None) == 'True'


def list_subdir(dirname):
    l = [[os.path.join(root, f) for f in files] for root, dirs, files in os.walk(dirname)]
    l = list(chain.from_iterable(l))
    l = [f for f in l if
         f.endswith('.py') or
         f.endswith('.conf') or
         f.endswith('.patterns') or
         f.endswith('.txt')
         ]
    l = [f for f in l if not f.endswith('.mmdb')]
    l = [f for f in l if not f.endswith('secrets.py')]
    return l

data_files = list_subdir('config')

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read().replace('.. :changelog:', '')

requirements = [
    'ujson', 'cryptography', 'pika', 'geoip2', 'psycopg2', 'subprocess32', 'hiredis',
    'ftfy', 'enum34', 'tornado', 'future', 'futures', 'jinja2', 'sortedcontainers', 'marshmallow', 'redis',
    'requests', 'six', 'pyparsing', 'toro', 'python-dateutil', 'regex', 'sphinx >= 1.3b3',
    'sphinx_readable_theme', 'sphinx-rtd-theme', 'Mock', 'wheel', 'twine', 'pytz'
]

if on_rtd:
    # C extensions can cause problems on readthedocs
    requirements.remove('cryptography')
    requirements.remove('pika')
    requirements.remove('subprocess32')
    requirements.remove('hiredis')

test_requirements = [
    # TODO: put package test requirements here
]

setup(
    name='pyloggr',
    version='0.1.0',
    description='Centralize, parse, store and search logs',
    long_description=readme + '\n\n' + history,
    author='Stephane Martin',
    author_email='stephane.martin_github@vesperal.eu',
    url='https://github.com/stephane-martin/pyloggr',
    packages=find_packages(exclude=['tests']),
    # todo: are you sure about setup_requires ?
    setup_requires=[
        'setuptools_git', 'setuptools', 'twine', 'wheel', 'Mock'
    ],
    include_package_data=True,
    install_requires=requirements,
    license="GPLv3+",
    zip_safe=False,
    keywords='syslog rabbitmq tornado postgresql elasticsearch logmanagement python',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
        'Natural Language :: English',
        'Programming Language :: Python :: 2.7',
        'Environment :: Console',
        'Environment :: Web Environment',
        'Operating System :: POSIX :: Linux',
        'Topic :: Internet :: Log Analysis',
    ],
    entry_points={
        'console_scripts': [
            'pyloggr_parser = pyloggr.scripts.pyloggr_parser:main',
            'pyloggr_shipper_pgsql = pyloggr.scripts.pyloggr_shipper_pgsql:main',
            'pyloggr_syslog_server = pyloggr.scripts.pyloggr_syslog_server:main',
            'pyloggr_web_frontend = pyloggr.scripts.pyloggr_web_frontend:main'
        ]
    },
    data_files=[('etc/pyloggr', data_files)],
    test_suite='tests',
    tests_require=test_requirements
)
