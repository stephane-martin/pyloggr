#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages
import os, sys
from os.path import dirname, abspath, join, commonprefix
on_rtd = os.environ.get('READTHEDOCS', None) == 'True'

here = abspath(dirname(__file__))


def list_subdir(subdirname):
    subdirname = join(here, subdirname)

    l = [(root, [
        os.path.join(root, f) for f in files if (not f.endswith("secrets.py")) and (
            f.endswith('.py') or
            f.endswith('.conf') or
            f.endswith('.patterns') or
            f.endswith('.txt'))
    ]) for root, dirs, files in os.walk(subdirname)]
    prefix_len = len(commonprefix(list(d[0] for d in l)))
    l = [(root[prefix_len+1:], list_of_files) for root, list_of_files in l if list_of_files]
    return l


with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read().replace('.. :changelog:', '')

requirements = [
    'ujson', 'cryptography', 'pika', 'geoip2', 'psycopg2', 'subprocess32', 'hiredis',
    'ftfy', 'enum34', 'tornado', 'future', 'futures', 'jinja2', 'sortedcontainers', 'marshmallow', 'redis',
    'requests', 'six', 'pyparsing', 'toro', 'python-dateutil', 'regex', 'sphinx >= 1.3', 'pyrabbit', 'argh', 'momoko',
    'sphinx_readable_theme', 'sphinx-rtd-theme', 'Mock', 'wheel', 'twine', 'pytz', 'arrow', 'httpagentparser',
    'requests_futures'
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


if __name__ == "__main__":
    setup(
        name='pyloggr',
        version='0.1.2',
        description='Centralize, parse, store and search logs',
        long_description=readme + '\n\n' + history,
        author='Stephane Martin',
        author_email='stephane.martin_github@vesperal.eu',
        url='https://github.com/stephane-martin/pyloggr',
        packages=find_packages(exclude=['tests']),
        setup_requires=[
            'setuptools_git', 'setuptools', 'twine', 'wheel', 'mock'
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

        data_files=[
            (join('~/.config', root), list_of_files) for root, list_of_files in list_subdir('config')
        ],

        test_suite='tests',
        tests_require=test_requirements
    )
