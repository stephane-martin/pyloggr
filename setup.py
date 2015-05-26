#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages
import os
from os.path import dirname, abspath, join, commonprefix, expanduser

on_rtd = os.environ.get('READTHEDOCS', None) == 'True'
here = abspath(dirname(__file__))

if not on_rtd:
    from Cython.Build import cythonize


def list_subdir(subdirname):
    subdirname = join(here, subdirname)

    l = [(root, [
        os.path.join(root, f) for f in files if (not f.endswith("secrets.py")) and (
            f.endswith('.conf') or
            f.endswith('.config') or
            f.endswith('_plugins') or
            f.endswith('.sample') or
            f.endswith('.sql') or
            f.endswith('.patterns') or
            f.endswith('.txt'))
    ]) for root, dirs, files in os.walk(subdirname)]
    prefix_len = len(commonprefix(list(d[0] for d in l)))
    l = [(root[prefix_len + 1:], list_of_files) for root, list_of_files in l if list_of_files]
    return l


with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read().replace('.. :changelog:', '')

requirements = [
    'ujson', 'cryptography', 'pika', 'geoip2', 'psycopg2', 'subprocess32', 'hiredis', 'spooky_hash',
    'ftfy', 'enum34', 'tornado', 'future', 'futures', 'jinja2', 'sortedcontainers', 'redis',
    'requests', 'six', 'pyparsing', 'toro', 'python-dateutil', 'regex', 'sphinx>=1.3', 'argh',
    'sphinx_readable_theme', 'sphinx-rtd-theme', 'Mock', 'wheel', 'twine', 'pytz', 'arrow', 'httpagentparser',
    'requests_futures', 'configobj', 'python-daemon', 'lockfile', 'psutil', 'watchdog', 'momoko', 'cytoolz',
    'msgpack-python', 'unidecode', 'elasticsearch'
]

if on_rtd:
    extensions_with_problems = [
        'cryptography', 'pika', 'subprocess32', 'hiredis', 'spooky_hash', 'watchdog', 'psutil', 'lockfile', 'cytoolz',
        'msgpack-python'
    ]
    for ext in extensions_with_problems:
        requirements.remove(ext)

test_requirements = [
    # TODO: put package test requirements here
]


if __name__ == "__main__":
    data_files = [(join(expanduser('~/.pyloggr'), root), list_of_files) for root, list_of_files in list_subdir('config')]
    setup(
        name='pyloggr',
        version='0.1.3',
        description='Centralize, parse, store and search logs',
        long_description=readme + '\n\n' + history,
        author='Stephane Martin',
        author_email='stephane.martin_github@vesperal.eu',
        url='https://github.com/stephane-martin/pyloggr',
        packages=find_packages(exclude=['tests']),
        setup_requires=[
            'setuptools_git', 'setuptools', 'twine', 'wheel', 'mock', 'cython'
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
                'pyloggr_ctl = pyloggr.scripts.pyloggr_ctl:main'
            ]
        },

        data_files=data_files,
        test_suite='tests',
        tests_require=test_requirements,
        ext_modules=None if on_rtd else cythonize("pyloggr/utils/fix_unicode.pyx")
    )
