============
Installation
============

Prerequisites
=============

- python 2.7

So far pyloggr is not compatible with python 3

- Redis

Redis is used for interprocess communication. It is also used as a "rescue" queue when RabbitMQ becomes
unavailable.

- RabbitMQ

RabbitMQ is used for passing syslog messages between pyloggr components.

- PostgreSQL

Pyloggr currently uses a PostgreSQL database to store syslog messages.

- A C compiler may be needed too for the python packages that pyloggr depends on.

Install with pip
================

When pyloggr will be usable it will be published to pip.

If you'd like to install it now, please use a virtualenv and the setup.py script.
