# encoding: utf-8

"""
The `main` subpackage contains code for the different pyloggr processes :

- :py:mod:`pyloggr.main.syslog_server`: Syslog server. Listens for syslog events and stores
  them in RabbitMQ queues.

- :py:mod:`pyloggr.main.filter_machine`: Event parser. Gets some events from RabbitMQ, apply filters, and
  stores events back in RabbitMQ.

- :py:mod:`pyloggr.main.shipper2pgsql`: Gets events from RabbitMQ, and ships them to a Postgresql database.

- :py:mod:`pyloggr.main.web_frontend`: Web interface. Provides monitoring of other processes, and log
  searching.

---------------------------------

"""

__author__ = 'stef'
