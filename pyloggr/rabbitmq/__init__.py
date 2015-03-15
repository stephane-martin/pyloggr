# encoding: utf-8

"""
The pyloggr.rabbitmq subpackage provides classes for publishing and consuming to/from RabbitMQ.
Pika library is used, but the pika callback style has been workarounded in coroutines.

------------------------
"""

__author__ = 'stef'


class RabbitMQConnectionError(Exception):
    """
    Exception triggered when connection to RabbitMQ fails
    """
    pass