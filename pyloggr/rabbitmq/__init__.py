# encoding: utf-8

"""
The RabbitMQ packages provides classes for publishing and consuming to/from RabbitMQ.


------------------------
"""

__author__ = 'stef'



class RabbitMQConnectionError(Exception):
    """
    Exception triggered when connection to RabbitMQ fails
    """
    pass