# encoding: utf-8
__author__ = 'stef'

import logging
import ujson
from datetime import timedelta

from tornado.gen import coroutine, TimeoutError

from .consumer import Consumer
from ..utils.observable import Observable

logger = logging.getLogger(__name__)


class NotificationsConsumer(Consumer, Observable):
    """
    Consumes notification that were posted in RabbitMQ

    Parameters
    ==========
    rabbitmq_config: pyloggr.config.RabbitMQBaseConfig
        RabbitMQ connection parameters (to consume notifications from RabbitMQ)
    binding_key: str
        Binding key to filter notifications
    """
    def __init__(self, rabbitmq_config):
        """
        :type rabbitmq_config: pyloggr.config.RabbitMQBaseConfig
        """
        Consumer.__init__(self, rabbitmq_config)
        Observable.__init__(self)

    @coroutine
    def start_consuming(self):
        """
        Start consuming notifications from RabbitMQ and notify observers.
        """
        message_queue = Consumer.start_consuming(self)
        while self.consuming:
            try:
                message = yield message_queue.get_wait(deadline=timedelta(seconds=1))
            except TimeoutError:
                pass
            else:
                try:
                    self.notify_observers(ujson.loads(message.body))
                except Exception:
                    logger.exception("Swallowed exception")

