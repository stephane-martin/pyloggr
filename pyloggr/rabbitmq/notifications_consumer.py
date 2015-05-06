# encoding: utf-8
__author__ = 'stef'

import logging
import ujson

from tornado.gen import coroutine

from .consumer import Consumer
from ..utils.observable import Observable

logger = logging.getLogger(__name__)


class NotificationsConsumer(Consumer, Observable):
    """
    Consumes notification that were posted in RabbitMQ
    """
    def __init__(self, rabbitmq_config, binding_key):
        """
        :type rabbitmq_config: pyloggr.config.RabbitMQBaseConfig
        """
        Consumer.__init__(self, rabbitmq_config, binding_key)
        Observable.__init__(self)

    @coroutine
    def start_consuming(self):
        """
        Start consuming notifications from RabbitMQ and notify observers.

        Note
        ====
        This coroutine never terminates
        """
        message_queue = Consumer.start_consuming(self)
        while True:
            message = yield message_queue.get()
            try:
                self.notify_observers(ujson.loads(message.body))
            except Exception:
                logger.exception("Swallowed exception")

