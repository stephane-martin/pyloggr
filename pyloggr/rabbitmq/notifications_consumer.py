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
        Consumer.__init__(self, rabbitmq_config, binding_key)
        Observable.__init__(self)

    @coroutine
    def start_consuming(self):
        message_queue = super(NotificationsConsumer, self).start_consuming()
        while True:
            message = yield message_queue.get()
            try:
                self.notify_observers(ujson.loads(message.body))
            except Exception:
                logger.exception("Swallowed exception")

