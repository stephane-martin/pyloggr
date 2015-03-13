# encoding: utf-8
__author__ = 'stef'

import logging
from abc import ABCMeta, abstractmethod
from tornado.gen import coroutine
import ujson


logger = logging.getLogger(__name__)


class Observable(object):
    """
    An observable produces notifications and sends them to observers
    """

    def __init__(self):
        self.observers = list()
        self.queue = None

    def register(self, observer):
        """
        Subscribe an observe for future notifications

        :param observer: object that implements the Observer interface
        :type observer: Observer
        """
        if observer not in self.observers:
            self.observers.append(observer)

    def unregister(self, observer):
        """
        Unsubscribe an observer

        :param observer: object that implements the Observer interface
        :type observer: Observer
        """
        if observer in self.observers:
            self.observers.remove(observer)

    def unregister_all(self):
        """
        Unsubscribe all observers
        """
        if self.observers:
            del self.observers[:]
        self.queue = None

    @coroutine
    def notify_observers(self, d, routing_key=None):
        """
        Notify observers that the observable has a message for them

        :param d: message
        :type d: dict
        :param routing_key: unused

        Note
        ====
        Tornado coroutine
        """

        for observer in self.observers:
            try:
                observer.notified(d)
            except Exception:
                logger.exception("Swallowing exception that happened in some observer")


class NotificationProducer(Observable):
    """
    A NotificationProducer produces some notifications and sends them to RabbitMQ
    """
    def register_queue(self, publisher):
        self.queue = publisher

    def unregister_queue(self):
        self.queue = None

    @coroutine
    def notify_observers(self, d, routing_key=None):
        """
        :param d: a message to send to observers
        :type d: dict
        :param routing_key: routing key for the message
        :type routing_key: str

        Note
        ====
        Tornado coroutine
        """
        for observer in self.observers:
            try:
                observer.notified(d)
            except Exception:
                logger.exception("Swallowing exception that happened in some observer")
        if not self.queue:
            logger.debug("No notification queue")
            return
        if not routing_key:
            return
        logger.debug("Sending notification with routing key '{}'".format(routing_key))
        try:
            json_message = ujson.dumps(d)
            yield self.queue.publish(
                exchange='pyloggr.pubsub',
                body=json_message,
                routing_key=routing_key,
                persistent=False
            )
        except Exception:
            logger.exception("Swallowing exception that happened in queue")


class Observer(object):
    """
    Implemented by classes that should be observers
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def notified(self, *args, **kwargs):
        pass

