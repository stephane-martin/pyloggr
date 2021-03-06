# encoding: utf-8
__author__ = 'stef'

import logging
from abc import ABCMeta, abstractmethod
from future.utils import with_metaclass
from tornado.ioloop import IOLoop
import ujson


logger = logging.getLogger(__name__)


class Observable(object):
    """
    An observable produces notifications and sends them to observers
    """

    def __init__(self):
        self.observers = list()
        self.publisher = None

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
        self.publisher = None

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
    def register_publisher(self, publisher):
        self.publisher = publisher

    def unregister_publisher(self):
        self.publisher = None

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
            # noinspection PyBroadException
            try:
                observer.notified(d)
            except Exception:
                logger.exception("notify_observers: swallowing exception")

        if not self.publisher:
            logger.debug("No notification queue")
            return

        logger.debug("Sending notification with routing key '{}'".format(routing_key))

        if not routing_key:
            routing_key = "pyloggr.generic.notification"

        json_message = ujson.dumps(d)
        future = self.publisher.publish(
            exchange='pyloggr.pubsub',
            body=json_message,
            routing_key=routing_key,
            persistent=False
        )
        IOLoop.current().add_future(future, self.after_published)

    @classmethod
    def after_published(cls, future):
        # noinspection PyBroadException
        try:
            future.result()
        except:
            logger.exception("Exception happened while publishing notification to RabbitMQ")


class Observer(with_metaclass(ABCMeta, object)):
    """
    Implemented by classes that should be observers
    """

    @abstractmethod
    def notified(self, *args, **kwargs):
        pass

