# encoding: utf-8
__author__ = 'stef'

import logging
from abc import ABCMeta, abstractmethod
from tornado.gen import coroutine
import ujson


logger = logging.getLogger(__name__)


class Observable(object):

    def __init__(self):
        self.observers = list()
        self.queue = None

    def register(self, observer):
        """
        :param observer: object that implements the Observer interface
        :type observer: Observer
        """
        if observer not in self.observers:
            self.observers.append(observer)

    def unregister(self, observer):
        """
        :param observer: object that implements the Observer interface
        :type observer: Observer
        """
        if observer in self.observers:
            self.observers.remove(observer)

    def unregister_all(self):
        if self.observers:
            del self.observers[:]
        self.queue = None

    @coroutine
    def notify_observers(self, d):
        for observer in self.observers:
            try:
                observer.notified(d)
            except Exception:
                logger.exception("Swallowing exception that happened in some observer")


class NotificationProducer(Observable):
    def register_queue(self, publisher):
        self.queue = publisher

    def unregister_queue(self):
        self.queue = None

    @coroutine
    def notify_observers(self, d):
        """
        :param d: a message to send to observers
        :type d: dict
        """
        for observer in self.observers:
            try:
                observer.notified(d)
            except Exception:
                logger.exception("Swallowing exception that happened in some observer")
        if not self.queue:
            logger.debug("No notification queue")
            return
        logger.debug("Sending notification with routing key '{}'".format(d['subject']))
        try:
            json_message = ujson.dumps(d)
            yield self.queue.publish(
                exchange='pyloggr.pubsub',
                body=json_message,
                routing_key=d['subject'],
                persistent=False
            )
        except Exception:
            logger.exception("Swallowing exception that happened in queue")


class Observer(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def notified(self, *args, **kwargs):
        pass

