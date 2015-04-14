# encoding: utf-8

"""
Packers are used to merge several events onto one single event.
"""

__author__ = 'stef'

from tornado.gen import coroutine
from tornado.concurrent import Future
from arrow import Arrow
from pyloggr.event import Event


class Queue(list):
    def __init__(self, publisher, exchange, routing_key, persistent):
        super(Queue, self).__init__()
        self.exchange = exchange
        self.routing_key = routing_key
        self.publisher = publisher
        self.persistent = persistent
        self.last_action = Arrow.utcnow()

    @coroutine
    def publish(self):
        self.last_action = Arrow.utcnow()
        l = len(self)
        if l == 0:
            return
        elif l == 1:
            merged_event = self[0]
        else:
            merged_event = Event.merge(self)
        # publish the merged event
        status, ev = yield self.publisher.publish_event(self.exchange, merged_event, self.routing_key, self.persistent)
        # proclaim that all the events that are store in this queue have been published
        for event in self:
            event.have_been_published.set_result(status)
        # empty the queue
        self[:] = []
        self.last_action = Arrow.utcnow()

    def append(self, event):
        super(Queue, self).append(event)
        event.have_been_published = Future()
        self.last_action = Arrow.utcnow()


class BasePacker(object):
    def __init__(self, publisher):
        """
        :type publisher: pyloggr.rabbitmq.publisher.Publisher
        """
        self.queues = dict()
        self.publisher = publisher

