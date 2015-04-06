# encoding: utf-8

"""
Time based packer.

"""

__author__ = 'stef'

from math import fabs

from tornado.gen import coroutine, Return
from spooky_hash import Hash128

from pyloggr.event import Event

# merge events when they were emitted in "interval" ms
interval = 250


class TimeBasedPacker(object):
    """
    A time based packer merges several events if they came from the same source and were sent roughly at the same
    time.
    """

    def __init__(self, publisher):
        """
        :type publisher: pyloggr.rabbitmq.publisher.Publisher
        """
        self.queues = dict()
        self.publisher = publisher

    @coroutine
    def publish_event(self, exchange, event, routing_key='', persistent=True):
        """
        Publish an event to RabbitMQ, after having merged it with similar events.

        :param exchange: RabbitMQ exchange to publish to
        :param event: event to publish
        :param routing_key: RabbitMQ routing key for this event
        :param persistent: whether the event should be disk-persisted by RabbitMQ
        :type exchange: str
        :type event: pyloggr.event.Event
        :type routing_key: str
        :type persistent: bool
        """

        if event.timereported is None:
            # we don't know when the event was emitted, so we just publish it
            status = yield self.publisher.publish_event(exchange, event, routing_key, persistent)
            raise Return((status, event))

        # separate events based on source, facility, severity, app_name
        key = Hash128()
        key.update(event.source.encode('utf-8'))
        key.update(event.facility.encode('utf-8'))
        key.update(event.severity.encode('utf-8'))
        key.update(event.app_name.encode('utf-8'))
        key = key.digest()

        if key not in self.queues:
            # that's the first event of its kind: store it and return
            self.queues[key] = [event]
            raise Return((None, None))
        if len(self.queues[key]) == 0:
            # that's the first event of its kind: store it and return
            self.queues[key] = [event]
            raise Return((None, None))

        first_event = self.queues[key][0]
        diff = event.timereported - first_event.timereported
        diff = fabs(diff.total_seconds()) * 1000
        if diff < interval:
            # same series of events
            self.queues[key].append(event)
            raise Return((None, None))

        # late: the last event is a 'new' event: merge and publish the previous ones
        merged_event = Event.merge(self.queues[key])
        # publish the merged event
        status, ev = yield self.publisher.publish_event(exchange, merged_event, routing_key, persistent)
        # the initial event is not part of the series, we store it
        self.queues[key] = [event]
        raise Return((status, merged_event))


