# encoding: utf-8

"""
Time based packer.

"""

__author__ = 'stef'

from math import fabs

from tornado.gen import coroutine, Return
from spooky_hash import Hash128
from arrow import Arrow

from . import BasePacker, Queue

# merge events when they were emitted in "interval" ms
MERGE_EVENT_WINDOW = 250

PACKER_QUEUE_MAX_AGE = 5000


class TimeBasedPacker(BasePacker):
    """
    A time based packer merges several events if they came from the same source and were sent roughly at the same
    time.
    """

    def __init__(self, publisher):
        """
        :type publisher: pyloggr.rabbitmq.publisher.Publisher
        """
        super(TimeBasedPacker, self).__init__(publisher)
        self.flushing = False

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
            self.queues[key] = Queue(self.publisher, exchange, routing_key, persistent)
        queue = self.queues[key]
        if len(queue) == 0:
            # that's the first event of its kind: store it
            queue.append(event)
            status = yield event.have_been_published
            raise Return((status, event))

        first_event = queue[0]
        diff = event.timereported - first_event.timereported
        diff = fabs(diff.total_seconds()) * 1000
        if diff >= MERGE_EVENT_WINDOW:
            yield queue.publish()

        queue.append(event)
        status = yield event.have_been_published
        raise Return((status, event))

    @coroutine
    def flush(self):
        """
        Periodically flush the events stored in Packer queues
        """
        if self.flushing:
            return
        self.flushing = True
        now = Arrow.utcnow()
        publications = list()
        for queue in self.queues:
            age = fabs((now - queue.last_action).total_seconds()) * 1000
            if age > PACKER_QUEUE_MAX_AGE and len(queue) > 0:
                publications.append(queue.publish())
        if publications:
            yield publications
        self.flushing = False
