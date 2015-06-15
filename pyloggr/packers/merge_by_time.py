# encoding: utf-8

"""
Time based packer: merge events when they were emitted in a narrow interval of time
"""

__author__ = 'stef'

from math import fabs

from tornado.gen import coroutine, Return
from tornado.ioloop import IOLoop
from spooky_hash import Hash128


from . import BasePacker, PackerQueue


class PackerByTime(BasePacker):
    """
    A time based packer merges several events if they came from the same source and were sent roughly at the same
    time.

    Parameters
    ==========
    publisher: BasePacker or pyloggr.rabbitmq.publisher.Publisher
        Publisher where to send merge events
    merge_event_window: int
        Events sent in the merge_event_window interval, in milliseconds, will be merged
    queue_max_age: int
        Queues older that queue_max_age, in milliseconds, will be published
    """

    def __init__(self, publisher, merge_event_window=250, queue_max_age=5000):
        """
        :type merge_event_window: int
        :type queue_max_age: int
        """
        super(PackerByTime, self).__init__(publisher, queue_max_age)
        self.merge_event_window = int(merge_event_window)

    @coroutine
    def publish_event(self, event, routing_key=''):
        """
        publish_event(event, routing_key='')
        Publish an event to RabbitMQ, after having merged it with similar events.

        :param event: event to publish
        :param routing_key: RabbitMQ routing key
        :type event: pyloggr.event.Event
        :type routing_key: str

        Note
        ====
        Tornado coroutine
        """

        if self.shutting_down:
            # the packer is shutting down, so it doesnt accept any more event
            raise Return((False, event))

        if event.timereported is None:
            # we don't know when the event was emitted, so we just publish it
            status, _ = yield self.publisher.publish_event(event, routing_key)
            raise Return((status, event))

        # separate events using (source, facility, app_name)
        key = Hash128()
        key.update(event.source.encode('utf-8'))
        key.update(event.facility.encode('utf-8'))
        key.update(event.app_name.encode('utf-8'))
        key = key.digest()

        if key not in self.queues:
            self.queues[key] = PackerQueue(self.publisher, routing_key)
        queue = self.queues[key]

        if len(queue) == 0:
            # that's the first event of its kind: store it in the queue
            status = yield queue.append(event)
            # yield returns only after the event has been published
            raise Return((status, event))

        first_event = queue[0]
        diff = event.timereported - first_event.timereported
        diff = fabs(diff.total_seconds()) * 1000
        if diff >= self.merge_event_window:
            # the last event was emitted too late: publish and empty the queue
            copy_of_queue = queue.copy_and_void()
            has_been_published = queue.append(event)
            IOLoop.current().add_callback(copy_of_queue.publish)
        else:
            has_been_published = queue.append(event)

        # store the current event in the queue
        status = yield has_been_published
        # yield returns only after the event has been published
        raise Return((status, event))
