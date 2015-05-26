# encoding: utf-8

"""
Packers are used to merge several events onto one single event.
"""

__author__ = 'stef'

import logging
from copy import copy
from math import fabs

from tornado.gen import coroutine
from tornado.concurrent import Future
from tornado.ioloop import PeriodicCallback
from arrow import Arrow
from sortedcontainers import SortedSet

from pyloggr.utils.constants import SEVERITY, SEVERITY_TO_INT

logger = logging.getLogger(__name__)


def merge_events(list_of_events):
    """
    Merge several events into a single one

    :param list_of_events: events to merge
    :type list_of_events: list of pyloggr.event.Event
    :return: global single Event
    :rtype: pyloggr.event.Event
    """
    if not list_of_events:
        return None

    unique_events = SortedSet(list_of_events)
    nb = len(unique_events)
    if nb == 1:
        return unique_events[0]
    # copy first event as a base for the merged event (so that the merged timestamps are correct)
    merged_event = copy(unique_events[0])
    # merge messages
    merged_event.message = u'\n'.join(event.message for event in unique_events)
    # merge severity
    merged_event.severity = SEVERITY[min(SEVERITY_TO_INT[event.severity] for event in unique_events)]

    # merge structured data (included tags and custom fields)
    list(merged_event.structured_data.update(event.structured_data) for event in unique_events)

    # generate a new UUID
    merged_event.generate_uuid()
    # generate a new HMAC if needed
    merged_event.generate_hmac(verify_if_exists=False)

    return merged_event


class PackerQueue(list):
    """
    PackerQueues temporarily store events that "match together" before they are published.
    """
    def __init__(self, publisher, routing_key, i=None):
        i = [] if i is None else i
        super(PackerQueue, self).__init__(i)
        self.publisher = publisher
        self.routing_key = routing_key
        self.last_action = Arrow.utcnow()

    def copy_and_void(self):
        """
        Empty the queue and return a copy of it
        """
        q = PackerQueue(self.publisher, self.routing_key, i=self)
        self[:] = []
        self.last_action = Arrow.utcnow()
        return q

    @coroutine
    def publish(self):
        """
        Merge and publish the events that have been stored in the queue
        """
        l = len(self)
        if l == 0:
            return
        copy_of_queue = self.copy_and_void()
        merged_event = merge_events(copy_of_queue)
        # publish the merged event to the next publisher
        status, ev = yield self.publisher.publish_event(merged_event, self.routing_key)
        # notify that all the events that are stored in this queue have been published
        for event in copy_of_queue:
            event.have_been_published.set_result(status)

    def append(self, event):
        """
        Append an event to the queue

        :param event: the event to append
        :type event: pyloggr.event.Event
        """
        super(PackerQueue, self).append(event)
        self.last_action = Arrow.utcnow()
        event.have_been_published = Future()
        return event.have_been_published


class BasePacker(object):
    """
    Boilerplate for concrete packers
    """
    def __init__(self, publisher, queue_max_age):
        self.queues = dict()
        self.publisher = publisher
        self.shutting_down = False
        self.queue_max_age = int(queue_max_age)
        self.flushing = False
        self.periodic = PeriodicCallback(self.flush, max(self.queue_max_age, 2000))
        self.periodic.start()

    def publish_event(self, event, routing_key):
        raise NotImplementedError

    def shutdown(self):
        """
        Cleanly stop the packer activity
        """
        if self.shutting_down:
            return
        self.shutting_down = True
        # notify the next packer
        if isinstance(self.publisher, BasePacker):
            self.publisher.shutdown()

    @coroutine
    def flush(self):
        """
        Periodically flush the events stored in Packer queues
        """
        if self.flushing:
            # for safety, but shouldn't happen
            return
        self.flushing = True
        now = Arrow.utcnow()
        publications = list()
        for queue in self.queues.values():
            age = fabs((now - queue.last_action).total_seconds()) * 1000
            if age > self.queue_max_age and len(queue) > 0:
                publications.append(queue.publish())
        if publications:
            yield publications

        # here queues are empty, so we can stop operations if needed
        if self.shutting_down and self.periodic is not None:
            self.periodic.stop()
            self.periodic = None

        self.flushing = False
