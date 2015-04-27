# encoding: utf-8

"""
Packers are used to merge several events onto one single event.
"""

__author__ = 'stef'

import logging
from copy import copy
from itertools import chain
from math import fabs

from tornado.gen import coroutine
from tornado.concurrent import Future
from tornado.ioloop import PeriodicCallback
from arrow import Arrow
from sortedcontainers import SortedSet

from pyloggr.utils.constants import SEVERITY, SEVERITY_TO_INT
from pyloggr.utils import to_unicode

logger = logging.getLogger(__name__)

def merge_events(list_of_events):
    """
    Merge several events into a single one
    :param list_of_events: events to merge
    :type list_of_events: list of Events
    :return: global single Event
    :rtype: Event
    """
    if not list_of_events:
        return None
    # we need a timereported to sort events correctly
    for event in list_of_events:
        if not event.timereported:
            event.timereported = event.timegenerated

    unique_events = SortedSet(list_of_events, key=lambda ev: ev.timereported)
    nb = len(unique_events)
    if nb == 1:
        return unique_events[0]
    # copy first event as a base for the merged event (so that the merged timestamps are correct)
    merged_event = copy(unique_events[0])
    # merge messages
    merged_event.message = u'\n'.join(event.message for event in unique_events)
    # merge tags
    merged_event._tags = set(chain.from_iterable(event.tags for event in unique_events))
    # merge severity
    merged_event.severity = SEVERITY[min(SEVERITY_TO_INT[event.severity] for event in unique_events)]

    # merge custom fields
    merged_custom_fields = {}
    merged_field_keys = {key for event in unique_events for key in event}
    for field_key in merged_field_keys:
        values = {to_unicode(event[field_key]) for event in unique_events if field_key in event}
        # multiples values from different events are merged in a comma separated list
        merged_custom_fields[field_key] = u','.join(values)
    merged_custom_fields['merged_from_nb_event'] = nb
    merged_event.custom_fields = merged_custom_fields

    # merge structured data
    # structured data is a dict
    # event.structured_data[SDID][SD_PARAM_NAME] = SD_PARAM_VALUE
    merged_structured_data = {}
    sdids = {sdid for event in unique_events for sdid in event.structured_data}
    for sdid in sdids:
        merged_structured_data[sdid] = {}
        events = {event for event in unique_events if sdid in event.structured_data}
        params_names = {param_name for event in events for param_name in event['sdid']}
        for param_name in params_names:
            param_values = {
                to_unicode(event['sdid'][param_name]) for event in events if param_name in event['sdid']
            }
            # multiples param_values from different events are merged in a comma separated list
            merged_structured_data[sdid][param_name] = u','.join(param_values)

    merged_event.structured_data = merged_structured_data

    # generate a new UUID
    merged_event.generate_uuid(overwrite=True)
    # generate a new HMAC if needed
    if any(event.hmac for event in unique_events):
        merged_event.generate_hmac(verify=False)

    return merged_event


class PackerQueue(list):
    def __init__(self, publisher):
        super(PackerQueue, self).__init__()
        self.publisher = publisher
        self.last_action = Arrow.utcnow()

    @coroutine
    def publish(self):
        """
        Merge and publish the events that have been stored in the queue
        """
        self.last_action = Arrow.utcnow()
        l = len(self)
        if l == 0:
            return
        elif l == 1:
            merged_event = self[0]
        else:
            merged_event = merge_events(self)

        # publish the merged event to the next publisher
        status, ev = yield self.publisher.publish_event(merged_event)

        # notify that all the events that are stored in this queue have been published
        for event in self:
            event.have_been_published.set_result(status)

        # empty the queue
        self[:] = []
        self.last_action = Arrow.utcnow()

    def append(self, event):
        """
        Append an event to the queue
        """
        super(PackerQueue, self).append(event)
        self.last_action = Arrow.utcnow()
        event.have_been_published = Future()
        return event.have_been_published


class BasePacker(object):
    def __init__(self, publisher, queue_max_age):
        self.queues = dict()
        self.publisher = publisher
        self.shutting_down = False
        self.queue_max_age = int(queue_max_age)
        self.flushing = False
        self.periodic = PeriodicCallback(self.flush, max(self.queue_max_age, 2000))
        self.periodic.start()

    def publish_event(self, event):
        raise NotImplementedError

    def shutdown(self):
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
        self.flushing = False

        # here queues are empty, so we can stop operations if needed
        if self.shutting_down and self.periodic is not None:
            self.periodic.stop()
            self.periodic = None
