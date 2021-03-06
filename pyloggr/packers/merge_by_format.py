# encoding: utf-8

"""
Format based packer: merge events that start with a defined pattern
"""

__author__ = 'stef'

import re
from tornado.gen import coroutine, Return
from tornado.ioloop import IOLoop

from pyloggr.utils import to_unicode
from . import BasePacker, PackerQueue


class Formatters(object):

    @staticmethod
    def make_formatter(frmat):
        frmat = frmat.replace("[", r"\[")
        frmat = frmat.replace("]", r"\]")
        frmat = frmat.replace("$WORD", r"\S+")
        frmat = frmat.replace("$INT", r"\d+")
        # 2014-12-14 01:44:16 or 141214 01:44:16 or 2015-05-18T20:04:51.660406+00:00
        frmat = frmat.replace("$DATETIME", r"(?P<DATETIME>\d\S+\d(\s|T)\d\S+\dZ?)")
        frmat = frmat.replace("$SEVERITY", r"(?P<SEVERITY>\S+)")
        frmat = frmat.replace("$FACILITY", r"(?P<FACILITY>\S+)")
        frmat = frmat.replace("$MESSAGE", r"(?P<MESSAGE>.*)")
        frmat = frmat.replace("$APP_NAME", r"(?P<APP_NAME>\S+")
        return re.compile(frmat)

    def __init__(self, list_of_formats):
        if isinstance(list_of_formats, list):
            self.formatters = [self.make_formatter(formt) for formt in list_of_formats]
        else:
            self.formatters = [self.make_formatter(list_of_formats)]

    def apply(self, event):
        for formatter in self.formatters:
            match_obj = formatter.search(event.message)
            if match_obj:
                # noinspection PyUnresolvedReferences
                d = match_obj.groupdict()
                if 'FACILITY' in d:
                    event.facility = event.make_facility(d['FACILITY'])
                if 'SEVERITY' in d:
                    event.severity = event.make_severity(d['SEVERITY'])
                if 'DATETIME' in d:
                    event._timereported = event.make_arrow_datetime(d['DATETIME'])
                if 'APP_NAME' in d:
                    event.app_name = to_unicode(d['APP_NAME'])
                if 'MESSAGE' in d:
                    event.message = to_unicode(d['MESSAGE'])
                event.add_tags(u"formatted")
                return True
        return False


class PackerByFormat(BasePacker):
    """
    A format based packer detects the beginning of an event using a pattern, and merges lines until the next
    time the format will be detected

    Parameters
    ==========
    publisher: BasePacker or pyloggr.rabbitmq.publisher.Publisher
        Publisher where to send merge events
    start_formats: str or list
        Format of messages that start a merged event
    queue_max_age: int
        Queues older that queue_max_age, in milliseconds, will be published
    """

    def __init__(self, publisher, start_formats, queue_max_age=5000):
        """
        :type publisher: BasePacker or pyloggr.rabbitmq.publisher.Publisher
        :type start_formats: str or list
        :type queue_max_age: int
        """
        super(PackerByFormat, self).__init__(publisher, queue_max_age)
        self.formatters = Formatters(start_formats)

    @coroutine
    def publish_event(self, event, routing_key=''):
        """
        Publish the event, after merging if necessary

        :param event: the event to publish
        :param routing_key: RabbitMQ routing key
        :type event: pyloggr.event.Event
        :type routing_key: str
        """

        if self.shutting_down:
            # the packer is shutting down, so it doesn't accept any more event
            raise Return((False, event))

        h = hash(event.source)
        if h not in self.queues:
            self.queues[h] = PackerQueue(self.publisher, routing_key)
        queue = self.queues[h]

        if len(queue) == 0:
            if self.formatters.apply(event):
                # the event matches start_format: store it in the empty queue
                status = yield queue.append(event)
                raise Return((status, event))
            else:
                # we don't have any event in queue, and the current event is not a "start event"
                # so we just publish it...
                status, _ = yield self.publisher.publish_event(event, routing_key)
                raise Return((status, event))

        # we have a previous start event in queue
        if self.formatters.apply(event):
            # the current event is also a "start event"
            # empty the queue
            copy_of_queue = queue.copy_and_void()
            # store the current event in the emptied queue
            have_been_published_future = queue.append(event)
            # publish the previous queue
            IOLoop.current().add_callback(copy_of_queue.publish)
            # wait that event has been published
            status = yield have_been_published_future
            raise Return((status, event))
        else:
            # the current event is not a start event, we just append it to the queue
            status = yield queue.append(event)
            raise Return((status, event))
