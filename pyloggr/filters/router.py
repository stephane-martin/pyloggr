# encoding: utf-8

"""
Router filter: choose RabbitMQ destination for the event
"""

from __future__ import absolute_import, division, print_function

__author__ = 'stef'

import logging

logger = logging.getLogger(__name__)

from .base import Engine

class RouterEngine(Engine):
    def apply(self, ev, args, kw):
        exchanges = args
        if kw.get('exchange', None):
            exchanges.extend(kw['exchange'].split(','))
        exchanges = map(lambda e: e.strip().lower(), exchanges)
        exchanges = filter(lambda e: bool(e), exchanges)
        ev.override_exchanges = exchanges
        if kw.get('event_type', None):
            event_type = kw['event_type'].strip().lower()
            if event_type:
                ev.override_event_type = event_type
