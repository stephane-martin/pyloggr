# encoding: utf-8

"""
Filter to parse user-agent strings
"""

__author__ = 'stef'

import logging
import httpagentparser
from .base import Engine

logger = logging.getLogger(__name__)


class UserAgentEngine(Engine):
    """
    A filter to parse browser user-agent strings

    Parameters
    ==========
    directory: str
        pyloggr main configuration directory
    """
    thread_safe = True

    def __init__(self, directory):
        super(UserAgentEngine, self).__init__(directory)

    def apply(self, ev, args, kw):
        """
        Parse a user-agent string in an event

        If apply succeeds, the following custom fields can be added to the event:

        - browser
        - browser_version
        - browser_os
        - browser_os_version

        If the user agent is identified as bot, the `bot` tag is added to the event.

        Custom fields names can be prefixed using the `prefix` kw argument.

        :type ev: pyloggr.event.Event
        """
        if not args:
            logger.debug("useragent filter needs at least one argument")
            return False
        prefix = kw.get('prefix', '')
        prefix = prefix[0] if prefix else ''
        res = False
        for arg in args:
            d = httpagentparser.detect(arg)
            if d.get('browser', None):
                name = d['browser'].get('name', None)
                version = d['browser'].get('version', None)
                if name:
                    ev[prefix + 'browser'].add(name)
                    res = True
                if version:
                    ev[prefix + 'browser_version'].add(version)
                    res = True
            if d.get('platform', None):
                op_sys = d['platform'].get('name', None)
                version = d['platform'].get('version', None)
                if op_sys:
                    ev[prefix + 'browser_os'].add(op_sys)
                    res = True
                if version:
                    ev[prefix + 'browser_os_version'].add(version)
                    res = True
            if d.get('bot'):
                ev.add_tags('bot')
        return res
