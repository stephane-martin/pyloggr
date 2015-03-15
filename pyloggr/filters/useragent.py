# encoding: utf-8
__author__ = 'stef'

import logging

import httpagentparser

logger = logging.getLogger(__name__)


class UserAgentEngine(object):
    thread_safe = True

    def __init__(self, directory):
        pass

    def open(self):
        pass

    def close(self):
        pass

    def apply(self, ev, arguments):
        """
        :type ev: pyloggr.event.Event
        """
        for arg in arguments:
            d = httpagentparser.detect(arg)
            if d.get('browser', None):
                name = d['browser'].get('name', None)
                version = d['browser'].get('version', None)
                if name:
                    ev['ua.browser'] = name
                if version:
                    ev['ua.browser.version'] = version
            if d.get('platform', None):
                op_sys = d['platform'].get('name', None)
                version = d['platform'].get('version', None)
                if op_sys:
                    ev['ua.os'] = op_sys
                if version:
                    ev['ua.os.version'] = version
            if d.get('bot'):
                ev.add_tags('bot')
