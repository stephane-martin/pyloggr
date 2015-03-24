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

    @classmethod
    def apply(cls, ev, args, kw):
        """
        :type ev: pyloggr.event.Event
        """
        if not args:
            logger.debug("useragent filter needs at least one argument")
            return False
        res = False
        prefix = kw.get('prefix', '')
        for arg in args:
            if not arg:
                continue
            d = httpagentparser.detect(arg)
            if d.get('browser', None):
                name = d['browser'].get('name', None)
                version = d['browser'].get('version', None)
                if name:
                    ev[prefix + 'browser'] = name
                    res = True
                if version:
                    ev[prefix + 'browser_version'] = version
                    res = True
            if d.get('platform', None):
                op_sys = d['platform'].get('name', None)
                version = d['platform'].get('version', None)
                if op_sys:
                    ev[prefix + 'browser_os'] = op_sys
                    res = True
                if version:
                    ev[prefix + 'browser_os_version'] = version
                    res = True
            if d.get('bot'):
                ev.add_tags('bot')
        return res
