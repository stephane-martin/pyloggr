# encoding: utf-8
__author__ = 'stef'

from ..event import Event

class AddTagEngine(object):
    def __init__(self, directory):
        pass

    def open(self):
        pass

    def close(self):
        pass

    def apply(self, ev, arguments):
        """
        :type ev: Event
        """
        ev.add_tags(arguments)
        return True
