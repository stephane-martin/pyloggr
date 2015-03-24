# encoding: utf-8
__author__ = 'stef'


class DropException(Exception):
    pass


class DropEngine(object):
    """
    Just drops the current event
    """
    thread_safe = True
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
        raise DropException()
