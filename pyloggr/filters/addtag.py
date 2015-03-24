# encoding: utf-8
__author__ = 'stef'


class AddTagEngine(object):
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
        if arguments:
            ev.add_tags(arguments)
        return True
