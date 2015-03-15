# encoding: utf-8
__author__ = 'stef'

class RemoveTagEngine(object):
    thread_safe = True

    def __init__(self, directory):
        pass

    def open(self):
        pass

    def close(self):
        pass

    def apply(self, ev, arguments):
        if arguments:
            ev.remove_tags(arguments)
        return True
