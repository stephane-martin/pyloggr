# encoding: utf-8
__author__ = 'stef'

class RemoveTagEngine(object):
    def __init__(self, directory):
        pass

    def open(self):
        pass

    def close(self):
        pass

    def apply(self, ev, arguments):
        ev.remove_tags(arguments)
        return True
