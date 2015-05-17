# encoding: utf-8

"""
Abstract base filter
"""

__author__ = 'stef'

from abc import ABCMeta, abstractmethod
from future.utils import with_metaclass


class Engine(with_metaclass(ABCMeta, object)):
    """
    Abstract base class for filters

    """
    thread_safe = True

    def __init__(self, directory):
        """
        :type directory: str
        """
        self.directory = directory

    def open(self):
        """
        Filter initialization
        """
        pass

    def close(self):
        """
        Filter cleaning
        """
        pass

    @abstractmethod
    def apply(self, ev, args, kw):
        """
        Apply the filter to the given event. TooOverride in concrete filters.

        :param ev: event
        :param args: filter arguments
        :param kw: filter keyword arguments
        """
        pass
