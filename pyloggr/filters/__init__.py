# encoding: utf-8

"""
The `filters` subpackage contains implementation of the different event filters. Event filters
are used by the `parser` process.

----------------------------

"""
__author__ = 'stef'

from os.path import join
from threading import Lock

from .build_config import ConfigParser
from .grok import GrokEngine
from .geoip import GeoIPEngine
from .addtag import AddTagEngine
from .removetag import RemoveTagEngine
from .drop import DropException, DropEngine
from .useragent import UserAgentEngine
from ..event import Event


class Filters(object):
    filters_modules = {
        'grok': GrokEngine,
        'geoip': GeoIPEngine,
        'addtag': AddTagEngine,
        'removetag': RemoveTagEngine,
        'drop': DropEngine,
        'useragent': UserAgentEngine
    }

    filters_locks = dict([(name, Lock()) for name in filters_modules])

    def __init__(self, config_directory):
        self.conf = ConfigParser().parse_config_file(join(config_directory, 'filters.conf'))
        self._filters = dict(
            [(name, module(config_directory)) for (name, module) in self.filters_modules.items()]
        )

    def open(self):
        for module in self._filters.values():
            module.open()

    def close(self):
        for module in self._filters.values():
            module.close()

    def apply(self, ev):
        """
        :type ev: Event
        """
        for statement in self.conf:
            if statement.condition.apply(ev):
                for action in statement.actions:
                    arguments = [arg.apply(ev) for arg in action.arguments]
                    if self._filters[action.name].thread_safe:
                        return_value = self._filters[action.name].apply(ev, arguments)
                    else:
                        with self.filters_locks[action.name]:
                            return_value = self._filters[action.name].apply(ev, arguments)
                    if not return_value:
                        break



