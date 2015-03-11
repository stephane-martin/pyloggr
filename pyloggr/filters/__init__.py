# encoding: utf-8
__author__ = 'stef'

from .filters_config import ConfigParser
from .grok import GrokEngine
from .geoip import GeoIPEngine
from .addtag import AddTagEngine
from .removetag import RemoveTagEngine
from ..event import Event


class Filters(object):
    filters_modules = {
        'grok': GrokEngine,
        'geoip': GeoIPEngine,
        'addtag': AddTagEngine,
        'removetag': RemoveTagEngine
    }

    def __init__(self, config_directory):
        self.conf = ConfigParser().parse_config_file(config_directory)
        self._filters = dict([(name, module(config_directory)) for (name, module) in self.filters_modules.items()])

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
                    return_value = self._filters[action.name].apply(ev, arguments)
                    if not return_value:
                        break



