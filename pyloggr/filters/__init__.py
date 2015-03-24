# encoding: utf-8

"""
The `filters` subpackage contains implementation of the different event filters. Event filters
are used by the `parser` process.

----------------------------

"""
__author__ = 'stef'

from os.path import join
from threading import Lock

from .build_config import ConfigParser, FilterBlock, IfBlock, IfFilterBlock
from .grok import GrokEngine
from .geoip import GeoIPEngine
from .addtag import AddTagEngine
from .removetag import RemoveTagEngine
from .drop import DropException, DropEngine
from .useragent import UserAgentEngine


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
        Apply the configured filters to the given event

        :param ev: event
        :type ev: pyloggr.event.Event
        """

        def apply_one_statement(statement):
            if isinstance(statement, FilterBlock):
                calculated_arguments = map(lambda argument: argument.apply(ev), statement.filter_arguments)
                if self._filters[statement.filter_name].thread_safe:
                    return_value = self._filters[statement.filter_name].apply(ev, calculated_arguments)
                else:
                    with self.filters_locks[statement.filter_name]:
                        return_value = self._filters[statement.filter_name].apply(ev, calculated_arguments)
                return return_value

            elif isinstance(statement, IfBlock):
                if statement.condition.apply(ev):
                    map(apply_one_statement, statement.statements)
                elif statement.else_statements:
                    map(apply_one_statement, statement.else_statements)

            elif isinstance(statement, IfFilterBlock):
                return_value = apply_one_statement(statement.filter)
                if return_value:
                    map(apply_one_statement, statement.statements)
                elif statement.else_statements:
                    map(apply_one_statement, statement.else_statements)

            else:
                raise RuntimeError("Unknown statement type")

        map(apply_one_statement, self.conf)

