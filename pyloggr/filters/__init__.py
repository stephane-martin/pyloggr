# encoding: utf-8

"""
The `filters` subpackage contains implementation of the different event filters. Event filters
are used by the `parser` process.

----------------------------

"""
__author__ = 'stef'

from os.path import join
import logging
from threading import Lock

from .build_config import ConfigParser, FilterBlock, IfBlock, IfFilterBlock, Assignment, TagsAssignment
from .grok import GrokEngine
from .geoip import GeoIPEngine
from .useragent import UserAgentEngine


class DropException(Exception):
    pass


class Filters(object):
    filters_modules = {
        'grok': GrokEngine,
        'geoip': GeoIPEngine,
        'useragent': UserAgentEngine
    }

    filters_locks = dict([(name, Lock()) for name in filters_modules])

    def __init__(self, config_directory):
        self.filter_instructions = ConfigParser().parse_config_file(join(config_directory, 'filters.conf'))
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

        logger = logging.getLogger(__name__)

        def apply_one_statement(statement):
            if isinstance(statement, FilterBlock):
                calculated_args = map(lambda argument: argument.apply(ev), statement.filter_arguments)
                calculated_kw_args = {k: v.apply(ev) for (k, v) in statement.filter_kw_arguments.items()}
                if self._filters[statement.filter_name].thread_safe:
                    return_value = self._filters[statement.filter_name].apply(ev, calculated_args, calculated_kw_args)
                else:
                    with self.filters_locks[statement.filter_name]:
                        return_value = self._filters[statement.filter_name].apply(ev, calculated_args, calculated_kw_args)
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

            elif isinstance(statement, Assignment) or isinstance(statement, TagsAssignment):
                statement.apply(ev)

            elif statement == 'drop':
                logger.debug("Drop the event...")
                raise DropException()
            elif statement == 'stop':
                logger.debug("Stop processing the event...")
                return

            else:
                raise RuntimeError("Unknown statement type")

        map(apply_one_statement, self.filter_instructions)

