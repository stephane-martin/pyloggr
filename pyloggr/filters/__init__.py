# encoding: utf-8

"""
The `filters` subpackage contains implementation of the different event filters. Event filters
are used by the `filtermachine` process.

----------------------------
"""

__author__ = 'stef'

from os.path import join
import logging
from threading import Lock
from itertools import chain, imap

from future.utils import viewitems, viewvalues
from .build_filters_config import parse_config_file, FilterBlock, IfBlock, IfFilterBlock, Assignment, TagsAssignment
from .grok import GrokEngine
from .geoip import GeoIPEngine
from .useragent import UserAgentEngine
from .router import RouterEngine


class DropException(Exception):
    """
    DropException is raised when an event should be dropped
    """


class Filters(object):
    """
    Provide the logic for applying the filters to syslog events
    """
    filters_modules = {
        'grok': GrokEngine,
        'geoip': GeoIPEngine,
        'useragent': UserAgentEngine,
        'router': RouterEngine
    }

    filters_locks = dict([(name, Lock()) for name in filters_modules])

    def __init__(self, config_directory, filename):
        self.filter_instructions = parse_config_file(join(config_directory, 'filters', filename + '.conf'))
        # here we initialize the filters
        self._filters = {
            name: module(config_directory) for (name, module) in viewitems(self.filters_modules)
        }

    def open(self):
        """
        Initialize the filters
        """
        [module.open() for module in viewvalues(self._filters)]

    def close(self):
        """
        Cleanly close the filters
        """
        [module.close() for module in viewvalues(self._filters)]

    def apply(self, ev):
        """
        Apply all the filters to the given event

        :param ev: event
        :type ev: pyloggr.event.Event
        """

        logger = logging.getLogger(__name__)

        def _apply_one_statement(statement):
            if isinstance(statement, FilterBlock):
                calculated_args = list(chain.from_iterable(
                    imap(lambda argument: argument.apply(ev), statement.filter_arguments)
                ))
                calculated_kw_args = {k: v.apply(ev) for (k, v) in viewitems(statement.filter_kw_arguments)}
                if self._filters[statement.filter_name].thread_safe:
                    return_value = self._filters[statement.filter_name].apply(
                        ev,
                        calculated_args,
                        calculated_kw_args
                    )
                else:
                    # this engine is not considered thread-safe: we acquire a lock
                    with self.filters_locks[statement.filter_name]:
                        return_value = self._filters[statement.filter_name].apply(
                            ev,
                            calculated_args,
                            calculated_kw_args
                        )
                return return_value

            elif isinstance(statement, IfBlock):
                if statement.condition.eval(ev):
                    map(_apply_one_statement, statement.statements)
                elif statement.else_statements:
                    map(_apply_one_statement, statement.else_statements)

            elif isinstance(statement, IfFilterBlock):
                return_value = _apply_one_statement(statement.filter)
                if return_value:
                    map(_apply_one_statement, statement.statements)
                elif statement.else_statements:
                    map(_apply_one_statement, statement.else_statements)

            elif isinstance(statement, Assignment) or isinstance(statement, TagsAssignment):
                statement.apply(ev)

            elif statement == 'drop':
                raise DropException()
            elif statement == 'stop':
                logger.debug("Stop processing the event...")
                return

            else:
                raise RuntimeError("Unknown statement type")

        map(_apply_one_statement, self.filter_instructions)
