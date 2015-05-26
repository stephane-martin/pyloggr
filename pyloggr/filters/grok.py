# encoding: utf-8

"""
Grok filter, like in logstash
"""

__author__ = 'stef'

import os
from os.path import basename, dirname, abspath, join
import re
import logging
from itertools import chain

# noinspection PyCompatibility
import regex
from future.utils import python_2_unicode_compatible
from pyloggr.utils import to_unicode
from .base import Engine

PATTERN_NODE_STR = u"""
Pattern:    {0.label}
Regexp:     {0.pattern}
Depends:    {0.depends_as_str}
"""

logger = logging.getLogger(__name__)


@python_2_unicode_compatible
class PatternNode(object):
    """
    Utility class to store grok patterns
    """
    def __init__(self, label, pattern, depends=None):
        self.label = label
        self.pattern = pattern
        self.compiled_pattern = None
        self.depends = depends

    @property
    def depends_as_str(self):
        return ','.join([depend.label for depend in self.depends])

    def str(self):
        return PATTERN_NODE_STR.format(self)

    def __str__(self):
        return self.str()

    def __repr__(self):
        return self.__str__()

    def __hash__(self):
        return hash(self.label)


class GrokEngine(Engine):
    """
    The grok filter matches syslog event with some regexp patterns to extract information
    """
    thread_safe = True

    def __init__(self, config_dir):
        super(GrokEngine, self).__init__(config_dir)
        self._raw_patterns = dict()
        self._pattern_nodes = dict()
        self._patterns_dir = join(config_dir, 'patterns')
        self._read_pattern_files()
        self._build_pattern_tree()
        self._solve_dependencies()
        self._compile()

    def _read_pattern_files(self):
        patterns_files = [
            join(self._patterns_dir, fname)
            for fname in os.listdir(self._patterns_dir)
            if fname.endswith('.patterns')
        ]
        self._raw_patterns = dict()

        # read the patterns files and store them in raw_patterns
        for fname in patterns_files:
            with open(fname, 'rb') as fhandle:
                for line in fhandle:
                    line = line.strip()
                    if len(line) > 0 and not line.startswith('#'):
                        (label, pattern) = line.split(None, 1)
                        self._raw_patterns[to_unicode(label.strip())] = to_unicode(pattern.strip())

    def _build_pattern_tree(self):
        """
        build the tree of patterns and their relations
        """
        self._pattern_nodes = dict()

        for (label, pattern) in self._raw_patterns.items():
            if label not in self._pattern_nodes:
                self._pattern_nodes[label] = PatternNode(label, pattern, [])
            else:
                logger.warning("Grok patterns: ignoring duplicate '{}'".format(label))

        for node in self._pattern_nodes.values():
            patterns_depends = [pattern_depend for (pattern_depend, variable) in re.findall(r'%{(\w+):(\w+)}', node.pattern)]
            patterns_depends.extend([pattern_depend for pattern_depend in re.findall(r'%{(\w+)}', node.pattern)])
            node.depends = set([self._pattern_nodes[label] for label in patterns_depends])

    def _named_grok_to_oniguruma(self, match):
        return "(?P<{}>{})".format(match.group(2), self._pattern_nodes[match.group(1)].pattern)

    def _anonymous_grok_to_regexp(self, match):
        return "({})".format(self._pattern_nodes[match.group(1)].pattern)

    def _solve_dependencies(self):

        while True:
            leaves = {node for node in self._pattern_nodes.values() if len(node.depends) == 0}
            depends_only_leaves = {node for node in self._pattern_nodes.values()
                                   if node.depends.issubset(leaves) and len(node.depends) > 0}
            if len(depends_only_leaves) == 0:
                break

            for node in depends_only_leaves:

                node.pattern = re.sub(
                    pattern=r'%{(\w+):(\w+)}',
                    repl=self._named_grok_to_oniguruma,
                    string=node.pattern
                )

                node.pattern = re.sub(
                    pattern=r'%{(\w+)}',
                    repl=self._anonymous_grok_to_regexp,
                    string=node.pattern
                )

                node.depends = set()

    def _compile(self):
        for node in self._pattern_nodes.values():
            try:
                node.compiled_pattern = regex.compile(node.pattern)
            except regex.error:
                logger.exception("Dropping bad pattern '{}'".format(node.label))

    def __getitem__(self, item):
        return self._pattern_nodes[item].compiled_pattern

    def __iter__(self):
        return self._pattern_nodes.__iter__()

    def __len__(self):
        return len(self._pattern_nodes)

    def keys(self):
        return self._pattern_nodes.keys()

    def search(self, s, patterns):
        if not isinstance(patterns, list):
            patterns = [patterns]
        for pattern in patterns:
            if not pattern:
                logger.warning("Grok: invalid pattern")
            elif pattern in self:
                m = self[pattern].search(s)
                if m:
                    return pattern, m.groupdict()
            else:
                logger.warning("Grok: Unknown pattern: {}".format(pattern))
        return None, None

    def apply(self, ev, args, kw):
        if not args:
            return
        prefix = kw.get('prefix', '')
        prefix = prefix[0] if prefix else ''

        (pattern_name, new_fields) = self.search(ev.message, args)
        if new_fields:
            new_fields = {prefix + label: field for label, field in new_fields.items() if field is not None}
            ev.update_cfields(new_fields)
            ev[prefix + 'grok_pattern'] = pattern_name
            # we stop after first match
            return True
        return False
