# encoding: utf-8
__author__ = 'stef'

import os
from os.path import basename, dirname, abspath, join
import re
import logging

import regex

from future.utils import python_2_unicode_compatible

from ..utils.fix_unicode import to_unicode


PATTERN_NODE_STR = u"""
Pattern:    {0.label}
Regexp:     {0.pattern}
Depends:    {0.depends_as_str}
"""

logger = logging.getLogger(__name__)


@python_2_unicode_compatible
class PatternNode(object):
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


class GrokEngine(object):
    def __init__(self, config_dir):
        self._raw_patterns = dict()
        self._pattern_nodes = dict()
        self._patterns_dir = join(config_dir, 'patterns')
        self._read_pattern_files()
        self._build_pattern_tree()
        self._solve_dependencies()
        self._compile()

    def _read_pattern_files(self):
        patterns_files = [join(self._patterns_dir, fname)
                          for fname in os.listdir(self._patterns_dir)
                          if fname.endswith('.patterns')]
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
        """build the tree of patterns and their relations"""
        self._pattern_nodes = dict()

        for (label, pattern) in self._raw_patterns.items():
            self._pattern_nodes[label] = PatternNode(label, pattern, [])

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
            node.compiled_pattern = regex.compile(node.pattern)

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
            if pattern in self:
                m = self[pattern].search(s)
                if m:
                    return pattern, m.groupdict()
            else:
                logger.warning("Grok: Unknown pattern: {}".format(pattern))
        return None, None

    def apply(self, ev, patterns):
        # search should be thread safe, but are we sure ?
        (pattern_name, new_fields) = self.search(ev.message, patterns)
        if new_fields:
            ev.update(new_fields)
            ev['grok_pattern'] = pattern_name
            return True
        return False

    def open(self):
        pass

    def close(self):
        pass
