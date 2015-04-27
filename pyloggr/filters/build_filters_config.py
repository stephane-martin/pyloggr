# encoding: utf-8

"""
Pyparsing stuff to parse `filters_config`
"""

__author__ = 'stef'


import logging
from pyparsing import Suppress, Forward, Optional, Group, OneOrMore, delimitedList, ParseException
from future.utils import raise_from
from pyloggr.utils.fix_unicode import to_unicode
from pyloggr.utils.parsing_classes import label, kw_arg_assign, built_string
from pyloggr.utils.parsing_classes import filter_name, tags, custom_field, assign, tags_assign
from pyloggr.utils.parsing_classes import comment_line, if_cond, drop, stop, condition
from pyloggr.utils.parsing_classes import else_cond, open_delim, close_delim


def make_kw_argument(toks):
    return toks[0][0], toks[0][1]


def make_if_block(toks):
    return IfBlock.from_tokens(toks[0])


def make_if_filter_block(toks):
    return IfFilterBlock.from_tokens(toks[0])


def make_assignment(toks):
    return Assignment.from_tokens(toks[0])


def make_tags_assignment(toks):
    return TagsAssignment.from_tokens(toks[0])


# noinspection PyUnusedLocal
def make_filter(s, loc, toks):
    return FilterBlock.from_tokens(toks)


filter_kw_argument = Group(label + kw_arg_assign + built_string).setParseAction(make_kw_argument)
filter_argument = built_string | filter_kw_argument
filter_arguments = Optional(delimitedList(filter_argument))
filter_statement = (filter_name + open_delim + filter_arguments + close_delim).setParseAction(
    make_filter
)

assignment = Group(custom_field + assign + built_string).setParseAction(
    make_assignment
)

tags_assignment = Group(Suppress(tags) + tags_assign + built_string).setParseAction(
    make_tags_assignment
)

if_block = Forward()
if_filter_block = Forward()

general_block = comment_line | filter_statement | if_block | if_filter_block | assignment | \
    tags_assignment | drop | stop

if_block << Group(
    if_cond +
    condition +
    open_delim + Group(OneOrMore(general_block)) + close_delim +
    Optional(else_cond + open_delim + Group(OneOrMore(general_block)) + close_delim)
)

if_filter_block << Group(
    if_cond +
    filter_statement +
    open_delim + Group(OneOrMore(general_block)) + close_delim +
    Optional(else_cond + open_delim + Group(OneOrMore(general_block)) + close_delim)
)

if_block.setParseAction(make_if_block)
if_filter_block.setParseAction(make_if_filter_block)




class Assignment(object):
    def __init__(self, left, right):
        self.left = left
        self.right = right

    def str(self):
        return '`{} := {}`'

    def __str__(self):
        return self.str()

    def __repr__(self):
        return self.str()

    def apply(self, ev):
        ev[self.left.name] = self.right.apply(ev)

    @classmethod
    def from_tokens(cls, toks):
        return Assignment(toks[0], toks[1])


class TagsAssignment(object):
    typ = "Assignment"

    def __init__(self, operand, right):
        self.operand = operand
        self.right = right

    def str(self):
        return '`tags mod: {} {}`'

    def __str__(self):
        return self.str()

    def __repr__(self):
        return self.str()

    @classmethod
    def from_tokens(cls, toks):
        return TagsAssignment(toks[0], toks[1])

    def apply(self, ev):
        tag = self.right.apply(ev)

        if self.operand == '+=':
            ev.add_tags(tag)
        elif self.operand == '-=':
            ev.remove_tags(tag)


class IfBlock(object):
    def __init__(self, cond, statements, else_statements=None):
        self.condition = cond
        self.statements = statements
        self.typ = "If block"
        self.else_statements = else_statements if else_statements else list()

    def __str__(self):
        return "IfBlock {} ({}) else ({})".format(
            self.condition,
            ','.join([getattr(statement, 'typ', 'Instruction') for statement in self.statements]),
            ','.join([getattr(statement, 'typ', 'Instruction') for statement in self.else_statements])
        )

    def __repr__(self):
        return self.__str__()

    @classmethod
    def from_tokens(cls, toks):
        return IfBlock(toks[0], toks[1], toks[2]) if len(toks) == 3 else IfBlock(toks[0], toks[1])


class IfFilterBlock(object):
    def __init__(self, filtr, statements, else_statements=None):
        self.filter = filtr
        self.statements = statements
        self.else_statements = else_statements if else_statements else list()
        self.typ = "IfFilter block"

    def __str__(self):
        return "IfFilter {} ({}) ({})".format(
            self.filter.filter_name,
            ','.join([str(arg) for arg in self.filter.filter_arguments]),
            ','.join([statement.typ for statement in self.statements]),
            ','.join([statement.typ for statement in self.else_statements])
        )

    def __repr__(self):
        return self.__str__()

    @classmethod
    def from_tokens(cls, toks):
        return IfFilterBlock(toks[0], toks[1], toks[2]) if len(toks) == 3 else IfFilterBlock(toks[0], toks[1])


class FilterBlock(object):
    def __init__(self, filtername, filter_args=None):
        self.filter_name = filtername.strip('" ')
        self.filter_arguments = filter(lambda arg: not isinstance(arg, tuple), filter_args)
        self.filter_kw_arguments = dict(filter(lambda arg: isinstance(arg, tuple), filter_args))
        self.typ = "Filter"

    def __str__(self):
        return "Filter {} ({})".format(self.filter_name, ','.join([str(arg) for arg in self.filter_arguments]))

    def __repr__(self):
        return self.__str__()

    @classmethod
    def from_tokens(cls, toks):
        return FilterBlock(toks[0], toks[1:])


parser = OneOrMore(general_block)

logger = logging.getLogger(__name__)


def parse_string(s):
    try:
        return parser.parseString(s, parseAll=True)
    except ParseException as ex:
        logger.exception(ex)
        raise_from(ValueError("Syntax Error in filters configuration"), ex)


def parse_config_file(filter_config_filename):
    with open(filter_config_filename, 'rb') as handle:
        s = handle.read()
    return parse_string(to_unicode(s))
