# encoding: utf-8

"""
Pyparsing stuff to parse `filters_config`
"""

__author__ = 'stef'


import logging
from pyparsing import Word, Keyword, Literal, Suppress, quotedString, pythonStyleComment, alphas, alphanums, Forward
from pyparsing import Optional, Group, OneOrMore, delimitedList, ParseException, operatorPrecedence, opAssoc
from future.utils import raise_from
from ..utils.fix_unicode import to_unicode
from ..utils.parsing_classes import Condition, PlainField, quoted_string_to_constant, CustomField, Predicate


# noinspection PyUnusedLocal
def make_filter(s, loc, toks):
    return FilterBlock.from_tokens(toks)


# noinspection PyUnusedLocal
def make_plain_field(s, loc, toks):
    return PlainField(toks[0])


class ConfigParser(object):

    def __init__(self):
        field_names = ["severity", "facility", "app_name", "source", "programname", "syslogtag", "message", "uuid",
                       "timereported", "timegenerated", "trusted_comm", "trusted_exe", "trusted_cmdline"]
        for field_name in field_names:
            self.__setattr__(field_name, Keyword(field_name).setParseAction(
                make_plain_field
            ))

        custom_fields   = Keyword("custom_fields")
        tags            = Keyword("tags")

        if_cond         = Suppress(Keyword("if"))
        else_cond       = Suppress(Keyword("else"))

        geoip           = Keyword("geoip")
        grok            = Keyword("grok")
        useragent       = Keyword("useragent")

        drop            = Keyword("drop")
        stop            = Keyword("stop")

        and_operand     = Keyword('and')
        or_operand      = Keyword('or')
        not_operand     = Keyword('not')

        equals          = Literal('==')
        assign          = Suppress(Literal(':='))
        kw_arg_assign   = Suppress(Literal('='))
        tags_assign     = Literal('+=') | Literal('-=')
        different       = Literal('!=')
        in_op           = Keyword('in')
        notin_op        = Keyword('notin')
        regexp_op       = Literal('~')
        regexp_i_op     = Literal('~*')

        open_par        = Suppress(Literal('('))
        open_acc        = Suppress(Literal('{'))
        close_par       = Suppress(Literal(')'))
        close_acc       = Suppress(Literal('}'))

        my_qs = quotedString.setParseAction(quoted_string_to_constant)
        label = Word(initChars=alphas, bodyChars=alphanums + "_")

        # noinspection PyUnresolvedReferences
        plain_field = self.severity | self.facility | self.app_name | self.source | self.programname | self.syslogtag \
                      | self.message | self.uuid | self.timereported | self.timegenerated | self.trusted_comm \
                      | self.trusted_exe | self.trusted_cmdline

        custom_field_name = my_qs | label
        custom_field = (Literal('[') + custom_field_name + Literal(']')).setParseAction(
            lambda s, loc, toks: CustomField(toks[1])
        )

        filter_name = (geoip | grok | useragent)

        comment_line = pythonStyleComment

        equals_predicate = (
            (plain_field | custom_field | my_qs)
            + equals
            + (plain_field | custom_field | my_qs)
        )

        different_predicate = (
            (plain_field | custom_field | my_qs)
            + different
            + (plain_field | custom_field | my_qs)
        )

        in_predicate = (
            (my_qs | label)
            + in_op
            + (tags | custom_fields)
        )

        notin_predicate = (
            (my_qs | label)
            + notin_op
            + (tags | custom_fields)
        )

        regexp_predicate = (
            (plain_field | custom_field)
            + regexp_op
            + my_qs
        )

        regexpi_predicate = (
            (plain_field | custom_field)
            + regexp_i_op
            + my_qs
        )

        predicate = (equals_predicate | different_predicate | in_predicate | notin_predicate | regexp_predicate | regexpi_predicate).setParseAction(
            lambda s, loc, toks: Predicate.factory(toks[1], toks[0], toks[2])
        )

        condition = operatorPrecedence(
            predicate,
            [
                (not_operand, 1, opAssoc.RIGHT),
                (and_operand, 2, opAssoc.LEFT),
                (or_operand, 2, opAssoc.LEFT),
            ]
        )
        condition = condition.setParseAction(compile)
        condition = condition.setResultsName('condition')

        built_string = Group(delimitedList(expr=(plain_field | custom_field | my_qs), delim='+')).setParseAction(
            make_built_string
        )

        filter_kw_argument = Group(label + kw_arg_assign + built_string).setParseAction(make_kw_argument)
        filter_argument = built_string | filter_kw_argument
        filter_arguments = Optional(delimitedList(filter_argument))
        filter_statement = (filter_name + open_par + filter_arguments + close_par).setParseAction(
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

        general_block = Suppress(comment_line) | filter_statement | if_block | if_filter_block | assignment | \
            tags_assignment | drop | stop

        if_block << Group(
            if_cond +
            condition +
            open_acc + Group(OneOrMore(general_block)) + close_acc +
            Optional(else_cond + open_acc + Group(OneOrMore(general_block)) + close_acc)
        )

        if_filter_block << Group(
            if_cond +
            filter_statement +
            open_acc + Group(OneOrMore(general_block)) + close_acc +
            Optional(else_cond + open_acc + Group(OneOrMore(general_block)) + close_acc)
        )

        if_block.setParseAction(make_if_block)
        if_filter_block.setParseAction(make_if_filter_block)

        self._parser = OneOrMore(general_block)

    def parse_string(self, s):
        try:
            return self._parser.parseString(s, parseAll=True)
        except ParseException as ex:
            logging.exception(ex)
            raise_from(ValueError("Syntax Error in filters configuration"), ex)

    def parse_config_file(self, filter_config_filename):
        with open(filter_config_filename, 'rb') as handle:
            s = handle.read()
        s = to_unicode(s)
        res = self.parse_string(s)

        return res


def make_built_string(toks):
    return BuiltString.from_tokens(toks[0])


def make_if_block(toks):
    return IfBlock.from_tokens(toks[0])


def make_if_filter_block(toks):
    return IfFilterBlock.from_tokens(toks[0])


def make_assignment(toks):
    return Assignment.from_tokens(toks[0])


def make_tags_assignment(toks):
    return TagsAssignment.from_tokens(toks[0])


class BuiltString(object):
    def __init__(self, part_strings):
        self.part_strings = part_strings

    def str(self):
        return "String({})".format('+'.join(str(part) for part in self.part_strings))

    def __str__(self):
        return self.str()

    def __repr__(self):
        return self.str()

    def apply(self, ev):
        strings = map(lambda s: s.apply(ev), self.part_strings)
        strings = filter(lambda s: s is not None, strings)
        return ''.join(strings)

    @classmethod
    def from_tokens(cls, toks):
        return BuiltString(toks)


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
    def __init__(self, condition, statements, else_statements=None):
        self.condition = condition
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
    def __init__(self, filter_name, filter_arguments=None):
        self.filter_name = filter_name.strip('" ')
        self.filter_arguments = filter(lambda arg: not isinstance(arg, tuple), filter_arguments)
        self.filter_kw_arguments = dict(filter(lambda arg: isinstance(arg, tuple), filter_arguments))
        self.typ = "Filter"

    def __str__(self):
        return "Filter {} ({})".format(self.filter_name, ','.join([str(arg) for arg in self.filter_arguments]))

    def __repr__(self):
        return self.__str__()

    @classmethod
    def from_tokens(cls, toks):
        return FilterBlock(toks[0], toks[1:])


def make_kw_argument(toks):
    return toks[0][0], toks[0][1]

def compile(toks):
    toks[0].gen_lambda()
    return toks