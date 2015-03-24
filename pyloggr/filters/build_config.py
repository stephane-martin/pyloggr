# encoding: utf-8

"""
Pyparsing stuff to parse `filters.conf`
"""

__author__ = 'stef'

import re

from pyparsing import Word, Keyword, Literal, Suppress, quotedString, pythonStyleComment, alphas, alphanums, Forward
from pyparsing import Optional, Group, OneOrMore, delimitedList, ParseException, operatorPrecedence, opAssoc
from ast import literal_eval

from future.utils import raise_from
from future.builtins import str as text
from ..utils.fix_unicode import to_unicode

# todo: permettre les affectations simples comme filtre


class Constant(object):
    def __init__(self, name):
        self.name = name

    def __str__(self):
        return "C({})".format(self.name)

    def __repr__(self):
        return self.__str__()

    # noinspection PyUnusedLocal
    def apply(self, ev):
        return self.name


# noinspection PyUnusedLocal
def quoted_string_to_constant(s, loc, toks):
    return Constant(literal_eval(toks[0]))


class Field(object):
    def __init__(self, name):
        self.name = name

    def __str__(self):
        return "F({})".format(self.name)

    def __repr__(self):
        return self.__str__()

    def apply(self, ev):
        raise NotImplementedError


class ExtendedField(Field):
    def __str__(self):
        return "EField({})".format(self.name)

    def apply(self, ev):
        name = self.name.name if isinstance(self.name, Constant) else self.name
        return ev.fields_as_dict.get(name, None)


class PlainField(Field):
    def __str__(self):
        return "PField({})".format(self.name)

    def apply(self, ev):
        return ev.__getattribute__(self.name)


class Condition(object):
    @classmethod
    def factory(cls, operande, left, right):
        if operande == "and":
            return AndCondition(left, right)
        elif operande == "or":
            return OrCondition(left, right)
        elif operande == "not":
            return NotCondition(left)

    def apply(self, ev):
        raise NotImplementedError


class NotCondition(Condition):
    def __init__(self, left):
        self.operand = "not"
        self.left = left

    def __str__(self):
        return "NOT({})".format(self.left)

    def __repr__(self):
        return self.__str__()

    def apply(self, ev):
        return not self.left.apply(ev)


class AndCondition(Condition):
    def __init__(self, left, right):
        self.operand = "and"
        self.left = left
        self.right = right

    def __str__(self):
        return "AND({}, {})".format(self.left, self.right)

    def __repr__(self):
        return self.__str__()

    def apply(self, ev):
        return self.left.apply(ev) and self.right.apply(ev)


class OrCondition(Condition):
    def __init__(self, left, right):
        self.operand = "or"
        self.left = left
        self.right = right

    def __str__(self):
        return "OR({}, {})".format(self.left, self.right)

    def __repr__(self):
        return self.__str__()

    def apply(self, ev):
        return self.left.apply(ev) or self.right.apply(ev)


class Predicate(Condition):
    @classmethod
    def factory(cls, operation, left, right):
        if operation == '==':
            return Equals(left, right)
        elif operation == '!=':
            return Different(left, right)
        elif operation == 'in':
            return In(left, right)
        elif operation == 'notin':
            return Notin(left, right)
        elif operation == '~':
            return RegexpP(left, right)
        elif operation == '~*':
            return RegexpIP(left, right)

    def apply(self, ev):
        raise NotImplementedError


class Equals(Predicate):
    def __init__(self, left, right):
        self.left = left
        self.right = right

    def __str__(self):
        return "EQ({} == {})".format(self.left, self.right)

    def __repr__(self):
        return self.__str__()

    def apply(self, ev):
        return self.left.apply(ev) == self.right.apply(ev)


class RegexpP(Predicate):
    def __init__(self, left, right):
        self.left = left
        self.right = right

    def __str__(self):
        return "REGEXP({} ~ {})".format(self.left, self.right)

    def __repr__(self):
        return self.__str__()

    def apply(self, ev):
        return re.search(self.right.apply(ev), self.left.apply(ev))


class RegexpIP(Predicate):
    def __init__(self, left, right):
        self.left = left
        self.right = right

    def __str__(self):
        return "REGEXP_I({} ~* {})".format(self.left, self.right)

    def __repr__(self):
        return self.__str__()

    def apply(self, ev):
        return re.search(self.right.apply(ev), self.left.apply(ev), flags=re.IGNORECASE)


class Different(Predicate):
    def __init__(self, left, right):
        self.left = left
        self.right = right

    def __str__(self):
        return "NEQ({} != {})".format(self.left, self.right)

    def __repr__(self):
        return self.__str__()

    def apply(self, ev):
        return self.left.apply(ev) != self.right.apply(ev)


class In(Predicate):
    def __init__(self, left, right):
        self.left = left
        self.right = right

    def __str__(self):
        return "IN({} in {})".format(self.left, self.right)

    def __repr__(self):
        return self.__str__()

    def apply(self, ev):
        left = self.left if isinstance(self.left, text) else self.left.apply(ev)
        if self.right == "tags":
            return left in ev.tags
        elif self.right == "fields":
            return left in ev.fields_as_dict
        else:
            raise ValueError


class Notin(Predicate):
    def __init__(self, left, right):
        self.left = left
        self.right = right

    def __str__(self):
        return "NOTIN({} notin {})".format(self.left, self.right)

    def __repr__(self):
        return self.__str__()

    def apply(self, ev):
        left = self.left if isinstance(self.left, text) else self.left.apply(ev)
        if self.right == "tags":
            return left not in ev.tags
        elif self.right == "fields":
            return left not in ev.fields_as_dict
        else:
            raise ValueError


def make_condition(toks):
    toks = toks[0]
    if len(toks) == 3:
        return Condition.factory(toks[1], toks[0], toks[2])
    elif len(toks) == 2:
        return Condition.factory(toks[0], toks[1], None)


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

        fields          = Keyword("fields")
        tags            = Keyword("tags")

        if_cond         = Suppress(Keyword("if"))
        else_cond       = Suppress(Keyword("else"))

        geoip           = Keyword("geoip")
        grok            = Keyword("grok")
        addtag          = Keyword("addtag")
        removetag       = Keyword("removetag")
        drop            = Keyword("drop")
        useragent       = Keyword("useragent")

        and_operand     = Keyword('and')
        or_operand      = Keyword('or')
        not_operand     = Keyword('not')

        equals          = Literal('==')
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

        extended_field_name = my_qs | label
        extended_field = (Literal('[') + extended_field_name + Literal(']')).setParseAction(
            lambda s, loc, toks: ExtendedField(toks[1])
        )

        filter_name = (geoip | grok | addtag | removetag | drop | useragent)

        comment_line = pythonStyleComment

        equals_predicate = (
            (plain_field | extended_field | my_qs)
            + equals
            + (plain_field | extended_field | my_qs)
        )

        different_predicate = (
            (plain_field | extended_field | my_qs)
            + different
            + (plain_field | extended_field | my_qs)
        )

        in_predicate = (
            (my_qs | label)
            + in_op
            + (tags | fields)
        )

        notin_predicate = (
            (my_qs | label)
            + notin_op
            + (tags | fields)
        )

        regexp_predicate = (
            (plain_field | extended_field)
            + regexp_op
            + my_qs
        )

        regexpi_predicate = (
            (plain_field | extended_field)
            + regexp_i_op
            + my_qs
        )

        predicate = (equals_predicate | different_predicate | in_predicate | notin_predicate | regexp_predicate | regexpi_predicate).setParseAction(
            lambda s, loc, toks: Predicate.factory(toks[1], toks[0], toks[2])
        )

        condition = operatorPrecedence(
            predicate,
            [
                (not_operand, 1, opAssoc.RIGHT, make_condition),
                (and_operand, 2, opAssoc.LEFT, make_condition),
                (or_operand, 2, opAssoc.LEFT, make_condition),
            ]
        )
        condition = condition.setResultsName('condition')

        filter_argument = my_qs | plain_field | extended_field
        filter_arguments = Optional(delimitedList(filter_argument)).setResultsName('arguments')
        filter_statement = (filter_name + Optional(open_par + filter_arguments + close_par)).setParseAction(make_filter)

        if_block = Forward()
        if_filter_block = Forward()
        general_block = Suppress(comment_line) | filter_statement | if_block | if_filter_block
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
            raise_from(ValueError("Syntax Error in filters configuration"), ex)

    def parse_config_file(self, filter_config_filename):
        with open(filter_config_filename, 'rb') as handle:
            s = handle.read()
        s = to_unicode(s)
        res = self.parse_string(s)
        return res


def make_if_block(toks):
    return IfBlock.from_tokens(toks[0])

def make_if_filter_block(toks):
    return IfFilterBlock.from_tokens(toks[0])


class IfBlock(object):
    def __init__(self, condition, statements, else_statements=None):
        self.condition = condition
        self.statements = statements
        self.typ = "If block"
        self.else_statements = else_statements if else_statements else list()

    def __str__(self):
        return "IfBlock {} ({}) else ({})".format(
            self.condition,
            ','.join([statement.typ for statement in self.statements]),
            ','.join([statement.typ for statement in self.else_statements])
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
        self.filter_arguments = filter_arguments
        self.typ = "Filter"

    def __str__(self):
        return "Filter {} ({})".format(self.filter_name, ','.join([str(arg) for arg in self.filter_arguments]))

    def __repr__(self):
        return self.__str__()

    @classmethod
    def from_tokens(cls, toks):
        return FilterBlock(toks[0], toks[1:])