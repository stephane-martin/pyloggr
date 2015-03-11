# encoding: utf-8
__author__ = 'stef'


from os.path import join

from pyparsing import Word, Keyword, Literal, Suppress, quotedString, pythonStyleComment, alphas, alphanums, Forward
from pyparsing import Optional, Group, OneOrMore, delimitedList, ParseException
from ast import literal_eval

from future.utils import raise_from
from future.builtins import str as text
from ..event import Event
from ..utils.fix_unicode import to_unicode

# todo: implement 'not', 'notin', regexp predicate, startswith, endswith
# todo: parenthesis should be optional, with precedence


class Constant(object):
    def __init__(self, name):
        self.name = name

    def __str__(self):
        return "C({})".format(self.name)

    def __repr__(self):
        return self.__str__()

    def apply(self, ev):
        return self.name


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
        assert(isinstance(ev, Event))
        name = self.name.apply(ev) if isinstance(self.name, Constant) else self.name
        return ev.fields_as_dict[name]


class PlainField(Field):
    def __str__(self):
        return "PField({})".format(self.name)

    def apply(self, ev):
        assert(isinstance(ev, Event))
        return ev.__getattribute__(self.name)


class Condition(object):
    @classmethod
    def factory(cls, operande, left, right):
        if operande == "and":
            return AndCondition(left, right)
        elif operande == "or":
            return OrCondition(left, right)

    def apply(self, ev):
        raise NotImplementedError


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
        assert(isinstance(ev, Event))
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
        assert(isinstance(ev, Event))
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
        assert(isinstance(ev, Event))
        return self.left.apply(ev) == self.right.apply(ev)


class Different(Predicate):
    def __init__(self, left, right):
        self.left = left
        self.right = right

    def __str__(self):
        return "NEQ({} != {})".format(self.left, self.right)

    def __repr__(self):
        return self.__str__()

    def apply(self, ev):
        assert(isinstance(ev, Event))
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
        assert(isinstance(ev, Event))
        left = self.left if isinstance(self.left, text) else self.left.apply(ev)
        if self.right == "tags":
            return left in ev.tags
        elif self.right == "fields":
            return left in ev.fields_as_dict
        else:
            raise ValueError


class Filter(object):
    def __init__(self, name, arguments=None):
        self.name = name.strip('" ')
        self.arguments = arguments

    def __str__(self):
        return "Filter {} ({})".format(self.name, ','.join([str(arg) for arg in self.arguments]))

    def __repr__(self):
        return self.__str__()

    @classmethod
    def from_tokens(cls, toks):
        return Filter(toks[0], toks[1:])


def make_condition(s, loc, toks):
    if len(toks[0]) == 1:
        return toks[0][0]
    else:
        return Condition.factory(toks[0][1], toks[0][0], toks[0][2])

def make_filter(s, loc, toks):
    return Filter.from_tokens(toks)

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

        geoip           = Keyword("geoip")
        grok            = Keyword("grok")
        addtag          = Keyword("addtag")

        and_operand     = Keyword('and')
        or_operand      = Keyword('or')

        equals          = Literal('==')
        different       = Literal('!=')
        in_op           = Keyword('in')

        open_par        = Suppress(Literal('('))
        close_par       = Suppress(Literal(')'))

        operand = and_operand | or_operand

        my_qs = (quotedString).setParseAction(quoted_string_to_constant)
        label = Word(initChars=alphas, bodyChars=alphanums+"_")

        plain_field = self.severity | self.facility | self.app_name | self.source | self.programname | self.syslogtag \
                      | self.message | self.uuid | self.timereported | self.timegenerated | self.trusted_comm \
                      | self.trusted_exe | self.trusted_cmdline

        extended_field_name = my_qs | label
        extended_field = (Literal('[') + extended_field_name + Literal(']')).setParseAction(
            lambda s, loc, toks: ExtendedField(toks[1])
        )

        filter_name = (geoip | grok | addtag)

        comment_line = pythonStyleComment

        equals_predicate = (
            open_par
            + (plain_field | extended_field | my_qs)
            + equals
            + (plain_field | extended_field | my_qs)
            + close_par
        )

        different_predicate = (
            open_par
            + (plain_field | extended_field | my_qs)
            + different
            + (plain_field | extended_field | my_qs)
            + close_par
        )

        in_predicate = (
            open_par
            + (my_qs | label)
            + in_op
            + (tags | fields)
            + close_par
        )

        predicate = (equals_predicate | different_predicate | in_predicate).setParseAction(
            lambda s, loc, toks: Predicate.factory(toks[1], toks[0], toks[2])
        )

        condition = Forward()
        condition << Group(
            predicate |
            (open_par + condition.setParseAction(make_condition) + operand +
             condition.setParseAction(make_condition) + close_par)
        )

        filter_argument = my_qs | plain_field | extended_field
        filter_arguments = Optional(delimitedList(filter_argument)).setResultsName('arguments')
        filter_statement = (filter_name + Optional(open_par + filter_arguments + close_par)).setParseAction(make_filter)
        filters = Group(OneOrMore(filter_statement)).setResultsName('actions')

        block = (Group(if_cond + condition + Suppress(Literal('{')) + filters + Suppress(Literal('}')))) | Suppress(comment_line)
        self._parser = OneOrMore(block)

    def parse_string(self, s):
        try:
            return self._parser.parseString(s, parseAll=True)
        except ParseException as ex:
            raise_from(ValueError("Syntax Error in filters configuration"), ex)

    def parse_config_file(self, directory):
        fname = join(directory, "filters")
        with open(fname, 'rb') as handle:
            s = handle.read()
        s = to_unicode(s)
        return self.parse_string(s)

