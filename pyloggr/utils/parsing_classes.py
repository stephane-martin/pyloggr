# encoding: utf-8

"""
General Pyparsing classes used to parse pyloggr configuration files
"""

__author__ = 'stef'

import ast
import re
import sys
from itertools import product
from copy import copy

from pyparsing import Keyword, Suppress, Literal, quotedString, Word, delimitedList, Group, pythonStyleComment
from pyparsing import alphanums, alphas, operatorPrecedence, opAssoc, nums
from future.builtins import str as text
from .fix_unicode import to_unicode


# noinspection PyUnusedLocal
def _make_plain_field(s, loc, toks):
    return PlainField(toks[0])


def _compile_condition(toks):
    toks = toks[0]
    if isinstance(toks, Condition):
        cond = toks
    else:
        cond = Condition.factory(toks[1], toks[0], toks[2])
    cond.gen_lambda()
    return cond


thismodule = sys.modules[__name__]
field_names = [
    "severity", "facility", "app_name", "source", "programname", "syslogtag", "message", "uuid",
    "timereported", "timegenerated", "trusted_comm", "trusted_exe", "trusted_cmdline"
]

for field_name in field_names:
    setattr(thismodule, field_name, Keyword(field_name).setParseAction(
        _make_plain_field
    ))


class BuiltString(object):
    """
    Complex built string
    """
    def __init__(self, part_strings):
        self.part_strings = part_strings

    def _str(self):
        return "BString({})".format('+'.join(str(part) for part in self.part_strings))

    def __str__(self):
        return self._str()

    def __repr__(self):
        return self._str()

    def apply(self, ev):
        """
        Return the string value for the given event

        :param ev: event
        """
        strings = map(lambda s: s.apply(ev), self.part_strings)
        # eliminate empty strings
        strings = filter(lambda s: bool(s), strings)
        # some of the calculated part_strings may actually be sets, because of structured data multiple values
        copy_of_strings = map(lambda s: {s} if not isinstance(s, set) else s, strings)
        combinators = product(*copy_of_strings)
        return [''.join(combi) for combi in combinators]

    @classmethod
    def from_tokens(cls, toks):
        return BuiltString(toks)


def _make_built_string(toks):
    return BuiltString(toks[0])


class Constant(object):
    """
    Represents a constant value
    """
    def __init__(self, name):
        self.name = to_unicode(name)

    def _str(self):
        return "C({})".format(self.name)

    def __str__(self):
        return self._str()

    def __repr__(self):
        return self._str()

    # noinspection PyUnusedLocal
    def apply(self, ev):
        """
        Return the value of constant

        :param ev: event
        """
        return self.name

    def gen_ast(self):
        """
        Generate the AST for the constant value
        """
        return ast.Str(self.name)


# noinspection PyUnusedLocal
def _quoted_string_to_constant(s, loc, toks):
    return Constant(ast.literal_eval(toks[0]))


class Field(object):
    """
    Abstract field
    """
    def __init__(self, name):
        self.name = to_unicode(name)

    def _str(self):
        return "F({})".format(self.name)

    def __str__(self):
        return self._str()

    def __repr__(self):
        return self._str()

    def apply(self, ev):
        """
        Return the value of the field for the given event

        :param ev: event
        """
        raise NotImplementedError

    def gen_ast(self):
        """
        Generate the abstract syntax tree corresponding to the field access
        """
        raise NotImplementedError


class CustomField(Field):
    """
    Represents access to a custom event field
    """
    def __init__(self, name):
        Field.__init__(self, name)
        self.name = to_unicode(self.name.name if isinstance(self.name, Constant) else self.name)

    def _str(self):
        return "EField({})".format(self.name)

    def __str__(self):
        return self._str()

    def __repr__(self):
        return self._str()

    def apply(self, ev):
        """
        Return the value of the custom field for the given event

        :param ev: event
        """
        return ev[self.name]

    def gen_ast(self):
        """
        Generate the Abstract syntax tree corresponding to the custom field access
        """
        return ast.Subscript(value=ast.Name(id='ev', ctx=ast.Load()), slice=ast.Index(value=ast.Str(s=self.name)))


class PlainField(Field):
    """
    Represents access to a plain event field
    """
    def _str(self):
        return "PField({})".format(self.name)

    def __str__(self):
        return self._str()

    def __repr__(self):
        return self._str()

    def apply(self, ev):
        """
        Return the field value for the given event

        :param ev: event
        """
        return ev.__getattribute__(self.name)

    def gen_ast(self):
        """
        Generate the Abstract syntax tree corresponding to the field access
        """
        return ast.Call(
            func=ast.Name(id='getattr', ctx=ast.Load()),
            args=[ast.Name(id='ev', ctx=ast.Load()), ast.Str(s=self.name)],
            keywords=[],
            starargs=None,
            kwargs=None
        )


class Condition(object):
    """
    Abstract condition
    """
    def __init__(self):
        self.fun = None

    @classmethod
    def factory(cls, operande, left, right):
        """
        Factory method to make the concrete conditions

        :param operande: 'and' or 'or' or 'not'
        :param left: left part of condition
        :param right: right part of condition (can be None)
        :return: concrete condition
        :rtype: Condition
        """
        if operande == "and":
            return AndCondition(left, right)
        elif operande == "or":
            return OrCondition(left, right)
        elif operande == "not":
            return NotCondition(left)

    def _str(self):
        raise NotImplementedError

    def __str__(self):
        return self._str()

    def __repr__(self):
        return self._str()

    def apply(self, ev):
        """
        Return the condition value for the given event

        :param ev: event
        """
        raise NotImplementedError

    def gen_ast(self):
        """
        Generate the abstract syntax tree corresponding to the condition
        """
        raise NotImplementedError

    def gen_lambda(self):
        """
        Compile the condition and store in self.fun
        """
        tree = ast.Expression(
            ast.Lambda(
                args=ast.arguments(
                    args=[ast.Name(id='ev', ctx=ast.Param())],
                    vararg=None, kwarg=None, defaults=[]
                ),
                body=self.gen_ast()
            )
        )
        compiled = compile(ast.fix_missing_locations(tree), '<string>', 'eval')
        self.fun = eval(compiled, {'re': re})

    def eval(self, ev):
        """
        Evaluate the condition using the compiled self.fun

        :param ev: event
        :return: bool
        """
        return self.fun(ev)


class AlwaysTrueCondition(Condition):
    def __init__(self):
        super(AlwaysTrueCondition, self).__init__()
        self.fun = lambda ev: True

    def apply(self, ev):
        return True


always_true_singleton = AlwaysTrueCondition()


class NotCondition(Condition):
    """
    NOT A condition
    """
    def __init__(self, left):
        super(NotCondition, self).__init__()
        self.operand = "not"
        self.left = left

    def _str(self):
        return "NOT({})".format(self.left)

    def apply(self, ev):
        """
        Test if the condition apply to the event

        :param ev: event
        :return: bool
        """
        return not self.left.apply(ev)

    def gen_ast(self):
        """
        Generate the Abstract syntax tree corresponding to the condition
        """
        return ast.UnaryOp(op=ast.Not(), operand=self.left.gen_ast())


class AndCondition(Condition):
    """
    A AND B condition
    """
    def __init__(self, left, right):
        super(AndCondition, self).__init__()
        self.operand = "and"
        self.left = left
        self.right = right

    def _str(self):
        return "AND({}, {})".format(self.left, self.right)

    def apply(self, ev):
        """
        Test if the condition apply to the event

        :param ev: event
        :return: bool
        """
        return self.left.apply(ev) and self.right.apply(ev)

    def gen_ast(self):
        """
        Generate the Abstract syntax tree corresponding to the condition
        """
        return ast.BoolOp(op=ast.And(), values=[self.left.gen_ast(), self.right.gen_ast()])


class OrCondition(Condition):
    """
    A OR B condition
    """
    def __init__(self, left, right):
        super(OrCondition, self).__init__()
        self.operand = "or"
        self.left = left
        self.right = right

    def _str(self):
        return "OR({}, {})".format(self.left, self.right)

    def apply(self, ev):
        """
        Test if the condition apply to the event

        :param ev: event
        :return: bool
        """
        return self.left.apply(ev) or self.right.apply(ev)

    def gen_ast(self):
        """
        Generate the Abstract syntax tree corresponding to the condition
        """
        return ast.BoolOp(op=ast.Or(), values=[self.left.gen_ast(), self.right.gen_ast()])


class Predicate(Condition):
    """
    Abstract predicate
    """
    def __init__(self):
        super(Predicate, self).__init__()

    @classmethod
    def factory(cls, operation, left, right):
        """
        Factory method the make the concrete objects

        :param operation: type of predicate
        :param left: left part of predicate
        :param right: right part of predicate
        :return: concrete predicate
        :rtype: Predicate
        """
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

    def _str(self):
        raise NotImplementedError

    def gen_ast(self):
        """
        Generate the Abstract syntax tree corresponding to the predicate
        """
        raise NotImplementedError

    def apply(self, ev):
        """
        Apply the predicate to the given event

        :param ev: event
        :return: bool
        """
        raise NotImplementedError


class Equals(Predicate):
    """
    EQUALS predicate
    """
    def __init__(self, left, right):
        super(Equals, self).__init__()
        self.left = left
        self.right = right

    def _str(self):
        return "EQ({} == {})".format(self.left, self.right)

    def apply(self, ev):
        """
        Apply the predicate to the given event

        :param ev: event
        :return: bool
        """
        return self.left.apply(ev) == self.right.apply(ev)

    def gen_ast(self):
        """
        Generate the Abstract syntax tree corresponding to the predicate
        """
        return ast.Compare(left=self.left.gen_ast(), ops=[ast.Eq()], comparators=[self.right.gen_ast()])


class RegexpP(Predicate):
    """
    CASE SENSITIVE predicate
    """
    def __init__(self, left, right):
        super(RegexpP, self).__init__()
        self.left = left
        self.right = right

    def _str(self):
        return "REGEXP({} ~ {})".format(self.left, self.right)

    def apply(self, ev):
        """
        Apply the predicate to the given event

        :param ev: event
        :return: bool
        """
        return re.search(self.right.apply(ev), self.left.apply(ev))

    def gen_ast(self):
        """
        Generate the Abstract syntax tree corresponding to the predicate
        """
        return ast.Call(
            func=ast.Attribute(
                value=ast.Name(id='re', ctx=ast.Load()),
                attr='search',
                ctx=ast.Load()
            ),
            args=[
                self.right.gen_ast(),
                self.left.gen_ast()
            ],
            keywords=[],
            starargs=None,
            kwargs=None
        )


class RegexpIP(Predicate):
    """
    CASE INSENSITIVE REGEXP predicate
    """
    def __init__(self, left, right):
        super(RegexpIP, self).__init__()
        self.left = left
        self.right = right

    def _str(self):
        return "REGEXP_I({} ~* {})".format(self.left, self.right)

    def apply(self, ev):
        """
        Apply the predicate to the given event

        :param ev: event
        :return: bool
        """
        return re.search(self.right.apply(ev), self.left.apply(ev), flags=re.IGNORECASE)

    def gen_ast(self):
        """
        Generate the Abstract syntax tree corresponding to the predicate
        """
        return ast.Call(
            func=ast.Attribute(
                value=ast.Name(id='re', ctx=ast.Load()),
                attr='search',
                ctx=ast.Load()
            ),
            args=[
                self.right.gen_ast(),
                self.left.gen_ast()
            ],
            keywords=[
                ast.keyword(
                    arg='flags',
                    value=ast.Attribute(
                        value=ast.Name(id='re', ctx=ast.Load()),
                        attr='IGNORECASE',
                        ctx=ast.Load()
                    )
                )
            ],
            starargs=None,
            kwargs=None
        )


class Different(Predicate):
    """
    DIFFERENT predicate
    """
    def __init__(self, left, right):
        super(Different, self).__init__()
        self.left = left
        self.right = right

    def _str(self):
        return "NEQ({} != {})".format(self.left, self.right)

    def apply(self, ev):
        """
        Apply the predicate to the given event

        :param ev: event
        :return: bool
        """
        return self.left.apply(ev) != self.right.apply(ev)

    def gen_ast(self):
        """
        Generate the Abstract syntax tree corresponding to the predicate
        """
        return ast.Compare(left=self.left.gen_ast(), ops=[ast.NotEq()],
                           comparators=[self.right.gen_ast()])


class In(Predicate):
    """
    IN predicate
    """
    def __init__(self, left, right):
        super(In, self).__init__()
        self.left = left
        self.right = right

    def _str(self):
        return "IN({} in {})".format(self.left, self.right)

    def apply(self, ev):
        """
        Apply the predicate to the given event

        :param ev: event
        :return: bool
        """
        left = self.left if isinstance(self.left, text) else self.left.apply(ev)
        if self.right == "tags":
            return left in ev.tags
        elif self.right == "custom_fields":
            return left in ev
        else:
            raise ValueError

    def gen_ast(self):
        """
        Generate the Abstract syntax tree corresponding to the predicate
        """
        left = ast.Str(self.left) if isinstance(self.left, text) else self.left.gen_ast()
        if self.right == "tags":
            return ast.Compare(left=left, ops=[ast.In()], comparators=[
                ast.Attribute(
                    value=ast.Name(id='ev', ctx=ast.Load()),
                    attr='tags',
                    ctx=ast.Load()
                )
            ])
        elif self.right == "custom_fields":
            return ast.Compare(left=left, ops=[ast.In()], comparators=[ast.Name(id='ev', ctx=ast.Load())])
        else:
            raise ValueError


class Notin(Predicate):
    """
    NOT IN predicate
    """
    def __init__(self, left, right):
        super(Notin, self).__init__()
        self.left = left
        self.right = right

    def _str(self):
        return "NOTIN({} notin {})".format(self.left, self.right)

    def apply(self, ev):
        """
        Apply the predicate to the given event

        :param ev: event
        :return: bool
        """
        left = self.left if isinstance(self.left, text) else self.left.apply(ev)
        if self.right == "tags":
            return left not in ev.tags
        elif self.right == "custom_fields":
            return left not in ev
        else:
            raise ValueError

    def gen_ast(self):
        """
        Generate the Abstract syntax tree corresponding to the predicate
        """
        left = ast.Str(self.left) if isinstance(self.left, text) else self.left.gen_ast()
        if self.right == "tags":
            return ast.Compare(left=left, ops=[ast.NotIn()], comparators=[
                ast.Attribute(
                    value=ast.Name(id='ev', ctx=ast.Load()),
                    attr='tags',
                    ctx=ast.Load()
                )
            ])
        elif self.right == "custom_fields":
            return ast.Compare(left=left, ops=[ast.NotIn], comparators=[ast.Name(id='ev', ctx=ast.Load())])
        else:
            raise ValueError


custom_fields   = Keyword("custom_fields")
tags            = Keyword("tags")

if_cond         = Suppress(Keyword("if"))
else_cond       = Suppress(Keyword("else"))

geoip           = Keyword("geoip")
grok            = Keyword("grok")
useragent       = Keyword("useragent")
router          = Keyword("router")

drop            = Keyword("drop")
stop            = Keyword("stop")

and_operand     = Keyword('and')
or_operand      = Keyword('or')
not_operand     = Keyword('not')

equals          = Literal('==')
assign          = Suppress(Literal(':='))
kw_arg_assign   = Suppress(Literal('=') | Literal(':'))
tags_assign     = Literal('+=') | Literal('-=')
different       = Literal('!=')
in_op           = Keyword('in')
notin_op        = Keyword('notin')
regexp_op       = Literal('~')
regexp_i_op     = Literal('~*')

open_par        = Literal('(')
open_acc        = Literal('{')
close_par       = Literal(')')
close_acc       = Literal('}')

open_delim      = Suppress(open_par | open_acc)
close_delim     = Suppress(close_par | close_acc)


integer         = Word(nums).setParseAction(lambda toks: int(toks[0]))

my_qs = quotedString.setParseAction(_quoted_string_to_constant)
label = Word(initChars=alphas, bodyChars=alphanums + "_")

# noinspection PyUnresolvedReferences
plain_field = severity | facility | app_name | source | programname | syslogtag | message | uuid | \
              timereported | timegenerated | trusted_comm | trusted_exe | trusted_cmdline

custom_field_name = my_qs | label
custom_field = (Literal('[') + custom_field_name + Literal(']')).setParseAction(
    lambda s, loc, toks: CustomField(toks[1])
)

filter_name = (geoip | grok | useragent | router)

comment_line = Suppress(pythonStyleComment)

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
condition = condition.setParseAction(_compile_condition)
condition = condition.setResultsName('condition')

built_string = Group(delimitedList(expr=(plain_field | custom_field | my_qs), delim='+')).setParseAction(
    _make_built_string
)