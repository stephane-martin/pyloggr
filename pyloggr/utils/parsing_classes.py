# encoding: utf-8

"""
Pyparsing classes
"""

__author__ = 'stef'

import ast
import re
from future.builtins import str as text
from .fix_unicode import to_unicode


class Constant(object):
    def __init__(self, name):
        self.name = to_unicode(name)

    def __str__(self):
        return "C({})".format(self.name)

    def __repr__(self):
        return self.__str__()

    # noinspection PyUnusedLocal
    def apply(self, ev):
        return self.name

    def gen_ast(self):
        return ast.Str(self.name)


# noinspection PyUnusedLocal
def quoted_string_to_constant(s, loc, toks):
    return Constant(ast.literal_eval(toks[0]))


class Field(object):
    def __init__(self, name):
        self.name = to_unicode(name)

    def __str__(self):
        return "F({})".format(self.name)

    def __repr__(self):
        return self.__str__()

    def apply(self, ev):
        raise NotImplementedError

    def gen_ast(self):
        raise NotImplemented


class CustomField(Field):
    def __init__(self, name):
        Field.__init__(self, name)
        self.name = to_unicode(self.name.name if isinstance(self.name, Constant) else self.name)

    def __str__(self):
        return "EField({})".format(self.name)

    def apply(self, ev):
        return ev[self.name]

    def gen_ast(self):
        return ast.Subscript(value=ast.Name(id='ev', ctx=ast.Load()), slice=ast.Index(value=ast.Str(s=self.name)))


class PlainField(Field):
    def __str__(self):
        return "PField({})".format(self.name)

    def apply(self, ev):
        return ev.__getattribute__(self.name)

    def gen_ast(self):
        return ast.Call(
            func=ast.Name(id='getattr', ctx=ast.Load()),
            args=[ast.Name(id='ev', ctx=ast.Load()), ast.Str(s=self.name)],
            keywords=[],
            starargs=None,
            kwargs=None
        )


class Condition(object):
    def __init__(self):
        self.fun = None

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

    def gen_ast(self):
        raise NotImplemented

    def gen_lambda(self):
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
        return self.fun(ev)


class NotCondition(Condition):
    def __init__(self, left):
        super(NotCondition, self).__init__()
        self.operand = "not"
        self.left = left

    def __str__(self):
        return "NOT({})".format(self.left)

    def __repr__(self):
        return self.__str__()

    def apply(self, ev):
        return not self.left.apply(ev)

    def gen_ast(self):
        return ast.UnaryOp(op=ast.Not(), operand=self.left.gen_ast())


class AndCondition(Condition):
    def __init__(self, left, right):
        super(AndCondition, self).__init__()
        self.operand = "and"
        self.left = left
        self.right = right

    def __str__(self):
        return "AND({}, {})".format(self.left, self.right)

    def __repr__(self):
        return self.__str__()

    def apply(self, ev):
        return self.left.apply(ev) and self.right.apply(ev)

    def gen_ast(self):
        return ast.BoolOp(left=self.left.gen_ast(), op=ast.And(), right=self.right.gen_ast())


class OrCondition(Condition):
    def __init__(self, left, right):
        super(OrCondition, self).__init__()
        self.operand = "or"
        self.left = left
        self.right = right

    def __str__(self):
        return "OR({}, {})".format(self.left, self.right)

    def __repr__(self):
        return self.__str__()

    def apply(self, ev):
        return self.left.apply(ev) or self.right.apply(ev)

    def gen_ast(self):
        return ast.BoolOp(left=self.left.gen_ast(), op=ast.Or(), right=self.right.gen_ast())


class Predicate(Condition):
    def __init__(self):
        super(Predicate, self).__init__()

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

    def gen_ast(self):
        raise NotImplementedError

    def apply(self, ev):
        raise NotImplementedError


class Equals(Predicate):
    def __init__(self, left, right):
        super(Equals, self).__init__()
        self.left = left
        self.right = right

    def __str__(self):
        return "EQ({} == {})".format(self.left, self.right)

    def __repr__(self):
        return self.__str__()

    def apply(self, ev):
        return self.left.apply(ev) == self.right.apply(ev)

    def gen_ast(self):
        return ast.Compare(left=self.left.gen_ast(), ops=[ast.Eq()], comparators=[self.right.gen_ast()])


class RegexpP(Predicate):
    def __init__(self, left, right):
        super(RegexpP, self).__init__()
        self.left = left
        self.right = right

    def __str__(self):
        return "REGEXP({} ~ {})".format(self.left, self.right)

    def __repr__(self):
        return self.__str__()

    def apply(self, ev):
        return re.search(self.right.apply(ev), self.left.apply(ev))

    def gen_ast(self):
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
    def __init__(self, left, right):
        super(RegexpIP, self).__init__()
        self.left = left
        self.right = right

    def __str__(self):
        return "REGEXP_I({} ~* {})".format(self.left, self.right)

    def __repr__(self):
        return self.__str__()

    def apply(self, ev):
        return re.search(self.right.apply(ev), self.left.apply(ev), flags=re.IGNORECASE)

    def gen_ast(self):
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
    def __init__(self, left, right):
        super(Different, self).__init__()
        self.left = left
        self.right = right

    def __str__(self):
        return "NEQ({} != {})".format(self.left, self.right)

    def __repr__(self):
        return self.__str__()

    def apply(self, ev):
        return self.left.apply(ev) != self.right.apply(ev)

    def gen_ast(self):
        return ast.Compare(left=self.left.gen_ast(), ops=[ast.NotEq()],
                           comparators=[self.right.gen_ast()])


class In(Predicate):
    def __init__(self, left, right):
        super(In, self).__init__()
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
        elif self.right == "custom_fields":
            return left in ev
        else:
            raise ValueError

    def gen_ast(self):
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
    def __init__(self, left, right):
        super(Notin, self).__init__()
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
        elif self.right == "custom_fields":
            return left not in ev
        else:
            raise ValueError

    def gen_ast(self):
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