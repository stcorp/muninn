#
# Copyright (C) 2014-2022 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

from muninn._compat import string_types as basestring

import copy
import datetime
import re
import uuid

import muninn.geometry as geometry

from muninn.enum import Enum
from muninn.exceptions import *
from muninn.function import Prototype, FunctionTable
from muninn.schema import *
from muninn.visitor import Visitor


#
# Table of all supported operators and functions
#
type_map = {
    UUID: Boolean,
}
function_table = FunctionTable(type_map=type_map)
#
# Logical operators
#
function_table.add(Prototype("not", (Boolean,), Boolean))
function_table.add(Prototype("and", (Boolean, Boolean), Boolean))
function_table.add(Prototype("or", (Boolean, Boolean), Boolean))
#
# Membership operators
#
function_table.add(Prototype("in", (Integer, Sequence), Boolean))
function_table.add(Prototype("in", (Long, Sequence), Boolean))
function_table.add(Prototype("in", (Real, Sequence), Boolean))
function_table.add(Prototype("in", (Text, Sequence), Boolean))
function_table.add(Prototype("not in", (Integer, Sequence), Boolean))
function_table.add(Prototype("not in", (Long, Sequence), Boolean))
function_table.add(Prototype("not in", (Real, Sequence), Boolean))
function_table.add(Prototype("not in", (Text, Sequence), Boolean))
#
# Comparison operators
#
function_table.add(Prototype("==", (Long, Long), Boolean))
function_table.add(Prototype("==", (Long, Integer), Boolean))
function_table.add(Prototype("==", (Integer, Long), Boolean))
function_table.add(Prototype("==", (Integer, Integer), Boolean))
function_table.add(Prototype("==", (Real, Real), Boolean))
function_table.add(Prototype("==", (Real, Long), Boolean))
function_table.add(Prototype("==", (Long, Real), Boolean))
function_table.add(Prototype("==", (Real, Integer), Boolean))
function_table.add(Prototype("==", (Integer, Real), Boolean))
function_table.add(Prototype("==", (Boolean, Boolean), Boolean))
function_table.add(Prototype("==", (Text, Text), Boolean))
function_table.add(Prototype("==", (Timestamp, Timestamp), Boolean))
function_table.add(Prototype("==", (UUID, UUID), Boolean))

function_table.add(Prototype("!=", (Long, Long), Boolean))
function_table.add(Prototype("!=", (Long, Integer), Boolean))
function_table.add(Prototype("!=", (Integer, Long), Boolean))
function_table.add(Prototype("!=", (Integer, Integer), Boolean))
function_table.add(Prototype("!=", (Real, Real), Boolean))
function_table.add(Prototype("!=", (Real, Long), Boolean))
function_table.add(Prototype("!=", (Long, Real), Boolean))
function_table.add(Prototype("!=", (Real, Integer), Boolean))
function_table.add(Prototype("!=", (Integer, Real), Boolean))
function_table.add(Prototype("!=", (Boolean, Boolean), Boolean))
function_table.add(Prototype("!=", (Text, Text), Boolean))
function_table.add(Prototype("!=", (Timestamp, Timestamp), Boolean))
function_table.add(Prototype("!=", (UUID, UUID), Boolean))

function_table.add(Prototype("<", (Long, Long), Boolean))
function_table.add(Prototype("<", (Long, Integer), Boolean))
function_table.add(Prototype("<", (Integer, Long), Boolean))
function_table.add(Prototype("<", (Integer, Integer), Boolean))
function_table.add(Prototype("<", (Real, Real), Boolean))
function_table.add(Prototype("<", (Real, Long), Boolean))
function_table.add(Prototype("<", (Long, Real), Boolean))
function_table.add(Prototype("<", (Real, Integer), Boolean))
function_table.add(Prototype("<", (Integer, Real), Boolean))
function_table.add(Prototype("<", (Text, Text), Boolean))
function_table.add(Prototype("<", (Timestamp, Timestamp), Boolean))

function_table.add(Prototype(">", (Long, Long), Boolean))
function_table.add(Prototype(">", (Long, Integer), Boolean))
function_table.add(Prototype(">", (Integer, Long), Boolean))
function_table.add(Prototype(">", (Integer, Integer), Boolean))
function_table.add(Prototype(">", (Real, Real), Boolean))
function_table.add(Prototype(">", (Real, Long), Boolean))
function_table.add(Prototype(">", (Long, Real), Boolean))
function_table.add(Prototype(">", (Real, Integer), Boolean))
function_table.add(Prototype(">", (Integer, Real), Boolean))
function_table.add(Prototype(">", (Text, Text), Boolean))
function_table.add(Prototype(">", (Timestamp, Timestamp), Boolean))

function_table.add(Prototype("<=", (Long, Long), Boolean))
function_table.add(Prototype("<=", (Long, Integer), Boolean))
function_table.add(Prototype("<=", (Integer, Long), Boolean))
function_table.add(Prototype("<=", (Integer, Integer), Boolean))
function_table.add(Prototype("<=", (Real, Real), Boolean))
function_table.add(Prototype("<=", (Real, Long), Boolean))
function_table.add(Prototype("<=", (Long, Real), Boolean))
function_table.add(Prototype("<=", (Real, Integer), Boolean))
function_table.add(Prototype("<=", (Integer, Real), Boolean))
function_table.add(Prototype("<=", (Text, Text), Boolean))
function_table.add(Prototype("<=", (Timestamp, Timestamp), Boolean))

function_table.add(Prototype(">=", (Long, Long), Boolean))
function_table.add(Prototype(">=", (Long, Integer), Boolean))
function_table.add(Prototype(">=", (Integer, Long), Boolean))
function_table.add(Prototype(">=", (Integer, Integer), Boolean))
function_table.add(Prototype(">=", (Real, Real), Boolean))
function_table.add(Prototype(">=", (Real, Long), Boolean))
function_table.add(Prototype(">=", (Long, Real), Boolean))
function_table.add(Prototype(">=", (Real, Integer), Boolean))
function_table.add(Prototype(">=", (Integer, Real), Boolean))
function_table.add(Prototype(">=", (Text, Text), Boolean))
function_table.add(Prototype(">=", (Timestamp, Timestamp), Boolean))

function_table.add(Prototype("~=", (Text, Text), Boolean))

function_table.add(Prototype("+", (Long,), Long))
function_table.add(Prototype("+", (Integer,), Integer))
function_table.add(Prototype("+", (Real,), Real))

function_table.add(Prototype("-", (Long,), Long))
function_table.add(Prototype("-", (Integer,), Integer))
function_table.add(Prototype("-", (Real,), Real))

function_table.add(Prototype("+", (Long, Long), Long))
function_table.add(Prototype("+", (Long, Integer), Long))
function_table.add(Prototype("+", (Integer, Long), Long))
function_table.add(Prototype("+", (Integer, Integer), Integer))
function_table.add(Prototype("+", (Real, Real), Real))
function_table.add(Prototype("+", (Real, Long), Real))
function_table.add(Prototype("+", (Long, Real), Real))
function_table.add(Prototype("+", (Real, Integer), Real))
function_table.add(Prototype("+", (Integer, Real), Real))

function_table.add(Prototype("-", (Long, Long), Long))
function_table.add(Prototype("-", (Long, Integer), Long))
function_table.add(Prototype("-", (Integer, Long), Long))
function_table.add(Prototype("-", (Integer, Integer), Integer))
function_table.add(Prototype("-", (Real, Real), Real))
function_table.add(Prototype("-", (Real, Long), Real))
function_table.add(Prototype("-", (Long, Real), Real))
function_table.add(Prototype("-", (Real, Integer), Real))
function_table.add(Prototype("-", (Integer, Real), Real))

function_table.add(Prototype("*", (Long, Long), Long))
function_table.add(Prototype("*", (Long, Integer), Long))
function_table.add(Prototype("*", (Integer, Long), Long))
function_table.add(Prototype("*", (Integer, Integer), Integer))
function_table.add(Prototype("*", (Real, Real), Real))
function_table.add(Prototype("*", (Real, Long), Real))
function_table.add(Prototype("*", (Long, Real), Real))
function_table.add(Prototype("*", (Real, Integer), Real))
function_table.add(Prototype("*", (Integer, Real), Real))

function_table.add(Prototype("/", (Long, Long), Long))
function_table.add(Prototype("/", (Long, Integer), Long))
function_table.add(Prototype("/", (Integer, Long), Long))
function_table.add(Prototype("/", (Integer, Integer), Integer))
function_table.add(Prototype("/", (Real, Real), Real))
function_table.add(Prototype("/", (Real, Long), Real))
function_table.add(Prototype("/", (Long, Real), Real))
function_table.add(Prototype("/", (Real, Integer), Real))
function_table.add(Prototype("/", (Integer, Real), Real))

function_table.add(Prototype("-", (Timestamp, Timestamp), Real))

#
# Functions.
#
function_table.add(Prototype("covers", (Geometry, Geometry), Boolean))
function_table.add(Prototype("covers", (Timestamp, Timestamp, Timestamp, Timestamp), Boolean))
function_table.add(Prototype("intersects", (Geometry, Geometry), Boolean))
function_table.add(Prototype("intersects", (Timestamp, Timestamp, Timestamp, Timestamp), Boolean))
function_table.add(Prototype("is_defined", (Long,), Boolean))
function_table.add(Prototype("is_defined", (Integer,), Boolean))
function_table.add(Prototype("is_defined", (Real,), Boolean))
function_table.add(Prototype("is_defined", (Boolean,), Boolean))
function_table.add(Prototype("is_defined", (Text,), Boolean))
function_table.add(Prototype("is_defined", (Namespace,), Boolean))
function_table.add(Prototype("is_defined", (Timestamp,), Boolean))
function_table.add(Prototype("is_defined", (UUID,), Boolean))
function_table.add(Prototype("is_defined", (Geometry,), Boolean))
function_table.add(Prototype("is_source_of", (UUID,), Boolean))
function_table.add(Prototype("is_source_of", (Boolean,), Boolean))
function_table.add(Prototype("is_derived_from", (UUID,), Boolean))
function_table.add(Prototype("is_derived_from", (Boolean,), Boolean))
function_table.add(Prototype("has_tag", (Text,), Boolean))
function_table.add(Prototype("now", (), Timestamp))


class TokenType(Enum):
    _items = ("TEXT", "UUID", "TIMESTAMP", "REAL", "INTEGER", "BOOLEAN", "NAME", "OPERATOR", "END")


class Token(object):
    def __init__(self, type_, value=None):
        self.type_ = type_
        self.value = value

    def __repr__(self):
        return "Token(type_ = TokenType.%s, value = %r)" % (TokenType.to_string(self.type_), self.value)


class TokenStream(object):
    _sub_patterns = \
        (
            r"""\"(?:[^\\"]|\\.)*\"""",                                      # Text literals
            r"""\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}:\d{2}(?:\.\d{0,6})?)?""",   # Timestamp literals
            r"""[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}""",  # UUID literals
            r"""\d+(?:\.\d*(?:[eE][+-]?\d+)?|[eE][+-]?\d+)""",               # Real literals
            r"""0x[0-9a-fA-F]+|0o[0-7]\d+|0b[0-1]+|\d+""",                   # Integer literals
            r"""<=|>=|==|!=|~=|not in|[*<>@()\[\],.+-/]""",                  # Operators and delimiters
            r"""[a-zA-Z]\w*""",                                              # Names (incl. true, false, in)
        )

    _pattern = r"""(?:%s)""" % ("|".join(["(%s)" % sub_pattern for sub_pattern in _sub_patterns]))
    _re_token = re.compile(_pattern)

    _re_datemin = re.compile(r"0000-00-00(?:T00:00:00(?:\.0{0,6})?)?$")
    _re_datemax = re.compile(r"9999-99-99(?:T99:99:99(?:\.9{0,6})?)?$")

    def __init__(self, text):
        self.text = text
        self.at_end = not self.text
        self.token_start_position, self.token_end_position = 0, 0
        self.next()

    def next(self):
        if self.at_end:
            raise Error("char %d: unexpected end of input" % (self.token_start_position + 1))

        self.token = self._next_token()
        return self.token

    def test(self, types, values=None):
        return False if not self._test_token_types(types) else (values is None or self._test_token_values(values))

    def accept(self, types, values=None):
        if not self.test(types, values):
            return False

        self.next()
        return True

    def expect(self, types, values=None):
        if not self.test(types, values):
            if self.token.type_ == TokenType.END:
                raise Error("char %d: unexpected end of input" % (self.token_start_position + 1))
            else:
                if self.token.value is None:
                    token_str = TokenType.to_string(self.token.type_)
                else:
                    token_str = "\"%s\"" % self.token.value

                expected_str = self._types_to_string(types) if values is None else self._values_to_string(values)
                raise Error("char %d: expected %s, got %s" % (self.token_start_position + 1, expected_str, token_str))

        token = self.token
        self.next()
        return token

    def _types_to_string(self, types):
        try:
            strings = map(TokenType.to_string, types)
        except TypeError:
            return TokenType.to_string(types)

        return "%s%s" % ("" if len(strings) == 1 else "one of: ", ", ".join(strings))

    def _values_to_string(self, values):
        if isinstance(values, basestring):
            return "\"%s\"" % values

        try:
            strings = ["\"%s\"" % value for value in values]
        except TypeError:
            return "\"%s\"" % values

        return "%s%s" % ("" if len(strings) == 1 else "one of: ", ", ".join(strings))

    def _test_token_types(self, types):
        try:
            return self.token.type_ in types
        except TypeError:
            return self.token.type_ == types

    def _test_token_values(self, values):
        if isinstance(values, basestring):
            return self.token.value == values

        try:
            return self.token.value in values
        except TypeError:
            return self.token.value == values

    def _next_token(self):
        self.token_start_position = self._skip_white_space(self.token_end_position)

        if self.token_start_position == len(self.text):
            self.at_end = True
            return Token(TokenType.END)

        match_object = self._re_token.match(self.text, self.token_start_position)
        if match_object is None:
            raise Error("char %d: syntax error: \"%s\"" % (self.token_start_position + 1,
                        self.text[self.token_start_position:]))

        self.token_start_position, self.token_end_position = match_object.span()
        text, timestamp, uuid_, real, integer, operator, name = match_object.groups()

        if text is not None:
            return Token(TokenType.TEXT, string_unescape(text[1:-1]))

        if timestamp is not None:
            return Token(TokenType.TIMESTAMP, self._parse_timestamp(timestamp))

        if uuid_ is not None:
            return Token(TokenType.UUID, uuid.UUID(uuid_))

        if real is not None:
            return Token(TokenType.REAL, float(real))

        if integer is not None:
            if integer.startswith('0x'):
                base = 16
            elif integer.startswith('0o'):
                base = 8
            elif integer.startswith('0b'):
                base = 2
            else:
                base = 10
            return Token(TokenType.INTEGER, int(integer, base))

        if operator is not None:
            return Token(TokenType.OPERATOR, operator)

        if name is not None:
            if name in ["true", "false"]:
                return Token(TokenType.BOOLEAN, name == "true")
            elif name == "in":
                return Token(TokenType.OPERATOR, name)
            else:
                return Token(TokenType.NAME, name)

        raise Error("char %d: syntax error: \"%s\"" % (self.token_start_position + 1, match_object.group()))

    def _skip_white_space(self, start):
        while start < len(self.text) and self.text[start].isspace():
            start += 1
        return start

    def _parse_timestamp(self, timestamp):
        if self._re_datemin.match(timestamp) is not None:
            return datetime.datetime.min

        if self._re_datemax.match(timestamp) is not None:
            return datetime.datetime.max

        for format_string in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
            try:
                return datetime.datetime.strptime(timestamp, format_string)
            except ValueError:
                pass

        raise Error("char %d: invalid timestamp: \"%s\"" % (self.token_start_position + 1, timestamp))


class AbstractSyntaxTreeNode(object):
    pass


class Literal(AbstractSyntaxTreeNode):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return "(%s %s)" % (type(self).__name__, self.value)


class Name(AbstractSyntaxTreeNode):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return "(%s %s)" % (type(self).__name__, self.value)


class List(AbstractSyntaxTreeNode):
    def __init__(self, values):
        self.values = values

    def __str__(self):
        return "(%s %s)" % (type(self).__name__, self.value)


class ParameterReference(AbstractSyntaxTreeNode):
    def __init__(self, name):
        self.name = name

    def __str__(self):
        return "(%s %s)" % (type(self).__name__, self.name)


class FunctionCall(AbstractSyntaxTreeNode):
    def __init__(self, name, *args):
        self.name = name
        self.arguments = list(args)

    def __str__(self):
        if not self.arguments:
            return "(%s %s)" % (type(self).__name__, self.name)
        return "(%s %s %s)" % (type(self).__name__, self.name, " ".join(map(str, self.arguments)))


def parse_sequence(stream, parse_item_function, start='(', end=')'):
    stream.expect(TokenType.OPERATOR, start)
    if stream.accept(TokenType.OPERATOR, end):
        return []

    sequence = [parse_item_function(stream)]
    while stream.accept(TokenType.OPERATOR, ","):
        sequence.append(parse_item_function(stream))
    stream.expect(TokenType.OPERATOR, end)
    return sequence


def parse_geometry_sequence(stream, parse_item_function):
    if stream.accept(TokenType.NAME, "EMPTY"):
        return []

    stream.expect(TokenType.OPERATOR, "(")
    sequence = [parse_item_function(stream)]
    while stream.accept(TokenType.OPERATOR, ","):
        sequence.append(parse_item_function(stream))
    stream.expect(TokenType.OPERATOR, ")")
    return sequence


def parse_signed_coordinate(stream):
    if stream.accept(TokenType.OPERATOR, "-"):
        token = stream.expect((TokenType.INTEGER, TokenType.REAL))
        return -float(token.value)

    stream.accept(TokenType.OPERATOR, "+")
    token = stream.expect((TokenType.INTEGER, TokenType.REAL))
    return float(token.value)


def parse_point_raw(stream):
    return geometry.Point(parse_signed_coordinate(stream),
                          parse_signed_coordinate(stream))


def parse_point(stream):
    stream.expect(TokenType.OPERATOR, "(")
    point = parse_point_raw(stream)
    stream.expect(TokenType.OPERATOR, ")")
    return point


def parse_line_string(stream):
    return geometry.LineString(parse_geometry_sequence(stream, parse_point_raw))


def parse_linear_ring(stream):
    points = parse_geometry_sequence(stream, parse_point_raw)
    if len(points) == 0:
        return geometry.LinearRing()

    if len(points) < 4:
        raise Error("char %d: linear ring should be empty or should contain >= 4 points" % stream.token_start_position)

    if points[-1] != points[0]:
        raise Error("char %d: linear ring should be closed" % stream.token_start_position)

    return geometry.LinearRing(points[:-1])


def parse_polygon(stream):
    return geometry.Polygon(parse_geometry_sequence(stream, parse_linear_ring))


def parse_multi_point(stream):
    return geometry.MultiPoint(parse_geometry_sequence(stream, parse_point))


def parse_multi_line_string(stream):
    return geometry.MultiLineString(parse_geometry_sequence(stream, parse_line_string))


def parse_multi_polygon(stream):
    return geometry.MultiPolygon(parse_geometry_sequence(stream, parse_polygon))


def parse_atom(stream):
    # Sub-expression.
    if stream.accept(TokenType.OPERATOR, "("):
        sub_expression = parse_expression(stream)
        stream.expect(TokenType.OPERATOR, ")")
        return sub_expression

    # Parameter reference.
    if stream.accept(TokenType.OPERATOR, "@"):
        name_token = stream.expect(TokenType.NAME)
        return ParameterReference(name_token.value)

    # Geometry literal, function call, or name.
    if stream.test(TokenType.NAME):
        name_token = stream.expect(TokenType.NAME)

        # Geometry literals.
        if name_token.value == "POINT":
            return Literal(parse_point(stream))
        elif name_token.value == "LINESTRING":
            return Literal(parse_line_string(stream))
        elif name_token.value == "POLYGON":
            return Literal(parse_polygon(stream))
        elif name_token.value == "MULTIPOINT":
            return Literal(parse_multi_point(stream))
        elif name_token.value == "MULTILINESTRING":
            return Literal(parse_multi_line_string(stream))
        elif name_token.value == "MULTIPOLYGON":
            return Literal(parse_multi_polygon(stream))

        # Function call.
        if stream.test(TokenType.OPERATOR, "("):
            return FunctionCall(name_token.value, *parse_sequence(stream, parse_expression))

        # Name (possibly qualified).
        parts = [name_token.value]
        while stream.accept(TokenType.OPERATOR, "."):
            name_token = stream.expect(TokenType.NAME)
            parts.append(name_token.value)
        return Name(".".join(parts))

    if stream.test(TokenType.OPERATOR, "["):
        return List(parse_sequence(stream, parse_expression, "[", "]"))

    # Literal.
    token = stream.expect((TokenType.TEXT, TokenType.TIMESTAMP, TokenType.UUID, TokenType.REAL, TokenType.INTEGER,
                           TokenType.BOOLEAN))
    return Literal(token.value)


def parse_term(stream):
    if stream.test(TokenType.OPERATOR, ("+", "-")):
        operator_token = stream.expect(TokenType.OPERATOR, ("+", "-"))
        return FunctionCall(operator_token.value, parse_term(stream))
    return parse_atom(stream)


def parse_arithmetic_expression(stream):
    lhs = parse_term(stream)
    if stream.test(TokenType.OPERATOR, ("+", "-", "*", "/")):
        operator_token = stream.expect(TokenType.OPERATOR, ("+", "-", "*", "/"))
        return FunctionCall(operator_token.value, lhs, parse_arithmetic_expression(stream))
    return lhs


def parse_comparison(stream):
    lhs = parse_arithmetic_expression(stream)
    if stream.test(TokenType.OPERATOR, ("<", ">", "==", ">=", "<=", "!=", "~=", "in", "not in")):
        operator_token = stream.expect(TokenType.OPERATOR, ("<", ">", "==", ">=", "<=", "!=", "~=", "in", "not in"))
        return FunctionCall(operator_token.value, lhs, parse_comparison(stream))
    return lhs


def parse_not_expression(stream):
    if stream.accept(TokenType.NAME, "not"):
        return FunctionCall("not", parse_not_expression(stream))
    return parse_comparison(stream)


def parse_and_expression(stream):
    lhs = parse_not_expression(stream)
    if stream.accept(TokenType.NAME, "and"):
        return FunctionCall("and", lhs, parse_and_expression(stream))
    return lhs


def parse_or_expression(stream):
    lhs = parse_and_expression(stream)
    if stream.accept(TokenType.NAME, "or"):
        return FunctionCall("or", lhs, parse_or_expression(stream))
    return lhs


def parse_expression(stream):
    return parse_or_expression(stream)


def _literal_type(literal):
    if isinstance(literal, list):  # TODO use Sequence.validate..?
        return Sequence

    for type in (Text, Timestamp, UUID, Boolean, Integer, Long, Real, Geometry):
        try:
            type.validate(literal)
        except ValueError:
            pass
        else:
            return type

    raise Error("unable to determine type of literal value: %r" % literal)


class SemanticAnalysis(Visitor):
    def __init__(self, namespace_schemas, parameters, having=False):
        super(SemanticAnalysis, self).__init__()
        self._namespace_schemas = namespace_schemas
        self._parameters = parameters
        self._having = having

    def visit_Literal(self, visitable):
        visitable.type = _literal_type(visitable.value)

    def visit_Name(self, visitable):
        if self._having:
            item = Identifier(visitable.value, self._namespace_schemas)
            visitable.type = item.muninn_type
            visitable.value = item
            return

        split_name = visitable.value.split(".")

        # namespace/implicit core property
        if len(split_name) == 1:
            if split_name[0] in self._namespace_schemas:
                namespace = split_name[0]
                name = None
            else:
                namespace, name = "core", split_name[0]

        # namespace.property
        elif len(split_name) == 2:
            namespace, name = split_name

        else:
            raise Error("invalid property name: \"%s\"" % visitable.value)

        # check that namespace exists
        try:
            schema = self._namespace_schemas[namespace]
        except KeyError:
            raise Error("undefined namespace: \"%s\"" % namespace)

        # namespace
        if name is None:
            visitable.value = split_name[0]
            visitable.type = Namespace

        # namespace.property
        else:
            try:
                type_ = schema[name]
            except KeyError:
                if len(split_name) == 2:
                    raise Error("undefined property: \"%s\"" % visitable.value)
                else:
                    raise Error("undefined name: \"%s\"" % name)

            visitable.value = "%s.%s" % (namespace, name)
            visitable.type = type_

    def visit_List(self, visitable):  # TODO check same literal type
        values = []
        for value in visitable.values:
            if not isinstance(value, Literal):
                raise Error("list contains non-literal")
            values.append(value.value)
        visitable.value = values
        visitable.type = Sequence

    def visit_ParameterReference(self, visitable):
        try:
            value = self._parameters[visitable.name]
        except KeyError:
            raise Error("no value for parameter: \"%s\"" % visitable.name)

        visitable.value = value
        visitable.type = _literal_type(value)

    def visit_FunctionCall(self, visitable):
        # Resolve the type of the function arguments.
        for argument in visitable.arguments:
            self.visit(argument)

        prototype = Prototype(visitable.name, [argument.type for argument in visitable.arguments])

        try:
            prototypes = function_table.resolve(prototype)
        except KeyError:
            prototypes = []

        if not prototypes:
            raise Error("undefined function: \"%s\"" % prototype)

        if len(prototypes) > 1:
            raise InternalError("cannot uniquely resolve function: \"%s\"" % prototype)

        prototype = prototypes[0]
        visitable.prototype = prototype
        visitable.type = prototype.return_type

    def visit_AbstractSyntaxTreeNode(self, visitable):
        if not hasattr(visitable, "type"):
            raise InternalError("encountered abstract syntax tree node without type attribute: %s" %
                                type(visitable).__name__)

    def default(self, visitable):
        raise InternalError("unsupported abstract syntax tree node type: %s" % type(visitable).__name__)


def parse(text):
    stream = TokenStream(text)
    abstract_syntax_tree = parse_expression(stream)
    if not stream.test(TokenType.END):
        raise Error("char %d: extra characters after expression: \"%s\"" % (stream.token_start_position + 1,
                                                                            text[stream.token_start_position:]))
    return abstract_syntax_tree


def analyze(abstract_syntax_tree, namespace_schemas={}, parameters={}, having=False):
    annotated_syntax_tree = copy.deepcopy(abstract_syntax_tree)
    SemanticAnalysis(namespace_schemas, parameters, having=having).visit(annotated_syntax_tree)
    return annotated_syntax_tree


def parse_and_analyze(text, namespace_schemas={}, parameters={}, having=False):
    return analyze(parse(text), namespace_schemas, parameters, having)


def string_unescape(text):
    '''
    Unescape special characters in a string.
    Python2 and 3 compatible, uses the native string type.
    In python2, the same effect can also be achieved with `string.decode("string-escape")`
    '''
    text = str(text)  # make sure we are using the native string type
    regex = re.compile('\\\\(\\\\|[\'"abfnrtv])')
    translator = {
        '\\': '\\',
        "'": "'",
        '"': '"',
        'a': '\a',
        'b': '\b',
        'f': '\f',
        'n': '\n',
        'r': '\r',
        't': '\t',
        'v': '\v',
    }

    def _replace(m):
        c = m.group(1)
        return translator[c]

    result = regex.sub(_replace, text)
    return result


class Identifier(object):
    def __init__(self, canonical_identifier, namespace_schemas):
        self.canonical = canonical_identifier
        self.subscript = None

        if canonical_identifier == 'tag':
            # the rules to get the namespace database table name also apply to 'tag'
            self.namespace = canonical_identifier
            self.identifier = canonical_identifier
            self.muninn_type = Text

        elif canonical_identifier == 'count':
            self.namespace = None
            self.identifier = canonical_identifier
            self.muninn_type = Long

        elif not re.match(r'[\w]+\.[\w.]+', canonical_identifier):
            raise Error("cannot resolve identifier: %r" % canonical_identifier)

        else:
            segments = canonical_identifier.split('.')

            if len(segments) == 1:
                self.namespace = 'core'
                self.identifier = segments[0]

            elif len(segments) == 2:
                if segments[0] in namespace_schemas:
                    self.namespace, self.identifier = segments
                else:
                    self.namespace = 'core'
                    self.identifier, self.subscript = segments

            elif len(segments) == 3:
                self.namespace, self.identifier, self.subscript = segments

            else:
                raise Error("cannot resolve identifier: %r" % canonical_identifier)

            # check if namespace is valid
            if self.namespace not in namespace_schemas:
                raise Error("undefined namespace: \"%s\"" % self.namespace)

            # check if property name is valid
            if self.identifier not in namespace_schemas[self.namespace]:
                if self.property_name != 'core.validity_duration':
                    raise Error("no property: %r defined within namespace: %r" % (self.identifier, self.namespace))

            # note: not checking if subscript is valid; the list of possible subscripts varies depending on context
            if self.property_name == 'core.validity_duration':
                self.muninn_type = None
            else:
                self.muninn_type = namespace_schemas[self.namespace][self.identifier]

    @property
    def property_name(self):
        return '%s.%s' % (self.namespace, self.identifier)

    @property
    def resolve(self):
        if self.canonical in ('count', 'tag'):
            return self.canonical
        elif self.subscript is None:
            return '%s.%s' % (self.namespace, self.identifier)
        else:
            return '%s.%s.%s' % (self.namespace, self.identifier, self.subscript)
