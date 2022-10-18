#
# Copyright (C) 2014-2022 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import collections

try:
    from collections.abc import MutableMapping
except ImportError:
    from collections import MutableMapping

import inspect

from muninn.exceptions import *
from muninn.function import Prototype
from muninn.language import parse_and_analyze, Literal, Name, Identifier
from muninn.schema import *
from muninn.visitor import Visitor


AGGREGATE_FUNCTIONS = collections.OrderedDict([
    (Long, ['min', 'max', 'sum', 'avg']),
    (Integer, ['min', 'max', 'sum', 'avg']),
    (Real, ['min', 'max', 'sum', 'avg']),
    # (Boolean, []),
    (Text, ['min', 'max']),
    (Timestamp, ['min', 'max']),
    # (UUID, []),
    # (Geometry, []),
    (None, ['min', 'max', 'sum', 'avg']),  # special case: validity_duration
])

GROUP_BY_FUNCTIONS = collections.OrderedDict([
    (Long, [None, ]),
    (Integer, [None, ]),
    # (Real, []),
    (Boolean, [None, ]),
    (Text, [None, 'length']),
    (Timestamp, ['year', 'month', 'yearmonth', 'date', 'day', 'hour', 'minute', 'second', 'time']),
    # (UUID, []),
    # (Geometry, []),
])


class TypeMap(MutableMapping):
    def __init__(self):
        self._types = {}

    def __getitem__(self, key):
        for type in inspect.getmro(key):
            try:
                return self._types[type]
            except KeyError:
                pass

        raise KeyError(key.__name__)

    def __setitem__(self, key, value):
        self._types[key] = value

    def __delitem__(self, key):
        del self._types[key]

    def __iter__(self):
        return iter(self._types)

    def __len__(self):
        return len(self._types)


def as_is(sql):
    return lambda: sql


def unary_operator_rewriter(operator):
    return lambda arg0: "%s (%s)" % (operator, arg0)


def binary_operator_rewriter(operator):
    return lambda arg0, arg1: "(%s) %s (%s)" % (arg0, operator, arg1)


def unary_function_rewriter(name):
    return lambda arg0: "%s(%s)" % (name, arg0)


def membership_operator_rewriter(operator):
    return lambda arg0, arg1: "(%s) %s %s" % (arg0, operator, arg1)


def binary_function_rewriter(name):
    return lambda arg0, arg1: "%s(%s, %s)" % (name, arg0, arg1)


def default_rewriter_table():
    #
    # Table of all supported operators and functions that can be rewritten in standard SQL and without backend specific
    # knowledge. NB. This is a subset of the set of all supported operators and functions. Database specific backends
    # should add custom implementations of the operators and functions not implemented here.
    #
    rewriter_table = {}

    #
    # Logical operators
    #
    rewriter_table[Prototype("not", (Boolean,), Boolean)] = unary_operator_rewriter("NOT")
    rewriter_table[Prototype("and", (Boolean, Boolean), Boolean)] = binary_operator_rewriter("AND")
    rewriter_table[Prototype("or", (Boolean, Boolean), Boolean)] = binary_operator_rewriter("OR")

    #
    # Membership operators
    #
    in_rewriter = membership_operator_rewriter("in")
    rewriter_table[Prototype("in", (Integer, Sequence), Boolean)] = in_rewriter
    rewriter_table[Prototype("in", (Long, Sequence), Boolean)] = in_rewriter
    rewriter_table[Prototype("in", (Real, Sequence), Boolean)] = in_rewriter
    rewriter_table[Prototype("in", (Text, Sequence), Boolean)] = in_rewriter
    not_in_rewriter = membership_operator_rewriter("not in")
    rewriter_table[Prototype("not in", (Integer, Sequence), Boolean)] = not_in_rewriter
    rewriter_table[Prototype("not in", (Long, Sequence), Boolean)] = not_in_rewriter
    rewriter_table[Prototype("not in", (Real, Sequence), Boolean)] = not_in_rewriter
    rewriter_table[Prototype("not in", (Text, Sequence), Boolean)] = not_in_rewriter

    #
    # Comparison operators
    #
    eq_rewriter = binary_operator_rewriter("=")
    rewriter_table[Prototype("==", (Long, Long), Boolean)] = eq_rewriter
    rewriter_table[Prototype("==", (Long, Integer), Boolean)] = eq_rewriter
    rewriter_table[Prototype("==", (Integer, Long), Boolean)] = eq_rewriter
    rewriter_table[Prototype("==", (Integer, Integer), Boolean)] = eq_rewriter
    rewriter_table[Prototype("==", (Real, Real), Boolean)] = eq_rewriter
    rewriter_table[Prototype("==", (Real, Long), Boolean)] = eq_rewriter
    rewriter_table[Prototype("==", (Long, Real), Boolean)] = eq_rewriter
    rewriter_table[Prototype("==", (Real, Integer), Boolean)] = eq_rewriter
    rewriter_table[Prototype("==", (Integer, Real), Boolean)] = eq_rewriter
    rewriter_table[Prototype("==", (Boolean, Boolean), Boolean)] = eq_rewriter
    rewriter_table[Prototype("==", (Text, Text), Boolean)] = eq_rewriter
    rewriter_table[Prototype("==", (Timestamp, Timestamp), Boolean)] = eq_rewriter
    rewriter_table[Prototype("==", (UUID, UUID), Boolean)] = eq_rewriter

    ne_rewriter = binary_operator_rewriter("!=")
    rewriter_table[Prototype("!=", (Long, Long), Boolean)] = ne_rewriter
    rewriter_table[Prototype("!=", (Long, Integer), Boolean)] = ne_rewriter
    rewriter_table[Prototype("!=", (Integer, Long), Boolean)] = ne_rewriter
    rewriter_table[Prototype("!=", (Integer, Integer), Boolean)] = ne_rewriter
    rewriter_table[Prototype("!=", (Real, Real), Boolean)] = ne_rewriter
    rewriter_table[Prototype("!=", (Real, Long), Boolean)] = ne_rewriter
    rewriter_table[Prototype("!=", (Long, Real), Boolean)] = ne_rewriter
    rewriter_table[Prototype("!=", (Real, Integer), Boolean)] = ne_rewriter
    rewriter_table[Prototype("!=", (Integer, Real), Boolean)] = ne_rewriter
    rewriter_table[Prototype("!=", (Boolean, Boolean), Boolean)] = ne_rewriter
    rewriter_table[Prototype("!=", (Text, Text), Boolean)] = ne_rewriter
    rewriter_table[Prototype("!=", (Timestamp, Timestamp), Boolean)] = ne_rewriter
    rewriter_table[Prototype("!=", (UUID, UUID), Boolean)] = ne_rewriter

    lt_rewriter = binary_operator_rewriter("<")
    rewriter_table[Prototype("<", (Long, Long), Boolean)] = lt_rewriter
    rewriter_table[Prototype("<", (Long, Integer), Boolean)] = lt_rewriter
    rewriter_table[Prototype("<", (Integer, Long), Boolean)] = lt_rewriter
    rewriter_table[Prototype("<", (Integer, Integer), Boolean)] = lt_rewriter
    rewriter_table[Prototype("<", (Real, Real), Boolean)] = lt_rewriter
    rewriter_table[Prototype("<", (Real, Long), Boolean)] = lt_rewriter
    rewriter_table[Prototype("<", (Long, Real), Boolean)] = lt_rewriter
    rewriter_table[Prototype("<", (Real, Integer), Boolean)] = lt_rewriter
    rewriter_table[Prototype("<", (Integer, Real), Boolean)] = lt_rewriter
    rewriter_table[Prototype("<", (Text, Text), Boolean)] = lt_rewriter
    rewriter_table[Prototype("<", (Timestamp, Timestamp), Boolean)] = lt_rewriter

    gt_rewriter = binary_operator_rewriter(">")
    rewriter_table[Prototype(">", (Long, Long), Boolean)] = gt_rewriter
    rewriter_table[Prototype(">", (Long, Integer), Boolean)] = gt_rewriter
    rewriter_table[Prototype(">", (Integer, Long), Boolean)] = gt_rewriter
    rewriter_table[Prototype(">", (Integer, Integer), Boolean)] = gt_rewriter
    rewriter_table[Prototype(">", (Real, Real), Boolean)] = gt_rewriter
    rewriter_table[Prototype(">", (Real, Long), Boolean)] = gt_rewriter
    rewriter_table[Prototype(">", (Long, Real), Boolean)] = gt_rewriter
    rewriter_table[Prototype(">", (Real, Integer), Boolean)] = gt_rewriter
    rewriter_table[Prototype(">", (Integer, Real), Boolean)] = gt_rewriter
    rewriter_table[Prototype(">", (Text, Text), Boolean)] = gt_rewriter
    rewriter_table[Prototype(">", (Timestamp, Timestamp), Boolean)] = gt_rewriter

    le_rewriter = binary_operator_rewriter("<=")
    rewriter_table[Prototype("<=", (Long, Long), Boolean)] = le_rewriter
    rewriter_table[Prototype("<=", (Long, Integer), Boolean)] = le_rewriter
    rewriter_table[Prototype("<=", (Integer, Long), Boolean)] = le_rewriter
    rewriter_table[Prototype("<=", (Integer, Integer), Boolean)] = le_rewriter
    rewriter_table[Prototype("<=", (Real, Real), Boolean)] = le_rewriter
    rewriter_table[Prototype("<=", (Real, Long), Boolean)] = le_rewriter
    rewriter_table[Prototype("<=", (Long, Real), Boolean)] = le_rewriter
    rewriter_table[Prototype("<=", (Real, Integer), Boolean)] = le_rewriter
    rewriter_table[Prototype("<=", (Integer, Real), Boolean)] = le_rewriter
    rewriter_table[Prototype("<=", (Text, Text), Boolean)] = le_rewriter
    rewriter_table[Prototype("<=", (Timestamp, Timestamp), Boolean)] = le_rewriter

    ge_rewriter = binary_operator_rewriter(">=")
    rewriter_table[Prototype(">=", (Long, Long), Boolean)] = ge_rewriter
    rewriter_table[Prototype(">=", (Long, Integer), Boolean)] = ge_rewriter
    rewriter_table[Prototype(">=", (Integer, Long), Boolean)] = ge_rewriter
    rewriter_table[Prototype(">=", (Integer, Integer), Boolean)] = ge_rewriter
    rewriter_table[Prototype(">=", (Real, Real), Boolean)] = ge_rewriter
    rewriter_table[Prototype(">=", (Real, Long), Boolean)] = ge_rewriter
    rewriter_table[Prototype(">=", (Long, Real), Boolean)] = ge_rewriter
    rewriter_table[Prototype(">=", (Real, Integer), Boolean)] = ge_rewriter
    rewriter_table[Prototype(">=", (Integer, Real), Boolean)] = ge_rewriter
    rewriter_table[Prototype(">=", (Text, Text), Boolean)] = ge_rewriter
    rewriter_table[Prototype(">=", (Timestamp, Timestamp), Boolean)] = ge_rewriter

    rewriter_table[Prototype("~=", (Text, Text), Boolean)] = binary_operator_rewriter("LIKE")

    #
    # Arithmetic operators.
    #
    plus_rewriter = unary_operator_rewriter("+")
    rewriter_table[Prototype("+", (Long,), Long)] = plus_rewriter
    rewriter_table[Prototype("+", (Integer,), Integer)] = plus_rewriter
    rewriter_table[Prototype("+", (Real,), Real)] = plus_rewriter

    minus_rewriter = unary_operator_rewriter("-")
    rewriter_table[Prototype("-", (Long,), Long)] = minus_rewriter
    rewriter_table[Prototype("-", (Integer,), Integer)] = minus_rewriter
    rewriter_table[Prototype("-", (Real,), Real)] = minus_rewriter

    add_rewriter = binary_operator_rewriter("+")
    rewriter_table[Prototype("+", (Long, Long), Long)] = add_rewriter
    rewriter_table[Prototype("+", (Long, Integer), Long)] = add_rewriter
    rewriter_table[Prototype("+", (Integer, Long), Long)] = add_rewriter
    rewriter_table[Prototype("+", (Integer, Integer), Integer)] = add_rewriter
    rewriter_table[Prototype("+", (Real, Real), Real)] = add_rewriter
    rewriter_table[Prototype("+", (Real, Long), Real)] = add_rewriter
    rewriter_table[Prototype("+", (Long, Real), Real)] = add_rewriter
    rewriter_table[Prototype("+", (Real, Integer), Real)] = add_rewriter
    rewriter_table[Prototype("+", (Integer, Real), Real)] = add_rewriter

    subtract_rewriter = binary_operator_rewriter("-")
    rewriter_table[Prototype("-", (Long, Long), Long)] = subtract_rewriter
    rewriter_table[Prototype("-", (Long, Integer), Long)] = subtract_rewriter
    rewriter_table[Prototype("-", (Integer, Long), Long)] = subtract_rewriter
    rewriter_table[Prototype("-", (Integer, Integer), Integer)] = subtract_rewriter
    rewriter_table[Prototype("-", (Real, Real), Real)] = subtract_rewriter
    rewriter_table[Prototype("-", (Real, Long), Real)] = subtract_rewriter
    rewriter_table[Prototype("-", (Long, Real), Real)] = subtract_rewriter
    rewriter_table[Prototype("-", (Real, Integer), Real)] = subtract_rewriter
    rewriter_table[Prototype("-", (Integer, Real), Real)] = subtract_rewriter

    multiply_rewriter = binary_operator_rewriter("*")
    rewriter_table[Prototype("*", (Long, Long), Long)] = multiply_rewriter
    rewriter_table[Prototype("*", (Long, Integer), Long)] = multiply_rewriter
    rewriter_table[Prototype("*", (Integer, Long), Long)] = multiply_rewriter
    rewriter_table[Prototype("*", (Integer, Integer), Integer)] = multiply_rewriter
    rewriter_table[Prototype("*", (Real, Real), Real)] = multiply_rewriter
    rewriter_table[Prototype("*", (Real, Long), Real)] = multiply_rewriter
    rewriter_table[Prototype("*", (Long, Real), Real)] = multiply_rewriter
    rewriter_table[Prototype("*", (Real, Integer), Real)] = multiply_rewriter
    rewriter_table[Prototype("*", (Integer, Real), Real)] = multiply_rewriter

    divide_rewriter = binary_operator_rewriter("/")
    rewriter_table[Prototype("/", (Long, Long), Long)] = divide_rewriter
    rewriter_table[Prototype("/", (Long, Integer), Long)] = divide_rewriter
    rewriter_table[Prototype("/", (Integer, Long), Long)] = divide_rewriter
    rewriter_table[Prototype("/", (Integer, Integer), Integer)] = divide_rewriter
    rewriter_table[Prototype("/", (Real, Real), Real)] = divide_rewriter
    rewriter_table[Prototype("/", (Real, Long), Real)] = divide_rewriter
    rewriter_table[Prototype("/", (Long, Real), Real)] = divide_rewriter
    rewriter_table[Prototype("/", (Real, Integer), Real)] = divide_rewriter
    rewriter_table[Prototype("/", (Integer, Real), Real)] = divide_rewriter

    #
    # Functions.
    #
    rewriter_table[Prototype("covers", (Timestamp, Timestamp, Timestamp, Timestamp), Boolean)] = \
        lambda left0, right0, left1, right1: "(%s) >= (%s) AND (%s) >= (%s) AND (%s) >= (%s) AND (%s) <= (%s)" % \
        (right0, left0, right1, left1, left1, left0, right1, right0)

    rewriter_table[Prototype("intersects", (Timestamp, Timestamp, Timestamp, Timestamp), Boolean)] = \
        lambda left0, right0, left1, right1: "(%s) >= (%s) AND (%s) >= (%s) AND (%s) >= (%s) AND (%s) <= (%s)" % \
        (right0, left0, right1, left1, right0, left1, left0, right1)

    return rewriter_table


class _WhereExpressionVisitor(Visitor):
    def __init__(self, rewriter_table, column_name_func, named_placeholder_func, root_visitor=None):
        super(_WhereExpressionVisitor, self).__init__()
        self._rewriter_table = rewriter_table
        self._column_name = column_name_func
        self._named_placeholder = named_placeholder_func

        self._root_visitor = root_visitor or self

    def do_visit(self, visitable):
        where, parameters, namespaces = self.visit(visitable)
        if isinstance(visitable, Literal) and visitable.type is UUID:
            where = ("(uuid = %s)" % where)
        return where, parameters, namespaces

    def visit(self, visitable):
        self._count, self._parameters, self._namespaces = 0, {}, set()
        return super(_WhereExpressionVisitor, self).visit(visitable), self._parameters, self._namespaces

    def visit_Literal(self, visitable):
        parameter_name = str(self._root_visitor._count)
        self._root_visitor._parameters[parameter_name] = visitable.value
        self._root_visitor._count += 1
        return self._named_placeholder(parameter_name, arg=visitable.value)

    def visit_Name(self, visitable):
        if isinstance(visitable.value, Identifier):
            item = visitable.value
            if item.canonical == 'count':
                return 'COUNT(*)'
            else:
                return '%s(%s)' % (item.subscript.upper(), self._column_name(item.namespace, item.identifier))

        namespace_name = visitable.value.split('.')
        if len(namespace_name) == 1:
            namespace = namespace_name[0]
            name = namespace
        else:
            namespace, name = namespace_name
            name = self._column_name(namespace, name)
        self._namespaces.add(namespace)
        return name

    def visit_List(self, visitable):  # TODO parameters?
        return '(' + ','.join(repr(v) for v in visitable.value) + ')'

    def visit_ParameterReference(self, visitable):
        if visitable.type is Sequence:
            result = []
            for value in visitable.value:
                parameter_name = str(self._root_visitor._count)
                self._root_visitor._parameters[parameter_name] = value
                self._root_visitor._count += 1
                result.append(self._named_placeholder(parameter_name, arg=value))
            return '(' + ','.join(result) + ')'
        else:
            parameter_name = str(self._root_visitor._count)
            self._root_visitor._parameters[parameter_name] = visitable.value
            self._root_visitor._count += 1
            return self._named_placeholder(parameter_name, arg=visitable.value)

    def visit_FunctionCall(self, visitable):
        try:
            rewriter_func = self._rewriter_table[visitable.prototype]
        except KeyError:
            raise Error("function not supported by backend: %s" % visitable.prototype)

        # sub-query
        prototype = visitable.prototype
        if (prototype.name in ('is_source_of', 'is_derived_from') and prototype.argument_types and
                prototype.argument_types[0].name() == 'boolean'):

            visitor = _WhereExpressionVisitor(self._rewriter_table, self._column_name, self._named_placeholder,
                                              self._root_visitor)
            where_expr, where_parameters, where_namespaces = visitor.visit(visitable.arguments[0])

            if 'core' in where_namespaces:
                where_namespaces.remove('core')

            return rewriter_func(where_expr, where_namespaces)

        # non-sub-query
        else:
            arguments = []
            for type_, argument in zip(visitable.prototype.argument_types, visitable.arguments):
                where = super(_WhereExpressionVisitor, self).visit(argument)
                if isinstance(argument, Literal) and argument.type is UUID and type_ is Boolean:
                    where = ("(uuid = %s)" % where)
                arguments.append(where)
            sql_expr = rewriter_func(*arguments)

            # SQL-improved NULL checking (e.g., field != "blah" also matches NULL)
            if visitable.name in ('==', '!=', '~='):
                if (isinstance(visitable.arguments[0], Name) and isinstance(visitable.arguments[1], Literal)):
                    name = arguments[0]
                elif (isinstance(visitable.arguments[0], Literal) and isinstance(visitable.arguments[1], Name)):
                    name = arguments[1]
                else:
                    return sql_expr

                if visitable.name == '!=':
                    return '(%s OR %s IS NULL)' % (sql_expr, name)
                else:
                    return '(%s AND %s IS NOT NULL)' % (sql_expr, name)

            return sql_expr

    def default(self, visitable):
        raise Error("unsupported abstract syntax tree node type: %r" % type(visitable).__name__)


class SQLBuilder(object):
    def __init__(self, namespace_schemas, type_map, rewriter_table, table_name_func, _named_placeholder_func,
                 _placeholder_func, rewriter_property_func):
        self._namespace_schemas = namespace_schemas
        self._type_map = type_map
        self._rewriter_table = rewriter_table
        self._table_name = table_name_func
        self._named_placeholder = _named_placeholder_func
        self._placeholder = _placeholder_func
        self._rewriter_property = rewriter_property_func

    def build_create_table_query(self, namespace):
        column_sql = []

        schema = self._namespace_schema(namespace)
        for identifier in schema:
            sql = identifier + " " + self._type(schema[identifier])
            if not schema.is_optional(identifier):
                sql = sql + " " + "NOT NULL"
            column_sql.append(sql)

        return "CREATE TABLE %s (%s)" % (self._table_name(namespace), ", ".join(column_sql))

    def build_count_query(self, where="", parameters={}):
        join_set = set()

        # Parse the where clause.
        where_clause, where_parameters = "", {}
        if where:
            ast = parse_and_analyze(where, self._namespace_schemas, parameters)
            visitor = _WhereExpressionVisitor(self._rewriter_table, self._column_name, self._named_placeholder)
            where_expr, where_parameters, where_namespaces = visitor.do_visit(ast)
            if where_expr:
                join_set.update(where_namespaces)
                where_clause = "WHERE %s" % where_expr

        # Generate the FROM clause.
        from_clause = "FROM %s" % self._table_name("core")

        join_set.discard("core")
        for namespace in join_set:
            from_clause = "%s LEFT JOIN %s USING (uuid)" % (from_clause, self._table_name(namespace))

        # Generate the complete query.
        query = "SELECT COUNT(*) AS count %s" % from_clause
        if where_clause:
            query = "%s %s" % (query, where_clause)

        return query, where_parameters

    def build_summary_query(self, where='', parameters=None, aggregates=None, group_by=None, group_by_tag=False,
                            having=None, order_by=None):
        aggregates = aggregates or []
        group_by = group_by or []
        order_by = order_by or []
        if group_by_tag:
            group_by = group_by + ['tag']

        join_set = set()
        result_fields = []
        for field in group_by + ['count'] + aggregates:
            ident = Identifier(field, self._namespace_schemas)
            result_fields.append(ident.resolve)
            join_set.add(ident.namespace)

        # Parse the WHERE clause.
        where_clause, where_parameters = '', {}
        if where:
            ast = parse_and_analyze(where, self._namespace_schemas, parameters)
            visitor = _WhereExpressionVisitor(self._rewriter_table, self._column_name, self._named_placeholder)
            where_expr, where_parameters, where_namespaces = visitor.do_visit(ast)
            if where_expr:
                join_set.update(where_namespaces)
                where_clause = 'WHERE %s' % where_expr

        # Generate the GROUP BY clause.
        group_by_clause = ''
        group_by_list = [str(i) for i in range(1, len(group_by) + 1)]
        if group_by_list:
            group_by_clause = 'GROUP BY %s' % ', '.join(group_by_list)

        # Generate the HAVING clause
        having_clause = ''
        if having is not None:
            ast = parse_and_analyze(having, self._namespace_schemas, parameters, having=True)
            visitor = _WhereExpressionVisitor(self._rewriter_table, self._column_name, self._named_placeholder,
                                              visitor if where else None)
            having_expr, having_parameters, _ = visitor.do_visit(ast)
            where_parameters.update(having_parameters)
            having_clause = 'HAVING %s' % having_expr

        # Parse the ORDER BY clause.
        order_by_clause = ''
        order_by_list = []
        for item in order_by:
            direction = 'DESC' if item.startswith('-') else 'ASC'
            name = item[1:] if item.startswith('+') or item.startswith('-') else item
            name = Identifier(name, self._namespace_schemas).resolve
            if name not in result_fields:
                raise Error("cannot order result by %r; field is not present in result" % name)
            order_by_list.append('"%s" %s' % (name, direction))
        order_by_list += [str(i) for i in range(1, len(group_by) + 1)]
        if order_by_list:
            order_by_clause = 'ORDER BY %s' % ', '.join(order_by_list)

        # Generate the SELECT clause.
        select_list = []
        # group by fields
        for item in group_by:
            item = Identifier(item, self._namespace_schemas)
            column_name = self._column_name(item.namespace, item.identifier)
            group_by_functions = GROUP_BY_FUNCTIONS.get(item.muninn_type)
            if not group_by_functions:  # item.muninn_type not in (Text, Boolean, Long, Integer):
                if item.muninn_type:
                    raise Error("property %r of type %r cannot be part of the group_by field specification" %
                                (item.property_name, item.muninn_type.name()))
                else:
                    raise Error("property %r cannot be part of the group_by field specification" %
                                (item.property_name, ))
            if item.subscript not in group_by_functions:
                if item.subscript:
                    allowed_message = "; it can be one of %r" % group_by_functions if group_by_functions != [None] \
                        else ""
                    raise Error(("group field specification subscript %r of %r is not allowed" + allowed_message) %
                                (item.subscript, item.canonical))
                else:
                    raise Error("property %r of type %r must specify a subscript (one of %r) to be part of the "
                                "group_by field specification" % (item.property_name, item.muninn_type.name(),
                                                                  group_by_functions))
            if item.subscript:
                column_name = self._rewriter_property(column_name, item.subscript)
            select_list.append('%s AS "%s"' % (column_name, item.resolve))

        # aggregated fields
        select_list.append('COUNT(*) AS count')  # always aggregate row count
        for item in aggregates:
            item = Identifier(item, self._namespace_schemas)
            join_set.add(item.namespace)

            if not AGGREGATE_FUNCTIONS.get(item.muninn_type):
                raise Error("property %r of type %r cannot be part of the summary field specification" %
                            (item.property_name, item.muninn_type.name()))
            elif item.subscript not in AGGREGATE_FUNCTIONS[item.muninn_type]:
                if item.subscript:
                    raise Error("summary field specification subscript %r of %r should be one of %r" %
                                (item.subscript, item.canonical, AGGREGATE_FUNCTIONS[item.muninn_type]))
                else:
                    raise Error("summary field specification %r must specify a subscript (one of %r)" %
                                (item.canonical, AGGREGATE_FUNCTIONS[item.muninn_type]))
            if item.property_name == 'core.validity_duration':
                start_column = self._column_name(item.namespace, 'validity_start')
                stop_column = self._column_name(item.namespace, 'validity_stop')
                column_name = self._rewriter_table[Prototype('-', (Timestamp, Timestamp), Real)](stop_column,
                                                                                                 start_column)
            else:
                column_name = self._column_name(item.namespace, item.identifier)
            select_list.append('%s(%s) AS "%s"' % (item.subscript.upper(), column_name, item.canonical))
        select_clause = 'SELECT %s' % ', '.join(select_list)

        # Generate the FROM clause.
        from_clause = 'FROM %s' % self._table_name('core')

        for namespace in join_set:
            if namespace not in ('core', None):
                from_clause = '%s LEFT JOIN %s USING (uuid)' % (from_clause, self._table_name(namespace))

        # Generate the complete query.
        query = '%s\n%s' % (select_clause, from_clause)
        if where_clause:
            query = '%s\n%s' % (query, where_clause)
        if group_by_clause:
            query = '%s\n%s' % (query, group_by_clause)
        if having_clause:
            query = '%s\n%s' % (query, having_clause)
        if order_by_clause:
            query = '%s\n%s' % (query, order_by_clause)

        return query, where_parameters, result_fields

    def build_search_query(self, where="", order_by=[], limit=None, parameters={}, namespaces=[], property_names=[]):
        if property_names:
            namespaces = []
            namespace_properties = {}
            for item in property_names:
                if '.' not in item:
                    item = "core." + item
                Identifier(item, self._namespace_schemas)  # check if the identifier is valid
                namespace, identifier = item.split('.')
                if namespace not in namespaces:
                    namespaces.append(namespace)
                    namespace_properties[namespace] = ['uuid']  # always add uuid to determine if namespace exists
                if identifier != 'uuid':
                    namespace_properties[namespace].append(identifier)
            join_set = set(namespaces)
            description = [(namespace, namespace_properties[namespace]) for namespace in namespaces]
        else:
            join_set = set(namespaces)
            description = [("core", list(self._namespace_schema("core")))]
            for namespace in join_set:
                description.append((namespace, ["uuid"] + list(self._namespace_schema(namespace))))

        # Parse the where clause.
        where_clause, where_parameters = "", {}
        if where:
            ast = parse_and_analyze(where, self._namespace_schemas, parameters)
            visitor = _WhereExpressionVisitor(self._rewriter_table, self._column_name, self._named_placeholder)
            where_expr, where_parameters, where_namespaces = visitor.do_visit(ast)
            if where_expr:
                join_set.update(where_namespaces)
                where_clause = "WHERE %s" % where_expr

        # Parse the order by clause.
        order_by_clause = ""
        if order_by:
            order_by_list, order_by_namespaces = self._build_order_by_list(order_by)
            if order_by_list:
                join_set.update(order_by_namespaces)
                order_by_clause = "ORDER BY %s" % ", ".join(order_by_list)

        # Parse the limit clause.
        limit_clause = ""
        if limit is not None:
            try:
                limit = int(limit)
            except (TypeError, ValueError):
                raise Error("limit %r must be a positive integer" % limit)

            if limit < 0:
                raise Error("limit %r must be a positive integer" % limit)

            limit_clause = "LIMIT %d" % limit

        # Generate the SELECT clause.
        select_list = []
        for namespace, identifiers in description:
            select_list.extend([self._column_name(namespace, identifier) for identifier in identifiers])
        select_clause = "SELECT %s" % ", ".join(select_list)

        # Generate the FROM clause.
        from_clause = "FROM %s" % self._table_name("core")

        join_set.discard("core")
        for namespace in join_set:
            from_clause = "%s LEFT JOIN %s USING (uuid)" % (from_clause, self._table_name(namespace))

        # Generate the complete query.
        query = "%s %s" % (select_clause, from_clause)
        if where_clause:
            query = "%s %s" % (query, where_clause)
        if order_by_clause:
            query = "%s %s" % (query, order_by_clause)
        if limit_clause:
            query = "%s %s" % (query, limit_clause)

        return query, where_parameters, description

    def _build_order_by_list(self, order_by_items):
        order_by_list, namespaces = [], set()
        for item in order_by_items:
            direction = "DESC" if item.startswith("-") else "ASC"
            name = item[1:] if item.startswith("+") or item.startswith("-") else item

            segments = name.split('.')
            if len(segments) == 1:
                namespace, name = 'core', segments[0]
            elif len(segments) == 2:
                namespace, name = name.split(".")
            else:
                raise Error("invalid property name: %r" % name)

            if name not in self._namespace_schema(namespace):
                raise Error("no property: %r defined within namespace: %r" % (name, namespace))

            namespaces.add(namespace)
            order_by_list.append(self._column_name(namespace, name) + " " + direction)

        return order_by_list, namespaces

    def _column_name(self, namespace, identifier):
        return self._table_name(namespace) + "." + identifier

    def _namespace_schema(self, namespace):
        try:
            return self._namespace_schemas[namespace]
        except KeyError:
            raise Error("undefined namespace: \"%s\"" % namespace)

    def _type(self, type):
        try:
            return self._type_map[type]
        except KeyError:
            raise Error("type not supported by backend: %r" % type.name())
