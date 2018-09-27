#
# Copyright (C) 2014-2018 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import collections
import inspect
import re

from muninn.exceptions import *
from muninn.function import Prototype
from muninn.language import parse_and_analyze
from muninn.schema import *
from muninn.visitor import Visitor


AGGREGATE_FUNCTIONS = {
    Long: ['min', 'max', 'sum', 'avg'],
    Integer: ['min', 'max', 'sum', 'avg'],
    Real: ['min', 'max', 'sum', 'avg'],
    # Boolean: [],
    Text: ['min', 'max'],
    Timestamp: ['min', 'max'],
    # UUID: [],
    # Geometry: [],
    None: ['min', 'max', 'sum', 'avg'],  # special case: validity_duration
}
GROUP_BY_FUNCTIONS = {
    Long: [None, ],
    Integer: [None, ],
    # Real: [],
    Boolean: [None, ],
    #Text: [None, ],  
    Text: [None],  # Text: [None, 'length'],
    Timestamp: ['year', 'month', 'yearmonth', 'date'],
    # UUID: [],
    # Geometry: [],
}

class TypeMap(collections.MutableMapping):
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
    # Logical operators.
    #
    rewriter_table[Prototype("not", (Boolean,), Boolean)] = unary_operator_rewriter("NOT")
    rewriter_table[Prototype("and", (Boolean, Boolean), Boolean)] = binary_operator_rewriter("AND")
    rewriter_table[Prototype("or", (Boolean, Boolean), Boolean)] = binary_operator_rewriter("OR")

    #
    # Comparison operators.
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

    is_defined_rewriter = lambda arg: "(%s) IS NOT NULL" % arg
    rewriter_table[Prototype("is_defined", (Long,), Boolean)] = is_defined_rewriter
    rewriter_table[Prototype("is_defined", (Integer,), Boolean)] = is_defined_rewriter
    rewriter_table[Prototype("is_defined", (Real,), Boolean)] = is_defined_rewriter
    rewriter_table[Prototype("is_defined", (Boolean,), Boolean)] = is_defined_rewriter
    rewriter_table[Prototype("is_defined", (Text,), Boolean)] = is_defined_rewriter
    rewriter_table[Prototype("is_defined", (Timestamp,), Boolean)] = is_defined_rewriter
    rewriter_table[Prototype("is_defined", (UUID,), Boolean)] = is_defined_rewriter
    rewriter_table[Prototype("is_defined", (Geometry,), Boolean)] = is_defined_rewriter

    return rewriter_table


class _WhereExpressionVisitor(Visitor):
    def __init__(self, rewriter_table, column_name_func, named_placeholder_func):
        super(_WhereExpressionVisitor, self).__init__()
        self._rewriter_table = rewriter_table
        self._column_name = column_name_func
        self._named_placeholder = named_placeholder_func

    def visit(self, visitable):
        self._count, self._parameters, self._namespaces = 0, {}, set()
        return super(_WhereExpressionVisitor, self).visit(visitable), self._parameters, self._namespaces

    def visit_Literal(self, visitable):
        parameter_name = str(self._count)
        self._parameters[parameter_name] = visitable.value
        self._count += 1
        return self._named_placeholder(parameter_name)

    def visit_Name(self, visitable):
        namespace, name = visitable.value.split(".")
        self._namespaces.add(namespace)
        return self._column_name(namespace, name)

    def visit_ParameterReference(self, visitable):
        parameter_name = str(self._count)
        self._parameters[parameter_name] = visitable.value
        self._count += 1
        return self._named_placeholder(parameter_name)

    def visit_FunctionCall(self, visitable):
        try:
            rewriter_func = self._rewriter_table[visitable.prototype]
        except KeyError:
            raise Error("function not supported by backend: %s" % visitable.prototype)

        arguments = [super(_WhereExpressionVisitor, self).visit(argument) for argument in visitable.arguments]
        return rewriter_func(*arguments)

    def default(self, visitable):
        raise Error("unsupported abstract syntax tree node type: %r" % type(visitable).__name__)


class Identifier(object):

    # @staticmethod
    def __init__(self, canonical_identifier, namespace_schemas):
        self.canonical = canonical_identifier
        if canonical_identifier == 'tag':
            # the rules to get the namespace database table name also apply to 'tag'
            self.namespace = canonical_identifier
            self.attribute = canonical_identifier
            self.subscript = None
            self.muninn_type = Text
        elif canonical_identifier == 'count':
            self.namespace = None
            self.attribute = canonical_identifier
            self.subscript = None
            self.muninn_type = Long
        elif not re.match(r'[\w]+\.[\w.]+', canonical_identifier):
            raise Error("cannot resolve identifier: %r" % canonical_identifier)
        else:
            split = canonical_identifier.split('.', 2)
            namespace = split[0]
            attribute = split[1]
            subscript = split[2] if len(split) > 2 else None

            # check if namespace is valid
            if namespace not in namespace_schemas:
                raise Error("undefined namespace: \"%s\"" % namespace)
            # check if attribute is valid
            if attribute not in namespace_schemas[namespace]:
                if (namespace, attribute) != ('core', 'validity_duration'):
                    raise Error("no attribute: %r defined within namespace: %r" % (attribute, namespace))
            # note: not checking if subscript is valid; the list of possible subscripts varies depending on context

            muninn_type = None
            if (namespace, attribute) != ('core', 'validity_duration'):
                muninn_type = namespace_schemas[namespace][attribute]

            self.namespace = namespace
            self.attribute = attribute
            self.muninn_type = muninn_type
            self.subscript = subscript

    @property
    def property(self):
        return '%s.%s' % (self.namespace, self.attribute)


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
        for name in schema:
            sql = name + " " + self._type(schema[name])
            if not schema.is_optional(name):
                sql = sql + " " + "NOT NULL"
            column_sql.append(sql)

        return "CREATE TABLE %s (%s)" % (self._table_name(namespace), ", ".join(column_sql))

    def build_count_query(self, where="", parameters={}):
        # Namespaces that appear in the "where" expression are combined via inner joins. This ensures that only those
        # products that actually have a defined value for a given attribute will be considered by the "where"
        # expression. This also means that products that do not occur in all of the namespaces referred to in the
        # "where" expression will be ignored.
        #
        inner_join_set = set()

        # Parse the where clause.
        where_clause, where_parameters = "", {}
        if where:
            ast = parse_and_analyze(where, self._namespace_schemas, parameters)
            visitor = _WhereExpressionVisitor(self._rewriter_table, self._column_name, self._named_placeholder)
            where_expr, where_parameters, where_namespaces = visitor.visit(ast)
            if where_expr:
                inner_join_set.update(where_namespaces)
                where_clause = "WHERE %s" % where_expr

        # Generate the FROM clause.
        from_clause = "FROM %s" % self._table_name("core")

        inner_join_set.discard("core")
        for namespace in inner_join_set:
            from_clause = "%s INNER JOIN %s USING (uuid)" % (from_clause, self._table_name(namespace))

        # Generate the complete query.
        query = "SELECT COUNT(*) AS count %s" % from_clause
        if where_clause:
            query = "%s %s" % (query, where_clause)

        return query, where_parameters

    def build_summary_query(self, where='', parameters=None, aggregates=None, group_by=None, group_by_tag=False, order_by=None):
        # Namespaces that appear in the "where" expression are combined via inner joins. This ensures that only those
        # products that actually have a defined value for a given attribute will be considered by the "where"
        # expression. This also means that products that do not occur in all of the namespaces referred to in the
        # "where" expression will be ignored.
        #
        # Other namespaces are combined via (left) outer joins, with the core namespace as the leftmost namespace. This
        # ensures that attributes will be returned of any product that occurs in zero or more of the requested
        # namespaces.

        aggregates = aggregates or []
        if group_by_tag:
            group_by = group_by + ['tag']
        result_fields = group_by + ['count'] + aggregates
        outer_join_set, inner_join_set = set(item.split('.')[0] for item in group_by), set()

        # Parse the WHERE clause.
        where_clause, where_parameters = '', {}
        if where:
            ast = parse_and_analyze(where, self._namespace_schemas, parameters)
            visitor = _WhereExpressionVisitor(self._rewriter_table, self._column_name, self._named_placeholder)
            where_expr, where_parameters, where_namespaces = visitor.visit(ast)
            if where_expr:
                inner_join_set.update(where_namespaces)
                where_clause = 'WHERE %s' % where_expr

        # Generate the GROUP BY clause.
        group_by_clause = ''
        group_by_list = [str(i) for i in range(1, len(group_by)+1)]
        if group_by_list:
            group_by_clause = 'GROUP BY %s' % ', '.join(group_by_list)

        # Parse the ORDER BY clause.
        order_by_clause = ''
        order_by_list = []
        for item in order_by:
            direction = 'DESC' if item.startswith('-') else 'ASC'
            name = item[1:] if item.startswith('+') or item.startswith('-') else item
            Identifier(name, self._namespace_schemas)  # check if the identifier is valid
            if name not in result_fields:
                raise Error("cannot order result by %r; field is not present in result" % name)
            order_by_list.append('"%s" %s' % (name, direction))
        order_by_list += [str(i) for i in range(1, len(group_by)+1)]
        if order_by_list:
            order_by_clause = 'ORDER BY %s' % ', '.join(order_by_list)

        # Generate the SELECT clause.
        select_list = []
        # group by fields
        for item in group_by:
            item = Identifier(item, self._namespace_schemas)
            column_name = self._column_name(item.namespace, item.attribute)
            group_by_functions = GROUP_BY_FUNCTIONS.get(item.muninn_type)
            if not group_by_functions:  # item.muninn_type not in (Text, Boolean, Long, Integer):
                if item.muninn_type:
                    raise Error("property %r of type %r cannot be part of the group_by field specification" % (item.property, item.muninn_type.name()))
                else:
                    raise Error("property %r cannot be part of the group_by field specification" % (item.property, ))
            if item.subscript not in group_by_functions:
                if item.subscript:
                    allowed_message = "; it can be one of %r" % group_by_functions if group_by_functions != [None] else ""
                    raise Error(("group field specification subscript %r of %r is not allowed" + allowed_message) % (item.subscript, item.canonical))
                else:
                    raise Error(
                        "property %r of type %r must specify a subscript (one of %r) to be part of the group_by field specification" %
                        (item.property, item.muninn_type.name(), group_by_functions)
                    )
            if item.subscript:
                column_name = self._rewriter_property(column_name, item.subscript)
            select_list.append('%s AS "%s"' % (column_name, item.canonical))
        # aggregated fields
        select_list.append('COUNT(*) AS count')  # always aggregate row count
        for item in aggregates:
            item = Identifier(item, self._namespace_schemas)
            if not AGGREGATE_FUNCTIONS.get(item.muninn_type):
                raise Error("property %r of type %r cannot be part of the summary field specification" % (item.property, item.muninn_type.name()))
            elif item.subscript not in AGGREGATE_FUNCTIONS[item.muninn_type]:
                if item.subscript:
                    raise Error("summary field specification subscript %r of %r should be one of %r" % (item.subscript, item.canonical, AGGREGATE_FUNCTIONS[item.muninn_type]))
                else:
                    raise Error("summary field specification %r must specify a subscript (one of %r)" % (item.canonical, AGGREGATE_FUNCTIONS[item.muninn_type]))
            if item.property == 'core.validity_duration':
                start_column = self._column_name(item.namespace, 'validity_start')
                stop_column = self._column_name(item.namespace, 'validity_stop')
                column_name = self._rewriter_table[Prototype('-', (Timestamp, Timestamp), Real)](stop_column, start_column)
            else:
                column_name = self._column_name(item.namespace, item.attribute)
            select_list.append('%s(%s) AS "%s"' % (item.subscript.upper(), column_name, item.canonical))
        select_clause = 'SELECT %s' % ', '.join(select_list)

        # Generate the FROM clause.
        from_clause = 'FROM %s' % self._table_name('core')

        outer_join_set.discard('core')
        inner_join_set.discard('core')
        for namespace in outer_join_set - inner_join_set:
            from_clause = '%s LEFT OUTER JOIN %s USING (uuid)' % (from_clause, self._table_name(namespace))
        for namespace in inner_join_set:
            from_clause = '%s INNER JOIN %s USING (uuid)' % (from_clause, self._table_name(namespace))

        # Generate the complete query.
        query = '%s\n%s' % (select_clause, from_clause)
        if where_clause:
            query = '%s\n%s' % (query, where_clause)
        if group_by_clause:
            query = '%s\n%s' % (query, group_by_clause)
        if order_by_clause:
            query = '%s\n%s' % (query, order_by_clause)

        return query, where_parameters, result_fields

    def build_search_query(self, where="", order_by=[], limit=None, parameters={}, namespaces=[]):
        # Namespaces are combined via (left) outer joins, with the core namespace as the leftmost namespace. This
        # ensures that attributes will be returned of any product that occurs in zero or more of the requested
        # namespaces.
        #
        # Namespaces that appear in the "where" and "order by" expressions, in contrast, are combined via inner joins.
        # This ensures that only those products that actually have a defined value for a given attribute will be
        # considered by the "where" and "order by" expressions. This also means that products that do not occur in all
        # of the namespaces referred to in the "where" and "order by" expressions will be ignored.
        #
        outer_join_set, inner_join_set = set(namespaces), set()

        description = [("core", list(self._namespace_schema("core")))]
        for namespace in outer_join_set:
            description.append((namespace, ["uuid"] + list(self._namespace_schema(namespace))))

        # Parse the where clause.
        where_clause, where_parameters = "", {}
        if where:
            ast = parse_and_analyze(where, self._namespace_schemas, parameters)
            visitor = _WhereExpressionVisitor(self._rewriter_table, self._column_name, self._named_placeholder)
            where_expr, where_parameters, where_namespaces = visitor.visit(ast)
            if where_expr:
                inner_join_set.update(where_namespaces)
                where_clause = "WHERE %s" % where_expr

        # Parse the order by clause.
        order_by_clause = ""
        if order_by:
            order_by_list, order_by_namespaces = self._build_order_by_list(order_by)
            if order_by_list:
                inner_join_set.update(order_by_namespaces)
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
        for namespace, attributes in description:
            select_list.extend([self._column_name(namespace, attribute) for attribute in attributes])
        select_clause = "SELECT %s" % ", ".join(select_list)

        # Generate the FROM clause.
        from_clause = "FROM %s" % self._table_name("core")

        outer_join_set.discard("core")
        inner_join_set.discard("core")
        for namespace in outer_join_set - inner_join_set:
            from_clause = "%s LEFT OUTER JOIN %s USING (uuid)" % (from_clause, self._table_name(namespace))
        for namespace in inner_join_set:
            from_clause = "%s INNER JOIN %s USING (uuid)" % (from_clause, self._table_name(namespace))

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

            try:
                namespace, attribute = name.split(".")
            except ValueError:
                raise Error("invalid attribute name: %r" % name)

            if attribute not in self._namespace_schema(namespace):
                raise Error("no attribute: %r defined within namespace: %r" % (attribute, namespace))

            namespaces.add(namespace)
            order_by_list.append(self._column_name(namespace, attribute) + " " + direction)

        return order_by_list, namespaces

    def _column_name(self, namespace, attribute):
        return self._table_name(namespace) + "." + attribute

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
