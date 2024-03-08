---
layout: page
title: Expression Language
permalink: /expr/
---

# Muninn Expression Language

To make it easy to search for products in an archive, muninn implements its own
expression language. The expression language is somewhat similar to the WHERE
clause in an SQL SELECT statement.

When a muninn extension includes namespace definitions, all product properties
defined in these namespaces can be used in expressions.

The details of the expression language are described below. See
[Data Types](../datatypes) for more information about the data types supported
by muninn.

# Property references

A product property ``x`` defined in namespace ``y`` is referred to using
``y.x``. If the namespace prefix ``y`` is omitted, it defaults to ``core``.
This means that any property from the ``core`` namespace may be referenced
directly.

Some examples of property references:

  ``uuid``

  ``validity_start``

  ``core.uuid``

  ``core.validity_start``

# Namespace references

A namespace ``x`` is referred to using simply ``x``. It is undesirable to
create a namespace with the same name as one of the properties in ``core``,
as this may lead to ambiguities.

# Parameter references

A name preceded by an at sign ``@`` denotes the value of the parameter with
that name. This is primarily useful when calling library functions that take an
expression as an argument. These functions will also take a dictionary of
parameters that will be used to resolved any parameter references present in
the expression.

Some examples of parameter references:

  ``@uuid``

  ``@start``

# Functions and operators

The supported logical operators are ``not``, ``and``, ``or``, in order of
decreasing precedence.

The comparison operators ``==`` (equal) and ``!=`` (not equal) are supported
for all types except geometry.

The comparison operators ``<`` (less than), ``>`` (greater than), ``<=`` (less
than or equal), ``>=`` (greater than or equal) are supported for all types
except boolean, uuid, and geometry.

The membership operators ``in`` and ``not in`` are supported for all types
except boolean, uuid, timestamp and geometry. They only work with lists of
literals. Example syntax:

    expression in [1, 2, 3]
    not text in ["text1", "text2"]

The comparison operator ``~=`` (matches pattern) is supported only for text.
The syntax is:

    text ~= pattern

Any character in the pattern matches itself, except the percent sign ``%``, the
underscore ``_``, and the backslash ``\``.

The percent sign ``%`` matches any sequence of zero or more characters. The
underscore ``_`` matches any single characters. To match a literal percent sign
or underscore, it must be preceded by a backslash ``\``. To match a literal
backslash, write four backslashes ``\\\\``.

The result of the comparison is true only if the pattern matches the text value
on the left hand side. Therefore, to match a pattern anywhere it should be
preceded and followed by a percent sign.

Some examples of the ``~=`` operator:

```
"foobarbaz" ~= "foobarbaz" (true)
"foobarbaz" ~= "foo" (false)
"foobarbaz" ~= "%bar%" (true)
"foobarbaz" ~= "%ba_" (true)
```

The unary and binary arithmetic operators ``+`` and ``-`` are supported for all
numeric types. Furthermore, the binary operator ``-`` applied to a pair of
timestamps returns the length of the time interval between the timestamps as a
fractional number of seconds. Due to the way timestamps are represented in
sqlite, time intervals are limited to millisecond precision when using the
sqlite backend.

The unary function ``is_defined`` is supported for all data types and returns
true if its argument is defined. This can be used to check whether optional
properties or namespaces are defined or not.

The function ``covers(timestamp, timestamp, timestamp, timestamp)`` returns
true if the time range formed by the pair of timestamps covers the time range
formed by the second pair of timestamps. Both time ranges are closed.

The function ``intersects(timestamp, timestamp, timestamp, timestamp)`` returns
true if the time range formed by the pair of timestamps intersects the time
range formed by the second pair of timestamps. Both time ranges are closed.

The function ``covers(geometry, geometry)`` returns true if the first geometry
covers the second geometry.

The function ``distance(geometry, geometry)`` returns the distance between
the two geometries with unit degrees.

The function ``intersects(geometry, geometry)`` returns true if the first
geometry intersects the second geometry.

The function ``is_source_of(uuid)`` returns true if the product under
consideration is a (direct) source of the product referred to by specified
uuid.

The function ``is_derived_from(uuid)`` returns true if the product under
consideration is (directly) derived from the product referred to by the
specified uuid.

For ``is_source_of`` and ``is_derived_from``, instead of a uuid, it is also
possible to specify a sub-expression resolving into one or multiple uuids (see
below for an example).

The function ``has_tag(text)`` returns true if the product under consideration
is tagged with the specified tag.

The function ``now()`` returns a timestamp that represents the current time in
UTC.

# Examples

  ``is_defined(core.validity_start) and core.validity_start < now()``

  ``covers(core.validity_start, core.validity_stop, @start, @stop)``

  ``not covers(core.footprint, POINT(5.0 52.0))``

  ``distance(core.footprint, POINT(5.0 52.0)) < 5``

  ``is_derived_from(32a61528-a712-427a-b28f-8ebd5b472b16)``

  ``is_derived_from(physical_name == "filename.txt")``

  ``validity_stop - validity_start > 300`` (timestamp differences are in seconds)
