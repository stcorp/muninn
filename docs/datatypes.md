---
layout: page
title: Data Types
permalink: /datatypes/
---

# Data types

Each product property can be of one of the following supported types: boolean,
integer, long, real, text, timestamp, uuid, geometry and json. These types are
described in detail below.

The boolean type represents a truth value and has two possible states: ``true``
and ``false``.

The valid literal boolean values are:

  ``true``

  ``false``

The integer types (integer and long) represent whole numbers. The integer type
is a 32-bit signed integer and can be used to represent values in the range
-2147483648 to +2147483647 (inclusive). The long type is a 64-bit signed
integer and can be used to represent values in the range -9223372036854775808
to +9223372036854775807 (inclusive).

Some examples of literal integer values:

  ``-3``

  ``0``

  ``10``

  ``+99``

The floating point type (real) represents fractional numbers. The real type is
a double precision floating point number and has a typical range of around
1E-307 to 1E+308 with a precision of at least 15 digits.

Some examples of literal real values:

  ``1E-5``

  ``1.E+10``

  ``-3.1415E0``

  ``1.0``

The text type represents text. Literal values are enclosed in double quotes and
most common backslash escape sequences are recognized. To include a double quote
or a backslash inside a text literal, they must be escaped with a backslash,
i.e. ``\"`` and ``\\``.

Some examples of literal text values:

  ``"Hello world!\n"``

  ``"This is a so-called \"text\" literal."``

The timestamp type represents an instance in time with microsecond resolution.
Time zone information is not included. Although throughout muninn all
timestamps are expressed in UTC, users (and especially product type plug-in
developers) can choose a different convention (e.g. local time) for custom
product properties.

The minimum and maximum timestamp values are ``0001-01-01T00:00:00.000000`` and
``9999-12-31T23:59:59.999999`` respectively, which may also be written as
``0000-00-00T00:00:00.000000`` and ``9999-99-99T99:99:99.999999`` for
convenience.

Some examples of literal timestamp values:

  ``2000-01-01``

  ``2000-01-01T00:00:00``

  ``2000-01-01T00:00:00.``

  ``2000-01-01T00:00:00.3``

  ``1999-12-21T23:59:59.999999``

  ``0000-00-00``

  ``0000-00-00T00:00:00``

  ``9999-99-99T99:99:99.99``

The uuid type represents a universally unique identifier, a 128-bit number that
is used to uniquely identify products in a muninn archive.

Some examples of literal uuid values:

  ``32a61528-a712-427a-b28f-8ebd5b472b16``

  ``873dd103-2115-4bf8-9f05-d0eb4b3f71ea``

  ``bdc10916-d89f-416c-8987-a9c2af9b1ef7``

The geometry type represents two-dimensional geometric objects. The spatial
reference system used is WGS84 (SRID=4326). Longitude is measured in degrees
East, latitude is measured in degrees North. The coordinates of a point are
ordered as (longitude, latitude).

The geometric objects currently supported are: Point, LineString, Polygon,
MultiPoint, MultiLineString, and MultiPolygon.

The linear ring(s) that make up a polygon should be topologically closed. In
other words, the start and end point of any linear ring should be equal. A
polygon of which the exterior ring is ordered anti-clockwise is seen from the
"top". Any interior rings should be ordered in the direction opposite to the
exterior ring.

A sub-set of the Well Known Text (WKT) markup language is used to represent
literal geometry values. This sub-set is limited to the supported geometric
objects listed above. Only two-dimensional coordinates are supported. Empty
geometries are supported. An empty geometry is represented by the name of the
geometry type followed by the keyword ``EMPTY``.

Some examples of literal geometry values:

  ``POINT (3.0 55.0)``

  ``LINESTRING (3.0 55.0, 3.0 80.0, 5.0 75.0)``

  ``POLYGON ((5.0 52.0, 6.0 53.0, 3.0 52.5, 5.0 52.0))``

  ``POLYGON EMPTY``

The json type represents JSON objects.
