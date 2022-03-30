#
# Copyright (C) 2014-2022 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

try:
    from collections.abc import MutableSequence
except ImportError:
    from collections import MutableSequence

from muninn.exceptions import Error


class Geometry(object):
    @property
    def min_x(self):
        return 0

    @property
    def max_x(self):
        return 0

    @property
    def min_y(self):
        return 0

    @property
    def max_y(self):
        return 0


class Point(Geometry):
    def __init__(self, x, y):
        self._coordinates = [x, y]

    @property
    def x(self):
        return self._coordinates[0]

    @x.setter
    def x(self, value):
        self._coordinates[0] = value

    @property
    def y(self):
        return self._coordinates[1]

    @y.setter
    def y(self, value):
        self._coordinates[1] = value

    @property
    def min_x(self):
        return self.x

    @property
    def max_x(self):
        return self.x

    @property
    def min_y(self):
        return self.y

    @property
    def max_y(self):
        return self.y

    @property
    def longitude(self):
        return self._coordinates[0]

    @longitude.setter
    def longitude(self, value):
        self._coordinates[0] = value

    @property
    def latitude(self):
        return self._coordinates[1]

    @latitude.setter
    def latitude(self, value):
        self._coordinates[1] = value

    def as_wkt(self, tagged=True):
        wkt = "(%f %f)" % (self.x, self.y)
        return "POINT " + wkt if tagged else wkt

    def __getitem__(self, index):
        return self._coordinates[index]

    def __setitem__(self, index, value):
        self._coordinates[index] = value

    def __len__(self):
        return 2

    def __eq__(self, other):
        return self._coordinates == other._coordinates

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return "Point(x=%f, y=%f)" % (self.x, self.y)

    def __str__(self):
        return self.as_wkt()


class GeometrySequence(Geometry, MutableSequence):
    def __init__(self, geometries=[]):
        self._geometries = list(geometries)

    @property
    def min_x(self):
        return min([g.min_x for g in self._geometries])

    @property
    def max_x(self):
        return max([g.max_x for g in self._geometries])

    @property
    def min_y(self):
        return min([g.min_y for g in self._geometries])

    @property
    def max_y(self):
        return max([g.max_y for g in self._geometries])

    def insert(self, index, geometry):
        self._geometries.insert(index, geometry)

    def __getitem__(self, index):
        return self._geometries[index]

    def __setitem__(self, index, point):
        self._geometries[index] = point

    def __delitem__(self, index):
        del self._geometries[index]

    def __len__(self):
        return len(self._geometries)

    def __eq__(self, other):
        return self._geometries == other._geometries

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return "GeometrySequence(geometries=%r)" % self._geometries


class LineString(GeometrySequence):
    def __init__(self, geometries=[]):
        super(LineString, self).__init__(geometries)

        if len(self) == 1:
            raise Error("line string should be empty or should contain >= 2 points")

    def point(self, index):
        return self[index]

    def is_closed(self):
        return not self or self[0] == self[-1]

    def as_wkt(self, tagged=True):
        wkt = "(" + ", ".join(["%f %f" % (point.x, point.y) for point in self]) + ")" if self else "EMPTY"
        return "LINESTRING " + wkt if tagged else wkt

    def __repr__(self):
        return "LineString(points=%r)" % self._geometries

    def __str__(self):
        return self.as_wkt()


class LinearRing(GeometrySequence):
    def __init__(self, geometries=[]):
        super(LinearRing, self).__init__(geometries)

        if self and len(self) < 3:
            raise ValueError("linear ring should be empty or should contain >= 3 points")

    def point(self, index):
        return self[index]

    def as_wkt(self, tagged=True):
        if not self:
            wkt = "EMPTY"
        else:
            wkt = "(%s, %f %f)" % (", ".join(["%f %f" % (point.x, point.y) for point in self]), self.point(0).x,
                                   self.point(0).y)
        return "LINESTRING " + wkt if tagged else wkt

    def __repr__(self):
        return "LinearRing(points=%r)" % self._geometries

    def __str__(self):
        return self.as_wkt()


class Polygon(GeometrySequence):
    def ring(self, index):
        return self[index]

    def exterior_ring(self):
        return self[0]

    def interior_ring(self, index):
        return self[index + 1]

    def as_wkt(self, tagged=True):
        wkt = "(" + ", ".join([geometry.as_wkt(False) for geometry in self]) + ")" if self else "EMPTY"
        return "POLYGON " + wkt if tagged else wkt

    def __repr__(self):
        return "Polygon(rings=%r)" % self._geometries

    def __str__(self):
        return self.as_wkt()


class MultiPoint(GeometrySequence):
    def point(self, index):
        return self[index]

    def as_wkt(self, tagged=True):
        wkt = "(" + ", ".join([geometry.as_wkt(False) for geometry in self]) + ")" if self else "EMPTY"
        return "MULTIPOINT " + wkt if tagged else wkt

    def __repr__(self):
        return "MultiPoint(points=%r)" % self._geometries

    def __str__(self):
        return self.as_wkt()


class MultiLineString(GeometrySequence):
    def line_string(self, index):
        return self[index]

    def as_wkt(self, tagged=True):
        wkt = "(" + ", ".join([geometry.as_wkt(False) for geometry in self]) + ")" if self else "EMPTY"
        return "MULTILINESTRING " + wkt if tagged else wkt

    def __repr__(self):
        return "MultiLineString(line_strings=%r)" % self._geometries

    def __str__(self):
        return self.as_wkt()


class MultiPolygon(GeometrySequence):
    def polygon(self, index):
        return self[index]

    def as_wkt(self, tagged=True):
        wkt = "(" + ", ".join([geometry.as_wkt(False) for geometry in self]) + ")" if self else "EMPTY"
        return "MULTIPOLYGON " + wkt if tagged else wkt

    def __repr__(self):
        return "MultiPolygon(polygons=%r)" % self._geometries

    def __str__(self):
        return self.as_wkt()


def as_point(iterable):
    return Point(*iterable)


def as_line_string(iterable):
    return LineString(map(as_point, iterable))


def as_linear_ring(iterable):
    return LinearRing(map(as_point, iterable))


def as_polygon(iterable):
    return Polygon(map(as_linear_ring, iterable))


def as_multi_point(iterable):
    return MultiPoint(map(as_point, iterable))


def as_multi_line_string(iterable):
    return MultiLineString(map(as_line_string, iterable))


def as_multi_polygon(iterable):
    return MultiPolygon(map(as_polygon, iterable))
