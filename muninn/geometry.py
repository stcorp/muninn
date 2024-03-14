#
# Copyright (C) 2014-2024 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import math
try:
    from collections.abc import MutableSequence
except ImportError:
    from collections import MutableSequence

from muninn.exceptions import Error


def polygon_rotation(pts):
    # return wether polygon is:
    #  1: anti-clockwise rotation (right-hand-rule) -> use inner area
    #  0: no rotation -> polygon is empty or invalid
    # -1: clockwise rotation (left-hand-rule) -> use outer area
    # this can be calculated by summing the outer products of consecutive pts (taking vectors from (0,0) to the pt)
    prev_pt = pts[0]
    sum = 0
    for pt in pts[1:]:
        sum += pt[1] * prev_pt[0] - pt[0] * prev_pt[1]
        prev_pt = pt
    if sum == 0:
        return 0
    return math.copysign(1, sum)


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

    @staticmethod
    def from_geojson(geojson):
        type_ = geojson['type']
        coordinates = geojson['coordinates']

        if type_ == 'Point':
            return as_point(coordinates)

        elif type_ == 'LineString':
            return as_line_string(coordinates)

        elif type_ == 'Polygon':
            return as_polygon(coordinates)

        elif type_ == 'MultiPoint':
            return as_multi_point(coordinates)

        elif type_ == 'MultiLineString':
            return as_multi_line_string(coordinates)

        elif type_ == 'MultiPolygon':
            return as_multi_polygon(coordinates)

        else:
            raise Error('cannot convert geojson type: %s' % type_)

    def wrap(self):
        """
        Convert the geometry from one on a sphere to one that fits on a 2D lat/lon canvas with
        -90 <= latitude <= 90 and -180 <= longitude <= 180.
        This involves splitting geometries around the dateline and dealing with geometries that cover the poles
        """
        return self


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

    def as_geojson(self):
        return {
            'type': 'Point',
            'coordinates': [self.x, self.y],
        }

    def as_wkt(self, tagged=True):
        wkt = "(%f %f)" % (self.x, self.y)
        return "POINT " + wkt if tagged else wkt

    def wrap(self):
        """
        This function assumes that the longitude is already in the range [-360,360].
        """
        # map lon to [-180, 180]
        lon = self.longitude
        return Point(lon + 360 if lon < -180 else (lon - 360 if lon > 180 else lon), self.latitude)

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

    def wrap(self):
        return self.__class__([geometry.wrap() for geometry in self._geometries])

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

    def as_geojson(self):
        return {
            'type': 'LineString',
            'coordinates': [[point.x, point.y] for point in self],
        }

    def as_wkt(self, tagged=True):
        wkt = "(" + ", ".join(["%f %f" % (point.x, point.y) for point in self]) + ")" if self else "EMPTY"
        return "LINESTRING " + wkt if tagged else wkt

    def wrap(self):
        """
        Lines will be divided into sub-lines if they cross the dateline
        """
        lon, lat = self[0].wrap()
        prev_lon, prev_lat = lon, lat
        pts = [Point(lon, lat)]
        pts_set = [pts]
        for point in self[1:]:
            lon, lat = point.wrap()
            # rel_lon = lon mapped to [prev_lon - 180, prev_lon + 180]
            rel_lon = lon + 360 if lon < prev_lon - 180 else (lon - 360 if lon > prev_lon + 180 else lon)
            if rel_lon < -180:
                # crossing the dateline meridian -> split line
                mid_lat = lat + ((-180 - rel_lon) / (prev_lon - rel_lon)) * (prev_lat - lat)
                pts.append(Point(-180, mid_lat))
                pts = [Point(180, mid_lat)]
                pts_set.append(pts)
            elif rel_lon > 180:
                # crossing the dateline meridian -> split line
                mid_lat = prev_lat + ((180 - prev_lon) / (rel_lon - prev_lon)) * (lat - prev_lat)
                pts.append(Point(180, mid_lat))
                pts = [Point(-180, mid_lat)]
                pts_set.append(pts)
            prev_lon, prev_lat = lon, lat
            pts.append(Point(lon, lat))
        if len(pts_set) > 1:
            return MultiLineString([LineString(pts) for pts in pts_set])
        else:
            return LineString(pts)

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

    def as_geojson(self):
        return {
            'type': 'Polygon',
            'coordinates': [[[point.x, point.y] for point in ring] for ring in self],
        }

    def wrap(self):
        """
        Polygons need to be split at the dateline. This also requires using a special 'unfolding' to make a polygon
        that covers the North and/or South pole to still cover the whole polar region on a flat 2D area.

        Any exclusion regions will be remove in the conversion.

        The special situation where a polygon covers both poles _and_ runs along the dateline will result in a single
        polygon with a wrong rotation. This type of polygon is turned into a geometry with a hole (i.e. outer polygon
        is the full earth bounding box, and the original polygon becomes the exclusion area).
        Input polygons should be properly oriented using the right-hand rule (= anti clockwise) or they may otherwise
        be turned into exclusions by this algorithm.
        """
        # We only wrap the outer ring
        ring = self.exterior_ring()
        lon, lat = ring[0].wrap()
        # current_area = {-1: lon < -180, 0: -180 <= lon <= 180, 1: lon >= 180}
        current_area = 0
        prev_lon, prev_lat = lon, lat
        pts = [Point(lon, lat)]
        pts_set = [pts]
        crossing_lat = []
        for point in ring[1:]:
            lon, lat = point.wrap()
            # rel_lon = lon mapped to [prev_lon - 180, prev_lon + 180]
            rel_lon = lon + 360 if lon < prev_lon - 180 else (lon - 360 if lon > prev_lon + 180 else lon)
            if rel_lon < -180:
                if current_area == -1:
                    # unsupported polygon
                    return self
                # crossing the dateline meridian -> split polygon
                mid_lat = lat + ((-180 - rel_lon) / (prev_lon - rel_lon)) * (prev_lat - lat)
                crossing_lat.append(mid_lat)
                pts.append(Point(-180, mid_lat))
                pts = [Point(180, mid_lat)]
                pts_set.append(pts)
                current_area -= 1
            elif rel_lon > 180:
                if current_area == 1:
                    # unsupported polygon
                    return self
                # crossing the dateline meridian -> split polygon
                mid_lat = prev_lat + ((180 - prev_lon) / (rel_lon - prev_lon)) * (lat - prev_lat)
                crossing_lat.append(mid_lat)
                pts.append(Point(180, mid_lat))
                pts = [Point(-180, mid_lat)]
                pts_set.append(pts)
                current_area += 1
            prev_lon, prev_lat = lon, lat
            pts.append(Point(lon, lat))
        if len(pts_set) == 1:
            assert len(crossing_lat) == 0
            if polygon_rotation(pts) < 0:
                world = LinearRing([Point(-180, -90), Point(180, -90), Point(180, 90), Point(-180, 90),
                                    Point(-180, -90)])
                return Polygon([world, LinearRing(pts)])
            else:
                return Polygon([LinearRing(pts)])
        # prepend final pts to first ring
        if pts[-1] == pts_set[0][0]:
            del pts[-1]
        pts.extend(pts_set[0])
        pts_set[0] = pts
        del pts_set[-1]
        # check if we need to connect via the north pole
        if len(crossing_lat) > 0:
            max_lat = max(crossing_lat)
            max_index = crossing_lat.index(max_lat)
            next_index = max_index + 1 if max_index < len(crossing_lat) - 1 else 0
            if pts_set[max_index][-1][0] > pts_set[next_index][0][0]:
                # connect pts via the north pole
                pts_set[max_index].append(Point(180, 90))
                pts_set[max_index].append(Point(-180, 90))
                if max_index != next_index:
                    pts_set[max_index].extend(pts_set[next_index])
                    pts_set[next_index] = pts_set[max_index]
                    del pts_set[max_index]
                    del crossing_lat[max_index]
        # check if we need to connect via the south pole
        if len(crossing_lat) > 0:
            min_lat = min(crossing_lat)
            min_index = crossing_lat.index(min_lat)
            next_index = min_index + 1 if min_index < len(crossing_lat) - 1 else 0
            if pts_set[min_index][-1][0] < pts_set[next_index][0][0]:
                # connect pts via the south pole
                pts_set[min_index].append(Point(-180, -90))
                pts_set[min_index].append(Point(180, -90))
                if min_index != next_index:
                    pts_set[min_index].extend(pts_set[next_index])
                    pts_set[next_index] = pts_set[min_index]
                    del pts_set[min_index]
                    del crossing_lat[min_index]
        for pts in pts_set:
            # close ring
            pts.append(pts[0])
        if len(pts_set) == 1:
            return Polygon([LinearRing(pts)])
        else:
            return MultiPolygon([Polygon([LinearRing(pts)]) for pts in pts_set])

    def __repr__(self):
        return "Polygon(rings=%r)" % self._geometries

    def __str__(self):
        return self.as_wkt()


class MultiPoint(GeometrySequence):
    def point(self, index):
        return self[index]

    def as_geojson(self):
        return {
            'type': 'MultiPoint',
            'coordinates': [[point.x, point.y] for point in self],
        }

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

    def as_geojson(self):
        return {
            'type': 'MultiLineString',
            'coordinates': [[[point.x, point.y] for point in linestring] for linestring in self],
        }

    def as_wkt(self, tagged=True):
        wkt = "(" + ", ".join([geometry.as_wkt(False) for geometry in self]) + ")" if self else "EMPTY"
        return "MULTILINESTRING " + wkt if tagged else wkt

    def wrap(self):
        lines = []
        for line in self._geometries:
            geometry = line.wrap()
            if type(geometry) is MultiLineString:
                lines.extend(geometry)
            else:
                lines.append(geometry)
        return MultiLineString(lines)

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

    def as_geojson(self):
        return {
            'type': 'MultiPolygon',
            'coordinates': [[[[point.x, point.y] for point in ring] for ring in polygon] for polygon in self],
        }

    def wrap(self):
        polys = []
        for poly in self._geometries:
            geometry = poly.wrap()
            if type(geometry) is MultiPolygon:
                polys.extend(geometry)
            else:
                polys.append(geometry)
        return MultiPolygon(polys)

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
