#
# Copyright (C) 2014-2022 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import sys
import struct

from muninn.enum import Enum
from muninn.exceptions import *
from muninn.geometry import *
from muninn.visitor import Visitor


class GeometryType(Enum):
    _items = ("GEOMETRY", "POINT", "LINESTRING", "POLYGON", "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON")


class BLOBGeometryEncoder(Visitor):
    def __init__(self, little_endian=True):
        self.endianness = int(little_endian)
        self.prefix = (">", "<")[little_endian]
        self.entity = self._encode("B", 0x69)

    def visit(self, visitable, tagged=True):
        return super(BLOBGeometryEncoder, self).visit(visitable, tagged)

    def visit_Point(self, visitable, tagged):
        wkb = self._encode("dd", visitable.x, visitable.y)
        return self._encode_tag(GeometryType.POINT) + wkb if tagged else wkb

    def visit_LineString(self, visitable, tagged):
        wkb = self._encode("I", len(visitable))
        wkb += b"".join([self.visit(point, False) for point in visitable])
        return self._encode_tag(GeometryType.LINESTRING) + wkb if tagged else wkb

    def visit_LinearRing(self, visitable, tagged):
        if len(visitable) == 0:
            wkb = self._encode("I", 0)
        else:
            wkb = self._encode("I", len(visitable) + 1)
            wkb += b"".join([self.visit(point, False) for point in visitable])
            wkb += self.visit(visitable.point(0), False)
        return self._encode_tag(GeometryType.LINESTRING) + wkb if tagged else wkb

    def visit_Polygon(self, visitable, tagged):
        wkb = self._encode("I", len(visitable))
        wkb += b"".join([self.visit(ring, False) for ring in visitable])
        return self._encode_tag(GeometryType.POLYGON) + wkb if tagged else wkb

    def visit_MultiPoint(self, visitable, tagged):
        wkb = self._encode("I", len(visitable))
        wkb += b"".join([self.entity + self.visit(point, True) for point in visitable])
        return self._encode_tag(GeometryType.MULTIPOINT) + wkb if tagged else wkb

    def visit_MultiLineString(self, visitable, tagged):
        wkb = self._encode("I", len(visitable))
        wkb += b"".join([self.entity + self.visit(line_string, True) for line_string in visitable])
        return self._encode_tag(GeometryType.MULTILINESTRING) + wkb if tagged else wkb

    def visit_MultiPolygon(self, visitable, tagged):
        wkb = self._encode("I", len(visitable))
        wkb += b"".join([self.entity + self.visit(polygon, True) for polygon in visitable])
        return self._encode_tag(GeometryType.MULTIPOLYGON) + wkb if tagged else wkb

    def default(self, visitable, tagged):
        raise Error("unsupported type: %s" % type(visitable).__name__)

    def _encode_tag(self, wkb_type):
        return self._encode("I", wkb_type)

    def _encode(self, format, *args):
        try:
            return struct.pack(self.prefix + format, *args)
        except struct.error as _error:
            raise Error("encoding error: %s" % str(_error))


class BLOBGeometryStream(object):
    def __init__(self, wkb):
        self.wkb = wkb
        self.offset = 0
        self.prefix = "="

    def decode(self, format):
        return self._decode(self.prefix, format)

    def tail(self):
        return self.wkb[self.offset:]

    def set_endian(self, little_endian):
        self.prefix = (">", "<")[little_endian]

    def _decode(self, prefix, format):
        format = prefix + format

        try:
            start, end = self.offset, self.offset + struct.calcsize(format)
            values = struct.unpack(format, self.wkb[start:end])
        except struct.error as _error:
            raise Error("decoding error: %s" % str(_error))

        self.offset = end
        return values[0] if len(values) == 1 else values


def _decode_point(stream):
    return Point(*stream.decode("dd"))


def _decode_line_string(stream):
    count = stream.decode("I")
    return LineString([_decode_point(stream) for _ in range(count)])


def _decode_linear_ring(stream):
    count = stream.decode("I")
    if count == 0:
        return LinearRing()

    if count < 4:
        raise Error("linear ring should be empty or should contain >= 4 points")

    points = [_decode_point(stream) for _ in range(count)]
    if points[-1] != points[0]:
        raise Error("linear ring should be closed")

    return LinearRing(points[:-1])


def _decode_polygon(stream):
    count = stream.decode("I")
    return Polygon([_decode_linear_ring(stream) for _ in range(count)])


def _decode_geometry_sequence(stream, expected_wkb_type):
    count = stream.decode("I")
    sequence = []
    for _ in range(count):
        entity = stream.decode("B")
        if entity != 0x69:
            raise Error("invalid SQLite BLOB-Geometry")
        sequence.append(_decode_wkb(stream, expected_wkb_type))
    return sequence


def _decode_multi_point(stream):
    return MultiPoint(_decode_geometry_sequence(stream, GeometryType.POINT))


def _decode_multi_line_string(stream):
    return MultiLineString(_decode_geometry_sequence(stream, GeometryType.LINESTRING))


def _decode_multi_polygon(stream):
    return MultiPolygon(_decode_geometry_sequence(stream, GeometryType.POLYGON))


def _decode_wkb(stream, expected_wkb_type=None):
    wkb_type = stream.decode("I")

    if expected_wkb_type is not None and wkb_type != expected_wkb_type:
        raise Error("unexpected WKB type code: %s (expected: %s)" % (wkb_type, expected_wkb_type))

    if wkb_type == GeometryType.POINT:
        return _decode_point(stream)
    elif wkb_type == GeometryType.LINESTRING:
        return _decode_line_string(stream)
    elif wkb_type == GeometryType.POLYGON:
        return _decode_polygon(stream)
    elif wkb_type == GeometryType.MULTIPOINT:
        return _decode_multi_point(stream)
    elif wkb_type == GeometryType.MULTILINESTRING:
        return _decode_multi_line_string(stream)
    elif wkb_type == GeometryType.MULTIPOLYGON:
        return _decode_multi_polygon(stream)

    raise Error("unsupported WKB type code: %d" % wkb_type)


def encode_blob_geometry(geometry):
    little_endian = (sys.byteorder == 'little')
    srid = 4326
    encoder = BLOBGeometryEncoder(little_endian)
    # Start
    blob = encoder._encode("B", 0)
    # ENDIAN
    blob += encoder._encode("B", int(little_endian))
    # SRID
    blob += encoder._encode("I", srid)
    # MBR_MIN_X
    blob += encoder._encode("d", geometry.min_x)
    # MBR_MIN_Y
    blob += encoder._encode("d", geometry.min_y)
    # MBR_MAX_X
    blob += encoder._encode("d", geometry.max_x)
    # MBR_MAX_Y
    blob += encoder._encode("d", geometry.max_y)
    # MBR_END
    blob += encoder._encode("B", 0x7c)
    # 'WKB' of geometry
    blob += encoder.visit(geometry)
    # LAST
    blob += encoder._encode("B", 0xfe)
    return blob


def decode_blob_geometry(blob):
    stream = BLOBGeometryStream(blob)
    if stream.decode("B") != 0:
        raise Error("invalid SQLite BLOB-Geometry")
    stream.set_endian(stream.decode("B"))
    srid = stream.decode("I")
    if srid != 4326:
        raise Error("unsupported SRID code: %d" % srid)
    # MBR
    stream.decode("dddd")
    if stream.decode("B") != 0x7c:
        raise Error("invalid SQLite BLOB-Geometry")
    geometry = _decode_wkb(stream)
    if stream.decode("B") != 0xfe:
        raise Error("invalid SQLite BLOB-Geometry")
    return geometry
