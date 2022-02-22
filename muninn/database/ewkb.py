#
# Copyright (C) 2014-2022 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import codecs
import struct

from muninn.enum import Enum
from muninn.exceptions import *
from muninn.geometry import *
from muninn.visitor import Visitor


class GeometryType(Enum):
    _items = ("GEOMETRY", "POINT", "LINESTRING", "POLYGON", "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON")


class EWKBEncoder(Visitor):
    def __init__(self, little_endian=True, srid=4326):
        self.endianness = int(little_endian)
        self.srid = srid
        self.prefix = (">", "<")[little_endian]

    def visit(self, visitable, tagged=True, srid=True):
        return super(EWKBEncoder, self).visit(visitable, tagged, srid)

    def visit_Point(self, visitable, tagged, srid):
        ewkb = self._encode("dd", visitable.x, visitable.y)
        return self._encode_tag(GeometryType.POINT, srid) + ewkb if tagged else ewkb

    def visit_LineString(self, visitable, tagged, srid):
        ewkb = self._encode("I", len(visitable))
        ewkb += b"".join([self.visit(point, False) for point in visitable])
        return self._encode_tag(GeometryType.LINESTRING, srid) + ewkb if tagged else ewkb

    def visit_LinearRing(self, visitable, tagged, srid):
        if len(visitable) == 0:
            ewkb = self._encode("I", 0)
        else:
            ewkb = self._encode("I", len(visitable) + 1)
            ewkb += b"".join([self.visit(point, False) for point in visitable])
            ewkb += self.visit(visitable.point(0), False)
        return self._encode_tag(GeometryType.LINESTRING, srid) + ewkb if tagged else ewkb

    def visit_Polygon(self, visitable, tagged, srid):
        ewkb = self._encode("I", len(visitable))
        ewkb += b"".join([self.visit(ring, False) for ring in visitable])
        return self._encode_tag(GeometryType.POLYGON, srid) + ewkb if tagged else ewkb

    def visit_MultiPoint(self, visitable, tagged, srid):
        ewkb = self._encode("I", len(visitable))
        ewkb += b"".join([self.visit(point, True, False) for point in visitable])
        return self._encode_tag(GeometryType.MULTIPOINT, srid) + ewkb if tagged else ewkb

    def visit_MultiLineString(self, visitable, tagged, srid):
        ewkb = self._encode("I", len(visitable))
        ewkb += b"".join([self.visit(line_string, True, False) for line_string in visitable])
        return self._encode_tag(GeometryType.MULTILINESTRING, srid) + ewkb if tagged else ewkb

    def visit_MultiPolygon(self, visitable, tagged, srid):
        ewkb = self._encode("I", len(visitable))
        ewkb += b"".join([self.visit(polygon, True, False) for polygon in visitable])
        return self._encode_tag(GeometryType.MULTIPOLYGON, srid) + ewkb if tagged else ewkb

    def default(self, visitable, tagged, srid):
        raise Error("unsupported type: %s" % type(visitable).__name__)

    def _encode_tag(self, geometry_type, srid):
        if srid and self.srid is not None:
            return self._encode("BII", self.endianness, geometry_type | 0x20000000, self.srid)
        return self._encode("BI", self.endianness, geometry_type)

    def _encode(self, format, *args):
        try:
            return struct.pack(self.prefix + format, *args)
        except struct.error as _error:
            raise Error("encoding error: %s" % str(_error))


class EWKBStream(object):
    def __init__(self, ewkb):
        self.ewkb = ewkb
        self.offset = 0
        self.prefix = (">", "<")[self._decode("=", "B")]

    def decode(self, format):
        return self._decode(self.prefix, format)

    def tail(self):
        return self.ewkb[self.offset:]

    def _decode(self, prefix, format):
        format = prefix + format

        try:
            start, end = self.offset, self.offset + struct.calcsize(format)
            values = struct.unpack(format, self.ewkb[start:end])
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


def _decode_geometry_sequence(stream, expected_ewkb_type):
    sequence, count = [], stream.decode("I")
    for _ in range(count):
        stream = EWKBStream(stream.tail())
        sequence.append(_decode_ewkb(stream, expected_ewkb_type))
    return sequence


def _decode_multi_point(stream):
    return MultiPoint(_decode_geometry_sequence(stream, GeometryType.POINT))


def _decode_multi_line_string(stream):
    return MultiLineString(_decode_geometry_sequence(stream, GeometryType.LINESTRING))


def _decode_multi_polygon(stream):
    return MultiPolygon(_decode_geometry_sequence(stream, GeometryType.POLYGON))


def _decode_ewkb(stream, expected_ewkb_type=None):
    ewkb_type = stream.decode("I")
    ewkb_type, ewkb_flags = ewkb_type & 0x00FFFFFF, ewkb_type >> 28

    if expected_ewkb_type is not None and ewkb_type != expected_ewkb_type:
        raise Error("unexpected EWKB type code: %s (expected: %s)" % (ewkb_type, expected_ewkb_type))

    if ewkb_flags == 0x02:
        srid = stream.decode("I")
        if srid != 4326:
            raise Error("unsupported SRID code: %d" % srid)
    elif ewkb_flags != 0x00:
        raise Error("unsupported EWKB type flags: %d" % ewkb_flags)

    if ewkb_type == GeometryType.POINT:
        return _decode_point(stream)
    elif ewkb_type == GeometryType.LINESTRING:
        return _decode_line_string(stream)
    elif ewkb_type == GeometryType.POLYGON:
        return _decode_polygon(stream)
    elif ewkb_type == GeometryType.MULTIPOINT:
        return _decode_multi_point(stream)
    elif ewkb_type == GeometryType.MULTILINESTRING:
        return _decode_multi_line_string(stream)
    elif ewkb_type == GeometryType.MULTIPOLYGON:
        return _decode_multi_polygon(stream)

    raise Error("unsupported EWKB type code: %d" % ewkb_type)


def encode_ewkb(geometry):
    return EWKBEncoder().visit(geometry)


def encode_hexewkb(geometry):
    return codecs.encode(encode_ewkb(geometry), "hex").decode().upper()


def decode_ewkb(ewkb):
    return _decode_ewkb(EWKBStream(ewkb))


def decode_hexewkb(hexewkb):
    return decode_ewkb(codecs.decode(hexewkb, "hex"))
