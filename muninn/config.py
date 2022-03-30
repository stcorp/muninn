#
# Copyright (C) 2014-2022 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

from muninn._compat import string_types as basestring

from muninn.schema import *
from muninn.exceptions import InternalError
from muninn.visitor import TypeVisitor


# TODO separate into configuration parser and individual field parser?
# TODO missing stuff eg, visit_Real..

class _ConfigParser(TypeVisitor):
    def visit(self, type, value):
        return super(_ConfigParser, self).visit(type, value, "")

    def visit_Integer(self, type, value, path):
        try:
            return int(value)
        except ValueError:
            raise ValueError(prefix_message_with_path(path, "invalid value %r for type %r" % (value, type.name())))

    def visit_Boolean(self, type, value, path):
        upper_case_value = value.upper()
        if upper_case_value in ("FALSE", "NO", "OFF", "0"):
            return False
        if upper_case_value in ("TRUE", "YES", "ON", "1"):
            return True

        raise ValueError(prefix_message_with_path(path, "invalid value %r for type %r" % (value, type.name())))

    def visit_Text(self, type, value, path):
        return value

    def visit_Mapping(self, type, value, path):
        path = "%s:" % type.name() if not path else path

        try:
            iterator = iter(value)
        except TypeError:
            raise ValueError(prefix_message_with_path(path, "expected a mapping"))

        mapping = {}
        for sub_name in iterator:
            if not isinstance(sub_name, basestring):
                raise ValueError(prefix_message_with_path(path, "invalid item name: %r" % sub_name))

            try:
                sub_value = value[sub_name]
            except TypeError:
                raise ValueError(prefix_message_with_path(path, "expected a mapping"))

            try:
                sub_type = type[sub_name]
            except KeyError:
                raise ValueError(prefix_message_with_path(join(path, sub_name), "unrecognized configuration option"))

            mapping[sub_name] = super(_ConfigParser, self).visit(sub_type, sub_value, join(path, sub_name))
        return mapping

    def visit_Sequence(self, type, value, path):
        path = "%s:" % type.name() if not path else path

        if not isinstance(value, basestring):
            raise ValueError(prefix_message_with_path(path, "invalid value %r for type %r" % (value, type.name())))

        sequence = []
        for index, sub_value in enumerate(value.split()):
            sub_path = path + "[%d]" % index
            sequence.append(super(_ConfigParser, self).visit(type.sub_type, sub_value, sub_path))
        return sequence

    def default(self, type, value, path):
        raise InternalError("unsupported type: %s" % type.__name__)


def parse(value, type):
    return _ConfigParser().visit(type, value)
