#
# Copyright (C) 2014-2019 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import datetime
from muninn._compat import with_metaclass, long
import uuid

import muninn.geometry as geometry


def prefix_message_with_path(path, message):
    if not path:
        return message
    if path.endswith(":"):
        return path + " " + message
    return path + ": " + message


def join(path, *args):
    if not path:
        return ".".join(args)
    if path.endswith(":"):
        return path + " " + ".".join(args)
    return path + "." + ".".join(args)


class optional(object):
    def __init__(self, type):
        self.type = type


class Type(object):
    @classmethod
    def name(cls):
        return getattr(cls, "_alias", cls.__name__.lower())


class Long(Type):
    @classmethod
    def validate(cls, value):
        if type(value) is not int and type(value) is not long or value < -9223372036854775808 \
                or value > 9223372036854775807:
            raise ValueError("invalid value %r for type %r" % (value, cls.name()))


class Integer(Type):
    @classmethod
    def validate(cls, value):
        if type(value) is not int and type(value) is not long or value < -2147483648 or value > 2147483647:
            raise ValueError("invalid value %r for type %r" % (value, cls.name()))


class Real(Type):
    @classmethod
    def validate(cls, value):
        if type(value) is not float:
            raise ValueError("invalid value %r for type %r" % (value, cls.name()))


class Boolean(Type):
    @classmethod
    def validate(cls, value):
        if type(value) is not bool:
            raise ValueError("invalid value %r for type %r" % (value, cls.name()))


class Text(Type):
    @classmethod
    def validate(cls, value):
        if type(value) is not str:
            raise ValueError("invalid value %r for type %r" % (value, cls.name()))


class Timestamp(Type):
    @classmethod
    def validate(cls, value):
        if type(value) is not datetime.datetime:
            raise ValueError("invalid value %r for type %r" % (value, cls.name()))


class UUID(Type):
    @classmethod
    def validate(cls, value):
        if type(value) is not uuid.UUID:
            raise ValueError("invalid value %r for type %r" % (value, cls.name()))


class Geometry(Type):
    @classmethod
    def validate(cls, value):
        if not isinstance(value, geometry.Geometry):
            raise ValueError("invalid value %r for type %r" % (value, cls.name()))


class Container(Type):
    pass


class Sequence(Container):
    sub_type = None

    @classmethod
    def validate(cls, value, partial=False, path=""):
        path = "%s:" % cls.name() if not path else path

        try:
            iterator = iter(value)
        except TypeError:
            raise ValueError(prefix_message_with_path(path, "expected a sequence"))

        for index, sub_value in enumerate(iterator):
            if issubclass(cls.sub_type, Container):
                cls.sub_type.validate(sub_value, partial, path + "[%d]" % index)
            else:
                try:
                    cls.sub_type.validate(sub_value)
                except ValueError as _error:
                    raise ValueError(prefix_message_with_path(path + "[%d]" % index, str(_error)))


class MetaMapping(type):
    def __new__(meta, name, bases, dct):
        class_dct = {}
        items = {}
        for key, value in dct.items():
            if isinstance(value, type) and issubclass(value, Type):
                items[key] = (value, False)
            elif type(value) is optional:
                items[key] = (value.type, True)
            else:
                class_dct[key] = value

        assert "_items" not in class_dct
        class_dct["_items"] = items
        return super(MetaMapping, meta).__new__(meta, name, bases, class_dct)

    def __len__(self):
        return self._len()

    def __getitem__(self, name):
        return self._getitem(name)

    def __iter__(self):
        return self._iter()

    def __contains__(self, name):
        return self._contains(name)


class Mapping(with_metaclass(MetaMapping, Container)):

    @classmethod
    def validate(cls, value, partial=False, path=""):
        path = "%s:" % cls.name() if not path else path

        validated = 0
        for sub_name, (sub_type, sub_optional) in cls._items.items():
            try:
                sub_value = value[sub_name]
            except TypeError:
                raise ValueError(prefix_message_with_path(path, "expected a mapping"))
            except KeyError:
                if not partial and not sub_optional:
                    raise ValueError(prefix_message_with_path(join(path, sub_name), "no value for mandatory item"))
            else:
                if not sub_optional or sub_value is not None:
                    if issubclass(sub_type, Container):
                        sub_type.validate(sub_value, partial, join(path, sub_name))
                    else:
                        try:
                            sub_type.validate(sub_value)
                        except ValueError as _error:
                            raise ValueError(prefix_message_with_path(join(path, sub_name), str(_error)))

                validated += 1

        if validated != len(value):
            extra_names = set(value) - set(cls._items)
            raise ValueError(prefix_message_with_path(path, "undefined item: %r" % extra_names.pop()))

    @classmethod
    def is_optional(cls, name):
        _, optional = cls._items[name]
        return optional

    @classmethod
    def _getitem(cls, name):
        type, _ = cls._items[name]
        return type

    @classmethod
    def _contains(cls, name):
        return name in cls._items

    @classmethod
    def _iter(cls):
        return iter(cls._items)

    @classmethod
    def _len(cls):
        return len(cls._items)
