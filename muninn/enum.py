#
# Copyright (C) 2014-2017 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

from muninn._compat import with_metaclass

class MetaEnum(type):
    def __new__(meta, name, bases, dct):
        for value, item in enumerate(dct["_items"]):
            dct[item] = value
        return super(MetaEnum, meta).__new__(meta, name, bases, dct)


class Enum(with_metaclass(MetaEnum)):
    _items = ()

    @classmethod
    def count(cls):
        return len(cls._items)

    @classmethod
    def valid(cls, value):
        return value >= 0 and value < len(cls._items)

    @classmethod
    def items(cls):
        return cls._items

    @classmethod
    def to_string(cls, value):
        if not cls.valid(value):
            raise ValueError("enumeration: %s has no item with value: %d" % (cls.__name__, value))
        return cls._items[value]

    @classmethod
    def from_string(cls, value):
        try:
            return cls._items.index(value)
        except ValueError:
            raise ValueError("enumeration: %s does not contain: %s" % (cls.__name__, value))
