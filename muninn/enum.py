#
# Copyright (C) 2014-2022 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function


class MetaEnum(type):
    def __new__(meta, name, bases, dct):
        class_dct = dct.copy()
        for value, item in enumerate(dct.get('_items', [])):
            class_dct[item] = value
        return super(MetaEnum, meta).__new__(meta, name, bases, class_dct)

    def count(cls):
        return len(cls._items)

    def valid(cls, value):
        return value >= 0 and value < len(cls._items)

    def items(cls):
        return cls._items

    def to_string(cls, value):
        if not cls.valid(value):
            raise ValueError("enumeration: %s has no item with value: %d" % (cls.__name__, value))
        return cls._items[value]

    def from_string(cls, value):
        try:
            return cls._items.index(value)
        except ValueError:
            raise ValueError("enumeration: %s does not contain: %s" % (cls.__name__, value))


Enum = MetaEnum('Enum', (), {})
