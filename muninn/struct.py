#
# Copyright (C) 2014-2022 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

from muninn.exceptions import Error


class Struct(object):
    def __init__(self, data=None, _depth=0):
        super(Struct, self).__init__()
        if data is not None:
            for key in data:
                if _depth == 0 and type(data[key]) == dict:
                    self[key] = Struct(data[key], _depth=_depth+1)
                else:
                    self[key] = data[key]

    def __getitem__(self, key):
        try:
            return getattr(self, key)
        except AttributeError:
            raise KeyError(key)

    def __setitem__(self, key, value):
        setattr(self, key, value)

    def __delitem__(self, key):
        try:
            return delattr(self, key)
        except AttributeError:
            raise KeyError(key)

    def __contains__(self, key):
        return hasattr(self, key)

    def __len__(self):
        return len(vars(self))

    def __iter__(self):
        return iter(vars(self))

    def __repr__(self):
        return "Struct(%r)" % vars(self)

    def update(self, other):
        '''update a struct, using the same semantics as dict.update()'''
        for key in other:
            other_item = other[key]
            if isinstance(other_item, Struct):
                if key not in self:
                    self[key] = Struct()
                else:
                    if not isinstance(self[key], Struct):
                        raise Error('Incompatible structs: %s vs %s' % (self, other))
                self[key].update(other_item)
            else:
                self[key] = other_item
