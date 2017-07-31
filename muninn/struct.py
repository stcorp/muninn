#
# Copyright (C) 2014-2017 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function


class Struct(object):
    def __init__(self, data=None):
        super(Struct, self).__init__()
        if data is not None:
            for key in data:
                if type(data[key]) == dict:
                    self[key] = Struct(data[key])
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
