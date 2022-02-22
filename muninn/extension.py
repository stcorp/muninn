#
# Copyright (C) 2014-2022 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

from muninn.enum import Enum


class CascadeRule(Enum):
    _items = ("IGNORE", "CASCADE_PURGE_AS_STRIP", "CASCADE_PURGE", "STRIP", "CASCADE", "PURGE")
