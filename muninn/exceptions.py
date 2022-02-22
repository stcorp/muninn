#
# Copyright (C) 2014-2022 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function


class Error(Exception):
    pass


class InternalError(Error):
    pass


class StorageError(Error):
    def __init__(self, orig, anything_stored):
        self.orig = orig
        self.anything_stored = anything_stored
