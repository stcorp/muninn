#
# Copyright (C) 2014-2017 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import os

from muninn.schema import *
from muninn.util import split_path
from muninn.remote import *


class ArchivePath(Text):
    _alias = "archive_path"

    @classmethod
    def validate(cls, value):
        super(ArchivePath, cls).validate(value)
        if os.path.isabs(value) or os.path.pardir in split_path(value):
            raise ValueError("invalid value %r for type %r" % (value, cls.name()))


class Basename(Text):
    @classmethod
    def validate(cls, value):
        super(Basename, cls).validate(value)
        if value != os.path.basename(value):
            raise ValueError("invalid value %r for type %r" % (value, cls.name()))


class Remote(Text):
    @classmethod
    def validate(cls, value):
        super(Remote, cls).validate(value)
        if not any([value.startswith(x) for x in REMOTE_PREFIXES.values()]):
            raise ValueError("The protocol of '%s' is not supported" % value)


class Core(Mapping):
    uuid = UUID
    active = Boolean
    hash = optional(Text)
    size = optional(Long)
    metadata_date = Timestamp
    archive_date = optional(Timestamp)
    archive_path = optional(ArchivePath)
    product_type = Text
    product_name = Text
    physical_name = Basename
    validity_start = optional(Timestamp)
    validity_stop = optional(Timestamp)
    creation_date = optional(Timestamp)
    footprint = optional(Geometry)
    remote_url = optional(Remote)
