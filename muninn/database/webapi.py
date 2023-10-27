#
# Copyright (C) 2014-2023 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import rpyc

import muninn.config
from muninn.schema import Mapping, Text, Integer

from .base import DatabaseBackend


class _WebAPIConfig(Mapping):
    _alias = "webapi"

    host = Text()
    port = Integer(optional=True)


def create(configuration):
    options = muninn.config.parse(configuration.get("webapi", {}), _WebAPIConfig)
    _WebAPIConfig.validate(options)
    return WebAPIBackend(**options)


class WebAPIBackend(DatabaseBackend):
    def __init__(self, host="localhost", port=12345):
        connection = rpyc.connect(host, port)
        self._remote_backend = connection.root

    def initialize(self, namespace_schemas):
        pass

    def search(self, *args, **kwargs):
        return self._remote_backend.search(*args, **kwargs)

    def summary(self, *args, **kwargs):
        return self._remote_backend.summary(*args, **kwargs)

    def disconnect(self):
        pass
