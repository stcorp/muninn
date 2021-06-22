#
# Copyright (C) 2014-2021 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

__version__ = "5.2"
__copyright__ = "Copyright (C) 2014-2021 S[&]T, The Netherlands."

__all__ = ["Error", "InternalError", "Struct", "Archive", "open", "config_path"]

import os as _os

from muninn.archive import Archive, create as _create_archive
from muninn.exceptions import *
from muninn.struct import Struct


def config_path():
    return _os.environ.get("MUNINN_CONFIG_PATH", "")


def open(id="", **kwargs):
    configuration = {} if not id else _read_archive_config_file(_locate_archive_config_file(id))
    _merge_configuration(configuration, kwargs)
    return _create_archive(configuration)


def _merge_configuration(configuration, other_configuration):
    for name, value in other_configuration.items():
        section = configuration.setdefault(name, {})
        section.update(value)


def _read_archive_config_file(path):
    try:
        from ConfigParser import ConfigParser
    except ImportError:
        from configparser import ConfigParser

    parser = ConfigParser()
    if not parser.read(path):
        raise Error("unable to read config file: \"%s\"" % path)
    return dict([(name, dict(parser.items(name))) for name in parser.sections()])


def _locate_archive_config_file(archive_id):
    config_file_name = "%s.cfg" % archive_id
    if _os.path.basename(config_file_name) != config_file_name:
        raise Error("invalid archive identifier: \"%s\"" % archive_id)

    for path in filter(None, config_path().split(":")):
        if _os.path.isfile(path):
            if _os.path.basename(path) == config_file_name:
                return path
        else:
            config_file_path = _os.path.join(path, config_file_name)
            if _os.path.isfile(config_file_path):
                return config_file_path

    raise Error("configuration file: \"%s\" not found on search path: \"%s\"" % (config_file_name, config_path()))
