#
# Copyright (C) 2014-2024 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

__version__ = "6.4.1"
__copyright__ = "Copyright (C) 2014-2024 S[&]T, The Netherlands."

__all__ = ["Error", "InternalError", "Struct", "Archive", "open", "config_path"]

import os as _os
import sys as _sys

from muninn._compat import urlparse as _urlparse
from muninn.archive import Archive
from muninn.exceptions import *
from muninn.struct import Struct


def config_path():
    """Return the value of the `MUNINN_CONFIG_PATH` environment variable."""
    return _os.environ.get("MUNINN_CONFIG_PATH", "")


def open(id=None, **kwargs):
    """Open an archive for the given archive id, by searching for the
    corresponding configuration file in the locations found in the
    `MUNINN_CONFIG_PATH` environment variable.
    Alternatively, the id can be a path/url that points directly to a
    muninn archive configuration file (Python 3 only). This path/url
    should still end with a `.cfg` extension.

    Arguments:
    id  --  Archive id (name of configuration file) or path/url to a configuration file

    Returns:
    An instance of `muninn.archive.Archive`
    """
    if id is None:
        configuration = {}
    else:
        configuration = _read_archive_config_file(_locate_archive_config_file(id))
        if id.endswith(".cfg"):
            if "/" in id:
                id = id.rsplit("/", 1)[1]
            id = _os.path.splitext(id)[0]

    for name, value in kwargs.items():
        section = configuration.setdefault(name, {})
        section.update(value)

    return Archive.create(configuration, id)


def list_archives():
    """Search locations found in the 'MUNINN_CONFIG_PATH' environment variable
    and return names of all found archives."""
    result = []
    for path in filter(None, config_path().split(":")):
        if _os.path.isfile(path):
            result.append(_os.path.basename(path)[:-4])
        else:
            result.extend([f[:-4] for f in _os.listdir(path) if f.endswith('.cfg')])

    return result


def _read_archive_config_file(path):
    try:
        from ConfigParser import ConfigParser
    except ImportError:
        from configparser import ConfigParser

    parser = ConfigParser()

    if _urlparse(path).scheme != '':
        if _sys.version_info[0] < 3:
            raise NotImplementedError("retrieving archive configuration files via an url requires python 3")
        import urllib.request
        with urllib.request.urlopen(path, timeout=60) as response:
            parser.read_string(response.read().decode("utf-8"))
    else:
        if not parser.read(path):
            raise Error("unable to read config file: \"%s\"" % path)

    return dict([(name, dict(parser.items(name))) for name in parser.sections()])


def _locate_archive_config_file(archive_id):
    if archive_id.endswith(".cfg"):
        # the archive id is already a path/url to the config file
        return archive_id
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
