#
# Copyright (C) 2014-2020 S[&]T, The Netherlands.
#
from __future__ import absolute_import, division, print_function

import logging
import os

import muninn.util as util

from muninn.exceptions import Error


class RemoteBackend(object):
    def __init__(self, prefix):
        self.prefix = prefix

    def indentify(self, url):
        result = False
        if self.prefix:
            result = url.startswith(self.prefix)
        return result


class UrlBackend(RemoteBackend):

    def pull(self, archive, product, use_enclosing_directory):
        if getattr(product.core, "archive_path", None) is None:
            raise Error("cannot pull files that do not have archive_path set")

        # Create a temporary directory and download the product there, then move the product to its
        # destination within the archive.
        with util.TemporaryDirectory(prefix=".pull-", suffix="-%s" % product.core.uuid.hex) as tmp_path:

            # Define a temp location and download the file
            tmp_file = os.path.join(tmp_path, product.core.physical_name)
            downloader = util.Downloader(product.core.remote_url, archive.auth_file())
            downloader.save(tmp_file)

            # TODO: implement extraction of downloaded archives
            # for ftp and file check if url ends with 'core.physical_name + <archive ext>'
            # for http/https check the header for the line:
            #    Content-Disposition: attachment; filename="**********"
            # end then use this ***** filename to match against core.physical_name + <archive ext>

            archive._storage.put([tmp_file], product, use_enclosing_directory)


REMOTE_BACKENDS = {
    'http': UrlBackend(prefix='http://'),
    'https': UrlBackend(prefix='https://'),
    'file': UrlBackend(prefix='file://'),
    'ftp': UrlBackend(prefix='ftp://'),
}


def pull(archive, product, use_enclosing_directory):
    # determine the backend to use
    backend = None
    url = product.core.remote_url
    for prot in archive.remote_backends():
        _backend = archive.remote_backend(prot)
        if _backend.indentify(url):
            backend = _backend
    if backend is None:
        raise Error("The protocol of '%s' is not supported" % url)

    backend.pull(archive, product, use_enclosing_directory)
