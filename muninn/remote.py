#
# Copyright (C) 2014-2018 S[&]T, The Netherlands.
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

    def pull(self, archive, product):
        if getattr(product.core, "archive_path", None) is None:
            raise Error("cannot pull files that do not have archive_path set")

        # Determine the (absolute) path in the archive that will contain the product and create it if required.
        abs_archive_path = os.path.realpath(os.path.join(archive._root, product.core.archive_path))
        abs_product_path = os.path.join(abs_archive_path, product.core.physical_name)

        # Create destination location for product
        try:
            util.make_path(abs_archive_path)
        except EnvironmentError as _error:
            raise Error("cannot create parent destination path '%s' [%s]" % (abs_archive_path, _error))

        plugin = archive.product_type_plugin(product.core.product_type)

        # Create a temporary directory and download the product there, then move the product to its
        # destination within the archive.
        try:
            with util.TemporaryDirectory(prefix=".pull-", suffix="-%s" % product.core.uuid.hex,
                                         dir=abs_archive_path) as tmp_path:

                # Create enclosing directory if required.
                if plugin.use_enclosing_directory:
                    tmp_path = os.path.join(tmp_path, product.core.physical_name)
                    util.make_path(tmp_path)

                # Define a temp location and download the file
                tmp_file = os.path.join(tmp_path, product.core.physical_name)
                downloader = util.Downloader(product.core.remote_url, archive.auth_file())
                downloader.save(tmp_file)

                # TODO: implement extraction of downloaded archives
                # for ftp and file check if url ends with 'core.physical_name + <archive ext>'
                # for http/https check the header for the line:
                #    Content-Disposition: attachment; filename="**********"
                # end then use this ***** filename to match against core.physical_name + <archive ext>

                # Move the transferred product into its destination within the archive.
                if plugin.use_enclosing_directory:
                    os.rename(tmp_path, abs_product_path)
                else:
                    os.rename(tmp_file, abs_product_path)

        except EnvironmentError as _error:
            raise Error("unable to transfer product to destination path '%s' [%s]" %
                        (abs_product_path, _error))


REMOTE_BACKENDS = {
    'http': UrlBackend(prefix='http://'),
    'https': UrlBackend(prefix='https://'),
    'file': UrlBackend(prefix='file://'),
    'ftp': UrlBackend(prefix='ftp://'),
}


def pull(archive, product):
    # determine the backend to use
    backend = None
    url = product.core.remote_url
    for prot in archive.remote_backends():
        _backend = archive.remote_backend(prot)
        if _backend.indentify(url):
            backend = _backend
    if backend is None:
        raise Error("The protocol of '%s' is not supported" % url)

    backend.pull(archive, product)
