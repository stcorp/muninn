#
# Copyright (C) 2014-2017 S[&]T, The Netherlands.
#
from __future__ import absolute_import, division, print_function

import logging
import os

import muninn.util as util

from muninn.exceptions import Error


class UrlBackend(object):

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


class ECMWFBackend(object):
    """
    'ecmwfapi' urls are custom defined urls for retrieving data from ECMWF MARS.
    It uses the following format:

        ecmwfapi:<filename>?<query>

    where 'filename' should equal the core.physical_name metadata field and
    'query' is a '&' separated list of key/value pairs for the ECMWF MARS request.

    The backend will use the ecmwf-api-client library to retrieve the given product.
    Note that you either need a .ecmwfapirc file with a ECMWF KEY in your home directory or
    you need to set the ECMWF_API_KEY, ECMWF_API_URL, ECMWF_API_EMAIL environment variables.
    
    The interface supports both access to public datasets:
        https://software.ecmwf.int/wiki/display/WEBAPI/Access+ECMWF+Public+Datasets
    as well as direct MARS access:
        https://software.ecmwf.int/wiki/display/WEBAPI/Access+MARS
    
    If the 'query' contains a 'dataset' parameter then the public dataset interface will be used.
    Otherwise the direct MARS access will be used.
    """

    def pull(self, archive, product):
        from ecmwfapi import ECMWFDataServer, ECMWFService
        dataserver = ECMWFDataServer(log=logging.info)
        marsservice = ECMWFService("mars", log=logging.info)

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

        requests = []
        for order in product.core.remote_url.split('?')[1].split('&concatenate&'):
            request = {}
            for param in order.split('&'):
                key, value = param.split('=')
                request[key] = value
            requests.append(request)

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

                # Download product.
                tmp_file_combined = os.path.join(tmp_path, product.core.physical_name)
                tmp_file_download = os.path.join(tmp_path, "request.grib")
                combined_file = open(tmp_file_combined, "w")
                for request in requests:
                    if 'dataset' in request:
                        request['target'] = tmp_file_download
                        dataserver.retrieve(request)
                    else:
                        marsservice.execute(request, tmp_file_download)
                    result_file = open(tmp_file_download, "r")
                    combined_file.write(result_file.read())
                    result_file.close()
                    os.remove(tmp_file_download)
                combined_file.close()

                # Move the retrieved product into its destination within the archive.
                if plugin.use_enclosing_directory:
                    os.rename(tmp_path, abs_product_path)
                else:
                    os.rename(tmp_file_combined, abs_product_path)

        except EnvironmentError as _error:
            raise Error("unable to transfer product to destination path '%s' [%s]" %
                        (abs_product_path, _error))


PROTOCOL_BACKENDS = {
    'http': UrlBackend(),
    'https': UrlBackend(),
    'file': UrlBackend(),
    'ftp': UrlBackend(),
    'ecmwfapi': ECMWFBackend(),
}

# protocol prefixes that are supported by muninn remote backends
REMOTE_PREFIXES = {'http': 'http://', 'https': 'https://', 'file': 'file://', 'ftp': 'ftp://', 'ecmwfapi': 'ecmwfapi:'}


def determine_backend(url):
    for prot, prefix in REMOTE_PREFIXES.items():
        if url.startswith(prefix):
            return PROTOCOL_BACKENDS[prot]


def pull(archive, product):
    # determine the backend to use
    backend = determine_backend(product.core.remote_url)
    backend.pull(archive, product)
