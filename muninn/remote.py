#
# Copyright (C) 2014-2021 S[&]T, The Netherlands.
#
from __future__ import absolute_import, division, print_function

import logging
import os
import re
import json
import ftplib
import zipfile

import muninn.util as util

from muninn._compat import urlparse
from muninn.exceptions import Error


class DownloadError(Error):
    pass


def get_credentials(archive, url):
    auth_file = archive.auth_file()
    if auth_file is not None:
        credentials = json.loads(open(auth_file).read())
        for key in credentials:
            if url.startswith(key):
                return credentials[key]
        hostname = urlparse(url).hostname
        if hostname in credentials:
            return credentials[hostname]
    return None


def download_http_oath2(url, target_dir, credentials, timeout=60):
    import requests
    from requests_oauthlib import OAuth2Session
    from oauthlib.oauth2 import LegacyApplicationClient
    from oauthlib.oauth2.rfc6749 import tokens

    assert credentials['grand_type'] == "ResourceOwnerPasswordCredentialsGrant"

    session = OAuth2Session(client=LegacyApplicationClient(client_id=credentials['client_id']))
    token = session.fetch_token(token_url=credentials['token_url'], username=credentials['username'],
                                password=credentials['password'], client_id=credentials['client_id'],
                                client_secret=credentials['client_secret'])
    try:
        r = session.get(url, timeout=timeout)
        r.raise_for_status()
        local_file = os.path.join(target_dir, os.path.basename(urlparse(r.url).path))
        if "Content-Disposition" in r.headers.keys():
            matches = re.findall("filename=\"?([^\"]+)\"?", r.headers["Content-Disposition"])
            if len(matches) > 0:
                local_file = os.path.join(target_dir, matches[-1])
        with open(local_file, 'wb') as output:
            for block in r.iter_content(1048576):  # use 1MB blocks
                output.write(block)
    except Exception as e:
        raise DownloadError('Error downloading %s (Reason: %s)' % (url, e))
    return local_file


def download_http(url, target_dir, credentials=None, timeout=60):
    import requests
    auth = None
    if credentials is not None:
        auth = (credentials['username'], credentials['password'])
    try:
        r = requests.get(url, timeout=timeout, auth=auth)
        r.raise_for_status()
        local_file = os.path.join(target_dir, os.path.basename(urlparse(r.url).path))
        if "Content-Disposition" in r.headers.keys():
            matches = re.findall("filename=\"?([^\"]+)\"?", r.headers["Content-Disposition"])
            if len(matches) > 0:
                local_file = os.path.join(target_dir, matches[-1])
        with open(local_file, 'wb') as output:
            for block in r.iter_content(1048576):  # use 1MB blocks
                output.write(block)
    except Exception as e:
        raise DownloadError('Error downloading %s (Reason: %s)' % (url, e))
    return local_file


def download_ftp(url, target_dir, credentials=None, timeout=60):
    url = urlparse(url)
    if url.username:
        username = url.username
        password = url.password
    elif credentials is not None:
        username = credentials['username']
        password = credentials['password']
    else:
        username = 'anonymous'
        password = 'guest'
    local_file = os.path.join(target_dir, os.path.basename(url.path))
    try:
        ftp = ftplib.FTP(url.hostname, username, password, timeout=timeout)
        ftp.cwd(os.path.dirname(url.path))
        ftp.set_pasv(True)
        ftp.retrbinary('RETR %s' % os.path.basename(url.path), open(local_file, 'wb').write)
        ftp.quit()
    except Exception as e:
        raise DownloadError('Error downloading %s (Reason: %s)' % (url.geturl(), e))
    return local_file


class RemoteBackend(object):
    def __init__(self, prefix):
        self.prefix = prefix

    def indentify(self, url):
        result = False
        if self.prefix:
            result = url.startswith(self.prefix)
        return result
    
    def auto_extract(self, file_path, product):
        filename = os.path.basename(file_path)
        if filename == product.core.physical_name + ".zip" or filename == product.core.physical_name + ".ZIP":
            with zipfile.ZipFile(file_path, 'r') as ziparchive:
                ziparchive.extractall(os.path.dirname(file_path))
                paths = set([p.split('/', 1)[0] for p in ziparchive.namelist()])
                return list(paths)
        return [file_path]


class FileBackend(RemoteBackend):
    def pull(self, archive, product, target_dir):
        source_path = urlparse(product.core.remote_url).path
        target_path = os.path.join(target_dir, os.path.basename(source_path))
        util.copy_path(source_path, target_path)
        return self.auto_extract(target_path, product)


class HTTPBackend(RemoteBackend):
    def pull(self, archive, product, target_dir):
        credentials = get_credentials(archive, product.core.remote_url)
        if credentials and 'auth_type' in credentials and credentials['auth_type'] == "oauth2":
            file_path = download_http_oath2(product.core.remote_url, target_dir, credentials)
        else:
            file_path = download_http(product.core.remote_url, target_dir, credentials=credentials)
        return self.auto_extract(file_path, product)


class FTPBackend(RemoteBackend):
    def pull(self, archive, product, target_dir):
        credentials = get_credentials(archive, product.core.remote_url)
        file_path = download_ftp(product.core.remote_url, target_dir, credentials=credentials)
        return self.auto_extract(file_path, product)


REMOTE_BACKENDS = {
    'http': HTTPBackend(prefix='http://'),
    'https': HTTPBackend(prefix='https://'),
    'file': FileBackend(prefix='file://'),
    'ftp': FTPBackend(prefix='ftp://'),
}


def pull(archive, product, use_enclosing_directory):
    if getattr(product.core, "archive_path", None) is None:
        raise Error("cannot pull files that do not have archive_path set")

    # determine the backend to use
    backend = None
    url = product.core.remote_url
    for prot in archive.remote_backends():
        _backend = archive.remote_backend(prot)
        if _backend.indentify(url):
            backend = _backend
    if backend is None:
        raise Error("The protocol of '%s' is not supported" % url)

    def retrieve_files(target_dir):
        return backend.pull(archive, product, target_dir)

    archive._storage.put(None, product, use_enclosing_directory, use_symlinks=False, retrieve_files=retrieve_files)
