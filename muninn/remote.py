#
# Copyright (C) 2014-2022 S[&]T, The Netherlands.
#
from __future__ import absolute_import, division, print_function

import logging
import os
import re
import json
import ftplib
import tarfile
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
    from requests_oauthlib import OAuth2Session
    from oauthlib.oauth2 import LegacyApplicationClient

    assert credentials['grant_type'] == "ResourceOwnerPasswordCredentialsGrant"

    session = OAuth2Session(client=LegacyApplicationClient(client_id=credentials['client_id']))
    session.fetch_token(token_url=credentials['token_url'], username=credentials['username'],
                        password=credentials['password'], client_id=credentials['client_id'],
                        client_secret=credentials['client_secret'])
    try:
        r = session.get(url, timeout=timeout, stream=True)
        r.raise_for_status()
        local_file = os.path.join(target_dir, os.path.basename(urlparse(r.url).path))
        if "content-disposition" in [k.lower() for k in r.headers.keys()]:
            matches = re.findall("filename=\"?([^\"]+)\"?", r.headers["content-disposition"])
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
        r = requests.get(url, timeout=timeout, stream=True, auth=auth)
        r.raise_for_status()
        local_file = os.path.join(target_dir, os.path.basename(urlparse(r.url).path))
        if "content-disposition" in [k.lower() for k in r.headers.keys()]:
            matches = re.findall("filename=\"?([^\"]+)\"?", r.headers["content-disposition"])
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
        ftp = ftplib.FTP()
        ftp.connect(url.hostname, url.port or 21, timeout=timeout)
        ftp.login(username, password)
        ftp.cwd(os.path.dirname(url.path))
        ftp.set_pasv(True)
        ftp.retrbinary('RETR %s' % os.path.basename(url.path), open(local_file, 'wb').write)
        ftp.quit()
    except Exception as e:
        raise DownloadError('Error downloading %s (Reason: %s)' % (url.geturl(), e))
    return local_file


def download_sftp(url, target_dir, credentials=None, timeout=60):
    import paramiko
    logging.getLogger("paramiko").setLevel(logging.WARNING)
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
        transport = paramiko.Transport((url.hostname, url.port or 22))
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        sftp.get(url.path, local_file)
        sftp.close()
        transport.close()
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
        dirname = os.path.dirname(file_path)
        filename = os.path.basename(file_path)

        zip_extensions = [".zip"]
        zip_extensions += [extension.upper() for extension in zip_extensions]
        for extension in zip_extensions:
            if filename == product.core.physical_name + extension:
                with zipfile.ZipFile(file_path, 'r') as ziparchive:
                    ziparchive.extractall(dirname)
                    paths = set([path.split('/', 1)[0] for path in ziparchive.namelist()])
                    paths = [os.path.join(dirname, path) for path in sorted(paths)]
                util.remove_path(file_path)
                return paths

        tar_extensions = [".tar", ".tgz", ".tar.gz", ".txz", ".tar.xz", ".tbz", ".tb2", "tar.bz2"]
        tar_extensions += [extension.upper() for extension in tar_extensions]
        for extension in tar_extensions:
            if filename == product.core.physical_name + extension:
                with tarfile.open(file_path) as tararchive:
                    tararchive.extractall(dirname)
                    paths = set([path.split('/', 1)[0] for path in tararchive.getnames()])
                    paths = [os.path.join(dirname, path) for path in sorted(paths)]
                util.remove_path(file_path)
                return paths

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


class SFTPBackend(RemoteBackend):
    def pull(self, archive, product, target_dir):
        credentials = get_credentials(archive, product.core.remote_url)
        file_path = download_sftp(product.core.remote_url, target_dir, credentials=credentials)
        return self.auto_extract(file_path, product)


REMOTE_BACKENDS = {
    'http': HTTPBackend(prefix='http://'),
    'https': HTTPBackend(prefix='https://'),
    'file': FileBackend(prefix='file://'),
    'ftp': FTPBackend(prefix='ftp://'),
    'sftp': SFTPBackend(prefix='sftp://'),
}


def retrieve_function(archive, product, verify_hash_download):
    if getattr(product.core, 'archive_path', None) is None and getattr(product.core, 'remote_url', None) is None:
        raise Error("product '%s' (%s) not available" % (product.core.product_name, product.core.uuid))

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
        paths = backend.pull(archive, product, target_dir)

        # check that download matches stored hash
        stored_hash = getattr(product.core, 'hash', None)
        if verify_hash_download and stored_hash is not None:
            hash_type = archive._extract_hash_type(stored_hash)
            if hash_type is None:
                stored_hash = 'sha1:' + stored_hash
                hash_type = 'sha1'
            calc_hash = util.product_hash(paths, hash_type=hash_type)
            if calc_hash != stored_hash:
                raise DownloadError("hash mismatch when pulling product '%s' (%s)" %
                                    (product.core.product_name, product.core.uuid))

        return paths

    return retrieve_files
