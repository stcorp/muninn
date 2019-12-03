#!/usr/bin/env python3
try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser

import glob
import logging
import os
import sys
import tarfile
import unittest

import pytest

import boto3
import botocore
logging.getLogger("boto3").setLevel(logging.CRITICAL)

import swiftclient
logging.getLogger("swiftclient").setLevel(logging.CRITICAL)

sys.path.insert(0, '..')
import muninn
os.environ['MUNINN_CONFIG_PATH'] = '.'

import shutil


class BaseChecker(object):
    def __init__(self, storage):
        self.storage = storage
        self.parser = ConfigParser()
        self.parser.read('test.cfg')


class FSChecker(BaseChecker):
    def __init__(self, *args, **kwargs):
        super(FSChecker, self).__init__(*args, **kwargs)
        self.root = self.parser.get('fs', 'root')

    def exists(self, path, size=None):
        path = os.path.join(self.root, path)

        if not os.path.isfile(path):
            return False
        if size is not None and os.path.getsize(path) != size:
            return False

        return True


class S3Checker(BaseChecker):
    def __init__(self, *args, **kwargs):
        super(S3Checker, self).__init__(*args, **kwargs)

        self.root = self.parser.get('fs', 'root')

        self.bucket = self.parser.get('s3', 'bucket')
        host = self.parser.get('s3', 'host')
        port = self.parser.get('s3', 'port')
        access_key = self.parser.get('s3', 'access_key')
        secret_access_key = self.parser.get('s3', 'secret_access_key')

        self._resource = boto3.resource(
            service_name='s3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_access_key,
            endpoint_url='http://%s:%s' % (host, port),
        )

    def exists(self, path, size=None):
        try:
            self._resource.Object(self.bucket, path).load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False
            else:
                raise
        return True


class SwiftChecker(BaseChecker):
    def __init__(self, *args, **kwargs):
        super(SwiftChecker, self).__init__(*args, **kwargs)

        self.container = self.parser.get('swift', 'container')

        self._conn = swiftclient.Connection(
            user=self.parser.get('swift', 'user'),
            key=self.parser.get('swift', 'key'),
            authurl=self.parser.get('swift', 'authurl'),
        )

    def exists(self, path, size=None):
        try:
            obj = self._conn.get_object(self.container, path)
        except swiftclient.exceptions.ClientException as e:
            if e.http_status == 404:
                return False
            else:
                raise
        return True


STORAGE_CHECKERS = {
    'fs': FSChecker,
    's3': S3Checker,
    'swift': SwiftChecker,
}


# TODO merge fixtures into one with multiple parameters?

@pytest.fixture(params=['sqlite', 'postgresql'])  # TODO add postgresql
def database(request):
    return request.param


@pytest.fixture(params=['fs', 's3', 'swift'])
def storage(request):
    return request.param


@pytest.fixture(params=['', 'archive/path'])
def archive_path(request):
    return request.param


@pytest.fixture(params=[True, False])
def use_enclosing_directory(request):
    return request.param


@pytest.fixture
def archive(database, storage, use_enclosing_directory, archive_path):
    template = open('test.cfg.template', 'r').read()
    data = template.replace('{database}', database)
    data = data.replace('{storage}', storage)
    open('test.cfg', 'w').write(data)

    template = open('product_type.py.template', 'r').read()
    data = template.replace('{archive_path}', archive_path)
    data = data.replace('{use_enclosing_directory}', str(use_enclosing_directory))  # TODO jinja?
    open('product_type.py', 'w').write(data)

    # refresh product type
    if 'product_type' in sys.modules:
        del sys.modules['product_type']
    os.system('rm *.pyc -f')

    # create clean archive
    archive = muninn.open('test')
    archive.destroy()
    archive.prepare()

    # store params  # TODO this could be nicer
    archive._params = {
        'database': database,
        'storage': storage,
        'use_enclosing_directory': use_enclosing_directory,
        'archive_path':  archive_path,
    }
    archive._checker = STORAGE_CHECKERS[storage](storage)

    yield archive

    # close archive
    archive.close()


class TestArchive:
    def _ingest_file(self, archive, use_symlinks=False, intra=False):
        name = 'pi.txt'
        path = 'data/%s' % name
        size = os.path.getsize(path)

        if intra: # relative symlink within archive
            dirpath = os.path.join(
                archive._checker.root,
                'een/twee'
            )

            if not os.path.isdir(dirpath):
                os.makedirs(dirpath)
            shutil.copy(path, dirpath)
            path = os.path.join(dirpath, name)

        properties = archive.ingest(
            [path],
            verify_hash=True,
            use_symlinks=use_symlinks
        )

        path = os.path.join(archive._params['archive_path'], 'pi.txt')

        if archive._params['use_enclosing_directory']:
            path = os.path.join(path, 'pi.txt')

        assert archive._checker.exists(path, size)

        if use_symlinks and archive._params['storage'] == 'fs':
            source_path = os.path.join(archive._checker.root, path)
            assert os.path.islink(source_path)
            assert os.path.isfile(os.path.realpath(source_path))

            if intra:
                # TODO remove this, as os.path.realpath already resolves
                target_path = 'een/twee/pi.txt'
                dotdots = 0 # don't use relpath on purpose for comparison
                if archive._params['use_enclosing_directory']:
                    dotdots += 1
                if archive._params['archive_path']:
                    dotdots += 2
                for i in range(dotdots):
                    target_path = os.path.join('..', target_path)
                assert os.readlink(source_path) == target_path
            else:
                target_path = os.path.join(os.getcwd(), 'data/pi.txt')
                assert os.readlink(source_path) == target_path

        return properties

    def _ingest_dir(self, archive, use_symlinks=False, intra=False):
        paths = glob.glob('data/multi/*')
        sizes = [os.path.getsize(p) for p in paths]

        if not archive._params['use_enclosing_directory']:
            properties = None
            with pytest.raises(muninn.exceptions.Error) as excinfo:
                properties = archive.ingest(
                    paths,
                    verify_hash=True,
                    use_symlinks=use_symlinks
                )
            assert 'cannot determine physical name for multi-part product' in str(excinfo)
            return

        if intra: # relative symlinks within archive
            dirpath = os.path.join(
                archive._checker.root,
                'drie/multi'
            )

            if not os.path.isdir(dirpath):
                os.makedirs(dirpath)
            for path in paths:
                shutil.copy(path, dirpath)

            paths = [os.path.join(dirpath, os.path.basename(path)) for path in paths]

        properties = archive.ingest(
            paths,
            verify_hash=True,
            use_symlinks=use_symlinks
        )

        for (path, size) in zip(paths, sizes):
            archive._checker.exists(path, size)

        if use_symlinks and archive._params['storage'] == 'fs':
            for path in paths:
                source_path = os.path.join(
                    archive._checker.root,
                    archive._params['archive_path'],
                    'multi',
                    os.path.basename(path)
                )
                assert os.path.isfile(os.path.realpath(source_path))

                if intra:
                    # TODO remove this, as os.path.realpath already resolves
                    target_path = os.path.join('drie/multi', os.path.basename(path))
                    dotdots = 1 # enclosing
                    if archive._params['archive_path']:
                        dotdots += 2
                    for i in range(dotdots):
                        target_path = os.path.join('..', target_path)
                    assert os.readlink(source_path) == target_path

                else:
                    target_path = os.path.join(
                        os.getcwd(),
                        'data/multi',
                        os.path.basename(path)
                    )
                    assert os.readlink(source_path) == target_path

        return properties

    def _pull(self, archive):
        props = archive.ingest(['data/README'], ingest_product=False)
        size = os.path.getsize('data/README')

        metadata = {
            'remote_url': 'http://archive.debian.org/README',
        }

        archive.update_properties(muninn.Struct({'core': metadata}), props.core.uuid)

        archive.pull("", verify_hash=True)

        path = os.path.join(archive._params['archive_path'], 'README')
        if archive._params['use_enclosing_directory']:
            path = os.path.join(path, 'README')

        assert archive._checker.exists(path, size)

        return props

    def test_ingest_file(self, archive):
        # copy
        self._ingest_file(archive)

        archive.remove()

        # symlink
        if archive._params['storage'] == 'fs':
            self._ingest_file(archive, use_symlinks=True)
        else:
            with pytest.raises(muninn.exceptions.Error) as excinfo:
                self._ingest_file(archive, use_symlinks=True)
            assert 'storage backend does not support symlinks' in str(excinfo)

        archive.remove()

        # intra-archive symlink
        if archive._params['storage'] == 'fs':
            self._ingest_file(archive, use_symlinks=True, intra=True)

    def test_remove_file(self, archive):
        self._ingest_file(archive)
        archive.remove()

        path = os.path.join(archive._params['archive_path'], 'pi.txt')
        if archive._params['use_enclosing_directory']:
            path = os.path.join(path, 'pi.txt')

        assert not archive._checker.exists(path)

    def test_ingest_dir(self, archive):
        # copy
        self._ingest_dir(archive)

        archive.remove()

        # symlink
        if archive._params['storage'] == 'fs':
            self._ingest_dir(archive, use_symlinks=True)

        elif archive._params['use_enclosing_directory']:
            with pytest.raises(muninn.exceptions.Error) as excinfo:
                self._ingest_dir(archive, use_symlinks=True)
            assert 'storage backend does not support symlinks' in str(excinfo)

        archive.remove()

        # intra-archive symlinks
        if archive._params['storage'] == 'fs':
            self._ingest_dir(archive, use_symlinks=True, intra=True)

    def test_remove_dir(self, archive):
        if archive._params['use_enclosing_directory']:
            self._ingest_dir(archive)

            archive.remove()

            for name in ('1.txt', '2.txt'):
                path = os.path.join(archive._params['archive_path'], 'multi', name)
                assert not archive._checker.exists(path)

    def test_pull(self, archive):
        self._pull(archive)

        path = os.path.join(archive._params['archive_path'], 'README')
        if archive._params['use_enclosing_directory']:
            path = os.path.join(path, 'README')

        assert archive._checker.exists(path)

    def test_search(self, archive):
        self._ingest_file(archive)

        # search all
        s = archive.search()
        assert len(s) == 1
        properties = s[0]
        assert properties.core.physical_name == 'pi.txt'

        # search on product_name
        s = archive.search('product_name == "pi.txt"')
        assert len(s) == 1
        s = archive.search('product_name == "pr.txt"')
        assert len(s) == 0

    def test_tag(self, archive):
        properties = self._ingest_file(archive)

        archive.tag(properties.core.uuid, 'mytag')

        s = archive.search('has_tag("mytag")')
        assert len(s) == 1
        s = archive.search('has_tag("niks")')
        assert len(s) == 0

    def test_rebuild_properties_file(self, archive):
        properties = self._ingest_file(archive)
        archive.rebuild_properties(properties.core.uuid)

    def test_rebuild_properties_dir(self, archive):
        if archive._params['use_enclosing_directory']:
            properties = self._ingest_dir(archive)
            archive.rebuild_properties(properties.core.uuid)

    def test_retrieve_file(self, archive):
        self._ingest_file(archive)

        name = 'pi.txt'
        size = os.path.getsize('data/pi.txt')

        # copy
        with muninn.util.TemporaryDirectory() as tmp_path:
            archive.retrieve(target_path=tmp_path)

            path = os.path.join(tmp_path, name)
            assert os.path.isfile(path)
            assert os.path.getsize(path) == size

        # symlink
        if archive._params['storage'] == 'fs':
            with muninn.util.TemporaryDirectory() as tmp_path:
                archive.retrieve(target_path=tmp_path, use_symlinks=True)

                path = os.path.join(tmp_path, name)
                assert os.path.islink(path)

                target_path = os.path.join(
                    archive._checker.root,
                    archive._params['archive_path'],
                    'pi.txt'
                )
                if archive._params['use_enclosing_directory']:
                    target_path = os.path.join(target_path, 'pi.txt')

                assert os.path.isfile(target_path)
                assert os.readlink(path) == target_path
        else:
            with pytest.raises(muninn.exceptions.Error) as excinfo:
                with muninn.util.TemporaryDirectory() as tmp_path:
                    archive.retrieve(target_path=tmp_path, use_symlinks=True)
            assert 'storage backend does not support symlinks' in str(excinfo)

    def test_retrieve_dir(self, archive):
        if archive._params['use_enclosing_directory'] == True:
            self._ingest_dir(archive)

            # copy
            with muninn.util.TemporaryDirectory() as tmp_path:
                archive.retrieve(target_path=tmp_path)

                for name in ('1.txt', '2.txt'):
                    path = os.path.join(tmp_path, name)
                    assert os.path.isfile(path)
                    assert os.path.getsize(path) == os.path.getsize('data/multi/%s' % name)

            # symlink
            if archive._params['storage'] == 'fs':
                with muninn.util.TemporaryDirectory() as tmp_path:
                    archive.retrieve(target_path=tmp_path, use_symlinks=True)

                    for name in ('1.txt', '2.txt'):
                        path = os.path.join(tmp_path, name)
                        assert os.path.islink(path)

                        target_path = os.path.join(
                            archive._checker.root,
                            archive._params['archive_path'],
                            'multi',
                            name
                        )
                        assert os.path.isfile(target_path)
                        assert os.readlink(path) == target_path
            else:
                with pytest.raises(muninn.exceptions.Error) as excinfo:
                    with muninn.util.TemporaryDirectory() as tmp_path:
                        archive.retrieve(target_path=tmp_path, use_symlinks=True)
                assert 'storage backend does not support symlinks' in str(excinfo)

    def test_export_file(self, archive):
        if archive._params['use_enclosing_directory']:  # TODO plugin doesn't compress single files?
            self._ingest_file(archive)

            with muninn.util.TemporaryDirectory() as tmp_path:
                archive.export(format='tgz', target_path=tmp_path)

                tarfile_path = os.path.join(
                                   tmp_path,
                                   archive._params['archive_path'],
                                   'pi.txt.tgz'
                               )

                tf = tarfile.open(tarfile_path)
                assert tf.getmember('pi.txt/pi.txt').size == 1015

    def test_export_dir(self, archive):
        if archive._params['use_enclosing_directory']:
            self._ingest_dir(archive)

            with muninn.util.TemporaryDirectory() as tmp_path:
                archive.export(format='tgz', target_path=tmp_path)

                tarfile_path = os.path.join(
                                   tmp_path,
                                   archive._params['archive_path'],
                                   'multi.tgz'
                               )

                tf = tarfile.open(tarfile_path)
                assert tf.getmember('multi/1.txt').size == 209
                assert tf.getmember('multi/2.txt').size == 229

    def test_rebuild_properties_file(self, archive):
        properties = self._ingest_file(archive)
        archive.rebuild_properties(properties.core.uuid)

    def test_rebuild_properties_dir(self, archive):
        if archive._params['use_enclosing_directory']:
            properties = self._ingest_dir(archive)
            archive.rebuild_properties(properties.core.uuid)

    def test_rebuild_pull_properties(self, archive):
        properties = self._pull(archive)
        archive.rebuild_pull_properties(properties.core.uuid, verify_hash=True)
