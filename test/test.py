#!/usr/bin/env python3
import atexit
try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser

import datetime
import glob
import logging
import multiprocessing
import os
import signal
import subprocess
import sys
import tarfile
import time
import unittest
import uuid
import warnings

import pytest

import boto3
import botocore
logging.getLogger("boto3").setLevel(logging.CRITICAL)

warnings.filterwarnings('ignore', 'distutils version classes', DeprecationWarning, 'swiftclient.client')
import swiftclient
logging.getLogger("swiftclient").setLevel(logging.CRITICAL)

sys.path.insert(0, '..')
import muninn
os.environ['MUNINN_CONFIG_PATH'] = '.'

import shutil

from muninn.geometry import Polygon, LinearRing, Point
from muninn.extension import CascadeRule

CFG = ConfigParser()
CFG.read(u'test.cfg')

STORAGE_BACKENDS = [s.strip() for s in CFG.get('DEFAULT', 'storage').split(',')]
DATABASE_BACKENDS = [s.strip() for s in CFG.get('DEFAULT', 'database').split(',')]
REMOTE_BACKENDS = [s.strip() for s in CFG.get('DEFAULT', 'remote_backends').split(',')]
ARCHIVE_PATHS = [s.strip() for s in CFG.get('DEFAULT', 'archive_path').split(',')]
USE_ENCLOSING_DIR = [s.strip() == 'true' for s in CFG.get('DEFAULT', 'use_enclosing_dir').split(',')]

PY3 = sys.version_info[0] == 3
MY_DIR = os.path.dirname(__file__)
PARENT_DIR = os.path.dirname(MY_DIR)

# ignore warnings over which we have no control
pytestmark = pytest.mark.filterwarnings(
    'ignore:using or importing the ABCs:DeprecationWarning:paramiko',
    'ignore:the imp module is deprecated:DeprecationWarning:invoke',
    'ignore::cryptography.utils.CryptographyDeprecationWarning',
    'ignore:distutils version classes:DeprecationWarning:pg8000',
)


class BaseChecker(object):
    def __init__(self, storage):
        self.storage = storage
        self.parser = ConfigParser()
        self.parser.read(u'my_arch.cfg')


class NoneChecker(BaseChecker):
    def __init__(self, *args, **kwargs):
        super(NoneChecker, self).__init__(*args, **kwargs)


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
        port = self.parser.get('s3', 'port', fallback=None)
        access_key = self.parser.get('s3', 'access_key')
        secret_access_key = self.parser.get('s3', 'secret_access_key')

        endpoint_url = host
        if ':' not in host:
            if port == 443:
                endpoint_url = 'https://' + endpoint_url
            else:
                endpoint_url = 'http://' + endpoint_url
                if port is not None and port != 80:
                    endpoint_url += ':' + port
        elif port is not None:
            endpoint_url += ':' + port

        self._resource = boto3.resource(
            service_name='s3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_access_key,
            endpoint_url=endpoint_url,
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
    'none': NoneChecker,
    'fs': FSChecker,
    's3': S3Checker,
    'swift': SwiftChecker,
}


def _makedirs(path):
    if not os.path.exists(path):
        os.makedirs(path)


# this is needed as direct subprocess.Popen is unsafe (eg with database adapter creating threads)

@pytest.fixture(scope='session', autouse=True)
def multiprocessing_context():
    global ctx, manager
    ctx = multiprocessing.get_context('forkserver')
    ctx.set_forkserver_preload(['subprocess'])
    manager = ctx.Manager()


def do_popen(cmd, return_dict, join):
    proc = subprocess.Popen(
            cmd,
            shell=True,
            stdout=subprocess.PIPE if join else subprocess.DEVNULL,
            stderr=subprocess.PIPE if join else subprocess.DEVNULL,
    )

    if join:
        output, errs = proc.communicate()

        return_dict['stdout'] = output
        return_dict['stderr'] = errs
        return_dict['returncode'] = proc.returncode

    else:
        return_dict['pid'] = proc.pid  # TODO ugh! how to kill automatically when wrapper process dies..


def safe_popen(cmd, join=False):
    return_dict = manager.dict()

    proc = ctx.Process(target=do_popen, args=(cmd, return_dict, join))
    proc.start()

    if join:
        proc.join()
        return return_dict['returncode'], return_dict['stdout'], return_dict['stderr']
    else:
        time.sleep(1)
        return proc, return_dict['pid']


# TODO merge fixtures into one with multiple parameters?

@pytest.fixture(params=DATABASE_BACKENDS)
def database(request):
    return request.param


@pytest.fixture(params=[s for s in STORAGE_BACKENDS if s != 'none'])
def storage(request):
    return request.param

@pytest.fixture(params=[s for s in STORAGE_BACKENDS if s == 'none'])
def storage_pure(request):
    return request.param


@pytest.fixture(params=ARCHIVE_PATHS)
def archive_path(request):
    return request.param


@pytest.fixture(params=USE_ENCLOSING_DIR)
def use_enclosing_directory(request):
    return request.param


def _archive(database, storage, use_enclosing_directory, archive_path):
    database, _, database_options = database.partition(':')

    # create my_arch.cfg by combining my_arch.cfg.template and test.cfg
    template = open('my_arch.cfg.template', 'r').read()
    data = template.replace('{database}', database)
    data = data.replace('{storage}', storage)
    with open('my_arch.cfg', 'w') as f:
        f.write(data)
        section = None
        for line in open('test.cfg'):
            if line.startswith('['):
                section = line.strip()
            elif '=' in line and section in ('[sqlite]', '[postgresql]') and database_options:
                key, _, value = line.partition('=')
                key, value = key.strip(), value.strip()
                for option in database_options.split(';'):
                    opt_key, opt_value = option.split('=')
                    if opt_key == key:
                        line = '%s = %s\n' % (opt_key, opt_value)
            if section != '[DEFAULT]':
                f.write(line)

    # create product type extension from template
    template = open('product_type.py.template', 'r').read()
    data = template.replace('{archive_path}', archive_path)
    data = data.replace('{use_enclosing_directory}', str(use_enclosing_directory))  # TODO jinja?
    open('product_type.py', 'w').write(data)

    # refresh product type, hook extension
    if 'product_type' in sys.modules:
        del sys.modules['product_type']
    if 'hook_extension' in sys.modules:
        del sys.modules['hook_extension']
    for f in glob.glob('*.pyc'):
        os.remove(f)

    # create empty dirs (can't commit in git)
    _makedirs('data/multi/emptydir')
    _makedirs('data/multi/dir/emptydir')

    # create clean archive
    with muninn.open('my_arch') as archive:
        assert archive.id == 'my_arch'

        archive.destroy()
        archive.prepare()

        # store params  # TODO this could be nicer
        archive._params = {
            'database': database,
            'database_options': database_options,
            'storage': storage,
            'use_enclosing_directory': use_enclosing_directory,
            'archive_path':  archive_path,
        }
        archive._checker = STORAGE_CHECKERS[storage](storage)

        yield archive


@pytest.fixture
def archive(database, storage, use_enclosing_directory, archive_path):
    yield from _archive(database, storage, use_enclosing_directory, archive_path)

@pytest.fixture
def archive_pure(database, storage_pure, use_enclosing_directory, archive_path):
    yield from _archive(database, storage_pure, use_enclosing_directory, archive_path)


@pytest.fixture(params=REMOTE_BACKENDS, scope='session')
def remote_backend(request):
    proc = None

    if ':' in request.param:
        param, _, options = request.param.partition(':')
        for option in options.split(';'):
            opt_key, opt_value = option.split('=')
            if opt_key == 'port':
                port = int(opt_value)
    else:
        param = request.param

    if param == 'file':
        yield 'file://' + os.path.realpath('.')

    elif param == 'http':
        proc, subpid = safe_popen('exec python3 -m http.server %d' % port)
        time.sleep(1)
        yield 'http://localhost:%d' % port

    elif param == 'ftp':
        proc, subpid = safe_popen('exec python3 -m pyftpdlib -p %d' % port)
        time.sleep(1)
        yield 'ftp://localhost:%d' % port

    elif param == 'sftp':
        proc, subpid = safe_popen('exec sftpserver -p %d -k ./sftp_server_rsa.key' % port)
        time.sleep(1)
        yield 'sftp://someuser:somepassword@localhost:%d' % port

    else:
        assert False

    if proc is not None:
        proc.terminate()
        proc.join()
        os.kill(subpid, signal.SIGTERM)


class TestArchive:
    def _ingest_file(self, archive, use_symlinks=False, intra=False, name=None):
        name = name or 'pi.txt'
        path = 'data/%s' % name
        size = os.path.getsize(path)

        if intra:  # relative symlink within archive
            dirpath = os.path.join(
                archive._checker.root,
                'one/two'
            )

            _makedirs(dirpath)
            shutil.copy(path, dirpath)
            path = os.path.join(dirpath, name)

        properties = archive.ingest(
            [path],
            verify_hash=True,
            use_symlinks=use_symlinks
        )
        path = os.path.join(archive._params['archive_path'], name)

        if archive._params['use_enclosing_directory']:
            path = os.path.join(path, name)

        assert archive._checker.exists(path, size)

        if use_symlinks and archive._params['storage'] == 'fs':
            source_path = os.path.join(archive._checker.root, path)
            assert os.path.islink(source_path)
            assert os.path.isfile(os.path.realpath(source_path))

            if intra:
                # TODO remove this, as os.path.realpath already resolves
                target_path = 'one/two/' + name
                dotdots = 0  # don't use relpath on purpose for comparison
                if archive._params['use_enclosing_directory']:
                    dotdots += 1
                if archive._params['archive_path']:
                    dotdots += 2
                for i in range(dotdots):
                    target_path = os.path.join('..', target_path)
                assert os.readlink(source_path) == target_path
            else:
                target_path = os.path.join(os.getcwd(), 'data/' + name)
                assert os.readlink(source_path) == target_path

        return properties

    def _ingest_multi_file(self, archive, use_symlinks=False, intra=False):
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
            assert 'cannot ingest multi-file product without enclosing directory' in str(excinfo)
            return

        if intra:  # relative symlinks within archive
            dirpath = os.path.join(
                archive._checker.root,
                'three/multi'
            )

            _makedirs(dirpath)
            for path in paths:
                if os.path.isdir(path):
                    shutil.copytree(path, os.path.join(dirpath, os.path.basename(path)))
                else:
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
                assert os.path.exists(os.path.realpath(source_path))

                if intra:
                    # TODO remove this, as os.path.realpath already resolves
                    target_path = os.path.join('three/multi', os.path.basename(path))
                    dotdots = 1  # enclosing
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

    def _ingest_dir(self, archive, use_symlinks=False, intra=False):
        # TODO add nested sub-directory
        dir_ = 'data/dir'

        properties = archive.ingest(
            [dir_],
            verify_hash=True,
            use_symlinks=use_symlinks
        )

        for name in ('pi.txt', 'multi/1.txt', 'multi/2.txt'):
            size = os.path.getsize(os.path.join(dir_, name))

            path = os.path.join(archive._params['archive_path'], 'dir')
            if archive._params['use_enclosing_directory']:
                path = os.path.join(path, 'dir')
            path = os.path.join(path, name)

            assert archive._checker.exists(path, size)

        return properties

    def _pull(self, archive, remote_backend, extract=False):
        URL = os.path.join(remote_backend, 'data/README')
        if extract:
            URL += '.zip'

        props = archive.ingest(['data/README'], ingest_product=False)
        size = os.path.getsize('data/README')

        metadata = {
            'remote_url': URL
        }

        archive.update_properties(muninn.Struct({'core': metadata}), props.core.uuid)

        archive.pull("", verify_hash=True, verify_hash_download=True)

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
        archive.remove()

        # post hook/hash verification failure: check that active=True
        with pytest.raises(ZeroDivisionError):  # hook raises exception for pi2.txt
            self._ingest_file(archive, name='pi2.txt')
        s = archive.search()
        assert len(s) == 1
        assert s[0].core.product_name == 'pi2.txt'
        assert s[0].core.active is True

    def test_reingest(self, archive):
        if archive._params['storage'] == 'fs':
            # ingest once
            properties = archive.ingest(['data/pi.txt'])

            # force reingest
            product_path = archive.product_path(properties)
            properties = archive.ingest(['data/pi.txt'], force=True)
            assert os.path.exists(product_path)

            # reingest without force, should raise unique constraint error
            with pytest.raises(muninn.exceptions.Error) as excinfo:
                properties = archive.ingest(['data/pi.txt'])
            assert 'unique constraint' in str(excinfo).lower()

            # force reingest _from archive_ (should not remove product!)
            product_path = archive.product_path(properties)
            if archive._params['use_enclosing_directory']:
                product_path = os.path.join(product_path, 'pi.txt')

            properties = archive.ingest(product_path, force=True)
            assert os.path.exists(product_path)

    def test_remove_file(self, archive):
        path = os.path.join(archive._params['archive_path'], 'pi.txt')
        if archive._params['use_enclosing_directory']:
            path = os.path.join(path, 'pi.txt')

        for method in ('remove', 'remove_by_uuid'):
            props = self._ingest_file(archive)

            if method == 'remove':
                count = archive.remove()
            elif method == 'remove_by_uuid':
                count = archive.remove(props.core.uuid)
            assert count == 1

            assert not archive._checker.exists(path)

        with pytest.raises(muninn.exceptions.Error) as excinfo:
            archive.remove(uuid.uuid4())
        assert 'No product found' in str(excinfo)

    def test_ingest_multi_file(self, archive):
        # copy
        self._ingest_multi_file(archive)
        archive.remove()

        # symlink
        if archive._params['storage'] == 'fs':
            self._ingest_multi_file(archive, use_symlinks=True)

        elif archive._params['use_enclosing_directory']:
            with pytest.raises(muninn.exceptions.Error) as excinfo:
                self._ingest_multi_file(archive, use_symlinks=True)
            assert 'storage backend does not support symlinks' in str(excinfo)

        archive.remove()

        # intra-archive symlinks
        if archive._params['storage'] == 'fs':
            self._ingest_multi_file(archive, use_symlinks=True, intra=True)

    def test_remove_multi_file(self, archive):
        if archive._params['use_enclosing_directory']:
            self._ingest_multi_file(archive)

            archive.remove()

            for name in ('1.txt', '2.txt'):
                path = os.path.join(archive._params['archive_path'], 'multi', name)
                assert not archive._checker.exists(path)

    def test_ingest_dir(self, archive):
        # copy
        self._ingest_dir(archive)

        # TODO: fs: symlinks/intra

    def test_remove_dir(self, archive):
        self._ingest_dir(archive)

        archive.remove()

        for name in ('pi.txt', 'multi/1.txt', 'multi/2.txt'):  # TODO merge existence checks
            path = os.path.join(archive._params['archive_path'], 'dir')
            if archive._params['use_enclosing_directory']:
                path = os.path.join(path, 'dir')
            path = os.path.join(path, name)

            assert not archive._checker.exists(path)

    def test_pull(self, archive, remote_backend):
        # normal pull
        properties = self._pull(archive, remote_backend)

        # autoextract
        archive.remove()
        properties = self._pull(archive, remote_backend, extract=True)

        # failing hook should result in product still being active
        archive.strip('')
        archive.update_properties(muninn.Struct({'mynamespace2': {'counter': 27}}), properties.core.uuid)

        with pytest.raises(ZeroDivisionError):  # hook raises exception for counter==27
            archive.pull("", verify_hash=True, verify_hash_download=True)
        s = archive.search()
        assert len(s) == 1
        assert s[0].core.product_name == 'README'
        assert s[0].core.active is True

    def test_strip(self, archive):
        path = os.path.join(archive._params['archive_path'], 'pi.txt')
        if archive._params['use_enclosing_directory']:
            path = os.path.join(path, 'pi.txt')

        for method in ('strip', 'strip_where', 'strip_by_uuid'):
            # ingest
            archive.remove()
            properties = archive.ingest(['data/pi.txt'])
            assert properties.core.archive_path is not None

            # strip
            if method == 'strip':
                nstripped = archive.strip()
            elif method == 'strip_where':
                nstripped = archive.strip('product_name == "pi.txt"')
            elif method == 'strip_by_uuid':
                nstripped = archive.strip(properties.core.uuid)
            assert nstripped == 1

            # check
            properties = archive.retrieve_properties(properties.core.uuid)
            assert 'archive_path' not in properties.core
            assert 'archive_date' not in properties.core

            assert not archive._checker.exists(path)

        with pytest.raises(muninn.exceptions.Error) as excinfo:
            nstripped = archive.strip(uuid.uuid4())
        assert 'No product found' in str(excinfo)

    def test_attach(self, archive):
        path = os.path.join(archive._params['archive_path'], 'pi.txt')
        if archive._params['use_enclosing_directory']:
            path = os.path.join(path, 'pi.txt')

        # strip
        properties = archive.ingest(['data/pi.txt'])
        archive.strip()

        properties = archive.retrieve_properties(properties.core.uuid)
        assert 'archive_path' not in properties.core
        assert 'archive_date' not in properties.core

        assert not archive._checker.exists(path)

        # attach
        archive.attach(['data/pi.txt'], verify_hash=True, verify_hash_before=True)

        properties = archive.retrieve_properties(properties.core.uuid)
        assert 'archive_path' in properties.core
        assert 'archive_date' in properties.core

        # size/hash checks
        properties.core.size = 12
        archive.update_properties(properties)
        archive.strip()
        with pytest.raises(muninn.exceptions.Error) as excinfo:
            archive.attach(['data/pi.txt'])
        assert 'size mismatch' in str(excinfo)

        properties.core.hash = properties.core.hash+'x'
        archive.update_properties(properties)
        archive.strip()
        with pytest.raises(muninn.exceptions.Error) as excinfo:
            archive.attach(['data/pi.txt'], force=True, verify_hash_before=True)
        assert 'hash mismatch' in str(excinfo)

        # no data
        with pytest.raises(muninn.exceptions.Error) as excinfo:
            archive.attach([])
        assert 'nothing to attach' in str(excinfo)

    def test_search(self, archive):  # TODO move to TestQuery?
        properties = self._ingest_file(archive)
        uuid = properties.core.uuid

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

        # search on uuid
        s = archive.search('uuid == %s' % uuid)
        assert len(s) == 1
        s = archive.search('%s' % uuid)
        assert len(s) == 1

        # search on datetime
        s = archive.search('archive_date > 2001-01-01')
        assert len(s) == 1
        s = archive.search('archive_date <= 2001-01-01')
        assert len(s) == 0
        s = archive.search('archive_date > 0000-00-00')
        assert len(s) == 1
        s = archive.search('archive_date >= 9999-99-99')
        assert len(s) == 0
        with pytest.raises(muninn.exceptions.Error) as excinfo:
            s = archive.search('archive_date > 2000-00-00')
        assert 'invalid timestamp' in str(excinfo)

        # 'covers' search on datetime field
        s = archive.search('covers(%s, %s, core.archive_date, core.archive_date)' %
                           ((datetime.datetime.now() - datetime.timedelta(hours=24)).isoformat(),
                            datetime.datetime.now().isoformat()))
        assert len(s) == 1

        s = archive.search('covers(@start, @stop, core.archive_date, core.archive_date)',
                           parameters={'start': datetime.datetime.now() - datetime.timedelta(hours=24),
                                       'stop': datetime.datetime.now()})
        assert len(s) == 1

        # use uuid as boolean
        s = archive.search('%s and product_name == "pi.txt"' % uuid)
        assert len(s) == 1

        # limit
        archive.ingest(['data/a.txt'])
        for x in range(4):
            s = archive.search(limit=x)
            assert len(s) == min(x, 2)

    def test_tags(self, archive):
        properties = self._ingest_file(archive)
        uuid = properties.core.uuid

        archive.tag(uuid, ['green', 'blue'])

        s = archive.search('has_tag("green")')  # TODO move to TestQuery?
        assert len(s) == 1
        s = archive.search('has_tag("yellow")')
        assert len(s) == 0

        archive.tag(uuid, 'yellow')
        tags = archive.tags(uuid)
        assert set(tags) == set(['green', 'blue', 'yellow'])

        archive.untag(uuid, ['blue', 'yellow'])
        archive.untag(uuid, 'blue')
        tags = archive.tags(uuid)
        assert tags == ['green']

        # untag all by default?
        archive.untag(uuid)
        tags = archive.tags(uuid)
        assert tags == []

    def test_linking(self, archive):
        uuid_a = archive.ingest(['data/a.txt']).core.uuid
        uuid_b = archive.ingest(['data/b.txt']).core.uuid
        uuid_c = archive.ingest(['data/c.txt']).core.uuid

        archive.link(uuid_b, [uuid_a])
        archive.link(uuid_c, [uuid_a, uuid_b])

        uuids = archive.derived_products(uuid_a)
        assert len(uuids) == 2
        uuids = archive.derived_products(uuid_b)
        assert len(uuids) == 1
        uuids = archive.derived_products(uuid_c)
        assert len(uuids) == 0

        archive.unlink(uuid_c, uuid_b)
        uuids = archive.derived_products(uuid_a)
        assert len(uuids) == 2
        uuids = archive.derived_products(uuid_b)
        assert len(uuids) == 0
        uuids = archive.derived_products(uuid_c)
        assert len(uuids) == 0

        archive.unlink(uuid_c, uuid_a)
        uuids = archive.derived_products(uuid_a)
        assert len(uuids) == 1
        uuids = archive.derived_products(uuid_b)
        assert len(uuids) == 0
        uuids = archive.derived_products(uuid_c)
        assert len(uuids) == 0

    def test_cascade(self, archive):
        plugin = archive._product_type_plugins['MY_TYPE']
        try:
            for cascade_rule in (CascadeRule.CASCADE, CascadeRule.STRIP):
                archive.remove()

                plugin.cascade_rule = cascade_rule

                uuid_a = archive.ingest(['data/a.txt']).core.uuid
                uuid_b = archive.ingest(['data/b.txt']).core.uuid
                uuid_c = archive.ingest(['data/c.txt']).core.uuid
                uuid_pi = archive.ingest(['data/pi.txt']).core.uuid

                time.sleep(2) # grace period # TODO use >= 0 in sql instead of > 0?

                archive.link(uuid_pi, [uuid_pi])  # TODO otherwise no source, so deleted?

                archive.link(uuid_b, [uuid_a])
                archive.link(uuid_c, [uuid_b])

                # also remove derived products b and c
                archive.remove(uuid_a)

                if cascade_rule == CascadeRule.CASCADE:
                    assert len(archive.search()) == 1
                else:
                    assert len(archive.search()) == 3
                    for uuid in uuid_b, uuid_c:
                        properties = archive.retrieve_properties(uuid)
                        assert not hasattr(properties.core, 'archive_path')
        finally:
            plugin.cascade_rule = CascadeRule.IGNORE

    def test_retrieve_file(self, archive):
        props = self._ingest_file(archive)

        name = 'pi.txt'
        size = os.path.getsize('data/pi.txt')

        # copy
        with muninn.util.TemporaryDirectory() as tmp_path:
            archive.retrieve(target_path=tmp_path)

            path = os.path.join(tmp_path, name)
            assert os.path.isfile(path)
            assert os.path.getsize(path) == size

            archive.retrieve(props.core.uuid, target_path=tmp_path)

            with pytest.raises(muninn.exceptions.Error) as excinfo:
                archive.retrieve(uuid.uuid4())
            assert 'No product found' in str(excinfo)

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

                target_path = os.path.realpath(target_path)

                assert os.path.isfile(target_path)
                assert os.readlink(path) == target_path
        else:
            with pytest.raises(muninn.exceptions.Error) as excinfo:
                with muninn.util.TemporaryDirectory() as tmp_path:
                    archive.retrieve(target_path=tmp_path, use_symlinks=True)
            assert 'storage backend does not support symlinks' in str(excinfo)

    def test_retrieve_multi_file(self, archive):
        if archive._params['use_enclosing_directory'] is True:
            self._ingest_multi_file(archive)

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

                        target_path = os.path.realpath(target_path)

                        assert os.path.isfile(target_path)
                        assert os.readlink(path) == target_path
            else:
                with pytest.raises(muninn.exceptions.Error) as excinfo:
                    with muninn.util.TemporaryDirectory() as tmp_path:
                        archive.retrieve(target_path=tmp_path, use_symlinks=True)
                assert 'storage backend does not support symlinks' in str(excinfo)

    def test_retrieve_dir(self, archive):  # TODO fs: symlink/intra
        self._ingest_dir(archive)

        with muninn.util.TemporaryDirectory() as tmp_path:
            archive.retrieve(target_path=tmp_path)

            for name in ('dir/pi.txt', 'dir/multi/1.txt', 'dir/multi/2.txt'):
                path = os.path.join(tmp_path, name)

                assert os.path.isfile(path)
                assert os.path.getsize(path) == os.path.getsize(os.path.join('data', name))

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

    def test_export_multi_file(self, archive):
        if archive._params['use_enclosing_directory']:
            self._ingest_multi_file(archive)

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

    def test_export_default(self, archive):
        if archive._params['use_enclosing_directory']:
            self._ingest_file(archive)

            with muninn.util.TemporaryDirectory() as tmp_path:
                # no format passed, and plugin has no 'export' method: use default (retrieve)!
                archive.export(target_path=tmp_path)
                path = os.path.join(
                           tmp_path,
                           'pi.txt'  # TODO does not add archive path?
                       )

                assert os.path.exists(path)
                assert os.path.getsize(path) == 1015

    def test_delete_properties(self, archive):
        product = archive.ingest(['data/a.txt'])
        assert archive.delete_properties("") == 1

        product = archive.ingest(['data/b.txt'])
        assert archive.delete_properties(product.core.uuid) == 1

        product = archive.ingest(['data/c.txt'])
        assert archive.delete_properties(product) == 1

        product = archive.ingest(['data/pi.txt'])
        assert archive.delete_properties([product]) == 1

        product = archive.ingest(['data/README'])
        assert archive.delete_properties([product.core.uuid]) == 1

        with pytest.raises(muninn.exceptions.Error) as excinfo:
            archive.delete_properties(14)
        assert 'Invalid' in str(excinfo)

        with pytest.raises(muninn.exceptions.Error) as excinfo:
            archive.delete_properties([""])
        assert 'Invalid' in str(excinfo)

    def test_rebuild_properties_file(self, archive):
        properties = self._ingest_file(archive)

        oldpath = os.path.join(
            archive._params['archive_path'],
            'pi.txt'
        )
        if archive._params['use_enclosing_directory']:
            oldpath = os.path.join(oldpath, 'pi.txt')

        assert archive._checker.exists(oldpath)

        sys.modules['product_type'].ARCHIVE_PATH = 'bicycle'
        archive.rebuild_properties(properties.core.uuid)

        assert not archive._checker.exists(oldpath)

        properties = archive.retrieve_properties(properties.core.uuid)
        assert properties.core.archive_path == 'bicycle'

        path = 'bicycle/pi.txt'
        if archive._params['use_enclosing_directory']:
            path = os.path.join(path, 'pi.txt')
        assert archive._checker.exists(path)

    def test_rebuild_properties_multi_file(self, archive):  # TODO would rebuild_properties_dir add anything?
        names = ['1.txt', '2.txt']

        if archive._params['use_enclosing_directory']:
            properties = self._ingest_multi_file(archive)

            for name in names:
                oldpath = os.path.join(
                    archive._params['archive_path'],
                    'multi',
                    name
                )
                assert archive._checker.exists(oldpath)

            sys.modules['product_type'].ARCHIVE_PATH = 'bicycle'
            archive.rebuild_properties(properties.core.uuid)

            for name in names:
                path = os.path.join(
                    'bicycle/multi',
                    name
                )
                assert archive._checker.exists(path)

            for name in names:
                oldpath = os.path.join(
                    archive._params['archive_path'],
                    'multi',
                    name
                )
                assert not archive._checker.exists(oldpath)

    def test_rebuild_properties_hash(self, archive):
        plugin = archive._product_type_plugins['MY_TYPE']

        try:
            # ingest product: hash_type is set to 'md5'
            product = self._ingest_file(archive)
            assert product.core.hash.startswith('md5:')

            # plugin.hash_type changed, so hash should change on rebuild_properties
            archive._product_type_plugins['MY_TYPE'].hash_type = 'sha1'
            archive.rebuild_properties(product.core.uuid)
            product = archive.retrieve_properties(product.core.uuid)
            assert product.core.hash.startswith('sha1:')

            # remove stored hash, so rebuild_properties should rebuild it
            product.core.hash = None
            archive.update_properties(product)
            archive.rebuild_properties(product.core.uuid)
            product = archive.retrieve_properties(product.core.uuid)
            assert product.core.hash.startswith('sha1:')

            # upgrade case: stored hash without prefix, so add it
            product.core.hash = product.core.hash[len('sha1:'):]
            archive.update_properties(product)
            archive.rebuild_properties(product.core.uuid)
            product = archive.retrieve_properties(product.core.uuid)
            assert product.core.hash.startswith('sha1:')

            # hashing was disabled, so remove hash
            archive._product_type_plugins['MY_TYPE'].hash_type = None
            archive.rebuild_properties(product.core.uuid)
            product = archive.retrieve_properties(product.core.uuid)
            assert 'hash' not in product.core

        finally:
            plugin.hash_type = 'md5'

    def test_update_properties(self, archive):
        product = self._ingest_file(archive)

        for create_namespaces in (True, False):
            props = muninn.Struct({'mynamespace': {'myjson': {'bla': 14, 'mylist': [1,2,3]}}})
            archive.update_properties(props, product.core.uuid, create_namespaces)

            props = archive.retrieve_properties(uuid=product.core.uuid, namespaces=['mynamespace'])
            myjson = props.mynamespace.myjson

            assert isinstance(myjson, dict)
            assert myjson['bla'] == 14
            assert myjson['mylist'] == [1,2,3]

    def test_verify_hash(self, archive):
        product = self._ingest_file(archive)

        # normal case: hash contains prefix
        failed = archive.verify_hash()
        assert len(failed) == 0

        # upgrade case: no hash prefix, so fail on md5 vs sha1
        product.core.hash = product.core.hash[len('md5:'):]
        archive.update_properties(product)
        failed = archive.verify_hash()
        assert len(failed) == 1

    def test_rebuild_pull_properties(self, archive, remote_backend):
        properties = self._pull(archive, remote_backend)
        archive.rebuild_pull_properties(properties.core.uuid, verify_hash=True)

    def test_summary(self, archive):
        product1 = archive.ingest(['data/a.txt'])
        year = product1.core.archive_date.year
        time.sleep(1)  # different dates
        product2 = archive.ingest(['data/b.txt'])

        # default summary (count all)
        data, headers = archive.summary()
        assert headers == ['count']
        assert data == [(2,)] or data == [[2]]  # TODO pg8000 gives list per row??

        # aggregate size.sum
        data, headers = archive.summary('size > 0', aggregates=['size.sum'])
        assert headers == ['count', 'core.size.sum']
        assert data == [(2, 2030)] or data == [[2, 2030]]  # TODO pg8000

        # non-core namespace
        data, headers = archive.summary(aggregates=['mynamespace2.counter.avg'])
        assert headers == ['count', 'mynamespace2.counter.avg']
        assert data == [(2, 28,)] or data == [[2, 28]]  # TODO pg8000

        # group by archive date plus subscript
        for subscript in [
            'year',
            'month',
            'yearmonth',
            'date',
            'day',
            'hour',
            'minute',
            'second',
            'time',
        ]:
            data, headers = archive.summary(group_by=['core.archive_date.' + subscript])

            if subscript == 'year':
                assert headers == ['core.archive_date.year', 'count']
                assert data == [(str(year), 2)] or data == [[str(year), 2]]

            elif subscript == 'second':
                assert headers == ['core.archive_date.second', 'count']
                assert len(data) == 2
                assert data[0][0] != data[1][0]
                assert data[0][1] == data[1][1] == 1

        data, headers = archive.summary(group_by=['core.product_type.length'])
        assert data == [(7, 2)] or data == [[7, 2]]  # TODO pg8000

        # order by
        data, headers = archive.summary(group_by=['archive_date.second'], order_by=['archive_date.second'])
        s1, s2 = int(data[0][0]), int(data[1][0])
        if s1 == 59:
            s2 += 60
        assert s1 < s2

        data, headers = archive.summary(group_by=['core.archive_date.second'], order_by=['-core.archive_date.second'])
        s1, s2 = int(data[0][0]), int(data[1][0])
        if s2 == 59:
            s1 += 60
        assert s1 > s2

        # having
        for having in ('archive_date.min', 'core.archive_date.min'):
            data, headers = archive.summary(group_by=['core.product_name'],
                                            aggregates=['core.archive_date.min', 'core.archive_date.max'],
                                            where='size > 0',
                                            order_by=['core.product_name'],
                                            having='%s >= 2020-02-02 and count >= 1' % having,
                                           )
            assert len(data) == 2

    def test_broken_plugin(self, archive):
        product = self._ingest_file(archive)
        product.core.product_type = 'broken'
        archive.update_properties(product)

        archive.remove(product)
        assert len(archive.search()) == 0

        archive.create_properties(product)
        assert len(archive.search()) == 1

    def test_sort(self, archive):
        def _create_product(name):
            product = muninn.struct.Struct()
            product.core = muninn.struct.Struct()
            product.core.uuid = uuid.uuid4()
            product.core.active = True
            product.core.product_type = 'foo'
            product.core.product_name = name
            product.core.physical_name = name
            archive.create_properties(product)
        _create_product('abc')
        _create_product('ABC')
        _create_product('A_AA')
        for prop in ('core.product_name', 'product_name'):
            s = archive.search(order_by=[prop])
            names = [p.core.product_name for p in s]
            assert names == ['ABC', 'A_AA', 'abc']


class TestArchivePureCatalogue:  # TODO merge with TestArchive?
    def test_ingest(self, archive_pure):
        properties = archive_pure.ingest(
            ['data/a.txt'],
        )
        assert properties.core.archive_path is None
        assert properties.core.remote_url == 'file://' + os.path.realpath('data/a.txt')
        assert len(archive_pure.search()) == 1

        assert archive_pure.product_path(properties).startswith('file://')

    def test_remove(self, archive_pure):
        properties = archive_pure.ingest(
            ['data/a.txt'],
        )
        archive_pure.remove(properties)
        assert len(archive_pure.search()) == 0

    def test_retrieve(self, archive_pure):
        properties = archive_pure.ingest(
            ['data/a.txt'],
        )
        with muninn.util.TemporaryDirectory() as tmp_path:
            archive_pure.retrieve(target_path=tmp_path)
            assert os.listdir(tmp_path) == ['a.txt']

    def test_retrieve_multi(self, archive_pure):
        if archive_pure._params['use_enclosing_directory']:  # TODO fix archive to accept?
            properties = archive_pure.ingest(
                ['data/dir/multi/1.txt', 'data/dir/multi/2.txt'],  # TODO does this cause other files under multi to be included?
            )
            with muninn.util.TemporaryDirectory() as tmp_path:
                archive_pure.retrieve(target_path=tmp_path)
                assert set(os.listdir(os.path.join(tmp_path, 'multi'))) == set(['1.txt', '2.txt'])

    def test_rebuild_properties(self, archive_pure):
        properties = archive_pure.ingest(
            ['data/a.txt'],
        )
        archive_pure.rebuild_properties(properties.core.uuid)  # TODO just pass properties?

    def test_export(self, archive_pure, remote_backend):
        properties = archive_pure.ingest(
            ['data/a.txt'],
        )

        remote_url = os.path.join(remote_backend, 'data/a.txt')
        archive_pure.update_properties(muninn.Struct({'core': {'remote_url': remote_url}}), properties.core.uuid, True)

        with muninn.util.TemporaryDirectory() as tmp_path:
            archive_pure.export(target_path=tmp_path)
            assert os.listdir(tmp_path) == ['a.txt']

        with muninn.util.TemporaryDirectory() as tmp_path:
            archive_pure.export(target_path=tmp_path, format='tgz')
            assert os.listdir(tmp_path) == ['a.txt.tgz']

    def test_verify_hash(self, archive_pure, remote_backend):
        properties = archive_pure.ingest(
            ['data/a.txt'],
        )

        remote_url = os.path.join(remote_backend, 'data/a.txt')
        archive_pure.update_properties(muninn.Struct({'core': {'remote_url': remote_url}}), properties.core.uuid, True)

        archive_pure.verify_hash()

    def test_errors(self, archive_pure):
        with pytest.raises(muninn.exceptions.Error) as excinfo:
            s = archive_pure.pull()
        assert '"pull" operation not available for storage=none' in str(excinfo)

        with pytest.raises(muninn.exceptions.Error) as excinfo:
            s = archive_pure.rebuild_pull_properties(None)
        assert '"rebuild_pull_properties" operation not available for storage=none' in str(excinfo)

        with pytest.raises(muninn.exceptions.Error) as excinfo:
            s = archive_pure.attach(None)
        assert '"attach" operation not available for storage=none' in str(excinfo)

        with pytest.raises(muninn.exceptions.Error) as excinfo:
            s = archive_pure.strip()
        assert '"strip" operation not available for storage=none' in str(excinfo)

class TestQuery:
    def _prep_data(self, archive):
        self.uuid_a = archive.ingest(['data/a.txt']).core.uuid
        self.uuid_b = archive.ingest(['data/b.txt']).core.uuid
        self.uuid_c = archive.ingest(['data/c.txt']).core.uuid

        archive.update_properties(muninn.Struct({'mynamespace': {'hello': 'hohoho'}}), self.uuid_a, True)
        archive.update_properties(muninn.Struct({'mynamespace': {'hello': 'hohoho'}}), self.uuid_b, True)

        polygon = Polygon([LinearRing([Point(0, 0), Point(4, 0), Point(4, 4), Point(0, 4)])])
        archive.update_properties(muninn.Struct({'core': {'footprint': polygon}}), self.uuid_c, True)

        archive.link(self.uuid_b, [self.uuid_a])
        archive.link(self.uuid_c, [self.uuid_a, self.uuid_b])

    def test_is_derived_from(self, archive):
        self._prep_data(archive)

        uuids = archive.derived_products(self.uuid_a)
        assert len(uuids) == 2

        for (count, name, uuid) in [
            (2, 'a.txt', self.uuid_a),
            (1, 'b.txt', self.uuid_b),
            (0, 'c.txt', self.uuid_c),
        ]:
            s = archive.search('is_derived_from(%s)' % uuid)
            assert len(s) == count
            s = archive.search('not is_derived_from(%s)' % uuid)
            assert len(s) == 3-count
            s = archive.search('is_derived_from(uuid==%s)' % uuid)
            assert len(s) == count
            s = archive.search('not is_derived_from(uuid==%s)' % uuid)
            assert len(s) == 3-count
            s = archive.search('is_derived_from(physical_name==\"%s\")' % name)
            assert len(s) == count
            s = archive.search('not is_derived_from(physical_name==\"%s\")' % name)
            assert len(s) == 3-count

        s = archive.search('is_derived_from(is_derived_from(physical_name==\"a.txt\"))')
        assert len(s) == 1
        assert s[0].core.uuid == self.uuid_c
        s = archive.search('not is_derived_from(is_derived_from(physical_name==\"a.txt\"))')
        assert len(s) == 2

        s = archive.search('is_derived_from(physical_name==\"a.txt\") or ' +
                           'is_derived_from(is_derived_from(physical_name==\"a.txt\"))')
        assert len(s) == 2
        s = archive.search('not (is_derived_from(physical_name==\"a.txt\") or ' +
                           'is_derived_from(is_derived_from(physical_name==\"a.txt\")))')
        assert len(s) == 1

    def test_is_source_of(self, archive):
        self._prep_data(archive)

        uuids = archive.source_products(self.uuid_b)
        assert len(uuids) == 1

        c = archive.count('is_source_of(%s)' % self.uuid_a)
        assert c == 0
        c = archive.count('is_source_of(%s)' % self.uuid_b)
        assert c == 1
        c = archive.count('is_source_of(%s)' % self.uuid_c)
        assert c == 2

        c = archive.count('is_source_of(physical_name==\"c.txt\")')
        assert c == 2
        c = archive.count('not is_source_of(physical_name==\"c.txt\")')
        assert c == 1

        c = archive.count('not is_source_of(%s)' % self.uuid_b)
        assert c == 2

    def test_namespaces(self, archive):
        self._prep_data(archive)

        s = archive.search('mynamespace.hello==\"hiya\"')
        assert len(s) == 0
        s = archive.search('\"hiya\"==mynamespace.hello')
        assert len(s) == 0
        s = archive.search('not mynamespace.hello==\"hiya\"')  # TODO move to logic operator tests
        assert len(s) == 3
        s = archive.search('mynamespace.hello!=\"hiya\"')
        assert len(s) == 3
        s = archive.search('not mynamespace.hello!=\"hiya\"')  # TODO move to logic operator tests, etc.
        assert len(s) == 0

        s = archive.search('size!=0')  # TODO to better place
        assert len(s) == 3
        s = archive.search('0<size')
        assert len(s) == 3
        s = archive.search('size==size')
        assert len(s) == 3
        s = archive.search('size!=size')
        assert len(s) == 0
        s = archive.search('size>size')
        assert len(s) == 0
        s = archive.search('7==7')
        assert len(s) == 3
        s = archive.search('7!=7')
        assert len(s) == 0
        s = archive.search('\"test\" == \"test\"')
        assert len(s) == 3
        s = archive.search('\"test\" != \"test\"')
        assert len(s) == 0

        s = archive.search('mynamespace.hello==\"hohoho\"')
        assert len(s) == 2
        s = archive.search('not mynamespace.hello==\"hohoho\"')
        assert len(s) == 1
        s = archive.search('mynamespace.hello!=\"hohoho\"')
        assert len(s) == 1
        s = archive.search('not mynamespace.hello!=\"hohoho\"')
        assert len(s) == 2
        s = archive.search('not \"hohoho\"!=mynamespace.hello')
        assert len(s) == 2

        s = archive.search('mynamespace.hello~=\"hi%\"')
        assert len(s) == 0
        s = archive.search('not mynamespace.hello~=\"hi%\"')
        assert len(s) == 3

        s = archive.search('mynamespace.hello~=\"ho%\"')
        assert len(s) == 2
        s = archive.search('not mynamespace.hello~=\"ho%\"')
        assert len(s) == 1

        s = archive.search('is_derived_from(mynamespace.hello==\"hohoho\")')
        assert len(s) == 2

        s = archive.search('is_derived_from(physical_name==\"a.txt\") and mynamespace.hello==\"hohoho\"')
        assert len(s) == 1

    def test_is_defined(self, archive):
        self._prep_data(archive)

        # namespace.property
        s = archive.search('is_defined(core.physical_name)')
        assert len(s) == 3
        s = archive.search('not is_defined(core.physical_name)')
        assert len(s) == 0
        s = archive.search('is_defined(mynamespace.hello)')
        assert len(s) == 2
        s = archive.search('not is_defined(mynamespace.hello)')
        assert len(s) == 1

        # namespace/core property
        s = archive.search('is_defined(core)')
        assert len(s) == 3
        s = archive.search('is_defined(mynamespace)')
        assert len(s) == 2
        s = archive.search('is_defined(physical_name)')
        assert len(s) == 3

    def test_in(self, archive):
        self._prep_data(archive)

        s = archive.search('size in [1015]')
        assert len(s) == 3
        s = archive.search('not size in [1015]')
        assert len(s) == 0
        s = archive.search('size not in [1015]')
        assert len(s) == 0

        s = archive.search('size in [1016]')
        assert len(s) == 0
        s = archive.search('not size in [1016]')
        assert len(s) == 3
        s = archive.search('size not in [1016]')
        assert len(s) == 3

        s = archive.search('size in [1015, 1016]')
        assert len(s) == 3
        s = archive.search('not size in [1015, 1016]')
        assert len(s) == 0
        s = archive.search('size not in [1015, 1016]')
        assert len(s) == 0

        s = archive.search('size in [1016, 1017]')
        assert len(s) == 0
        s = archive.search('not size in [1016, 1017]')
        assert len(s) == 3
        s = archive.search('size not in [1016, 1017]')
        assert len(s) == 3

        s = archive.search('physical_name in ["a.txt"]')
        assert len(s) == 1
        s = archive.search('not physical_name in ["a.txt"]')
        assert len(s) == 2
        s = archive.search('physical_name not in ["a.txt"]')
        assert len(s) == 2

        s = archive.search('physical_name in ["b.txt", "c.txt"]')
        assert len(s) == 2
        s = archive.search('not physical_name in ["b.txt", "c.txt"]')
        assert len(s) == 1
        s = archive.search('physical_name not in ["b.txt", "c.txt"]')
        assert len(s) == 1

        # parameters
        s = archive.search('physical_name in @somelist', parameters={'somelist': ["b.txt", "c.txt"]})
        assert len(s) == 2
#        s = archive.search('physical_name in [@b, "c.txt"]', parameters={'b': "b.txt"})
#        assert len(s) == 2

        s = archive.search('size in @sizes', parameters={'sizes': [1015]})
        assert len(s) == 3

    def test_remove_properties(self, archive):  # TODO to TestArchive
        self._prep_data(archive)

        p = archive.retrieve_properties(uuid=self.uuid_a, namespaces=['mynamespace'])
        assert hasattr(p, 'core')
        assert hasattr(p, 'mynamespace')
        assert hasattr(p.mynamespace, 'hello')

        # remove property
        archive.update_properties(muninn.Struct({'mynamespace': muninn.Struct({'hello': None})}), p.core.uuid)
        p = archive.retrieve_properties(uuid=self.uuid_a, namespaces=['mynamespace'])
        assert not hasattr(p.mynamespace, 'hello')

        # remove namespace
        archive.update_properties(muninn.Struct({'mynamespace': None}), p.core.uuid)
        p = archive.retrieve_properties(uuid=self.uuid_a, namespaces=['mynamespace'])
        assert not hasattr(p, 'mynamespace')

    def test_geometry(self, archive):
        self._prep_data(archive)

        # covers
        # TODO note that covers raises an error if used on two polygons (only for postgresql..)
        s = archive.search('covers(core.footprint, POINT (1.0 3.0))')
        assert len(s) == 1
        assert s[0].core.uuid == self.uuid_c

        s = archive.search('not covers(core.footprint, POINT (1.0 3.0))')
        if archive._params['database'] == 'postgresql':
            assert len(s) == 0  # TODO sqlite/postgresql difference
        else:
            assert len(s) == 2

        s = archive.search('covers(core.footprint, POINT (5.0 5.0))')
        assert len(s) == 0

        s = archive.search('not covers(core.footprint, POINT (5.0 5.0))')
        if archive._params['database'] == 'postgresql':
            assert len(s) == 1  # TODO sqlite/postgresql difference
            assert s[0].core.uuid == self.uuid_c
        else:
            assert len(s) == 3

        # intersects
        s = archive.search('intersects(core.footprint, POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1)))')
        assert len(s) == 1
        s = archive.search('intersects(core.footprint, POLYGON ((11 11, 13 11, 13 13, 11 13, 11 11)))')
        assert len(s) == 0

        s = archive.search('intersects(core.footprint, MULTIPOLYGON (((1 1, 3 1, 3 3, 1 3, 1 1), (7 7, 8 7, 7 8, 7 7))))')
        assert len(s) == 1
        s = archive.search('intersects(core.footprint, MULTIPOLYGON (((11 11, 13 11, 13 13, 11 13, 11 11))))')
        assert len(s) == 0

        if archive._params['database'] != 'sqlite':  # TODO
            s = archive.search('intersects(core.footprint, POLYGON EMPTY)')
            assert len(s) == 0

    def test_alt_number_bases(self, archive):
        self._prep_data(archive)
        s = archive.search('size == %s' % hex(1015))
        assert len(s) == 3

        s = archive.search('size == %s' % oct(1015))
        assert len(s) == 3

        s = archive.search('size == %s' % bin(1015))
        assert len(s) == 3

        s = archive.search('size == %s' % hex(1014))
        assert len(s) == 0

    def test_errors(self, archive):
        self._prep_data(archive)

        with pytest.raises(muninn.exceptions.Error) as excinfo:
            s = archive.search('size in [1015')
        assert 'char 14: unexpected end of input' in str(excinfo)

        with pytest.raises(muninn.exceptions.Error) as excinfo:
            s = archive.search('size in [1015[')
        assert 'char 14: expected "]", got "["' in str(excinfo)

        with pytest.raises(muninn.exceptions.Error) as excinfo:
            s = archive.search('size in @missing')
        assert 'no value for parameter: "missing"' in str(excinfo)

        with pytest.raises(muninn.exceptions.Error) as excinfo:
            s = archive.search('size in')
        assert 'unexpected end of input' in str(excinfo)

        with pytest.raises(muninn.exceptions.Error) as excinfo:
            s = archive.search('size in [] xyz')
        assert 'extra characters after expression' in str(excinfo)

class TestTools:  # TODO more result checking, preferrably using tools
    def _run(self, tool, args='', action='', archive='my_arch', should_fail=False):
        python_path = 'PYTHONPATH=%s:.:$PYTHONPATH' % PARENT_DIR
        cmd = '%s python%s ../muninn/tools/%s.py %s %s %s 2>&1' % \
              (python_path, '3' if PY3 else '', tool, action, archive, args)

        returncode, output, errs = safe_popen(cmd, join=True)

        if returncode != 0:
            for line in output.decode().splitlines():
                print(line)
            for line in errs.decode().splitlines():
                print(line)
        if should_fail:
            assert returncode != 0
        else:
            assert returncode == 0
            assert not errs
        return output.decode().splitlines()

    def test_search(self, archive):
        output = self._run('search', '""')
        assert len(output) == 2 # header
        output = self._run('ingest', 'data/pi.txt')
        output = self._run('search', '"" -p \\*')
        assert len(output) == 3
        output = self._run('search', '"" -c')
        assert output == ['1']
        output = self._run('search', '"" --uuid')
        assert len(output) == 1
        uuid.UUID(output[0])
        output = self._run('search', '"" --paths')
        assert len(output) == 1
        assert 'pi.txt' in output[0]
        output = self._run('search', '"" -f psv -p archive_date -p core.physical_name -o +size')
        assert len(output) == 2
        assert output[1].count('|') == 3
        output = self._run('search', '"" -f csv -p archive_path -p size -o physical_name')
        assert len(output) == 2
        assert output[1].count(',') == 1
        output = self._run('ingest', 'data/a.txt')
        output = self._run('search', '"" -l 1')
        assert len(output) == 3

    def test_ingest(self, archive):
        output = self._run('ingest', 'data/pi.txt')
        output = self._run('search', '"" -c')
        assert output == ['1']
        archive.remove()

        # parallel
        output = self._run('ingest', '--parallel data/a.txt data/b.txt data/c.txt')
        output = self._run('search', '"" -c')
        assert output == ['3']

    def test_remove(self, archive):
        output = self._run('ingest', 'data/pi.txt')
        output = self._run('remove', '""')
        output = self._run('search', '"" -c')
        assert output == ['0']
        archive.remove()

        # parallel
        output = self._run('ingest', '--parallel data/a.txt data/b.txt data/c.txt')
        output = self._run('remove', '"" --parallel')
        output = self._run('search', '"" -c')
        assert output == ['0']

    def test_tag(self, archive):
        output = self._run('ingest', 'data/pi.txt')
        output = self._run('tag', '"" mytag')
        output = self._run('list_tags', '""')
        assert len(output) == 1
        assert output[0].endswith(': mytag')
        archive.remove()

        # parallel
        output = self._run('ingest', '--parallel data/a.txt data/b.txt data/c.txt')
        output = self._run('tag', '"" --parallel mytag')
        output = self._run('list_tags', '""')
        assert len(output) == 3
        for line in output:
            assert line.endswith(': mytag')

    def test_untag(self, archive):
        output = self._run('ingest', 'data/pi.txt')
        output = self._run('tag', '"" mytag')
        output = self._run('untag', '"" mytag')
        output = self._run('list_tags', '""')
        assert len(output) == 1
        assert not output[0].endswith(': mytag')
        archive.remove()

        # parallel
        output = self._run('ingest', '--parallel data/a.txt data/b.txt data/c.txt')
        output = self._run('tag', '"" --parallel mytag')
        output = self._run('untag', '--parallel "" mytag')  # TODO why can't we use --parallel mytag here
        output = self._run('list_tags', '""')
        assert len(output) == 3
        for line in output:
            assert not line.endswith(': mytag')

    def test_listtags(self, archive):
        output = self._run('ingest', 'data/pi.txt')
        output = self._run('tag', '"" mytag')
        output = self._run('list_tags', '""')

        assert len(output) == 1
        assert output[0].endswith(': mytag')

    def test_prepare(self, archive):
        self._run('destroy', '-y')
        self._run('prepare')
        output = self._run('search', '"" -c')
        assert output == ['0']

    def test_summary(self, archive):
        output = self._run('ingest', 'data/pi.txt')
        output = self._run('summary', '"" -f csv -H')
        assert len(output) == 2
        assert output[1].split(',')[0] == '"1"'
        output = self._run('summary', '"" -f psv')
        assert len(output) == 2

    def test_retrieve(self, archive):  # TODO nprocesses setting & fixture?
        with muninn.util.TemporaryDirectory() as tmp_path:
            output = self._run('ingest', 'data/pi.txt')
            output = self._run('retrieve', '"" -d %s' % tmp_path)
            assert os.listdir(tmp_path) == ['pi.txt']
        archive.remove()

        # parallel
        with muninn.util.TemporaryDirectory() as tmp_path:
            output = self._run('ingest', '--parallel data/a.txt data/b.txt data/c.txt')
            output = self._run('retrieve', '"" --parallel -d %s' % tmp_path)
            assert set(os.listdir(tmp_path)) == set(['a.txt', 'b.txt', 'c.txt'])

    def test_strip(self, archive):
        output = self._run('ingest', 'data/pi.txt')
        output = self._run('strip', '""')
        archive.remove()

        # parallel
        output = self._run('ingest', '--parallel data/a.txt data/b.txt data/c.txt')
        output = self._run('strip', '"" --parallel')

    def test_attach(self, archive):
        output = self._run('ingest', 'data/pi.txt')
        output = self._run('strip', '""')
        output = self._run('attach', 'data/pi.txt')
        archive.remove()

        # parallel
        output = self._run('ingest', '--parallel data/a.txt data/b.txt data/c.txt')
        output = self._run('strip', '""')
        output = self._run('attach', '--parallel data/a.txt data/b.txt data/c.txt')

    def test_pull(self, archive):  # TODO parameterize over remote backends?
        uuid = archive.ingest(['data/README'], ingest_product=False).core.uuid  # TODO get uuid via tools

        metadata = {
            'remote_url': 'file://' + os.path.realpath('data/README')
        }
        archive.update_properties(muninn.Struct({'core': metadata}), uuid)
        output = self._run('pull', '""')  # TODO check
        archive.remove()

        # parallel
        files = ['a.txt', 'b.txt', 'c.txt']
        uuids = [archive.ingest('data/'+name, ingest_product=False).core.uuid for name in files]
        for name, uuid in zip(files, uuids):
            metadata = {
                'remote_url': 'file://' + os.path.realpath('data/'+name)
            }
            archive.update_properties(muninn.Struct({'core': metadata}), uuid)

        output = self._run('pull', '"" --parallel')  # TODO check

    def test_update(self, archive):  # TODO parallel
        output = self._run('ingest', 'data/pi.txt')
        output = self._run('update', '""', 'ingest')
        output = self._run('update', '""', 'pull')
        output = self._run('update', '"" prodtype', 'retype')

    def test_hash(self, archive):
        output = self._run('ingest', 'data/a.txt')
        output = self._run('ingest', 'data/b.txt')

        # verify: 2 correct products
        output = self._run('hash', '""', 'verify')

        # verify: --parallel
        output = self._run('hash', '""', 'verify --parallel')

        # verify: 1 failing products
        product = archive.search()[0]
        product.core.hash = 'broken'
        archive.update_properties(product)
        output = self._run('hash', '""', 'verify', should_fail=True)
        assert [line for line in output if 'failed hash verification' in line]  # TODO just get stderr?

        # calc
        output = self._run('hash', 'calc data/a.txt data/b.txt', archive='')
        assert len(output) == 2
        assert 'sha1:' in output[0]

        output = self._run('hash', 'calc data/a.txt data/b.txt --hash-type=md5', archive='')
        assert len(output) == 2
        assert 'md5:' in output[1]

        output = self._run('hash', 'calc -s data/pi', archive='')
        assert len(output) == 1

    def test_export(self, archive):
        output = self._run('ingest', 'data/pi.txt')
        with muninn.util.TemporaryDirectory() as tmp_path:
            output = self._run('export', '"" -d %s' % tmp_path)
            assert set(os.listdir(tmp_path)) == set(['pi.txt'])
        archive.remove()

        # parallel
        with muninn.util.TemporaryDirectory() as tmp_path:
            output = self._run('ingest', '--parallel data/a.txt data/b.txt data/c.txt')
            output = self._run('export', '"" --parallel -d %s' % tmp_path)
            assert set(os.listdir(tmp_path)) == set(['a.txt', 'b.txt', 'c.txt'])

    def test_destroy(self, archive):
        self._run('destroy', '-y')
