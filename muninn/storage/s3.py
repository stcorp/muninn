import json
import logging
import os

from .base import StorageBackend

from muninn.schema import Mapping, Text, Integer
import muninn.config as config
import muninn.util as util
from muninn.exceptions import Error, StorageError

import boto3
import boto3.s3
import botocore

logging.getLogger("boto3").setLevel(logging.CRITICAL)


class _S3Config(Mapping):
    _alias = "s3"

    host = Text()
    port = Integer(optional=True)
    bucket = Text()
    access_key = Text(optional=True)
    secret_access_key = Text(optional=True)
    region = Text(optional=True)
    prefix = Text(optional=True)
    download_args = Text(optional=True)  # JSON representation of boto3 download_file ExtraArgs parameter
    upload_args = Text(optional=True)  # JSON representation of boto3 upload_file ExtraArgs parameter
    copy_args = Text(optional=True)  # JSON representation of boto3 copy ExtraArgs parameter
    transfer_config = Text(optional=True)  # JSON representation of boto3.s3.transfer.TransferConfig parameters


def create(configuration, tempdir, auth_file):
    options = config.parse(configuration.get("s3", {}), _S3Config)

    # if access_key and secret_access_key missing, use auth_file
    if (auth_file is not None and
            'access_key' not in options and
            'secret_access_key' not in options and
            'host' in options and
            'bucket' in options):
        credentials = json.loads(open(auth_file).read())
        for key, value in credentials.items():
            if value.get('auth_type') == 'S3' and key == options['host'] and value.get('bucket') == options['bucket']:
                for option in ('access_key', 'secret_access_key', 'port', 'bucket', 'region'):
                    if option in value and option not in options:
                        options[option] = value[option]
                break

    # check that mandatory options are configured
    for option in ('access_key', 'secret_access_key'):
        if option not in options:
            raise Error("'%s' not configured" % option)

    _S3Config.validate(options)
    return S3StorageBackend(**options, tempdir=tempdir)


class S3StorageBackend(StorageBackend):  # TODO '/' in keys to indicate directory, 'dir/' with contents?
    def __init__(self, bucket, host, access_key, secret_access_key, port=None, region=None, prefix='',
                 download_args=None, upload_args=None, copy_args=None, transfer_config=None, tempdir=None):
        super(S3StorageBackend, self).__init__(tempdir)

        self.bucket = bucket

        if prefix and not prefix.endswith('/'):
            prefix += '/'
        self._prefix = prefix

        endpoint_url = host
        if ':' not in host:
            if port == 443:
                endpoint_url = 'https://' + endpoint_url
            else:
                endpoint_url = 'http://' + endpoint_url
                if port is not None and port != 80:
                    endpoint_url += ':%d' % port
        elif port is not None:
            endpoint_url += ':%d' % port
        self.global_prefix = os.path.join(endpoint_url, bucket, prefix)

        self._root = bucket

        self._resource = boto3.resource(
            service_name='s3',
            region_name=region,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_access_key,
            endpoint_url=endpoint_url,
        )

        self._download_args = None
        if download_args:
            self._download_args = json.loads(download_args)
        self._upload_args = None
        if upload_args:
            self._upload_args = json.loads(upload_args)
        self._copy_args = None
        if copy_args:
            self._copy_args = json.loads(copy_args)
        if transfer_config:
            self._transfer_config = boto3.s3.transfer.TransferConfig(**json.loads(transfer_config))
        else:
            self._transfer_config = boto3.s3.transfer.TransferConfig()

    def _bucket_exists(self):
        try:
            self._resource.meta.client.head_bucket(Bucket=self.bucket)
            return True
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False
            else:
                raise

    def _prefix_exists(self):
        if self._prefix:  # TODO created but still empty archive
            objs = list(self._resource.Bucket(self.bucket).objects.limit(count=1).filter(Prefix=self._prefix))
            return len(objs) == 1
        else:
            return True

    def prepare(self):
        if not self._bucket_exists():
            self._resource.create_bucket(Bucket=self.bucket)

    def exists(self):
        return self._bucket_exists() and self._prefix_exists()

    def destroy(self):
        if self._prefix:
            self._resource.Bucket(self.bucket).objects.filter(Prefix=self._prefix).delete()
        elif self._bucket_exists():
            bucket = self._resource.Bucket(self.bucket)
            bucket.objects.all().delete()
            bucket.delete()

    def product_path(self, product):  # TODO needed?
        return os.path.join(product.core.archive_path, product.core.physical_name)

    def current_archive_path(self, paths, properties):
        raise Error("S3 storage backend does not support ingesting already archived products")

    def _upload_file(self, key, path):
        obj = self._resource.Object(self.bucket, key)
        if os.path.getsize(path) == 0:  # TODO otherwise upload_file hangs sometimes!?
            self._resource.Object(self.bucket, key).put()
        else:
            obj.upload_file(path, ExtraArgs=self._upload_args, Config=self._transfer_config)

    def _create_dir(self, key):
        # using put, as upload_file/upload_fileobj do not like the trailish slash
        self._resource.Object(self.bucket, key+'/').put()

    def put(self, paths, properties, use_enclosing_directory, use_symlinks=None,
            retrieve_files=None, run_for_product=None):

        if use_symlinks:
            raise Error("S3 storage backend does not support symlinks")

        anything_stored = False

        try:
            archive_path = properties.core.archive_path
            physical_name = properties.core.physical_name

            if not use_enclosing_directory and retrieve_files is None:
                assert(len(paths) == 1 and os.path.basename(paths[0]) == physical_name)

            tmp_root = self.get_tmp_root(properties)
            with util.TemporaryDirectory(dir=tmp_root, prefix=".put-",
                                         suffix="-%s" % properties.core.uuid.hex) as tmp_path:
                if retrieve_files:
                    paths = retrieve_files(tmp_path)

                # Upload file(s)
                for path in paths:
                    key = self._prefix + os.path.join(archive_path, physical_name)

                    # Add enclosing dir
                    if use_enclosing_directory:
                        key = os.path.join(key, os.path.basename(path))

                    if os.path.isdir(path):
                        self._create_dir(key)
                        anything_stored = True

                        for root, subdirs, files in os.walk(path):
                            rel_root = os.path.relpath(root, path)

                            for subdir in subdirs:
                                dirkey = os.path.normpath(os.path.join(key, rel_root, subdir))
                                self._create_dir(dirkey)
                                anything_stored = True

                            for filename in files:
                                filekey = os.path.normpath(os.path.join(key, rel_root, filename))
                                filepath = os.path.join(root, filename)
                                self._upload_file(filekey, filepath)
                                anything_stored = True

                    else:
                        self._upload_file(key, path)
                        anything_stored = True

                if run_for_product is not None:
                    run_for_product(paths)

        except Exception as e:
            raise StorageError(e, anything_stored)

    def get(self, product, product_path, target_path, use_enclosing_directory, use_symlinks=None):
        if use_symlinks:
            raise Error("S3 storage backend does not support symlinks")

        archive_path = product.core.archive_path
        prefix = self._prefix + product_path

        objs = list(self._resource.Bucket(self.bucket).objects.filter(Prefix=prefix))
        if not objs:
            raise Error("no data for product '%s' (%s)" % (product.core.product_name, product.core.uuid))

        for obj in objs:
            rel_path = os.path.relpath(obj.key, self._prefix + archive_path)
            if use_enclosing_directory:
                rel_path = '/'.join(rel_path.split('/')[1:])
            target = os.path.normpath(os.path.join(target_path, rel_path))

            if obj.key.endswith('/'):
                util.make_path(target)
            else:
                util.make_path(os.path.dirname(target))
                self._resource.Object(self.bucket, obj.key).download_file(target, ExtraArgs=self._download_args,
                                                                          Config=self._transfer_config)

    def delete(self, product_path, properties):
        prefix = self._prefix + product_path
        for obj in self._resource.Bucket(self.bucket).objects.filter(Prefix=prefix):
            obj.delete()

    def size(self, product_path):
        total = 0
        prefix = self._prefix + product_path
        for obj in self._resource.Bucket(self.bucket).objects.filter(Prefix=prefix):
            total += obj.size
        return total

    def move(self, product, archive_path, paths=None):
        # Ignore if product already there
        if product.core.archive_path == archive_path:
            return paths

        product_path = self._prefix + self.product_path(product)
        new_product_path = self._prefix + os.path.join(archive_path, product.core.physical_name)

        objs = list(self._resource.Bucket(self.bucket).objects.filter(Prefix=product_path))
        if not objs:
            raise Error("no data for product '%s' (%s)" % (product.core.product_name, product.core.uuid))

        for obj in objs:
            new_key = os.path.normpath(os.path.join(new_product_path, os.path.relpath(obj.key, product_path)))
            self._resource.Object(self.bucket, new_key).copy(CopySource={'Bucket': self.bucket, 'Key': obj.key},
                                                             ExtraArgs=self._copy_args, Config=self._transfer_config)
            self._resource.Object(self.bucket, obj.key).delete()

        return paths
