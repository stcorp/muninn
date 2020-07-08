import logging
import os

from .base import StorageBackend

from muninn.schema import Mapping, Text, Integer
import muninn.config as config
import muninn.util as util
from muninn.exceptions import Error

import boto3
import botocore

logging.getLogger("boto3").setLevel(logging.CRITICAL)


class _S3Config(Mapping):
    _alias = "s3"

    host = Text()
    port = Integer()
    bucket = Text()
    access_key = Text()
    secret_access_key = Text()
    prefix = Text(optional=True)
    tmp_root = Text(optional=True)


def create(configuration):
    options = config.parse(configuration.get("s3", {}), _S3Config)
    _S3Config.validate(options)
    return S3StorageBackend(**options)


class S3StorageBackend(StorageBackend):  # TODO '/' in keys to indicate directory, 'dir/' with contents?
    def __init__(self, bucket, host, port, access_key, secret_access_key, prefix='', tmp_root=None):
        super(S3StorageBackend, self).__init__()

        self.bucket = bucket
        if prefix and not prefix.endswith('/'):
            prefix += '/'
        self._prefix = prefix

        self._root = bucket
        if tmp_root:
            tmp_root = os.path.realpath(tmp_root)
            util.make_path(tmp_root)
        self._tmp_root = tmp_root

        self._resource = boto3.resource(
            service_name='s3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_access_key,
            endpoint_url='http://%s:%s' % (host, port),
        )

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
        if self._prefix: # TODO created but still empty archive
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

    def current_archive_path(self, paths):
        raise Error("S3 storage backend does not support ingesting already archived products")

    def put(self, paths, properties, use_enclosing_directory, use_symlinks=None, move_files=False, retrieve_files=None):
        if use_symlinks:
            raise Error("S3 storage backend does not support symlinks")

        archive_path = properties.core.archive_path
        physical_name = properties.core.physical_name

        tmp_root = self.get_tmp_root(properties)
        with util.TemporaryDirectory(dir=tmp_root, prefix=".put-", suffix="-%s" % properties.core.uuid.hex) as tmp_path:
            if retrieve_files:
                paths = retrieve_files(tmp_path)

            # Upload file(s)
            for path in paths:
                key = self._prefix + os.path.join(archive_path, physical_name)

                # Add enclosing dir
                if use_enclosing_directory:
                    key = os.path.join(key, os.path.basename(path))

                if os.path.isdir(path):
                    for root, subdirs, files in os.walk(path):
                        rel_root = os.path.relpath(root, path)
                        for filename in files:
                            filekey = os.path.normpath(os.path.join(key, rel_root, filename))
                            filepath = os.path.join(root, filename)
                            self._resource.Object(self.bucket, filekey).upload_file(filepath)
                else:
                    self._resource.Object(self.bucket, key).upload_file(path)

    def get(self, product, product_path, target_path, use_enclosing_directory, use_symlinks=None):
        if use_symlinks:
            raise Error("S3 storage backend does not support symlinks")

        archive_path = product.core.archive_path
        prefix = self._prefix + product_path

        for obj in self._resource.Bucket(self.bucket).objects.filter(Prefix=prefix):
            rel_path = os.path.relpath(obj.key, self._prefix + archive_path)
            if use_enclosing_directory:
                rel_path = '/'.join(rel_path.split('/')[1:])
            target = os.path.normpath(os.path.join(target_path, rel_path))
            util.make_path(os.path.dirname(target))
            self._resource.Object(self.bucket, obj.key).download_file(target)

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

    def move(self, product, archive_path):
        # Ignore if product already there
        if product.core.archive_path == archive_path:
            return

        product_path = self._prefix + self.product_path(product)
        new_product_path = self._prefix + os.path.join(archive_path, product.core.physical_name)

        for obj in self._resource.Bucket(self.bucket).objects.filter(Prefix=product_path):  # TODO slow?
            new_key = os.path.normpath(os.path.join(new_product_path, os.path.relpath(obj.key, product_path)))

            self._resource.Object(self.bucket, new_key).copy_from(CopySource=os.path.join(self.bucket, obj.key))
            self._resource.Object(self.bucket, obj.key).delete()
