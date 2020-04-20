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
    tmp_root = Text(optional=True)


def create(configuration):
    options = config.parse(configuration.get("s3", {}), _S3Config)
    _S3Config.validate(options)
    return S3StorageBackend(**options)


class S3StorageBackend(StorageBackend):  # TODO '/' in keys to indicate directory, 'dir/' with contents?
    def __init__(self, bucket, host, port, access_key, secret_access_key, tmp_root=None):
        super(S3StorageBackend, self).__init__()

        self.bucket = bucket
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

    def prepare(self):
        if not self.exists():
            self._resource.create_bucket(Bucket=self.bucket)

    def exists(self):
        try:
            # TODO ugly trick using creation_date
            creation_date = self._resource.Bucket(self.bucket).creation_date
            return (creation_date is not None)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False
            else:
                raise

    def destroy(self):  # TODO individually deleting objects?
        if self.exists():
            bucket = self._resource.Bucket(self.bucket)
            bucket.objects.all().delete()
            bucket.delete()

    def product_path(self, product):  # TODO needed?
        return os.path.join(product.core.archive_path, product.core.physical_name)

    def current_archive_path(self, paths):
        raise Error("S3 storage backend does not (yet) support ingesting already ingested products")

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
                key = os.path.join(archive_path, physical_name)

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

        for obj in self._resource.Bucket(self.bucket).objects.filter(Prefix=product_path):  # TODO slow?
            rel_path = os.path.relpath(obj.key, archive_path)
            if use_enclosing_directory:
                rel_path = '/'.join(rel_path.split('/')[1:])
            target = os.path.normpath(os.path.join(target_path, rel_path))
            util.make_path(os.path.dirname(target))
            self._resource.Object(self.bucket, obj.key).download_file(target)

    def delete(self, product_path, properties):
        for obj in self._resource.Bucket(self.bucket).objects.filter(Prefix=product_path):  # TODO slow?
            obj.delete()

    def size(self, product_path, use_enclosing_directory):
        total = 0
        for obj in self._resource.Bucket(self.bucket).objects.filter(Prefix=product_path):  # TODO slow?
            total += obj.size
        return total

    def move(self, product, archive_path, use_enclosing_directory):
        # Ignore if product already there
        if product.core.archive_path == archive_path:
            return

        old_key = self.product_path(product)
        moves = []

        if use_enclosing_directory:
            for obj in self._resource.Bucket(self.bucket).objects.filter(Prefix=old_key):  # TODO slow?
                new_key = os.path.join(archive_path, product.core.physical_name, os.path.basename(obj.key))
                moves.append((obj.key, new_key))
        else:
            new_key = os.path.join(archive_path, product.core.physical_name)
            moves.append((old_key, new_key))

        for old_key, new_key in moves:
            self._resource.Object(self.bucket, new_key).copy_from(CopySource=os.path.join(self.bucket, old_key))
            self._resource.Object(self.bucket, old_key).delete()
