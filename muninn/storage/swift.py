import logging
import os

from .base import StorageBackend

from muninn.schema import Mapping, Text
from muninn.exceptions import Error
import muninn.util as util
import muninn.config as config

import swiftclient

logging.getLogger("swiftclient").setLevel(logging.CRITICAL)


class _SwiftConfig(Mapping):
    _alias = "swift"

    container = Text()
    user = Text()
    key = Text()
    authurl = Text()
    tmp_root = Text(optional=True)


def create(configuration):
    options = config.parse(configuration.get("swift", {}), _SwiftConfig)
    _SwiftConfig.validate(options)
    return SwiftStorageBackend(**options)


class SwiftStorageBackend(StorageBackend):  # TODO '/' in keys to indicate directory, 'dir/' with contents?
    def __init__(self, container, user, key, authurl, tmp_root=None):
        super(SwiftStorageBackend, self).__init__()

        self.container = container
        self._root = container
        if tmp_root:
            tmp_root = os.path.realpath(tmp_root)
            util.make_path(tmp_root)
        self._tmp_root = tmp_root

        self._conn = swiftclient.Connection(
            user=user,
            key=key,
            authurl=authurl
        )

    def _object_keys(self, product_path):
        sub_objects = self._conn.get_container(self.container, prefix=product_path)[1]
        return [sub_object['name'] for sub_object in sub_objects]

    def prepare(self):
        if not self.exists():
            self._conn.put_container(self.container)

    def exists(self):
        try:
            self._conn.get_container(self.container)
            return True
        except swiftclient.exceptions.ClientException as e:
            if e.http_status == 404:
                return False
            else:
                raise

    def destroy(self):  # TODO individually deleting objects
        if self.exists():
            for data in self._conn.get_container(self.container)[1]:
                self._conn.delete_object(self.container, data['name'])
            self._conn.delete_container(self.container)

    def product_path(self, product):  # TODO needed?
        return os.path.join(product.core.archive_path, product.core.physical_name)

    def current_archive_path(self, paths):
        raise Error("Swift storage backend does not support ingesting already archived products")

    def put(self, paths, properties, use_enclosing_directory, use_symlinks=None, move_files=False, retrieve_files=None):
        if use_symlinks:
            raise Error("Swift storage backend does not support symlinks")

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
                            with open(filepath, 'rb') as f:
                                self._conn.put_object(self.container, filekey, contents=f.read())
                else:
                    with open(path, 'rb') as f:
                        self._conn.put_object(self.container, key, contents=f.read())

    def get(self, product, product_path, target_path, use_enclosing_directory, use_symlinks=None):
        if use_symlinks:
            raise Error("Swift storage backend does not support symlinks")

        archive_path = product.core.archive_path

        for key in self._object_keys(product_path):
            rel_path = os.path.relpath(key, archive_path)
            if use_enclosing_directory:
                rel_path = '/'.join(rel_path.split('/')[1:])
            target = os.path.normpath(os.path.join(target_path, rel_path))
            util.make_path(os.path.dirname(target))
            binary = self._conn.get_object(self.container, key)[1]
            with open(target, 'wb') as f:
                f.write(binary)

    def delete(self, product_path, properties):
        for key in self._object_keys(product_path):
            self._conn.delete_object(self.container, key)

    def size(self, product_path):
        total = 0
        for data in self._conn.get_container(self.container, prefix=product_path)[1]:
            total += data['bytes']
        return total

    def move(self, product, archive_path):
        # Ignore if product already there
        if product.core.archive_path == archive_path:
            return

        product_path = self.product_path(product)
        new_product_path = os.path.join(archive_path, product.core.physical_name)

        for key in self._object_keys(product_path):
            new_key = os.path.normpath(os.path.join(new_product_path, os.path.relpath(key, product_path)))
            self._conn.copy_object(self.container, key, os.path.join(self.container, new_key))
            self._conn.delete_object(self.container, key)
