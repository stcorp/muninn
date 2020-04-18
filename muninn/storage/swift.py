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

    def prepare(self):
        if not self.exists():
            self._conn.put_container(self.container)

    def exists(self):
        try:
            self._conn.get_container(self.container)
            return True
        except swiftclient.exceptions.ClientException as e:
            if e.http_status==404:
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
        raise Error("Swift storage backend does not (yet) support ingesting already ingested products")

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
                    for fname in os.listdir(path): # TODO nesting?
                        fkey = os.path.join(key, fname)
                        fpath = os.path.join(path, fname)
                        with open(fpath, 'rb') as f:
                            self._conn.put_object(self.container, fkey, contents=f.read())
                else:
                    with open(path, 'rb') as f:
                        self._conn.put_object(self.container, key, contents=f.read())

    def get(self, product, product_path, target_path, use_enclosing_directory, use_symlinks=None):
        if use_symlinks:
            raise Error("Swift storage backend does not support symlinks")

        if use_enclosing_directory:
            for data in self._conn.get_container(self.container, path=product_path)[1]:
                basename = os.path.basename(data['name'])
                target = os.path.join(target_path, basename)

                binary = self._conn.get_object(self.container, data['name'])[1]
                with open(target, 'wb') as f:
                    f.write(binary)
        else:
            binary = self._conn.get_object(self.container, product_path)[1]
            target = os.path.join(target_path, os.path.basename(product_path))
            with open(target, 'wb') as f:
                f.write(binary)

    def delete(self, product_path, properties, use_enclosing_directory):
        if use_enclosing_directory:
            for data in self._conn.get_container(self.container, path=product_path)[1]:
                self._conn.delete_object(self.container, data['name'])
        else:
            self._conn.delete_object(self.container, product_path)

    def size(self, product_path, use_enclosing_directory):
        total = 0
        if use_enclosing_directory:
            for data in self._conn.get_container(self.container, path=product_path)[1]:
                total += data['bytes']
        else:
            data = self._conn.get_object(self.container, product_path)  # TODO slow?
            total = int(data[0]['content-length'])

        return total

    def move(self, product, archive_path, use_enclosing_directory):
        # Ignore if product already there
        if product.core.archive_path == archive_path:
            return

        old_key = self.product_path(product)
        moves = []

        if use_enclosing_directory:
            for data in self._conn.get_container(self.container, path=old_key)[1]:
                new_key = os.path.join(archive_path, product.core.physical_name, os.path.basename(data['name']))
                moves.append((data['name'], new_key))
        else:
            new_key = os.path.join(archive_path, product.core.physical_name)
            moves.append((old_key, new_key))

        for old_key, new_key in moves:
            self._conn.copy_object(self.container, old_key, os.path.join(self.container, new_key))
            self._conn.delete_object(self.container, old_key)
