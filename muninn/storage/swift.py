import logging
import os

from .base import StorageBackend

from muninn.schema import Mapping, Text
from muninn.exceptions import Error
import muninn.config as config

import swiftclient

logging.getLogger("swiftclient").setLevel(logging.CRITICAL)


class _SwiftConfig(Mapping):
    _alias = "swift"

    container = Text
    user = Text
    key = Text
    authurl = Text


def create(configuration):
    options = config.parse(configuration.get("swift", {}), _SwiftConfig)
    _SwiftConfig.validate(options)
    return SwiftStorageBackend(**options)

class SwiftStorageBackend(StorageBackend):  # TODO '/' in keys to indicate directory, 'dir/' with contents?
    def __init__(self, container, user, key, authurl):
        super(SwiftStorageBackend, self).__init__()

        self.container = container
        self._root = container

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

    def put(self, paths, properties, plugin, use_symlinks):
        if use_symlinks:
            raise Error("Swift storage backend does not support symlinks")

        archive_path = plugin.archive_path(properties)
        properties.core.archive_path = archive_path
        physical_name = properties.core.physical_name

        # Upload file(s)
        for path in paths:
            key = os.path.join(archive_path, physical_name)

            # Add enclosing dir
            if plugin.use_enclosing_directory:
                key = os.path.join(key, os.path.basename(path))

            # Upload file
            with open(path, 'rb') as f:
                self._conn.put_object(self.container, key, contents=f.read())

    def put2(self, file_path, archive, product):
        plugin = archive.product_type_plugin(product.core.product_type)

        archive_path = product.core.archive_path
        physical_name = product.core.physical_name

        key = os.path.join(archive_path, physical_name)
        if plugin.use_enclosing_directory:
            key = os.path.join(key, physical_name)

        with open(file_path, 'rb') as f:
            self._conn.put_object(self.container, key, contents=f.read())

    def get(self, product, product_path, target_path, plugin, use_symlinks=False):
        if use_symlinks:
            raise Error("Swift storage backend does not support symlinks")

        if plugin.use_enclosing_directory:
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

    def delete(self, product_path, properties, plugin):
        if plugin.use_enclosing_directory:
            for data in self._conn.get_container(self.container, path=product_path)[1]:
                self._conn.delete_object(self.container, data['name'])
        else:
            self._conn.delete_object(self.container, product_path)

    def size(self, product_path, plugin):
        total = 0
        if plugin.use_enclosing_directory:
            for data in self._conn.get_container(self.container, path=product_path)[1]:
                total += data['bytes']
        else:
            data = self._conn.get_object(self.container, product_path)  # TODO slow?
            total = int(data[0]['content-length'])

        return total

    def move(self, product, archive_path, plugin):
        old_key = self.product_path(product)
        moves = []

        if plugin.use_enclosing_directory:
            for data in self._conn.get_container(self.container, path=old_key)[1]:
                new_key = os.path.join(archive_path, product.core.physical_name, os.path.basename(data['name']))
                moves.append((data['name'], new_key))
        else:
            new_key = os.path.join(archive_path, product.core.physical_name)
            moves.append((old_key, new_key))

        for old_key, new_key in moves:
            self._conn.copy_object(self.container, old_key, os.path.join(self.container, new_key))
            self._conn.delete_object(self.container, old_key)
