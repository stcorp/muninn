import os.path
import shutil
import tempfile

import muninn.util as util


class TemporaryCopy(object):
    def __init__(self, tmp_path, paths):
        self.tmp_path = tmp_path
        self.paths = paths

    def __enter__(self):
        return self.paths

    def __exit__(self, *args):
        if self.tmp_path is not None:
            shutil.rmtree(self.tmp_path)


class NullContextManager(object):
    def __init__(self, resource):
        self.resource = resource

    def __enter__(self):
        return self.resource

    def __exit__(self, *args):
        pass


class StorageBackend(object):
    def __init__(self):
        self.supports_symlinks = False
        self.global_prefix = ''

    def get_tmp_root(self, product):
        if self._tmp_root:
            tmp_root = os.path.join(self._tmp_root, product.core.archive_path)
            util.make_path(tmp_root)
            return tmp_root

    def prepare(self):
        # Prepare storage for use.
        raise NotImplementedError()

    def exists(self):
        # Check that storage exists.
        raise NotImplementedError()

    def initialize(self, configuration):
        # Initialize storage.
        raise NotImplementedError()

    def destroy(self):
        # Destroy storage
        raise NotImplementedError()

    def product_path(self, product):  # TODO refactor away?
        # Product path within storage
        raise NotImplementedError()

    # TODO lower-granularity put/get/delete

    def put(self, paths, properties, use_enclosing_directory, use_symlinks=False, move_files=False, tmp_path=None):
        # Place product file(s) into storage
        raise NotImplementedError()

    def get(self, product, product_path, target_path, use_enclosing_directory, use_symlinks=None):
        # Retrieve product file(s) from storage
        raise NotImplementedError()

    def get_tmp(self, product, use_enclosing_directory):
        tmp_root = self.get_tmp_root(product)
        product_path = self.product_path(product)
        tmp_path = tempfile.mkdtemp(dir=tmp_root, prefix=".muninn-get-tmp-", suffix="-%s" % product.core.uuid.hex)
        try:
            self.get(product, product_path, tmp_path, use_enclosing_directory)
            paths = [os.path.join(tmp_path, basename) for basename in os.listdir(tmp_path)]
            return TemporaryCopy(tmp_path, paths)
        except:
            shutil.rmtree(tmp_path)
            raise

    def size(self, product_path):
        # Return product storage size
        raise NotImplementedError()

    def delete(self, product_path, properties):
        # Delete product file(s) from storage
        raise NotImplementedError()

    def move(self, product, archive_path):
        # Move product
        raise NotImplementedError()
