import os.path

import muninn.util as util


class StorageBackend(object):
    def __init__(self):
        self.supports_symlinks = False
        self.global_prefix = ''

    def get_tmp_root(self, product):
        if self._tmp_root:
            tmp_root = os.path.join(self._tmp_root, product.core.archive_path)
            util.make_path(tmp_root)
            return tmp_root

    def run_for_product(self, product, fn, use_enclosing_directory):
        tmp_root = self.get_tmp_root(product)
        product_path = self.product_path(product)
        with util.TemporaryDirectory(dir=tmp_root, prefix=".run_for_product-",
                                     suffix="-%s" % product.core.uuid.hex) as tmp_path:
            self.get(product, product_path, tmp_path, use_enclosing_directory)

            # Determine product hash
            paths = [os.path.join(tmp_path, basename) for basename in os.listdir(tmp_path)]
            return fn(paths)

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

    def put(self, paths, properties, use_enclosing_directory, use_symlinks=None):
        # Place product file(s) into storage
        raise NotImplementedError()

    def get(self, product, product_path, target_path, use_enclosing_directory, use_symlinks=None):
        # Retrieve product file(s) from storage
        raise NotImplementedError()

    def size(self, product_path):
        # Return product storage size
        raise NotImplementedError()

    def delete(self, product_path, properties):
        # Delete product file(s) from storage
        raise NotImplementedError()

    def move(self, product, archive_path):
        # Move product
        raise NotImplementedError()
