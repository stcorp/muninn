import os.path

import muninn.util as util


class StorageBackend(object):
    def __init__(self, tempdir=None):
        self.supports_symlinks = False
        self.global_prefix = ''

        if tempdir is not None:
            tmp_root = os.path.realpath(tempdir)
            util.make_path(tmp_root)
            self._tmp_root = tmp_root
        else:
            self._tmp_root = None

    def get_tmp_root(self, product):
        if self._tmp_root is not None:
            tmp_root = os.path.join(self._tmp_root, product.core.archive_path)
            util.make_path(tmp_root)
            return tmp_root

    def run_for_product(self, product, fn, use_enclosing_directory):
        tmp_root = self.get_tmp_root(product)
        product_path = self.product_path(product)
        with util.TemporaryDirectory(dir=tmp_root, prefix=".run_for_product-",
                                     suffix="-%s" % product.core.uuid.hex) as tmp_path:
            self.get(product, product_path, tmp_path, use_enclosing_directory)
            paths = [os.path.join(tmp_path, basename) for basename in os.listdir(tmp_path)]
            return fn(paths)

    def prepare(self):  # pragma: no cover
        # Prepare storage for use.
        raise NotImplementedError()

    def exists(self):  # pragma: no cover
        # Check that storage exists.
        raise NotImplementedError()

    def initialize(self, configuration):  # pragma: no cover
        # Initialize storage.
        raise NotImplementedError()

    def destroy(self):  # pragma: no cover
        # Destroy storage
        raise NotImplementedError()

    # TODO refactor away?
    def product_path(self, product):  # pragma: no cover
        # Product path within storage
        raise NotImplementedError()

    def put(self, paths, properties, use_enclosing_directory, use_symlinks=None,
            retrieve_files=None, run_for_product=None):  # pragma: no cover
        # Place product file(s) into storage
        raise NotImplementedError()

    def get(self, product, product_path, target_path, use_enclosing_directory, use_symlinks=None):  # pragma: no cover
        # Retrieve product file(s) from storage
        raise NotImplementedError()

    def size(self, product_path):  # pragma: no cover
        # Return product storage size
        raise NotImplementedError()

    def delete(self, product_path, properties):  # pragma: no cover
        # Delete product file(s) from storage
        raise NotImplementedError()

    def move(self, product, archive_path, paths=None):  # pragma: no cover
        # Move product
        raise NotImplementedError()

    def current_archive_path(self, paths, properties):  # pragma: no cover
        raise NotImplementedError()
