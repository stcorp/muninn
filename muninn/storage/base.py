class StorageBackend(object):
    def __init__(self):
        self.supports_symlinks = False

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

    def size(self, product_path, use_enclosing_directory):
        # Return product storage size
        raise NotImplementedError()

    def delete(self, product_path, properties, use_enclosing_directory):
        # Delete product file(s) from storage
        raise NotImplementedError()

    def move(self, product, archive_path, use_enclosing_directory):
        # Move product
        raise NotImplementedError()
