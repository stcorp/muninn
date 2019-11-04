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

    def put(self, paths, properties, plugin, use_current_path, use_symlinks):
        # Place product file(s) into storage
        raise NotImplementedError()

    def get(self, product_path, target_path, plugin, use_symlinks):
        # Retrieve product file(s) from storage
        raise NotImplementedError()

    def delete(self, product_path):
        # Delete product file(s) from storage
        raise NotImplementedError()
