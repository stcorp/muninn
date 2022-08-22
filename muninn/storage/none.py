import os

from .base import StorageBackend

from muninn.schema import Mapping, Text, Boolean
import muninn.util as util
from muninn.exceptions import Error, StorageError
import muninn.config as config
from muninn.util import product_size


class _FSConfig(Mapping):
    _alias = "none"


def create(configuration, tempdir):
    return NoStorageBackend()


class NoStorageBackend(StorageBackend):
    def __init__(self):
        super(NoStorageBackend, self).__init__()

    def exists(self):
        return False

    def run_for_product(self, product, fn, use_enclosing_directory):
        product_path = product.core.remote_url[7:]
        if os.path.isdir(product_path):
            paths = [os.path.join(product_path, basename) for basename in os.listdir(product_path)]
        else:
            paths = [product_path]
        return fn(paths)

    def destroy(self):
        pass

    def prepare(self):
        pass
