import os

from .base import StorageBackend

from muninn.schema import Mapping, Text, Boolean
import muninn.util as util
from muninn.exceptions import Error, StorageError
import muninn.config as config
from muninn.util import product_size


class _FSConfig(Mapping):
    _alias = "none"
    tmp_root = Text(optional=True)


def create(configuration):
    return NoStorageBackend()


class NoStorageBackend(StorageBackend):
    def __init__(self, tmp_root=None):
        super(NoStorageBackend, self).__init__()

        if tmp_root:
            tmp_root = os.path.realpath(tmp_root)
            util.make_path(tmp_root)
        self._tmp_root = tmp_root

    def exists(self):
        return False

    def run_for_product(self, product, fn, use_enclosing_directory):
        tmp_root = self.get_tmp_root(product)
        product_path = product.core.remote_url[7:]

        with util.TemporaryDirectory(dir=tmp_root, prefix=".run_for_product-",
                                     suffix="-%s" % product.core.uuid.hex) as tmp_path:
            if os.path.isdir(product_path):
                for basename in os.listdir(product_path):
                    util.copy_path(os.path.join(product_path, basename), tmp_path, resolve_root=True)
            else:
                util.copy_path(product_path, tmp_path, resolve_root=True)

            paths = [os.path.join(tmp_path, basename) for basename in os.listdir(tmp_path)]

            return fn(paths)

    def destroy(self):
        pass

    def prepare(self):
        pass
