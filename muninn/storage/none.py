import os

from .base import StorageBackend

from muninn.schema import Mapping, Text, Boolean
import muninn.util as util
from muninn.exceptions import Error, StorageError
import muninn.config as config
from muninn.util import product_size


class _FSConfig(Mapping):
    _alias = "none"


def create(configuration):
    return NoStorageBackend()


class NoStorageBackend(StorageBackend):
    def __init__(self):
        super(NoStorageBackend, self).__init__()

    def exists(self):
        return False

    def destroy(self):
        pass

    def prepare(self):
        pass
