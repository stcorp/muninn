import os

from .base import StorageBackend

from muninn.schema import Mapping, Text, Boolean
import muninn.util as util
from muninn.exceptions import Error, StorageError
import muninn.config as config


class _FSConfig(Mapping):
    _alias = "fs"

    root = Text()
    use_symlinks = Boolean(optional=True)


def create(configuration, tempdir, auth_file):
    options = config.parse(configuration.get("fs", {}), _FSConfig)
    _FSConfig.validate(options)
    return FilesystemStorageBackend(**options, tempdir=tempdir)


class FilesystemStorageBackend(StorageBackend):
    def __init__(self, root, use_symlinks=None, tempdir=None):
        super(FilesystemStorageBackend, self).__init__(tempdir)

        self._root = os.path.realpath(root)
        self._use_symlinks = use_symlinks or False
        self.supports_symlinks = True

    def prepare(self):
        # Create the archive root path.
        try:
            util.make_path(self._root)
        except EnvironmentError as _error:
            raise Error("unable to create archive root path '%s' [%s]" % (self._root, _error))

    # tempdirs must be on the same file system for moves (below) to be atomic!
    def get_tmp_root(self, product):
        tmp_root = os.path.join(self._root, product.core.archive_path)
        util.make_path(tmp_root)
        return tmp_root

    def run_for_product(self, product, fn, use_enclosing_directory):
        product_path = self.product_path(product)
        if use_enclosing_directory:
            paths = [os.path.join(product_path, basename) for basename in os.listdir(product_path)]
        else:
            paths = [product_path]
        return fn(paths)

    def exists(self):
        return os.path.isdir(self._root)

    def destroy(self):
        if self.exists():
            try:
                util.remove_path(self._root)
            except EnvironmentError as _error:
                raise Error("unable to remove archive root path '%s' [%s]" % (self._root, _error))

    def product_path(self, product):
        return os.path.join(self._root, product.core.archive_path, product.core.physical_name)

    def current_archive_path(self, paths, properties):
        for path in paths:
            if not util.is_sub_path(os.path.realpath(path), self._root, allow_equal=True):
                raise Error("cannot ingest a file in-place if it is not inside the muninn archive root")

        abs_archive_path = os.path.dirname(os.path.realpath(paths[0]))

        if len(paths) > 1:
            # check whether all files have the right enclosing directory
            for path in paths:
                enclosing_directory = os.path.basename(os.path.dirname(os.path.realpath(path)))
                if enclosing_directory != properties.core.physical_name:
                    raise Error("multi-part product has invalid enclosing directory for in-place ingestion")
            abs_archive_path = os.path.dirname(abs_archive_path)

        # strip archive root
        return os.path.relpath(abs_archive_path, start=os.path.realpath(self._root))

    def put(self, paths, properties, use_enclosing_directory, use_symlinks=None,
            retrieve_files=None, run_for_product=None):

        if use_symlinks is None:
            use_symlinks = self._use_symlinks

        physical_name = properties.core.physical_name
        archive_path = properties.core.archive_path
        uuid = properties.core.uuid

        abs_archive_path = os.path.realpath(os.path.join(self._root, archive_path))
        abs_product_path = os.path.join(abs_archive_path, physical_name)

        # TODO separate this out like 'current_archive_path'
        if paths is not None and util.is_sub_path(os.path.realpath(paths[0]), abs_product_path, allow_equal=True):
            # Product should already be in the target location
            for path in paths:
                if not os.path.exists(path):
                    raise Error("product source path does not exist '%s'" % (path,))
                if not util.is_sub_path(os.path.realpath(path), abs_product_path, allow_equal=True):
                    raise Error("cannot ingest product where only part of the files are already at the "
                                "destination location")
        else:
            # Create destination location for product
            try:
                util.make_path(abs_archive_path)
            except EnvironmentError as _error:
                raise Error("cannot create parent destination path '%s' [%s]" % (abs_archive_path, _error))

            anything_stored = False

            # Create a temporary directory and transfer the product there, then move the product to its
            # destination within the archive.
            try:
                tmp_root = self.get_tmp_root(properties)
                with util.TemporaryDirectory(prefix=".put-", suffix="-%s" % uuid.hex,
                                             dir=tmp_root) as tmp_path:
                    try:
                        # Create enclosing directory if required.
                        if use_enclosing_directory:
                            tmp_path = os.path.join(tmp_path, physical_name)
                            util.make_path(tmp_path)

                        # Transfer the product (parts).
                        if retrieve_files:
                            # Retrieve product (parts).
                            paths = retrieve_files(tmp_path)
                        else:
                            if use_symlinks:
                                # Create symbolic link(s) for the product (parts).
                                if use_enclosing_directory:
                                    abs_path = abs_product_path
                                else:
                                    abs_path = abs_archive_path

                                for path in paths:
                                    if util.is_sub_path(path, self._root):
                                        # Create a relative symbolic link when the target is part of the archive
                                        # (i.e. when creating an intra-archive symbolic link). This ensures the
                                        # archive can be relocated without breaking intra-archive symbolic links.
                                        os.symlink(os.path.relpath(path, abs_path),
                                                   os.path.join(tmp_path, os.path.basename(path)))
                                    else:
                                        os.symlink(path, os.path.join(tmp_path, os.path.basename(path)))
                            else:
                                # Copy product (parts).
                                for path in paths:
                                    util.copy_path(path, tmp_path, resolve_root=True)

                        # Move the transferred product into its destination within the archive.
                        if use_enclosing_directory:
                            os.rename(tmp_path, abs_product_path)
                        else:
                            assert(len(paths) == 1 and os.path.basename(paths[0]) == physical_name)
                            tmp_product_path = os.path.join(tmp_path, physical_name)
                            os.rename(tmp_product_path, abs_product_path)
                        anything_stored = True
                    except EnvironmentError as _error:
                        raise Error("unable to transfer product to destination path '%s' [%s]" %
                                    (abs_product_path, _error))

                    # Run optional function on result
                    if run_for_product is not None:
                        self.run_for_product(properties, run_for_product, use_enclosing_directory)

            except Exception as e:
                raise StorageError(e, anything_stored)

    # TODO product_path follows from product
    def get(self, product, product_path, target_path, use_enclosing_directory, use_symlinks=None):
        if use_symlinks is None:
            use_symlinks = self._use_symlinks

        try:
            if use_symlinks:
                if use_enclosing_directory:
                    for basename in os.listdir(product_path):
                        os.symlink(os.path.join(product_path, basename), os.path.join(target_path, basename))
                else:
                    os.symlink(product_path, os.path.join(target_path, os.path.basename(product_path)))
            else:
                if use_enclosing_directory:
                    for basename in os.listdir(product_path):
                        util.copy_path(os.path.join(product_path, basename), target_path, resolve_root=True)
                else:
                    util.copy_path(product_path, target_path, resolve_root=True)

        except EnvironmentError as _error:
            raise Error("unable to retrieve product '%s' (%s) [%s]" % (product.core.product_name, product.core.uuid,
                                                                       _error))

    def size(self, product_path):
        return util.product_size(product_path)

    def delete(self, product_path, properties):
        if not os.path.lexists(product_path):
            # If the product does not exist, do not consider this an error.
            return

        try:
            tmp_root = self.get_tmp_root(properties)
            with util.TemporaryDirectory(prefix=".remove-", suffix="-%s" % properties.core.uuid.hex,
                                         dir=tmp_root) as tmp_path:

                # Move product into the temporary directory. When the temporary directory will be removed at the end of
                # this scope, the product will be removed along with it.
                assert properties.core.physical_name == os.path.basename(product_path)
                os.rename(product_path, os.path.join(tmp_path, os.path.basename(product_path)))

        except EnvironmentError as _error:
            raise Error("unable to remove product '%s' (%s) [%s]" % (properties.core.product_name, properties.core.uuid,
                                                                     _error))

    def move(self, product, archive_path, paths=None):
        # Ignore if product already there
        if product.core.archive_path == archive_path:
            return paths

        # Make target archive path
        abs_archive_path = os.path.realpath(os.path.join(self._root, archive_path))
        util.make_path(abs_archive_path)

        # Move files there
        product_path = self.product_path(product)
        os.rename(product_path, os.path.join(abs_archive_path, product.core.physical_name))

        # Optionally rewrite (local) paths
        if paths is not None:
            paths = [os.path.join(self._root, archive_path,
                                  os.path.relpath(path, os.path.join(self._root, product.core.archive_path)))
                     for path in paths]
        return paths
