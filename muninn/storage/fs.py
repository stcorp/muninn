import os

from .base import StorageBackend

from muninn.schema import Mapping, Text
import muninn.util as util
from muninn.exceptions import Error
import muninn.config as config
from muninn.util import product_size


class _FSConfig(Mapping):
    _alias = "fs"

    root = Text


def create(configuration):
    fs_section = configuration.get("fs", {})
    if not fs_section:  # backward compatibility
        arch_section = configuration.get('archive')
        try:
            options = {'root': arch_section['root']}
        except KeyError:
            raise ValueError('archive: storage: fs: no value for mandatory item "root"')
    else:
        options = config.parse(fs_section, _FSConfig)
        _FSConfig.validate(options)

    return FilesystemStorageBackend(**options)


class FilesystemStorageBackend(StorageBackend):
    def __init__(self, root):
        super(FilesystemStorageBackend, self).__init__()

        self._root = root
        self.supports_symlinks = True

    def prepare(self):
        # Create the archive root path.
        try:
            util.make_path(self._root)
        except EnvironmentError as _error:
            raise Error("unable to create archive root path '%s' [%s]" % (self._root, _error))

    def exists(self):
        return os.path.isdir(self._root)

    def destroy(self):
        if self.exists():
            try:
                util.remove_path(self._root)
            except EnvironmentError as _error:
                raise Error("unable to remove archive root path '%s' [%s]" % (self._root, _error))

    def product_path(self, product):  # TODO needed?
        return os.path.join(self._root, product.core.archive_path, product.core.physical_name)

    def current_archive_path(self, paths):
        for path in paths:
            if not util.is_sub_path(os.path.realpath(path), self._root, allow_equal=True):
                raise Error("cannot ingest a file in-place if it is not inside the muninn archive root")
        if len(paths) > 1:
            # check whether all files have the right enclosing directory
            for path in paths:
                enclosing_directory = os.path.basename(os.path.dirname(os.path.realpath(path)))
                if enclosing_directory != properties.core.physical_name:
                    raise Error("multi-part product has invalid enclosing directory for in-place ingestion")
            # strip the archive root
            return os.path.relpath(
                os.path.dirname(os.path.dirname(os.path.realpath(paths[0]))),
                start=os.path.realpath(self._root))
        else:
            # strip the archive root
            return os.path.relpath(
                os.path.dirname(os.path.realpath(paths[0])),
                start=os.path.realpath(self._root))

    def put(self, paths, properties, plugin, use_symlinks):
        abs_archive_path = os.path.realpath(os.path.join(self._root, properties.core.archive_path))
        abs_product_path = os.path.join(abs_archive_path, properties.core.physical_name)

        if util.is_sub_path(os.path.realpath(paths[0]), abs_product_path, allow_equal=True):
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

            # Create a temporary directory and transfer the product there, then move the product to its
            # destination within the archive.
            try:
                with util.TemporaryDirectory(prefix=".ingest-", suffix="-%s" % properties.core.uuid.hex,
                                             dir=abs_archive_path) as tmp_path:

                    # Create enclosing directory if required.
                    if plugin.use_enclosing_directory:
                        tmp_path = os.path.join(tmp_path, properties.core.physical_name)
                        util.make_path(tmp_path)

                    # Transfer the product (parts).
                    if use_symlinks:
                        # Create symbolic link(s) for the product (parts).
                        for path in paths:
                            if util.is_sub_path(path, self._root):
                                # Create a relative symbolic link when the target is part of the archive
                                # (i.e. when creating an intra-archive symbolic link). This ensures the
                                # archive can be relocated without breaking intra-archive symbolic links.
                                if plugin.use_enclosing_directory:
                                    abs_path = abs_product_path
                                else:
                                    abs_path = abs_archive_path
                                os.symlink(os.path.relpath(path, abs_path),
                                           os.path.join(tmp_path, os.path.basename(path)))
                            else:
                                os.symlink(path, os.path.join(tmp_path, os.path.basename(path)))
                    else:
                        # Copy product (parts).
                        for path in paths:
                            util.copy_path(path, tmp_path, resolve_root=True)

                    # Move the transferred product into its destination within the archive.
                    if plugin.use_enclosing_directory:
                        os.rename(tmp_path, abs_product_path)
                    else:
                        assert len(paths) == 1 and \
                            properties.core.physical_name == os.path.basename(paths[0])
                        tmp_product_path = os.path.join(tmp_path, properties.core.physical_name)
                        os.rename(tmp_product_path, abs_product_path)

            except EnvironmentError as _error:
                raise Error("unable to transfer product to destination path '%s' [%s]" %
                            (abs_product_path, _error))

    def put2(self, file_path, archive, product):
        physical_name = product.core.physical_name
        archive_path = product.core.archive_path

        abs_archive_path = os.path.realpath(os.path.join(self._root, archive_path))
        abs_product_path = os.path.join(abs_archive_path, physical_name)

        # Create destination location for product
        try:
            util.make_path(abs_archive_path)
        except EnvironmentError as _error:
            raise Error("cannot create parent destination path '%s' [%s]" % (abs_archive_path, _error))

        # Create enclosing directory if required.
        plugin = archive.product_type_plugin(product.core.product_type)
        if plugin.use_enclosing_directory:
            try:
                util.make_path(abs_product_path)
                abs_product_path = os.path.join(abs_product_path, physical_name)
            except EnvironmentError as _error:
                raise Error("cannot create parent destination path '%s' [%s]" % (abs_product_path, _error))

        # Move the file into its destination
        try:
            os.rename(file_path, abs_product_path)

        except EnvironmentError as _error:
            raise Error("unable to transfer product to destination path '%s' [%s]" %
                        (abs_product_path, _error))

    def get(self, product_path, target_path, plugin, use_symlinks=False):
        try:
            if use_symlinks:
                if plugin.use_enclosing_directory:
                    for basename in os.listdir(product_path):
                        os.symlink(os.path.join(product_path, basename), os.path.join(target_path, basename))
                else:
                    os.symlink(product_path, os.path.join(target_path, os.path.basename(product_path)))
            else:
                if plugin.use_enclosing_directory:
                    for basename in os.listdir(product_path):
                        util.copy_path(os.path.join(product_path, basename), target_path, resolve_root=True)
                else:
                    util.copy_path(product_path, target_path, resolve_root=True)

        except EnvironmentError as _error: #  TODO product undefined
            raise Error("unable to retrieve product '%s' (%s) [%s]" % (product.core.product_name, product.core.uuid,
                                                                       _error))

    def size(self, product_path, plugin):
        return util.product_size(product_path)

    def delete(self, product_path, properties, plugin):
        if not os.path.lexists(product_path):
            # If the product does not exist, do not consider this an error.
            return

        try:
            with util.TemporaryDirectory(prefix=".remove-", suffix="-%s" % properties.core.uuid.hex,
                                         dir=os.path.dirname(product_path)) as tmp_path:

                # Move product into the temporary directory. When the temporary directory will be removed at the end of
                # this scope, the product will be removed along with it.
                assert properties.core.physical_name == os.path.basename(product_path)
                os.rename(product_path, os.path.join(tmp_path, os.path.basename(product_path)))

        except EnvironmentError as _error:
            raise Error("unable to remove product '%s' (%s) [%s]" % (properties.core.product_name, properties.core.uuid,
                                                                     _error))

    def move(self, product, archive_path, plugin):
        # Make target archive path
        abs_archive_path = os.path.realpath(os.path.join(self._root, archive_path))
        util.make_path(abs_archive_path)

        # Move files there
        product_path = self.product_path(product)
        os.rename(product_path, os.path.join(abs_archive_path, product.core.physical_name))
