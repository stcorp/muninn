#
# Copyright (C) 2014-2022 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function
from muninn._compat import string_types as basestring

import collections
import copy
import datetime
import errno
import functools
import inspect
import hashlib
import os
import re
import sys
from uuid import UUID, uuid4
import warnings

import muninn.config as config
import muninn.util as util

from muninn.core import Core
from muninn.exceptions import Error, StorageError
from muninn.extension import CascadeRule
from muninn.schema import Text, Boolean, Integer, Sequence, Mapping
from muninn.struct import Struct
from muninn import remote

HASH_ALGORITHMS = set(hashlib.algorithms_guaranteed)


class _ExtensionName(Text):
    _alias = "extension_name"

    @classmethod
    def validate(cls, value):
        super(_ExtensionName, cls).validate(value)
        if not re.match(r"[a-z][_a-z]*(\.[a-z][_a-z]*)*", value):
            raise ValueError("invalid value %r for type %r" % (value, cls.name()))


class _ExtensionList(Sequence):
    _alias = "extension_list"
    sub_type = _ExtensionName


class _ArchiveConfig(Mapping):
    _alias = "archive"

    root = Text(optional=True)
    backend = Text(optional=True)
    database = Text(optional=True)
    storage = Text(optional=True)
    use_symlinks = Boolean(optional=True)
    cascade_grace_period = Integer(optional=True)
    max_cascade_cycles = Integer(optional=True)
    namespace_extensions = _ExtensionList(optional=True)
    product_type_extensions = _ExtensionList(optional=True)
    remote_backend_extensions = _ExtensionList(optional=True)
    hook_extensions = _ExtensionList(optional=True)
    auth_file = Text(optional=True)


def _load_backend_module(name):
    module_name = "muninn.database.%s" % name

    try:
        __import__(module_name)
    except ImportError as e:
        raise Error("import of database %r (module %r) failed (%s)" % (name, module_name, e))

    return sys.modules[module_name]


def _load_storage_module(name):
    module_name = "muninn.storage.%s" % name

    try:
        __import__(module_name)
    except ImportError as e:
        raise Error("import of storage %r (module %r) failed (%s)" % (name, module_name, e))

    return sys.modules[module_name]


def _load_extension(name):
    try:
        __import__(name)
    except ImportError as e:
        raise Error("import of extension %r failed (%s)" % (name, e))

    return sys.modules[name]


def _inspect_nargs(func):
    try:
        getargspec = inspect.getfullargspec
    except AttributeError:
        getargspec = inspect.getargspec

    return len(getargspec(func).args)


def create(configuration, id=None):
    options = config.parse(configuration.get("archive", {}), _ArchiveConfig)
    _ArchiveConfig.validate(options)

    # Load and create the backend.
    if 'backend' in options:
        print("WARNING: the 'backend' option will be removed. Please use 'database' instead.", file=sys.stderr)
        backend = options.pop('backend')
    else:
        backend = options.pop('database')
    backend_module = _load_backend_module(backend)
    backend = backend_module.create(configuration)

    # Load and create the storage backend.
    storage_module = _load_storage_module(options.pop("storage", "fs"))
    storage = storage_module.create(configuration)

    # Create the archive.
    namespace_extensions = options.pop("namespace_extensions", [])
    product_type_extensions = options.pop("product_type_extensions", [])
    remote_backend_extensions = options.pop("remote_backend_extensions", [])
    hook_extensions = options.pop("hook_extensions", [])
    archive = Archive(backend=backend, storage=storage, id=id, **options)

    # Register core namespace.
    archive.register_namespace("core", Core)

    # Register custom namespaces.
    for name in namespace_extensions:
        extension = _load_extension(name)
        try:
            for namespace in extension.namespaces():
                archive.register_namespace(namespace, extension.namespace(namespace))
        except AttributeError:
            raise Error("extension %r does not implement the namespace extension API" % name)

    # Register product types.
    for name in product_type_extensions:
        extension = _load_extension(name)
        try:
            for product_type in extension.product_types():
                archive.register_product_type(product_type, extension.product_type_plugin(product_type))
        except AttributeError:
            raise Error("extension %r does not implement the product type extension API" % name)

    # Register custom remote backends.
    for name in remote_backend_extensions:
        extension = _load_extension(name)
        try:
            for remote_backend in extension.remote_backends():
                archive.register_remote_backend(remote_backend, extension.remote_backend(remote_backend))
        except AttributeError:
            raise Error("extension %r does not implement the remote backend extension API" % name)

    # Register hook extensions.
    for name in hook_extensions:
        extension = _load_extension(name)
        try:
            for hook_extension in extension.hook_extensions():
                archive.register_hook_extension(hook_extension, extension.hook_extension(hook_extension))
        except AttributeError:
            raise Error("extension %r does not implement the hook extension API" % name)

    return archive


_CORE_PROP_NAMES = [
    'uuid',
    'active',
    'product_name',
    'archive_path',
    'physical_name',
    'product_type',
]


class Archive(object):
    """Archive class

    The Archive class is used to represent and interact with Muninn archives.
    It provides functionality such as querying existing or ingesting new
    products. While at the core of the Muninn command-line tools, it can also
    be used directly.

    It is typically instantiated and used as follows:

        with muninn.open(archive_name) as archive:
            product = archive.ingest(file_path)

    Please see the Muninn documentation for details about how to configure
    a Muninn archive (and also set an environment variable so Muninn can find
    its configuration file.)

    """

    #: Archive id (usually name of configuration file)
    id = None

    def __init__(self, backend, storage, cascade_grace_period=0,
                 max_cascade_cycles=25, auth_file=None, id=None):
        self._cascade_grace_period = datetime.timedelta(minutes=cascade_grace_period)
        self._max_cascade_cycles = max_cascade_cycles
        self._auth_file = auth_file

        self._namespace_schemas = {}
        self._product_type_plugins = {}
        self._remote_backend_plugins = copy.copy(remote.REMOTE_BACKENDS)
        self._hook_extensions = collections.OrderedDict()
        self._export_formats = set()

        self._database = backend
        self._database.initialize(self._namespace_schemas)

        self._storage = storage

        self.id = id

    def register_namespace(self, namespace, schema):
        """Register a namespace.

        Arguments:
        namespace -- Namespace name
        schema    -- Schema definition of the namespace.
        """
        if not re.match(r"[a-z][_a-z]*(\.[a-z][_a-z]*)*", namespace):
            raise ValueError("invalid namespace name %s" % namespace)
        if namespace in self._namespace_schemas:
            raise Error("redefinition of namespace: \"%s\"" % namespace)

        self._namespace_schemas[namespace] = schema

    def namespace_schema(self, namespace):
        """Return the schema definition of the specified namespace.

        Arguments:
        namespace -- Namespace name
        """
        try:
            return self._namespace_schemas[namespace]
        except KeyError:
            raise Error("undefined namespace: \"%s\"; defined namespaces: %s" %
                        (namespace, util.quoted_list(self._namespace_schemas.keys())))

    def namespaces(self):
        """Return a list of registered namespaces."""
        return list(self._namespace_schemas.keys())

    def register_product_type(self, product_type, plugin):
        """Register a product type.

        Arguments:
        product_type -- Product type name
        plugin       -- Reference to an object that implements the product type
                        plugin API and as such takes care of the details of
                        extracting product properties from products of the
                        specified product type.
        """
        if product_type in self._product_type_plugins:
            raise Error("redefinition of product type: \"%s\"" % product_type)

        # Quick verify of the plugin interface
        for attr in ['use_enclosing_directory']:
            if not hasattr(plugin, attr):
                raise Error("missing '%s' attribute in plugin for product type \"%s\"" % (attr, product_type))

        if hasattr(plugin, 'use_hash') and not hasattr(plugin, 'hash_type'):
            warnings.warn("'use_hash' option is deprecated (use 'hash_type')")

        methods = ['identify', 'analyze', 'archive_path']
        if plugin.use_enclosing_directory:
            methods += ['enclosing_directory']
        for method in methods:
            if not hasattr(plugin, method):
                raise Error("missing '%s' method in plugin for product type \"%s\"" % (method, product_type))

        self._product_type_plugins[product_type] = plugin
        self._update_export_formats(plugin)

    def product_type_plugin(self, product_type):
        """Return a reference to the specified product type plugin.

        product_type -- Product type name
        """
        try:
            return self._product_type_plugins[product_type]
        except KeyError:
            raise Error("undefined product type: \"%s\"; defined product types: %s" %
                        (product_type, util.quoted_list(self._product_type_plugins.keys())))

    def product_types(self):
        """Return a list of registered product types."""
        return list(self._product_type_plugins.keys())

    def register_remote_backend(self, remote_backend, plugin):
        """Register a remote backend

        Arguments:
        remote_backend -- Remote backend name.
        plugin         -- Reference to an object that implements the remote
                          backend plugin API and as such takes care of the
                          details of extracting product properties from
                          products of the specified remote backend.

        """
        if remote_backend in self._remote_backend_plugins:
            raise Error("redefinition of remote backend: \"%s\"" % remote_backend)

        self._remote_backend_plugins[remote_backend] = plugin

    def remote_backend(self, remote_backend):
        """Return a reference to the specified remote backend plugin

        Arguments:
        remote_backend -- Remote backend name
        """
        try:
            return self._remote_backend_plugins[remote_backend]
        except KeyError:
            raise Error("undefined remote backend: \"%s\"; defined remote backends: %s" %
                        (remote_backend, util.quoted_list(self._remote_backend.keys())))

    def remote_backends(self):
        """Return a list of supported remote backends."""
        return list(self._remote_backend_plugins.keys())

    def register_hook_extension(self, hook_extension, plugin):
        """Register a hook extension.

        Arguments:
        hook_extension -- Hook extension name
        plugin         -- Reference to an object that implements the hook
                          extension plugin API
        """
        if hook_extension in self._hook_extensions:
            raise Error("redefinition of hook extension: \"%s\"" % hook_extension)

        self._hook_extensions[hook_extension] = plugin

    def hook_extension(self, hook_extension):
        """Return the hook extension with the specified name.

        Arguments:
        hook_extension -- Hook extension name
        """
        try:
            return self._hook_extensions[hook_extension]
        except KeyError:
            raise Error("undefined hook extension: \"%s\"; defined hook extensions: %s" %
                        (hook_extension, util.quoted_list(self._hook_extensions.keys())))

    def hook_extensions(self):
        """Return a list of supported hook extensions."""
        return list(self._hook_extensions.keys())

    def _plugin_hash_type(self, plugin):
        if hasattr(plugin, 'hash_type'):
            if not plugin.hash_type:
                return None
            else:
                return plugin.hash_type

        elif hasattr(plugin, 'use_hash'):
            if plugin.use_hash:
                return 'sha1'
            else:
                return None

        else:
            return 'md5'  # default after use_hash deprecation

    def _extract_hash_type(self, hash_value):
        prefix, middle, _ = hash_value.partition(':')
        if middle == ':' and prefix in HASH_ALGORITHMS:
            return prefix

    def cleanup_derived_products(self):
        """Clean up all derived products for which the source products no
        longer exist, as specified by the cascade rule configured in the
        respective product type plugins.

        Please see the Muninn documentation for more information on how
        to configure cascade rules.
        """
        repeat = True
        cycle = 0
        while repeat and cycle < self._max_cascade_cycles:
            repeat = False
            cycle += 1
            for product_type in self.product_types():
                plugin = self.product_type_plugin(product_type)

                cascade_rule = getattr(plugin, "cascade_rule", CascadeRule.IGNORE)
                if cascade_rule == CascadeRule.IGNORE:
                    continue

                strip = cascade_rule in (CascadeRule.CASCADE_PURGE_AS_STRIP, CascadeRule.STRIP)
                products = self._database.find_products_without_source(product_type, self._cascade_grace_period, strip)
                if products:
                    repeat = True

                if strip:
                    for product in products:
                        self._strip(product)
                else:
                    for product in products:
                        self._purge(product)

                if cascade_rule in (CascadeRule.CASCADE_PURGE_AS_STRIP, CascadeRule.CASCADE_PURGE):
                    continue

                products = self._database.find_products_without_available_source(product_type)
                if products:
                    repeat = True

                if cascade_rule in (CascadeRule.STRIP, CascadeRule.CASCADE):
                    for product in products:
                        self._strip(product)
                else:
                    for product in products:
                        self._purge(product)

    def _get_product(self, uuid=None, namespaces=None, property_names=None, must_exist=True, **kwargs):
        if uuid is not None:
            kwargs['uuid'] = uuid

        if namespaces is None:
            namespaces = self.namespaces()

        cond = ' and '.join(['%s == @%s' % (key, key) for key in kwargs])

        products = self.search(cond, parameters=kwargs, namespaces=namespaces, property_names=property_names)

        if len(products) == 0:
            if must_exist:
                cond = ' and '.join(['%s=%s' % item for item in kwargs.items()])
                raise Error('No product found: %s' % cond)
            else:
                return None

        else:
            assert len(products) == 1
            return products[0]

    # TODO for performance, we may want to call something like 'run_for_uuids' instead of search
    def _get_products(self, where, parameters=None, namespaces=None, property_names=None):
        if isinstance(where, basestring):
            return self.search(where, parameters=parameters, namespaces=namespaces, property_names=property_names)

        if isinstance(where, UUID):
            where = [where]
        elif isinstance(where, Struct):
            where = [where]
        else:
            try:
                where = list(where)
            except:
                raise Error('Invalid product selection')

        products = []
        for term in where:
            if isinstance(term, UUID):
                product_uuid = term
            elif isinstance(term, Struct):
                product_uuid = term.core.uuid
            else:
                raise Error('Invalid product selection')

            where = 'uuid == @uuid'  # TODO simplify to _get_product(product_uuid)..?
            parameters = {'uuid': product_uuid}
            matches = self.search(where, parameters=parameters, namespaces=namespaces, property_names=property_names)
            if len(matches) == 0:
                raise Error("product with uuid '%s' not found" % product_uuid)
            else:
                assert len(matches) == 1

            products.append(matches[0])

        return products

    def _product_path(self, product):
        if getattr(product.core, "archive_path", None) is None:
            return None

        return self._storage.product_path(product)

    def _purge(self, product):
        # get full product
        product = self._get_product(product.core.uuid)

        # Remove the product from the product catalogue.
        self._database.delete_product_properties(product.core.uuid)

        # Remove any data in storage associated with the product.
        self._remove(product)

        # Run the post remove hook (if defined by the product type plug-in or hook extensions).
        plugin = self.product_type_plugin(product.core.product_type)
        self._run_hooks('post_remove_hook', plugin, product, reverse=True)

    def _relocate(self, product, properties=None, paths=None):
        """Relocate a product to the archive_path reported by the product type plugin.
        Returns the new archive_path if the product was moved and optionally updated
        (local) product paths.
        """
        product_archive_path = product.core.archive_path
        if properties:
            product = copy.deepcopy(product)
            product.update(properties)
        plugin = self.product_type_plugin(product.core.product_type)
        plugin_archive_path = plugin.archive_path(product)

        if product_archive_path != plugin_archive_path:
            paths = self._storage.move(product, plugin_archive_path, paths)
            return plugin_archive_path, paths
        else:
            return None, paths

    def _remove(self, product):
        # If the product has no data in storage associated with it, return.
        product_path = self._product_path(product)
        if product_path is None:
            # If the product does not exist, do not consider this an error.
            return

        # Remove the data associated with the product from storage.
        self._storage.delete(product_path, product)

    def _retrieve(self, product, target_path, use_symlinks=False):
        # Determine the path of the product in storage.
        product_path = self._product_path(product)
        if product_path is None:
            raise Error("no data available for product '%s' (%s)" % (product.core.product_name, product.core.uuid))

        # Get the product type specific plug-in.
        plugin = self.product_type_plugin(product.core.product_type)
        use_enclosing_directory = plugin.use_enclosing_directory

        # Symbolic link or copy the product at or to the specified target directory.
        self._storage.get(product, product_path, target_path, use_enclosing_directory, use_symlinks)

        return os.path.join(target_path, os.path.basename(product_path))

    def _strip(self, product):
        # Set the archive path to None to indicate the product has no data in storage associated with it.
        self.update_properties(Struct({'core': {'active': True, 'archive_path': None, 'archive_date': None}}),
                               product.core.uuid)

        # Remove any data in storage associated with the product.
        self._remove(product)

    def _update_export_formats(self, plugin):
        # Find all callables of which the name starts with "export_". The remainder of the name is used as the name of
        # the export format.
        for export_method_name in dir(plugin):
            if not export_method_name.startswith("export_"):
                continue

            _, _, export_format = export_method_name.partition("_")
            if not export_format:
                continue

            export_method = getattr(plugin, export_method_name)
            if export_method is None or not hasattr(export_method, "__call__"):
                continue

            self._export_formats.add(export_format)

    def _update_metadata_date(self, properties):
        """ add a core.metadata_date field if it did not yet exist and set it to the current date """
        if "core" not in properties:
            properties.core = Struct()
        properties.core.metadata_date = self._database.server_time_utc()

    def _check_paths(self, paths, action):
        if isinstance(paths, basestring):
            paths = [paths]

        if not paths:
            raise Error("nothing to %s" % action)

        # Use absolute paths to make error messages more useful, and to avoid broken links when ingesting/attaching
        # a product using symbolic links.
        paths = [os.path.realpath(path) for path in paths]

        # Ensure that the set of files and / or directories that make up the product does not contain duplicate
        # basenames.
        basenames = [os.path.basename(path) for path in paths]
        if len(set(basenames)) < len(basenames):
            raise Error("basename of each part should be unique for multi-part products")

        return paths

    def _analyze_paths(self, plugin, paths):
        metadata = plugin.analyze(paths)

        if hasattr(plugin, 'namespaces'):
            namespaces = plugin.namespaces
        else:
            namespaces = []

        for namespace in metadata:
            if namespace != 'core' and namespace not in namespaces:
                warnings.warn("plugin.namespaces does not contain '%s'" % namespace, DeprecationWarning)

        if isinstance(metadata, (tuple, list)):
            properties, tags = metadata
        else:
            properties, tags = metadata, []

        return properties, tags

    def attach(self, paths, product_type=None, use_symlinks=None,
               verify_hash=False, verify_hash_before=False,
               use_current_path=False, force=False):
        """Add a product to the archive using an existing metadata record in the database.

        This function acts as the inverse of a strip(). A metadata record for this product should already exist in
        the database and no product should exist for it in the archive.

        The existing metadata record is found by performing a search based on product_type and physical_name.

        Arguments:
        paths            -- List of paths pointing to product files.
        product_type     -- Product type of the product to ingest. If left unspecified, an attempt will be made to
                            determine the product type automatically. By default, the product type will be determined
                            automatically.
        use_symlinks     -- If set to True, symbolic links to the original product will be stored in the archive
                            instead of a copy of the original product. If set to None, the value of the corresponding
                            archive wide configuration option will be used. By default, the archive configuration will
                            be used.
                            This option is ignored if use_current_path=True.
        verify_hash      -- If set to True then, after the ingestion, the product in the archive will be matched against
                            the hash from the metadata (only if the metadata contained a hash).
        verify_hash_before  --  If set to True then, before the product is attached to the archive, it will be matched
                            against the metadata hash (if it exists).
        use_current_path -- Ingest the product by keeping the file(s) at the current path (which must be inside the
                            root directory of the archive).
                            This option is ignored if ingest_product=False.
        force            -- If set to True, then skip default size check between product and existing metadata.

        Returns:
        The attached product.
        """
        paths = self._check_paths(paths, 'attach')

        if product_type is None:
            product_type = self.identify(paths)
        plugin = self.product_type_plugin(product_type)

        properties, tags = self._analyze_paths(plugin, paths)

        # Determine physical product name
        if plugin.use_enclosing_directory:
            physical_name = plugin.enclosing_directory(properties)
        elif len(paths) == 1:
            physical_name = os.path.basename(paths[0])
        else:
            raise Error("cannot determine physical name for multi-part product")

        # Find product in catalogue
        product = self._get_product(product_type=product_type, physical_name=physical_name,
                                    namespaces=get(plugin, 'namespaces', []))

        # Determine archive path
        if 'archive_path' in product.core:
            raise Error("product with physical_name '%s' is already in the archive" % physical_name)
        if use_current_path:
            archive_path = self._storage.current_archive_path(paths, product)
        else:
            archive_path = plugin.archive_path(product)

        # Check size match
        size = util.product_size(paths)
        if not force and size != product.core.size:
            raise Error("size mismatch between product and existing metadata")

        # Check hash match
        if verify_hash_before:
            stored_hash = getattr(product.core, 'hash', None)
            if stored_hash is not None:
                hash_type = self._extract_hash_type(stored_hash)
                if hash_type is None:
                    stored_hash = 'sha1:' + stored_hash
                    hash_type = 'sha1'
                product_hash = util.product_hash(paths, hash_type=hash_type)
                if product_hash != stored_hash:
                    raise Error("hash mismatch between product and existing metadata")

        # Determine hash using plugin hash type
        hash_type = self._plugin_hash_type(plugin)
        if hash_type is not None:
            product_hash = util.product_hash(paths, hash_type=hash_type)
        else:
            product_hash = None

        # Set properties and deactivate while we attach the product
        product.core.active = False
        product.core.size = size
        product.core.archive_path = archive_path
        metadata = {
            'active': False,
            'size': size,
            'archive_path': archive_path
        }
        if hash_type is not None:
            metadata['hash'] = product_hash
        self.update_properties(Struct({'core': metadata}), product.core.uuid)

        # Store the product into the archive.
        use_enclosing_directory = plugin.use_enclosing_directory
        try:
            self._storage.put(paths, product, use_enclosing_directory, use_symlinks)
        except Exception as e:
            if not (isinstance(e, StorageError) and e.anything_stored):
                # reset state to before attach
                metadata = {'active': True, 'archive_path': None}
                self.update_properties(Struct({'core': metadata}), product.core.uuid)
            if isinstance(e, StorageError):
                raise e.orig
            else:
                raise

        # Verify product hash after copy
        if verify_hash:
            if self.verify_hash(product.core.uuid):
                raise Error("ingested product has incorrect hash")

        # Activate product
        product.core.active = True
        product.core.archive_date = self._database.server_time_utc()
        metadata = {
            'active': True,
            'archive_date': product.core.archive_date,
        }
        self.update_properties(Struct({'core': metadata}), product.core.uuid)

        return product

    def auth_file(self):
        """Return the path of the authentication file to download from remote locations."""
        return self._auth_file

    def close(self):
        """Close the archive immediately instead of when (and if) the archive
        instance is collected.

        Using the archive after calling this function results in undefined behavior.
        """
        self._database.disconnect()

    def count(self, where="", parameters={}):
        """Return the number of products matching the specified search expression.

        Arguments:
        where       --  Search expression.
        parameters  --  Parameters referenced in the search expression (if any).

        """
        return self._database.count(where, parameters)

    def create_properties(self, properties, disable_hooks=False):
        """Create a record for the given product in the product catalogue.
        An important side effect of this operation is that it will fail if:

            1. The core.uuid is not unique within the product catalogue.
            2. The combination of core.archive_path and core.physical_name is
               not unique within the product catalogue.

        Arguments:
        properties    -- Product properties
        disable_hooks -- Do not execute any hooks (default False)
        """
        self._update_metadata_date(properties)
        self._database.insert_product_properties(properties)

        # Run the post create hook (if defined by the product type plug-in or hook extensions).
        if not disable_hooks:
            plugin = self.product_type_plugin(properties.core.product_type)
            self._run_hooks('post_create_hook', plugin, properties)

    def delete_properties(self, where="", parameters={}):
        """Remove properties for one or more products from the catalogue.

        This function will _not_ remove any product files from storage and
        will _not_ trigger any of the specific cascade rules.

        Arguments:
        where       --  Search expression or one or more product uuid(s) or properties.
        parameters  --  Parameters referenced in the search expression (if any).

        Returns:
        The number of updated products
        """
        products = self._get_products(where, parameters, property_names=['uuid'])
        for product in products:
            self._database.delete_product_properties(product.core.uuid)
        return len(products)

    def derived_products(self, uuid):
        """Return the UUIDs of the products that are linked to the given
        product as derived products.

        Arguments:
        uuid -- Product UUID
        """
        return self._database.derived_products(uuid)

    def destroy(self):
        """Completely remove the archive, including both the products and the
        product catalogue.

        Using the archive after calling this function results in undefined
        behavior. The prepare() function can be used to bring the archive back
        into a useable state.
        """
        self.destroy_catalogue()

        self._storage.destroy()

    def destroy_catalogue(self):
        """Completely remove the catalogue database, but leaving the datastore in storage untouched.

        Using the archive after calling this function results in undefined behavior.
        Using the prepare_catalogue() function and ingesting all products again, can bring the archive
        back into a useable state.
        """
        # Call the backend to remove anything related to the archive.
        if self._database.exists():
            self._database.destroy()

    def export(self, where="", parameters={}, target_path=os.path.curdir, format=None):
        """Export one or more products from the archive.

        By default, a copy of the original product will be retrieved from the archive. This default behavior can be
        customized by the product type plug-in. For example, the custom implementation for a certain product type might
        retrieve one or more derived products and bundle them together with the product itself.

        Arguments:
        where           --  Search expression or one or more product uuid(s) or properties.
        parameters      --  Parameters referenced in the search expression (if any).
        target_path     --  Directory in which the retrieved products will be stored.
        format          --  Format in which the products will be exported.

        Returns:
        A list with the export paths for the exported products (when a search expression or multiple properties/uuids
        were passed), or a single export path.
        """
        export_method_name = "export"
        if format is not None:
            if re.match("[a-zA-Z]\\w*$", format) is None:
                raise Error("invalid export format '%s'" % format)
            export_method_name += "_" + format

        products = self._get_products(where, parameters, namespaces=self.namespaces())

        result = []
        for product in products:
            if not product.core.active:
                raise Error("product '%s' (%s) not available" % (product.core.product_name, product.core.uuid))

            # Use the format specific export_<format>() method from the product type plug-in, or the default export()
            # method if no format was specified. Call self._retrieve() as a fall back if the product type plug-in does
            # not define its own export() method.
            #
            # Note the use of getattr() / hasattr() instead of a try + except AttributeError block, to avoid hiding
            # AttributeError instances raised by the plug-in.

            plugin = self.product_type_plugin(product.core.product_type)
            export_method = getattr(plugin, export_method_name, None)

            if export_method is not None:
                if _inspect_nargs(export_method) == 5:
                    def _export(paths):
                        exported_path = export_method(self, product, target_path, paths)
                        result.append(exported_path)
                    self._storage.run_for_product(product, _export, plugin.use_enclosing_directory)
                else:
                    exported_path = export_method(self, product, target_path)
                    result.append(exported_path)

            elif format is not None:
                raise Error("export format '%s' not supported for product '%s' (%s)" %
                            (format, product.core.product_name, product.core.uuid))

            else:
                exported_path = self._retrieve(product, target_path, False)
                result.append(exported_path)

        if isinstance(where, UUID):
            return result[0]
        else:
            return result

    def export_formats(self):
        """Return a list of supported alternative export formats."""
        return list(self._export_formats)

    @staticmethod
    def generate_uuid():
        """Return a new generated UUID that can be used as UUID for a product metadata record"""
        return uuid4()

    def identify(self, paths):
        """Determine the product type of the product (specified as a single path, or a list of paths if it is a
        multi-part product).

        Arguments:
        paths            -- List of paths pointing to product files.

        Returns:
        Determined product type
        """
        for product_type, plugin in self._product_type_plugins.items():
            if plugin.identify(paths):
                return product_type

        raise Error("unable to identify product: \"%s\"" % paths)

    def ingest(self, paths, product_type=None, properties=None, ingest_product=True, use_symlinks=None,
               verify_hash=False, use_current_path=False, force=False):
        """Ingest a product into the archive. Multiple paths can be specified, but the set of files and/or directories
        these paths refer to is always ingested as a single logical product.

        Product ingestion consists of two steps. First, product properties are extracted from the product and are used
        to create an entry for the product in the product catalogue. Second, the product itself is ingested, either by
        copying the product or by creating symbolic links to the product.

        If the product to be ingested is already located at the target location within the archive (and there was not
        already another catalogue entry pointing to it), muninn will leave the product at its location as-is, and won't
        try to copy/symlink it.

        Arguments:
        paths            -- List of paths pointing to product files.
        product_type     -- Product type of the product to ingest. If left unspecified, an attempt will be made to
                            determine the product type automatically. By default, the product type will be determined
                            automatically.
        properties       -- Used as product properties if specified. No properties will be extracted from the product
                            in this case.
        ingest_product   -- If set to False, the product itself will not be ingested into the archive, only its
                            properties. By default, the product will be ingested.
        use_symlinks     -- If set to True, symbolic links to the original product will be stored in the archive
                            instead of a copy of the original product. If set to None, the value of the corresponding
                            archive wide configuration option will be used. By default, the archive configuration will
                            be used.
                            This option is ignored if use_current_path=True.
        verify_hash      -- If set to True then, after the ingestion, the product in the archive will be matched against
                            the hash from the metadata (only if the metadata contained a hash).
        use_current_path -- Ingest the product by keeping the file(s) at the current path (which must be inside the
                            root directory of the archive).
                            This option is ignored if ingest_product=False.
        force            -- If set to True then any existing product with the same type and name (unique constraint)
                            will be removed before ingestion, including partially ingested products.
                            NB. Depending on product type specific cascade rules, removing a product can result in one
                            or more derived products being removed (or stripped) along with it.

        Returns:
        The ingested product
        """
        paths = self._check_paths(paths, 'ingest')

        # Get the product type plug-in.
        if product_type is None:
            product_type = self.identify(paths)
        plugin = self.product_type_plugin(product_type)

        # Extract product metadata.
        if properties is None:
            properties, tags = self._analyze_paths(plugin, paths)
        else:
            properties, tags = copy.deepcopy(properties), []

        assert properties is not None and "core" in properties
        assert "product_name" in properties.core and properties.core.product_name, \
            "product_name is required in core.properties"

        # Set core product properties that are not determined by the plugin.
        # Note that metadata_date is set automatically by create_properties()
        # and archive_date is properly set when we activate the product.
        properties.core.uuid = self.generate_uuid()
        properties.core.active = False
        properties.core.hash = None
        properties.core.size = util.product_size(paths)  # TODO determine after?
        properties.core.metadata_date = None
        properties.core.archive_date = None
        properties.core.archive_path = None
        properties.core.product_type = product_type
        properties.core.physical_name = None

        # Determine physical product name.
        if plugin.use_enclosing_directory:
            properties.core.physical_name = plugin.enclosing_directory(properties)
        elif len(paths) == 1:
            properties.core.physical_name = os.path.basename(paths[0])
        else:
            raise Error("cannot determine physical name for multi-part product")

        # Determine archive path
        if ingest_product:
            if use_current_path:
                properties.core.archive_path = self._storage.current_archive_path(paths, properties)
            else:
                properties.core.archive_path = plugin.archive_path(properties)

        # Remove existing product with the same product type and name before ingesting
        if force:
            existing = self._get_product(product_type=properties.core.product_type,
                                         product_name=properties.core.product_name,
                                         must_exist=False)
            if existing is not None:
                if 'archive_path' in existing.core and existing.core.archive_path is not None:
                    ingest_path = os.path.dirname(paths[0])
                    if plugin.use_enclosing_directory:
                        ingest_path = os.path.dirname(ingest_path)
                    current_path = self.root()
                    if existing.core.archive_path:
                        current_path = os.path.join(current_path, existing.core.archive_path)
                    if existing.core.archive_path != properties.core.archive_path:
                        raise Error('cannot force ingest because of archive_path mismatch')
                    if ingest_path == current_path:
                        # do not remove the product being ingested (only remove from catalogue)
                        self.delete_properties(existing.core.uuid)
                    else:
                        self.remove(existing.core.uuid, force=True)
                else:
                    self.delete_properties(existing.core.uuid)

        self.create_properties(properties, disable_hooks=True)

        # Try to determine the product hash and ingest the product into the archive.
        try:
            # Determine product hash. Since it is an expensive operation, the hash is computed after inserting the
            # product properties so we won't needlessly compute it for products that fail ingestion into the catalogue.
            hash_type = self._plugin_hash_type(plugin)
            if hash_type is not None:
                try:
                    properties.core.hash = util.product_hash(paths, hash_type=hash_type)
                except EnvironmentError as _error:
                    raise Error("cannot determine product hash [%s]" % (_error,))
                # Update the product hash in the product catalogue.
                self.update_properties(Struct({'core': {'hash': properties.core.hash}}), properties.core.uuid)

            if ingest_product:
                use_enclosing_directory = plugin.use_enclosing_directory
                self._storage.put(paths, properties, use_enclosing_directory, use_symlinks)
                properties.core.archive_date = self._database.server_time_utc()

        except Exception as e:
            if not (isinstance(e, StorageError) and e.anything_stored):
                # Try to remove the entry for this product from the product catalogue.
                self._database.delete_product_properties(properties.core.uuid)
            if isinstance(e, StorageError):
                raise e.orig
            else:
                raise

        # Update archive date.
        metadata = {
            'archive_date': properties.core.archive_date,
        }
        self.update_properties(Struct({'core': metadata}), properties.core.uuid)

        # Set product tags.
        self._database.tag(properties.core.uuid, tags)

        # Verify product hash after copy
        if ingest_product and verify_hash:
            if self.verify_hash(properties.core.uuid):
                raise Error("ingested product has incorrect hash")

        # Run post create/ingest hooks (if defined by the product type plug-in or hook extensions).
        if not ingest_product:
            self._run_hooks('post_create_hook', plugin, properties)
        else:
            self._run_hooks('post_ingest_hook', plugin, properties, paths=paths)

        # Activate product.
        properties.core.active = True
        metadata = {
            'active': properties.core.active,
        }
        self.update_properties(Struct({'core': metadata}), properties.core.uuid)

        return properties

    def _run_hooks(self, hook_name, plugin, properties, reverse=False, paths=None):
        plugins = [plugin] + list(self._hook_extensions.values())
        if reverse:
            plugins = reversed(plugins)

        for plugin in plugins:
            hook_method = getattr(plugin, hook_name, None)
            if hook_method is not None:
                if _inspect_nargs(hook_method) == 4:
                    hook_method(self, properties, paths)
                else:
                    hook_method(self, properties)

    def link(self, uuid, source_uuids):
        """Link a product to one or more source products.

        Arguments:
        uuid         -- Product UUID
        source_uuids -- Source UUIDs
        """
        if isinstance(source_uuids, UUID):
            source_uuids = [source_uuids]

        self._database.link(uuid, source_uuids)

    def prepare(self, force=False):
        """Prepare the archive for (first) use.

        The root path will be created and the product catalogue will be
        initialized such that the archive is ready for use.

        Arguments:
        force   --  If set to True then any existing products and / or product
                    catalogue will be removed.
        """
        if not force:
            if self._storage.exists():
                raise Error("storage already exists")
            if self._database.exists():
                raise Error("database already exists")

        # Remove anything related to the archive.
        self.destroy()

        # Prepare the archive for use.
        self._database.prepare()
        self._storage.prepare()

    def prepare_catalogue(self, dry_run=False):
        """Prepare the catalogue of the archive for (first) use."""
        return self._database.prepare(dry_run=dry_run)

    def product_path(self, uuid_or_properties):
        """Return the path in storage to the specified product.

        Arguments:
        uuid_or_properties: UUID or product
        """
        if isinstance(uuid_or_properties, Struct):
            product = uuid_or_properties
        else:
            property_names = ['archive_path', 'physical_name']
            if isinstance(uuid_or_properties, UUID):
                product = self._get_product(uuid_or_properties,
                                            property_names=property_names)
            else:
                products = self._get_product(product_name=uuid_or_properties,
                                             property_names=property_names)

        return os.path.join(self._storage.global_prefix, self._product_path(product))

    def pull(self, where="", parameters={}, verify_hash=False, verify_hash_download=False):
        """Pull one or more remote products into the archive.

        Products should have a valid remote_url core metadata field and they should not yet exist in the local
        archive (i.e. the archive_path core metadata field should not be set).

        Arguments:
        where         --  Search expression or one or more product uuid(s) or properties.
        parameters    --  Parameters referenced in the search expression (if any).
        verify_hash   --  If set to True then, after the pull, the product in the archive will be matched against
                          the hash from the metadata (only if the metadata contained a hash).
        verify_hash_download  --  If set to True then, before the product is stored in the archive, the pulled
                          product will be matched against the metadata hash (if it exists).

        Returns:
        The number of pulled products
        """
        products = self._get_products(where, parameters, namespaces=self.namespaces())

        for product in products:
            if not product.core.active:
                raise Error("product '%s' (%s) not available" % (product.core.product_name, product.core.uuid))
            if 'archive_path' in product.core:
                raise Error("product '%s' (%s) is already in the local archive" %
                            (product.core.product_name, product.core.uuid))
            if 'remote_url' not in product.core:
                raise Error("product '%s' (%s) does not have a remote_url" %
                            (product.core.product_name, product.core.uuid))

            plugin = self.product_type_plugin(product.core.product_type)
            product.core.archive_path = plugin.archive_path(product)
            use_enclosing_directory = plugin.use_enclosing_directory

            # set archive_path and deactivate while we pull it in
            metadata = {'active': False, 'archive_path': product.core.archive_path}
            self.update_properties(Struct({'core': metadata}), product.core.uuid)

            def _pull(paths):
                # update archive_date, size
                product_path = self._product_path(product)
                size = self._storage.size(product_path)
                metadata = {'archive_date': self._database.server_time_utc(), 'size': size}
                self.update_properties(Struct({'core': metadata}), product.core.uuid)

                # verify product hash.
                if verify_hash and 'hash' in product.core:
                    if self.verify_hash(product.core.uuid):
                        raise Error("pulled product '%s' (%s) has incorrect hash" %
                                    (product.core.product_name, product.core.uuid))

                # Run the post pull hook (if defined by the product type plug-in or hook extensions).
                self._run_hooks('post_pull_hook', plugin, product, paths=paths)

            # pull product
            try:
                retrieve_files = remote.retrieve_function(self, product, verify_hash_download)
                self._storage.put(None, product, use_enclosing_directory, use_symlinks=False,
                                  retrieve_files=retrieve_files, run_for_product=_pull)
            except Exception as e:
                if not (isinstance(e, StorageError) and e.anything_stored):
                    # reset state to before pull
                    metadata = {'active': True, 'archive_path': None, 'archive_date': None}
                    self.update_properties(Struct({'core': metadata}), product.core.uuid)
                if isinstance(e, StorageError):
                    raise e.orig
                else:
                    raise

            # activate product
            metadata = {'active': True}
            self.update_properties(Struct({'core': metadata}), product.core.uuid)

        return len(products)

    def rebuild_properties(self, uuid, disable_hooks=False, use_current_path=False):
        """Rebuild product properties by re-extracting these properties (using product type plug-ins) from the
        products stored in the archive.
        Only properties and tags that are returned by the product type plug-in will be updated. Other properties or
        tags will remain as they were.

        Arguments:
        uuid             -- Product UUID
        disable_hooks    --  Disable product type hooks (not meant for routine operation).
        use_current_path -- Do not attempt to relocate the product to the location specified in the product
                            type plug-in. Useful for read-only archives.

        """
        restricted_properties = set(["uuid", "active", "hash", "size", "metadata_date", "archive_date", "archive_path",
                                     "product_type", "physical_name"])

        product = self._get_product(uuid)
        if not product.core.active:
            raise Error("product '%s' (%s) not available" % (product.core.product_name, product.core.uuid))

        # Determine the path of the product within storage
        product_path = self._product_path(product)
        if product_path is None:
            raise Error("no data available for product '%s' (%s)" % (product.core.product_name, product.core.uuid))

        # Extract product metadata.
        plugin = self.product_type_plugin(product.core.product_type)
        use_enclosing_directory = plugin.use_enclosing_directory

        def _rebuild_properties(paths):
            properties, tags = self._analyze_paths(plugin, paths)

            # Remove properties that should not be changed.
            assert "core" in properties
            for name in restricted_properties:
                try:
                    delattr(properties.core, name)
                except AttributeError:
                    pass

            # update size
            properties.core.size = self._storage.size(product_path)

            # Make sure product is stored in the correct location
            if not use_current_path:
                new_archive_path, paths = self._relocate(product, properties, paths)
                if new_archive_path is not None:
                    properties.core.archive_path = new_archive_path

            # if product type has disabled hashing, remove existing hash values
            stored_hash = getattr(product.core, 'hash', None)
            plugin_hash_type = self._plugin_hash_type(plugin)

            if plugin_hash_type is None:
                if stored_hash is not None:
                    properties.core.hash = None

            # if product type has different hash algorithm, update hash values
            else:
                if stored_hash is None:
                    properties.core.hash = util.product_hash(paths, hash_type=plugin_hash_type)
                else:
                    hash_type = self._extract_hash_type(stored_hash)
                    if hash_type is None and plugin_hash_type == 'sha1':
                        properties.core.hash = plugin_hash_type + ':' + stored_hash
                    elif hash_type != plugin_hash_type:
                        properties.core.hash = util.product_hash(paths, hash_type=plugin_hash_type)

            # Update product properties.
            self.update_properties(properties, uuid=product.core.uuid, create_namespaces=True)

            # Update tags.
            self.tag(product.core.uuid, tags)

            # Run the post ingest hook (if defined by the product type plug-in or hook extensions).
            if not disable_hooks:
                product.update(properties)
                if 'hash' not in product.core:
                    product.core.hash = None
                self._run_hooks('post_ingest_hook', plugin, product, paths=paths)

        self._storage.run_for_product(product, _rebuild_properties, use_enclosing_directory)

    def rebuild_pull_properties(self, uuid, verify_hash=False, disable_hooks=False, use_current_path=False):
        """Refresh products by re-running the pull, but using the existing products stored in the archive.

        Arguments:
        uuid             -- Product UUID
        verify_hash   --  If set to True then the product in the archive will be matched against
                          the hash from the metadata (only if the metadata contained a hash).
        disable_hooks --  Disable product type hooks (not meant for routine operation).
        use_current_path -- Do not attempt to relocate the product to the location specified in the product
                            type plug-in. Useful for read-only archives.
        """
        product = self._get_product(uuid)
        if 'archive_path' not in product.core:
            raise Error("cannot update missing product")
        if 'remote_url' not in product.core:
            raise Error("cannot pull products that have no remote_url")

        plugin = self.product_type_plugin(product.core.product_type)
        use_enclosing_directory = plugin.use_enclosing_directory

        # make sure product is stored in the correct location
        if not use_current_path:
            new_archive_path, _ = self._relocate(product)
            if new_archive_path is not None:
                metadata = {'archive_path': new_archive_path}
                self.update_properties(Struct({'core': metadata}), product.core.uuid)
                product.core.archive_path = new_archive_path

        # update size
        product_path = self._product_path(product)
        product.core.size = self._storage.size(product_path)

        def _rebuild_pull_properties(paths):
            # verify product hash.
            if verify_hash and 'hash' in product.core:
                if not self._verify_hash(product, paths):
                    raise Error("pulled product '%s' (%s) has incorrect hash" %
                                (product.core.product_name, product.core.uuid))

            # Run the post pull hook (if defined by the product type plug-in or hook extensions).
            if not disable_hooks:
                self._run_hooks('post_pull_hook', plugin, product, paths=paths)

        self._storage.run_for_product(product, _rebuild_pull_properties, use_enclosing_directory)

    def remove(self, where="", parameters={}, force=False, cascade=True):
        """Remove one or more products from the archive, both from storage as well as from the product catalogue.
        Return the number of products removed.

        NB. Depending on product type specific cascade rules, removing a product can result in one or more derived
        products being removed (or stripped) along with it. Such products are _not_ included in the returned count.

        Arguments:
        where       --  Search expression or one or more product uuid(s) or properties.
        parameters  --  Parameters referenced in the search expression (if any).
        force       --  If set to True, also remove partially ingested products. This affects products for which a
                        failure occured during ingestion, as well as products in the process of being ingested. Use
                        this option with care.
        cascade     --  Apply cascade rules to strip/remove dependent products.
        """
        products = self._get_products(where, parameters, property_names=_CORE_PROP_NAMES)

        for product in products:
            if not product.core.active and not force:
                raise Error("product '%s' (%s) not available" % (product.core.product_name, product.core.uuid))

            self._purge(product)

        # Remove (or strip) derived products if necessary.
        if cascade and len(products) > 0:
            self.cleanup_derived_products()

        return len(products)

    def retrieve(self, where="", parameters={}, target_path=os.path.curdir, use_symlinks=False):
        """Retrieve one or more products from the archive.

        Arguments:
        where           --  Search expression or one or more product uuid(s) or properties.
        parameters      --  Parameters referenced in the search expression (if any).
        target_path     --  Directory under which the retrieved products will be stored.
        use_symlinks    --  If set to True, products will be retrieved as symbolic links to the original products kept
                            in the archive. If set to False, products will retrieved as copies of the original products.
                            By default, products will be retrieved as copies.

        Returns:
        A list with the target paths for the retrieved products (when a search expression or multiple properties/uuids
        were passed), or a single target path.
        """
        products = self._get_products(where, parameters, property_names=_CORE_PROP_NAMES)

        result = []
        for product in products:
            if not product.core.active or 'archive_path' not in product.core:
                raise Error("product '%s' (%s) not available" % (product.core.product_name, product.core.uuid))
            result.append(self._retrieve(product, target_path, use_symlinks))

        if isinstance(where, UUID):
            return result[0]
        else:
            return result

    def retrieve_properties(self, uuid, namespaces=[], property_names=[]):
        """Return properties for the specified product.

        Arguments:
        uuid        -- Product UUID
        namespaces  -- List of namespaces of which the properties should be retrieved. By default, only properties
                       defined in the "core" namespace will be retrieved.
        """
        return self._get_product(uuid, namespaces=namespaces, property_names=property_names)

    def root(self):
        """Return the archive root path."""
        return self._storage._root

    def search(self, where="", order_by=[], limit=None, parameters={}, namespaces=[], property_names=[]):
        """Search the product catalogue for products matching the specified search expression.

        Arguments:
        where       --  Search expression.
        order_by    --  A list of property names that determines the ordering of the results. If the list is empty, the
                        order of the results in undetermined and can very between calls to this function. Each property
                        name in this list can be provided with a '+' or '-' prefix, or without a prefix. A '+' prefix,
                        or no predix denotes ascending sort order, a '-' prefix denotes decending sort order.
        limit       --  Limit the maximum number of results to the specified number.
        parameters  --  Parameters referenced in the search expression (if any).
        namespaces  --  List of namespaces of which the properties should be retrieved. By default, only properties
                        defined in the "core" namespace will be retrieved.
        property_names
                    --  List of property names that should be returned. By default all properties of the "core"
                        namespace and those of the namespaces in the namespaces argument are included.
                        If this parameter is a non-empty list then only the referenced properties will be returned.
                        Properties are specified as '<namespace>.<identifier>'
                        (the namespace can be omitted for the 'core' namespace).
                        If the property_names parameter is provided then the namespaces parameter is ignored.

        Returns:
        A list of matching products
        """
        return self._database.search(where, order_by, limit, parameters, namespaces, property_names)

    def source_products(self, uuid):
        """Return the UUIDs of the products that are linked to the given product as source products.

        Arguments:
        uuid -- Product UUID
        """
        return self._database.source_products(uuid)

    def strip(self, where="", parameters={}, force=False, cascade=True):
        """Remove one or more products from storage only (not from the product catalogue).

        NB. Depending on product type specific cascade rules, stripping a product can result in one or more derived
        products being stripped (or removed) along with it.

        Arguments:
        where       --  Search expression or one or more product uuid(s) or properties.
        parameters  --  Parameters referenced in the search expression (if any).
        force       --  If set to True, also strip partially ingested products. This affects products for which a
                        failure occured during ingestion, as well as products in the process of being ingested. Use
                        this option with care.
        cascade     --  Apply cascade rules to strip/purge dependent products.

        Returns:
        The number of stripped products
        """
        products = self._get_products(where, parameters, property_names=_CORE_PROP_NAMES)

        for product in products:
            if not 'archive_path' in product.core:
                continue
            if not product.core.active and not force:
                raise Error("product '%s' (%s) not available" % (product.core.product_name, product.core.uuid))

            self._strip(product)

        # Strip (or remove) derived products if necessary.
        if cascade and len(products) > 0:
            self.cleanup_derived_products()

        return len(products)

    def summary(self, where="", parameters=None, aggregates=None, group_by=None, group_by_tag=False, order_by=None):
        """Return a summary of the products matching the specified search expression.

        Arguments:
        where         --  Search expression.
        parameters    --  Parameters referenced in the search expression (if any).
        aggregates    --  A list of property aggregates defined as "<property_name>.<reduce_fn>".
                          Properties need to be of type long, integer, real, text or timestamp.
                          The reduce function can be 'min', 'max', 'sum', or 'avg'.
                          'sum' and 'avg' are not possible for text and timestamp properties.
                          A special property 'validity_duration' (defined as validity_stop - validity_start) can also
                          be used.
        group_by      --  A list of property names whose values are used for grouping the aggregation results.
                          There will be a separate result row for each combination of group_by property values.
                          Properties need to be of type long, integer, boolean, text or timestamp.
                          Timestamps require a binning subscript which can be 'year', 'month', 'yearmonth', or 'date'
                          (e.g. 'validity_start.yearmonth').
        group_by_tag  --  If set to True, results will also be grouped by available tag values.
                          Note that products will be counted multiple times if they have multiple tags.
        order_by      --  A list of result column names that determines the ordering of the results. If the list is
                          empty, the order of the results is ordered by the `group_by` specification. Each name in the
                          list can have a '+' (ascending) or '-' (descending) prefix, or no prefix (ascending).

        Note that the property names must always include the namespace. 'core' is not assumed.
        """
        return self._database.summary(where, parameters, aggregates, group_by, group_by_tag, order_by)

    def tag(self, where=None, tags=None, parameters={}):
        """Set one or more tags on one or more product(s).

        Arguments:
        where       --  Search expression or one or more product uuid(s) or properties.
        tags        --  One or more tags
        parameters  --  Parameters referenced in the search expression (if any).
        """
        if isinstance(tags, basestring):
            tags = [tags]
        for tag in tags:
            if not isinstance(tag, basestring):
                raise Error('tag must be a string')

        if isinstance(where, UUID):
            self._database.tag(where, tags)
        else:
            products = self._get_products(where, parameters, property_names=['uuid'])
            for product in products:
                self._database.tag(product.core.uuid, tags)

    def tags(self, uuid):
        """Return the tags of a product.

        Arguments:
        uuid -- Product UUID
        """
        return self._database.tags(uuid)

    def unlink(self, uuid, source_uuids=None):
        """Remove the link between a product and one or more of its source products.

        Arguments:
        uuid         -- Product UUID
        source_uuids -- Source product UUIDs
        """
        if isinstance(source_uuids, UUID):
            source_uuids = [source_uuids]

        self._database.unlink(uuid, source_uuids)

    def untag(self, where=None, tags=None, parameters={}):
        """Remove one or more tags from one or more product(s).

        Arguments:
        where       --  Search expression or one or more product uuid(s) or properties.
        tags        --  One or more tags (default all existing tags)
        parameters  --  Parameters referenced in the search expression (if any).
        """
        if isinstance(tags, basestring):
            tags = [tags]

        if isinstance(where, UUID):
            self._database.untag(where, tags)
        else:
            products = self._get_products(where, parameters, property_names=['uuid'])
            for product in products:
                self._database.untag(product.core.uuid, tags)

    def update_properties(self, properties, uuid=None, create_namespaces=False):
        """Update product properties in the product catalogue. The UUID of the product to update will be taken from the
        "core.uuid" property if it is present in the specified properties. Otherwise, the UUID should be provided
        separately.

        This function allows any property to be changed with the exception of the product UUID, and therefore needs to
        be used with care. The recommended way to update product properties is to first retrieve them using either
        retrieve_properties() or search(), change the properties, and then use this function to update the product
        catalogue.

        Argument:
        properties         -- Product properties
        uuid               --  UUID of the product to update. By default, the UUID will be taken from the "core.uuid"
                               property.
        create_namespaces  --  Test if all namespaces are already defined for the product, and create them if needed
                               (default False)
        """
        if create_namespaces:
            if 'core' in properties and 'uuid' in properties.core:
                uuid = properties.core.uuid if uuid is None else uuid
                if uuid != properties.core.uuid:
                    raise Error("specified uuid does not match uuid included in the specified product properties")

            existing_product = self._get_product(uuid)
            new_namespaces = list(set(vars(properties)) - set(vars(existing_product)))
        else:
            new_namespaces = None
        self._update_metadata_date(properties)
        self._database.update_product_properties(properties, uuid=uuid, new_namespaces=new_namespaces)

    def _verify_hash(self, product, paths=None):
        if 'archive_path' in product.core:
            if 'hash' not in product.core:
                raise Error("no hash available for product '%s' (%s)" %
                            (product.core.product_name, product.core.uuid))

            product_path = self._product_path(product)
            if product_path is None:
                raise Error("no data available for product '%s' (%s)" % (product.core.product_name, product.core.uuid))

            stored_hash = product.core.hash
            hash_type = self._extract_hash_type(stored_hash)
            plugin = self.product_type_plugin(product.core.product_type)

            if hash_type is None:
                hash_type = 'sha1'  # default before use_hash deprecation
                stored_hash = 'sha1:' + stored_hash

            if paths is None:
                product_hash = functools.partial(util.product_hash, hash_type=hash_type)
                current_hash = self._storage.run_for_product(product, product_hash, plugin.use_enclosing_directory)
            else:
                current_hash = util.product_hash(paths, hash_type=hash_type)

            if current_hash != stored_hash:
                return False

        return True

    def verify_hash(self, where="", parameters={}):
        """Verify the hash for one or more products in the archive.

        Products that are not active or are not in the archive will be skipped.
        If there is no hash available in the metadata for a product then an
        error will be raised.

        Arguments:
        where           --  Search expression or one or more product uuid(s) or properties.
        parameters      --  Parameters referenced in the search expression (if any).

        Returns:
        A list of UUIDs of products for which the verification failed.
        """
        property_names = _CORE_PROP_NAMES + ['hash']
        products = self._get_products(where, parameters, property_names=property_names)

        failed_products = []
        for product in products:
            if not self._verify_hash(product):
                failed_products.append(product.core.uuid)

        return failed_products

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()
