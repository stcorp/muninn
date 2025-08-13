---
layout: page
title: Product Type Extension API
permalink: /ext_product_types/
---

# Product type extension API

## Global functions

``product_types([config])``
>   Return a list containing all product types for which this extension defines
>   plug-ins.
>   The ``config`` parameter is optional for the function definition.
>   If it exists, muninn will pass the content of the ``extension:<product_type_plugin>``
>   section as a single ``muninn.Struct`` positional argument to this function.

``product_type_plugin(product_type)``
>   Return an instance of a class that adheres to the product type plug-in API
>   (see below) and that implements this interface for the specified product
>   type. An exception should be raised if the extension does not support the
>   specified product type.

## Attributes

``use_enclosing_directory``
>   This variable should equal True if products of the type the plug-in is
>   designed to handle consist of multiple files, False otherwise.
>
>   In the majority of cases, a product is represented by a single path (i.e.
>   file, or directory). For such cases, this attribute should be set to
>   ``False``, and the ``analyze()`` method defined below can expect to be
>   called with a list containing a single path.
>
>   If a product consist of two or more files that belong together (without
>   them already being grouped together into a single top-level directory),
>   this attribute should be set to ``True``.

``hash_type``
>   Determines the hashing algorithm to be used for products of the type the
>   plug-in is designed to handle, e.g. ``md5`` or ``sha1``. The available
>   algorithms are those supported by the standard Python ``hashlib`` module.
>
>   If the attribute is set to ``None`` or the empty string, hashing is
>   disabled for the respective products. This can be useful, as hashing is
>   an expensive operation.
>
>   If the attribute is not set, the ``md5`` algorithm is used by default.

``cascade_rule``
>   Determines what should happen to products of the type the plug-in is
>   designed to handle when all products linked to these products (as source
>   products) have been stripped or removed. (A stripped product is a product
>   for which the data on disk has been deleted, but the entry in the product
>   catalogue has been kept).
>
>   Possible values are defined by the ``muninn.extension.CascadeRule``
>   enumeration and are given below:
>
>   ``CascadeRule.IGNORE``
>       Do nothing.
>
>   ``CascadeRule.CASCADE_PURGE_AS_STRIP``
>       If all source products of a product have been removed, strip the
>       product. If all source products of a product have been stripped, do
>       nothing.
>
>   ``CascadeRule.CASCADE_PURGE``
>       If all source products of a product have been removed, remove the
>       product. If all source products of a product have been stripped, do
>       nothing.
>
>   ``CascadeRule.STRIP``
>       If all source products of a product have been removed, strip the
>       product. If all source products of a product have been stripped, strip
>       the product.
>
>   ``CascadeRule.CASCADE``
>       If all source products of a product have been removed, remove the
>       product. If all source products of a product have been stripped, strip
>       the product.
>
>   ``CascadeRule.PURGE``
>       If all source products of a product have been removed, remove the
>       product. If all source products of a product have been stripped, remove
>       the product.
>
>   This attribute is optional. If it is left undefined, ``CascadeRule.IGNORE``
>   is assumed.

``namespaces``
>   This (optional) variable contains a list with all non-core namespace
>   names of all properties that the ``analyze()`` function (see below) may
>   return.

## Methods

``identify(self, paths)``
>   Returns ``True`` if the specified list of paths constitutes a product of
>   the product type the plug-in is designed to handle, ``False`` otherwise.
>
>   Note that a return value of ``True`` does not necessarily imply that
>   properties can be extracted from the product without errors. For example,
>   a valid implementation of this method could be as simple as checking the
>   (base) names of the specified paths against an expected pattern.

``analyze(self, paths)``
>   Return properties extracted from the product that consists of the specified
>   list of paths as a nested ``Struct`` (key, value) pair structure.
>   Note that muninn will itself set the core metadata properties for ``uuid``,
>   ``active``, ``hash``, ``size``, ``metadata_date``, ``archive_date``,
>   ``archive_path``, ``product_type``, and ``physical_name``. So these do not
>   have the be returned by the ``analyze()`` function (they will be ignored if
>   provided)
>
>   Optionally, a list of tags can be returned from this method in addition to
>   the extracted product properties. Any tags returned will be applied to the
>   product once it has been successfully ingested.
>
>   To include a list of tags, the method should return a tuple (or list) of
>   two elements. The first element should be the nested Struct (key, value)
>   pair structure containing product properties, and the second element should
>   be the list of tags.
>
>   See also the ``namespaces`` attribute above.

``enclosing_directory(self, properties)``
>   Return the name to be used for the enclosing directory.
>
>   Within the archive, any product is represented by a single path. For
>   products that consist of multiple paths, this is achieved by transparently
>   wrapping everything in an enclosing directory inside the archive.
>
>   A commonly used implementation of this method is to return the product
>   name, i.e. ``properties.core.product_name``.
>
>   The returned value will be used for the ``physical_name`` property.
>
>   This method is optional if ``use_enclosing_directory`` is ``False``.

``archive_path(self, properties)``
>   Return the path, relative to the root of the archive, where the product, of
>   the product type this plug-in is designed to handle, should be stored,
>   based on the product properties passed in as a nested ``Struct``
>   (key, value) pair structure.
>
>   That is, this method uses the product properties passed in to generate a
>   relative path inside the archive where the product will be stored.
>
>   A commonly used implementation is to return
>   `<product type>/<year>/<month>/<day>/<physical product name>`,
>   where the date corresponds to the validity start of the product.
>
>   In some cases, a different implementation is required. For example, when
>   products cannot be said to cover a time range, as is the case for some
>   auxiliary products.

``post_ingest_hook(self, archive, properties, paths)``
>   This function is optional. If it exists, it will be called after a
>   successful ingest of the product.

``post_pull_hook(self, archive, properties, paths)``
>   This function is optional. If it exists, it will be called after a
>   successful pull of the product.

``post_remove_hook(self, archive, properties)``
>   This function is optional. If it exists, it will be called after a
>   successful remove of the product.

``export_<format name>(self, archive, product, target_path, paths)``
>   Methods starting with ``export_`` can be used to implement product type
>   specific export functionality. For example, a method ``export_tgz`` could
>   be implemented that exports a product as a gzipped tarball. The return
>   value is the absolute path of the exported product.
>
>   These methods can use the archive instance passed in to, for example,
>   locate associated products to be included in the exported product.
>
>   The target path is a path to the directory in which the exported product
>   should be stored. The export method is free to create additional
>   directories under this path, for example to create a
>   `<year>/<month>/<day>` structure.
>
>   These methods are optional.
