---
layout: page
title: Extensions
permalink: /extensions_dev/
---

Extension developer documentation

* toc
{:toc}

This document is intended for muninn extension developers. Muninn is a generic
archiving framework. To be able to use it to archive specific (types of)
products, it is necessary to install (or implement) one or more extensions.

Readers of this document are assumed to be familiar with the content of the
muninn README.rst file, in particular the sections "Extensions", "Data types",
"Namespaces", and "Links".

A muninn extension is a Python module or package that implements the muninn
extension interface. Muninn defines three types of extensions: namespace
extensions (that contain namespace definitions), product type extensions
(that contain product type plug-ins) and remote backend extensions (that
contain remote backend plug-ins).

A namespace is a set of related properties, i.e. a set of (key, value) pairs.
The namespace definition specifies the keys (field names) available within the
namespace, their type, and whether or not they are optional.

For example, this is the definition of the ``core`` namespace of muninn (see
also the file ``core.py`` included in the muninn source distribution):

```
from muninn.schema import *

class Core(Mapping):
  uuid = UUID()
  active = Boolean(index=True)
  hash = Text(optional=True, index=True)
  size = Long(optional=True, index=True)
  metadata_date = Timestamp(index=True)
  archive_date = Timestamp(optional=True, index=True)
  archive_path = ArchivePath(optional=True)
  product_type = Text(index=True)
  product_name = Text(index=True)
  physical_name = Basename(index=True)
  validity_start = Timestamp(optional=True, index=True)
  validity_stop = Timestamp(optional=True, index=True)
  creation_date = Timestamp(optional=True, index=True)
  footprint = Geometry(optional=True)
  remote_url = Remote(optional=True)
```

By default, properties are required. As can be seen in the example, this can be
changed by specifying ``optional=True``. By default, properties are also not
indexed in the database backend. This can be changed by specifying
``index=True``.

The uuid of the core schema is a primary key and therefore does not require
an explicit index setting. All other namespaces will automatically have an
implicit primary key called ``uuid`` added that will act as a foreign key to
``core.uuid``. This ``uuid`` field should not be explicitly defined in
namespace extensions.

A product type plug-in is an instance of a class that handles all product type
specific details. The most important function of a product type plug-in is to
extract properties from a product and return them in a form the archiving
framework understands.

To represent product properties, a class called ``muninn.Struct`` is used,
which is essentially an empty class derived from object. Product properties are
added to this class via injection. Think of it as a dictionary, except that you
can also use ``.`` to access the value bound to a specific product property.
A ``muninn.Struct`` can be initialized with a python dictionary. This will also
convert all members that are dictionaries into ``muninn.Struct`` objects.

By convention, product properties are named <namespace name>.<property name>.
This means you usually have a single top-level Struct instance, that contains a
separate Struct instance for each namespace. For example:

```
from muninn import Struct

properties = Struct()
properties.core = Struct()
properties.core.product_type = "ABCD"
properties.core.creation_date = datetime.datetime.utcnow()
... more of the same ...

properties.xml_pi = Struct()
properties.xml_pi.startTime = datetime.datetime.utcnow()
... more of the same ...
```

A hook extension is an instance of a class that defines methods to be
executed at certain times, such as product ingestion or removal. When multiple
extensions or product type plug-ins define the same hooks, they are run for any
plug-in first, then in the order of the extensions as they are listed in the
configuration file. For the post_remove_hook hook, they are run in reverse
order.

A remote backend plug-in adds the ability of an archive to pull products
from remote sources using a protocol beyond the basic file/ftp/http/https
protocols.

# Namespace extension API

All attributes, functions, and methods described in this section are mandatory,
unless explicitly stated otherwise.

## Exceptions

Extensions are only allowed to raise muninn.Error or instances of exception
classes derived from ``muninn.Error``. If an extension raises an exception that
does not derive from ``muninn.Error``, or allows exceptions from underlying
modules to propagate outside of the extension, this should be considered a bug.

## Global functions

``namespaces()``
    Return a list containing the names of all namespaces defined by the
    extension.

``namespace(namespace_name)``
    Return the namespace definition of the specified namespace. An exception
    should be raised if the specified namespace is not defined by the
    extension.

# Product type extension API

All attributes, functions, and methods described in this section are mandatory,
unless explicitly stated otherwise.

## Exceptions

Extensions are only allowed to raise ``muninn.Error`` or instances of exception
classes derived from ``muninn.Error``. If an extension raises an exception that
does not derive from ``muninn.Error``, or allows exceptions from underlying
modules to propagate outside of the extension, this should be considered a bug.

## Global functions

``product_types()``
    Return a list containing all product types for which this extension defines
    plug-ins.

``product_type_plugin(product_type)``
    Return an instance of a class that adheres to the product type plug-in API
    (see below) and that implements this interface for the specified product
    type. An exception should be raised if the extension does not support the
    specified product type.


# Remote backend extension API

All attributes, functions, and methods described in this section are mandatory,
unless explicitly stated otherwise.

## Exceptions

Extensions are only allowed to raise muninn.Error or instances of exception
classes derived from ``muninn.Error``. If an extension raises an exception that
does not derive from ``muninn.Error``, or allows exceptions from underlying
modules to propagate outside of the extension, this should be considered a bug.

## Global functions

``remote_backends()``
    Return a list containing the names of all remote backends defined by the
    extension.

``remote_backend(name)``
    Return the remote backend definition of the specified remote backend. An 
    exception should be raised if the specified remote backend is not defined
    by the extension.


# Product type plug-in API

A product type plug-in is an instance of a class that implements the interface
defined in this section.

All attributes, functions, and methods described in this section are mandatory,
unless explicitly stated otherwise.

## Exceptions

Product type plug-ins are only allowed to raise ``muninn.Error`` or instances
of exception classes derived from ``muninn.Error``. If an extension raises an
exception that does not derive from ``muninn.Error``, or allows exceptions from
underlying modules to propagate outside of the extension, this should be
considered a bug.

## Attributes

``use_enclosing_directory``
    This variable should equal True if products of the type the plug-in is
    designed to handle consist of multiple files, False otherwise.

    In the majority of cases, a product is represented by a single path (i.e.
    file, or directory). For such cases, this attribute should be set to
    ``False``, and the ``analyze()`` method defined below can expect to be
    called with a list containing a single path.

    If a product consist of two or more files that belong together (without
    them already being grouped together into a single top-level directory),
    this attribute should be set to ``True``.

``hash_type``
    Determines the hashing algorithm to be used for products of the type the
    plug-in is designed to handle, e.g. ``md5`` or ``sha1``. The available
    algorithms are those supported by the standard Python ``hashlib`` module.

    If the attribute is set to ``None`` or the empty string, hashing is
    disabled for the respective products. This can be useful, as hashing is
    an expensive operation.

    If the attribute is not set, the ``md5`` algorithm is used by default.

``cascade_rule``
    Determines what should happen to products of the type the plug-in is
    designed to handle when all products linked to these products (as source
    products) have been stripped or removed. (A stripped product is a product
    for which the data on disk has been deleted, but the entry in the product
    catalogue has been kept).

    Possible values are defined by the ``muninn.extension.CascadeRule``
    enumeration and are given below:

    ``CascadeRule.IGNORE``
        Do nothing.

    ``CascadeRule.CASCADE_PURGE_AS_STRIP``
        If all source products of a product have been removed, strip the
        product. If all source products of a product have been stripped, do
        nothing.

    ``CascadeRule.CASCADE_PURGE``
        If all source products of a product have been removed, remove the
        product. If all source products of a product have been stripped, do
        nothing.

    ``CascadeRule.STRIP``
        If all source products of a product have been removed, strip the
        product. If all source products of a product have been stripped, strip
        the product.

    ``CascadeRule.CASCADE``
        If all source products of a product have been removed, remove the
        product. If all source products of a product have been stripped, strip
        the product.

    ``CascadeRule.PURGE``
        If all source products of a product have been removed, remove the
        product. If all source products of a product have been stripped, remove
        the product.

    This attribute is optional. If it is left undefined, ``CascadeRule.IGNORE``
    is assumed.

``namespaces``
    This (optional) variable contains a list with all non-core namespace
    names of all properties that the ``analyze()`` function (see below) may
    return.

## Methods

``identify(self, paths)``
    Returns ``True`` if the specified list of paths constitutes a product of
    the product type the plug-in is designed to handle, ``False`` otherwise.

    Note that a return value of ``True`` does not necessarily imply that
    properties can be extracted from the product without errors. For example,
    a valid implementation of this method could be as simple as checking the
    (base) names of the specified paths against an expected pattern.

``analyze(self, paths)``
    Return properties extracted from the product that consists of the specified
    list of paths as a nested ``Struct`` (key, value) pair structure.
    Note that muninn will itself set the core metadata properties for ``uuid``,
    ``active``, ``hash``, ``size``, ``metadata_date``, ``archive_date``,
    ``archive_path``, ``product_type``, and ``physical_name``. So these do not
    have the be returned by the ``analyze()`` function (they will be ignored if
    provided).

    Optionally, a list of tags can be returned from this method in addition to
    the extracted product properties. Any tags returned will be applied to the
    product once it has been successfully ingested.

    To include a list of tags, the method should return a tuple (or list) of
    two elements. The first element should be the nested Struct (key, value)
    pair structure containing product properties, and the second element should
    be the list of tags.

    See also the ``namespaces`` attribute above.

``enclosing_directory(self, properties)``
    Return the name to be used for the enclosing directory.

    Within the archive, any product is represented by a single path. For
    products that consist of multiple paths, this is achieved by transparently
    wrapping everything in an enclosing directory inside the archive.

    A commonly used implementation of this method is to return the product
    name, i.e. ``properties.core.product_name``.

    The returned value will be used for the ``physical_name`` property.

    This method is optional if ``use_enclosing_directory`` is ``False``.

``archive_path(self, properties)``
    Return the path, relative to the root of the archive, where the product, of
    the product type this plug-in is designed to handle, should be stored,
    based on the product properties passed in as a nested ``Struct``
    (key, value) pair structure.

    That is, this method uses the product properties passed in to generate a
    relative path inside the archive where the product will be stored.

    A commonly used implementation is to return <product type>/<year>/<month>/
    <day>/<physical product name>, where the date corresponds to the validity
    start of the product.

    In some cases, a different implementation is required. For example, when
    products cannot be said to cover a time range, as is the case for some
    auxiliary products.

``post_ingest_hook(self, archive, properties, paths)``
    This function is optional. If it exists, it will be called after a
    successful ingest of the product.

``post_pull_hook(self, archive, properties, paths)``
    This function is optional. If it exists, it will be called after a
    successful pull of the product.

``post_remove_hook(self, archive, properties)``
    This function is optional. If it exists, it will be called after a
    successful remove of the product.

``export_<format name>(self, archive, product, target_path, paths)``
    Methods starting with ``export_`` can be used to implement product type
    specific export functionality. For example, a method ``export_tgz`` could
    be implemented that exports a product as a gzipped tarball. The return
    value is the absolute path of the exported product.

    These methods can use the archive instance passed in to, for example,
    locate associated products to be included in the exported product.

    The target path is a path to the directory in which the exported product
    should be stored. The export method is free to create additional
    directories under this path, for example to create a <year>/<month>/<day>
    structure.

    These methods are optional.

# Hook extension API

A hook extension is an instance of a class that implements the interface
defined in this section.

## Exceptions

Hook extensions are only allowed to raise ``muninn.Error`` or instances
of exception classes derived from ``muninn.Error``. If an extension raises an
exception that does not derive from ``muninn.Error``, or allows exceptions from
underlying modules to propagate outside of the extension, this should be
considered a bug.

## Methods

All methods described here are optional. When a method changes a product
property, it is not automatically saved.

``post_ingest_hook(self, archive, product, paths)``
    Executed after a product is ingested via archive.ingest, but not
    catalogue-only (ingest_product == True).

``post_create_hook(self, archive, product)``
    Executed after a product is ingested catalogue-only via archive.ingest
    (ingest_product == False), or after a call to archive.create_properties.

``post_pull_hook(self, archive, product, paths)``
    Executed after a pull.

``post_remove_hook(self, archive, product)``
    Executed after a product removal.

## Global functions

``hook_extensions()``
    Return a list containing the names of all hooks defined by the extension.

``hook_extension(name)``
    Return an instance of a class that implements one or more hook methods
    (see above). An exception should be raised if the extension does not
    support the specified hook.


# Remote backend plug-in API

A Remote backend plug-in is an instance of a class that implements the
interface defined in this section.

All attributes, functions, and methods described in this section are mandatory,
unless explicitly stated otherwise.

## Exceptions

Remote backend plug-ins are only allowed to raise ``muninn.Error`` or instances
of exception classes derived from ``muninn.Error``. If an extension raises an
exception that does not derive from ``muninn.Error``, or allows exceptions from
underlying modules to propagate outside of the extension, this should be
considered a bug.

## Methods

``pull(self, archive, product, target_path)``
    Download the product specified.
    The product should be downloaded in the path indicated by ``target_path``.
    The function should return the full path(s) to the file(s) that are
    downloaded.
    Muninn will then take care that it is put in the
    If enclosing_directory is True then ``core.physical_name`` indicates the
    directory in which the product file(s) will be stored, otherwise it
    indicates the target filename of the product.
