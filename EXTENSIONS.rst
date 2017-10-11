Muninn extension developer documentation
========================================

This document is intended for muninn extension developers. Muninn is a generic
archiving framework. To be able to use it to archive specific (types of)
products, it is necessary to install (or implement) one or more extensions.

Readers of this document are assumed to be familiar with the content of the
muninn README.rst file, in particular the sections "Extensions", "Data types",
"Namespaces", and "Links".

A muninn extension is a Python module or package that implements the muninn
extension interface. Muninn defines two types of extensions: namespace
extensions (that contain namespace definitions) and product type extensions
(that contain product type plug-ins).

A namespace is a set of related attributes, i.e. a set of (key, value) pairs.
The namespace definition specifies the keys (field names) available within the
namespace, their type, and whether or not they are optional.

For example, this is the definition of the ``core`` namespace of muninn (see
also the file ``core.py`` included in the muninn source distribution): ::

  from muninn.schema import *

  class Core(Mapping):
      uuid = UUID
      active = Boolean
      hash = optional(Text)
      size = optional(Long)
      metadata_date = Timestamp
      archive_date = optional(Timestamp)
      archive_path = optional(ArchivePath)
      product_type = Text
      product_name = Text
      physical_name = Basename
      validity_start = optional(Timestamp)
      validity_stop = optional(Timestamp)
      creation_date = optional(Timestamp)
      footprint = optional(Geometry)
      remote_url = optional(Remote)

A product type plug-in is an instance of a class that handles all product type
specific details. The most important function of a product type plug-in is to
extract attributes from a product and return them in a form the archiving
framework understands.

To represent product attributes, a class called ``muninn.Struct`` is used,
which is essentially an empty class derived from object. Product attributes are
added to this class via injection. Think of it as a dictionary, except that you
can also use ``.`` to access the value bound to a specific product attribute.
A ``muninn.Struct`` can be initialized with a python dictionary. This will also
convert all members that are dictionaries into ``muninn.Struct`` objects.

By convention, product attributes are named <namespace name>.<attribute name>.
This means you usually have a single top-level Struct instance, that contains a
separate Struct instance for each namespace. For example: ::

  from muninn import Struct

  attributes = Struct()
  attributes.core = Struct()
  attributes.core.product_type = "ABCD"
  attributes.core.creation_date = datetime.datetime.utcnow()
  ... more of the same ...

  attributes.xml_pi = Struct()
  attributes.xml_pi.startTime = datetime.datetime.utcnow()
  ... more of the same ...


Namespace extension API
~~~~~~~~~~~~~~~~~~~~~~~
All attributes, functions, and methods described in this section are mandatory,
unless explicitly stated otherwise.

Exceptions
----------
Extensions are only allowed to raise muninn.Error or instances of exception
classes derived from ``muninn.Error``. If an extension raises an exception that
does not derive from ``muninn.Error``, or allows exceptions from underlying
modules to propagate outside of the extension, this should be considered a bug.

Global functions
----------------
``namespaces()``
    Return a list containing the names of all namespaces defined by the
    extension.

``namespace(namespace_name)``
    Return the namespace definition of the specified namespace. An exception
    should be raised if the specified namespace is not defined by the
    extension.


Product type extension API
~~~~~~~~~~~~~~~~~~~~~~~~~~
All attributes, functions, and methods described in this section are mandatory,
unless explicitly stated otherwise.

Exceptions
----------
Extensions are only allowed to raise ``muninn.Error`` or instances of exception
classes derived from ``muninn.Error``. If an extension raises an exception that
does not derive from ``muninn.Error``, or allows exceptions from underlying
modules to propagate outside of the extension, this should be considered a bug.

Global functions
----------------
``product_types()``
    Return a list containing all product types for which this extension defines
    plug-ins.

``product_type_plugin(product_type)``
    Return an instance of a class that adheres to the product type plug-in API
    (see below) and that implements this interface for the specified product
    type. An exception should be raised if the extension does not support the
    specified product type.


Product type plug-in API
~~~~~~~~~~~~~~~~~~~~~~~~
A product type plug-in is an instance of a class that implements the interface
defined in this section.

All attributes, functions, and methods described in this section are mandatory,
unless explicitly stated otherwise.

Exceptions
----------
Product type plug-ins are only allowed to raise ``muninn.Error`` or instances
of exception classes derived from ``muninn.Error``. If an extension raises an
exception that does not derive from ``muninn.Error``, or allows exceptions from
underlying modules to propagate outside of the extension, this should be
considered a bug.

Attributes
----------
``product_type``
    Product type this plug-in is designed to handle.

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

``use_hash``
    Determines if a SHA1 hash will be computed for products of the type the
    plug-in is designed to handle. Since computing a hash is an expensive
    operation, it is useful to set this attribute to False if storing a hash
    is not required.

``is_auxiliary_product``
    Should be set to ``True`` if products of the type the plug-in is designed
    to handle can be considered to be auxiliary, ``False`` otherwise.

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

Methods
-------
``identify(self, paths)``
    Returns ``True`` if the specified list of paths constitutes a product of
    the product type the plug-in is designed to handle, ``False`` otherwise.

    Note that a return value of ``True`` does not necessarily imply that
    attributes can be extracted from the product without errors. For example,
    a valid implementation of this method could be as simple as checking the
    (base) names of the specified paths against an expected pattern.

``analyze(self, paths)``
    Return attributes extracted from the product that consists of the specified
    list of paths as a nested ``Struct`` (key, value) pair structure.
    Note that muninn will itself set the core metadata properties for ``uuid``,
    ``active``, ``hash``, ``size``, ``metadata_date``, ``archive_date``,
    ``archive_path``, ``product_type``, and ``physical_name``. So these do not
    have the be returned by the ``analyze()`` function (they will be ignored if
    provided).

    Optionally, a list of tags can be returned from this method in addition to
    the extracted product attributes. Any tags returned will be applied to the
    product once it has been successfully ingested.

    To include a list of tags, the method should return a tuple (or list) of
    two elements. The first element should be the nested Struct (key, value)
    pair structure containing product attributes, and the second element should
    be the list of tags.

``enclosing_directory(self, attributes)``
    Return the name to be used for the enclosing directory.

    Within the archive, any product is represented by a single path. For
    products that consist of multiple paths, this is achieved by transparently
    wrapping everything in an enclosing directory inside the archive.

    A commonly used implementation of this method is to return the product
    name, i.e. ``attributes.core.product_name``.

    This method is optional if ``use_enclosing_directory`` is ``False``.

``archive_path(self, attributes)``
    Return the path, relative to the root of the archive, where the product, of
    the product type this plug-in is designed to handle, should be stored,
    based on the product attributes passed in as a nested ``Struct``
    (key, value) pair structure.

    That is, this method uses the product attributes passed in to generate a
    relative path inside the archive where the product will be stored.

    A commonly used implementation is to return <product type>/<year>/<month>/
    <day>/<uuid>/<logical product name>, where the date corresponds to the
    validity start of the product.

    In some cases, a different implementation is required. For example, when
    products cannot be said to cover a time range, as is the case for some
    auxiliary products.

``post_ingest_hook(self, archive, attributes)``
    This function is optional. If it exists, it will be called after a
    successful ingest of the product.

``post_pull_hook(self, archive, attributes)``
    This function is optional. If it exists, it will be called after a
    successful pull of the product.

``post_create_hook(self, archive, attributes)``
    This function is optional. If it exists, it will be called after a
    successful creation of the product (either ingest or pull). It is called
    after the more specific hooks.

``export_<format name>(self, archive, product, target_path)``
    Methods starting with ``export_`` can be used to implement product type
    specific export functionality. For example, a method ``export_tgz`` could
    be implemented that exports a product as a gzipped tarball.

    These methods can use the archive instance passed in to, for example,
    locate associated products to be included in the exported product.

    The target path is a path to the directory in which the exported product
    should be stored. The export method is free to create additional
    directories under this path, for example to create a <year>/<month>/<day>
    structure.

    These methods are optional.
