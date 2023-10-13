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

Readers of this document are assumed to be familiar with
[Extensions](../extensions), [Data Types](../datatypes),
[Namespaces](../namespaces) and [Links](../links).

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

By convention, product properties are named `<namespace name>.<property name>`.
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

# Exceptions

Extensions are only allowed to raise muninn.Error or instances of exception
classes derived from ``muninn.Error``. If an extension raises an exception that
does not derive from ``muninn.Error``, or allows exceptions from underlying
modules to propagate outside of the extension, this should be considered a bug.

# Extension Types

All attributes, functions, and methods described in the below sections are
mandatory, unless explicitly stated otherwise.

[Product Type Extensions](../ext_product_types)

[Namespace Extensions](../ext_namespaces)

[Remote Backend Extensions](../ext_remote)

[Hook Extensions](../ext_hooks)
