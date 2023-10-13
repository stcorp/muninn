---
layout: page
title: Extensions
permalink: /extensions/
---

# Extensions

Muninn is a generic archiving framework. To be able to use it to archive
specific (types of) products, it is necessary to install one or more
extensions.

A muninn extension is a Python module or package that implements the muninn
extension interface. Muninn defines four main types of extensions: namespace
extensions (that contain namespace definitions), product type extensions
(that contain product type plug-ins), hook extensions (allowing
functions to be executed at certain times, e.g. product creation/removal), and
remote backend extensions (to support custom URL protocols for downloads
performed by muninn).

A namespace is a named set of product properties (see
[Namespaces](../namespaces)).
Muninn defines a namespace called ``core`` that contains a small set of
properties that muninn needs to archive a product. For example, it contains the
name of the product, its hash, UUID, and archive date.
The core namespace also contains several optional common properties for
spatiotemporal data such as time stamps and geolocation footprint.

Namespace extensions contain additional namespace definitions to allow storage
of other product properties of interest. For example, an extension for
archiving satellite products could define a namespace that contains properties
such as satellite instrument, measurement mode, orbit number, file version,
and so on. An extension for archiving music could define a namespace that
contains properties such as artist, genre, duration, and so forth.

A product type plug-in is an instance of a class that implements the muninn
product type plug-in interface. The main responsibility of a product type plug-
in is to extract product properties and tags from products of its supported
product type(s). At the minimum, this involves extracting all the required
properties defined in the "core" namespace. Without this information, muninn
cannot archive the product.

Product type plug-ins can also be used to tailor certain aspects of muninn. For
example, the plug-in controls what happens to a product (of the type it
supports) when all of the products it is linked to (see [Links](../links)) have
been removed from the archive.

A fourth type of extension is the remote backend extension. This type of
extension is specifically for muninn-pull and can introduce support for
retrieving data using protocols other than the built-in support that muninn
already has for http/https/ftp/file.

For details concerning the actual implementation of extensions see
[here](../extensions_dev).
