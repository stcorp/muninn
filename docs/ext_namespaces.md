---
layout: page
title: Namespace Extension API
permalink: /ext_namespaces/
---

# Namespace extension API

## Global functions

``namespaces()``
>   Return a list containing the names of all namespaces defined by the
>   extension.

``namespace(namespace_name)``
>   Return the namespace definition of the specified namespace. An exception
>   should be raised if the specified namespace is not defined by the
>   extension.
>   The namespace definition should be a subclass of ``muninn.schema.Mapping``.
