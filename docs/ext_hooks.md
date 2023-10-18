---
layout: page
title: Hook Extension API
permalink: /ext_hooks/
---

# Hook extension API

## Global functions

``hook_extensions()``
>   Return a list containing the names of all hooks defined by the extension.

``hook_extension(name)``
>   Return an instance of a class that implements one or more hook methods
>   (see above). An exception should be raised if the extension does not
>   support the specified hook.

## Methods

All methods described here are optional. When a method changes a product
property, it is not automatically saved.

``post_ingest_hook(self, archive, product, paths)``
>   Executed after a product is ingested via archive.ingest, but not
>   catalogue-only (ingest_product == True).

``post_create_hook(self, archive, product)``
>   Executed after a product is ingested catalogue-only via archive.ingest
>   (ingest_product == False), or after a call to archive.create_properties.

``post_pull_hook(self, archive, product, paths)``
>   Executed after a pull.

``post_remove_hook(self, archive, product)``
>   Executed after a product removal.
