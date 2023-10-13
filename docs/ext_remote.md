---
layout: page
title: Remote Backend Extension API
permalink: /ext_remote/
---

# Remote backend extension API

## Global functions

``remote_backends()``
>   Return a list containing the names of all remote backends defined by the
>   extension.

``remote_backend(name)``
>   Return the remote backend definition of the specified remote backend. An
>   exception should be raised if the specified remote backend is not defined
>   by the extension.

## Methods

``pull(self, archive, product, target_path)``
>   Download the product specified.
>   The product should be downloaded in the path indicated by ``target_path``.
>   The function should return the full path(s) to the file(s) that are
>   downloaded.

``identify(self, url)``
>   Returns ``True`` if the plug-in is designed to handle the specified URL,
>   ``False`` otherwise.
