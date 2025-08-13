---
layout: page
title: Synchronizer Extension API
permalink: /ext_synchronizers/
---

# Synchronizer extension API

## Global functions

``synchronizer(config: muninn.Struct)``
>   Return an instance of a class that adheres to the synchronizer plug-in API
>   (see below).
>   muninn will pass the content of the ``synchronizer:<name>`` section
>   (minus the ``module`` setting) as a single positional argument to this function.
>   The implementation of this function should only setup the configuration state
>   and not initiate any network connections yet.

## Methods

``sync(self, archive: muninn.Archive, product_types: typing.List[str] = None, start: datetime.datetime = None,
       end: datetime.datetime = None, force: bool = False)``
>   A synchronizer object should have a ``sync`` method that performs the catalogue sync.
>   It should update the state of the catalogue of the archive based on the state of an external database.
>   Synchronizations should be performed on a product type by product type basis. The global list of applicable product
>   types can be hard-coded in the extension or provided as configuration to the ``synchronizer`` global function.
>   The ``sync`` method itself should limit itself to the list of product types given by the ``product_types`` parameter
>   if this parameter was set (and otherwise sync all product types).
>   The sync should ideally be performed in chronological order, following the timestamps of modifications in the external database.
>   The ``start`` and ``end`` parameters are meant to define the time range within the chronological timeline to synchronize.
>   The ``sync`` method should only update the catalogue database by means of e.g. ``muninn.create_properties``,
>   ``muninn.update_properties``, and ``muninn.remove`` calls. It should explicitly not perform any ``muninn.pull`` calls.
>   The ``force`` option indicates that the ``sync`` method should always call a ``muninn.update_properties`` for entries
>   that were already in the muninn catalogue.
>   Any network activity should only happen within the scope of this ``sync`` method.
>   The synchronizer object should not keep any connection state active after the method returns.
