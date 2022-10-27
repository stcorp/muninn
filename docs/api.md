---
layout: page
title: Python API
permalink: /api/
---

* toc
{:toc}

<a name="muninn"></a>

# muninn

<a id="muninn.config_path"></a>

#### config\_path

```python
def config_path()
```

Return the value of the `MUNINN_CONFIG_PATH` environment variable.

<a id="muninn.open"></a>

#### open

```python
def open(id=None, **kwargs)
```

Open an archive for the given archive id, by searching for the
corresponding configuration file in the locations found in the
`MUNINN_CONFIG_PATH` environment variable.

**Arguments**:

- `id` - Archive id (name of configuration file)
  

**Returns**:

  An instance of `muninn.archive.Archive`

<a id="muninn.archive"></a>

# muninn.archive

<a id="muninn.archive.Archive"></a>

## Archive Objects

```python
class Archive(object)
```

Archive class

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

<a id="muninn.archive.Archive.register_namespace"></a>

#### register\_namespace

```python
def register_namespace(namespace, schema)
```

Register a namespace. A valid namespace identifier starts
with a lowercase character, and can contain any number of
additional lowercase characters, underscores, or digits.

**Arguments**:

- `namespace` - Namespace identifier.
- `schema` - Schema definition of the namespace.

<a id="muninn.archive.Archive.namespace_schema"></a>

#### namespace\_schema

```python
def namespace_schema(namespace)
```

Return the schema definition of the specified namespace.

<a id="muninn.archive.Archive.namespaces"></a>

#### namespaces

```python
def namespaces()
```

Return a list containing all registered namespaces.

<a id="muninn.archive.Archive.register_product_type"></a>

#### register\_product\_type

```python
def register_product_type(product_type, plugin)
```

Register a product type.

**Arguments**:

- `product_type` - Product type name
- `plugin` - Reference to an object that implements the product type
  plugin API and as such takes care of the details of
  extracting product properties from products of the
  specified product type.

<a id="muninn.archive.Archive.product_type_plugin"></a>

#### product\_type\_plugin

```python
def product_type_plugin(product_type)
```

Return a reference to the specified product type plugin.

product_type -- Product type name.

<a id="muninn.archive.Archive.product_types"></a>

#### product\_types

```python
def product_types()
```

Return a list of registered product types.

<a id="muninn.archive.Archive.register_remote_backend"></a>

#### register\_remote\_backend

```python
def register_remote_backend(remote_backend, plugin)
```

Register a remote backend.

**Arguments**:

- `remote_backend` - Remote backend name.
- `plugin` - Reference to an object that implements the remote
  backend plugin API and as such takes care of the
  details of extracting product properties from
  products of the specified remote backend.

<a id="muninn.archive.Archive.remote_backend"></a>

#### remote\_backend

```python
def remote_backend(remote_backend)
```

Return a reference to the specified remote backend plugin.

**Arguments**:

- `remote_backend` - Remote backend name.

<a id="muninn.archive.Archive.remote_backends"></a>

#### remote\_backends

```python
def remote_backends()
```

Return a list of supported remote backends.

<a id="muninn.archive.Archive.register_hook_extension"></a>

#### register\_hook\_extension

```python
def register_hook_extension(hook_extension, plugin)
```

Register a hook extension.

**Arguments**:

- `hook_extension` - Hook extension name
- `plugin` - Reference to an object that implements the hook
  extension plugin API

<a id="muninn.archive.Archive.hook_extension"></a>

#### hook\_extension

```python
def hook_extension(hook_extension)
```

Return the hook extension with the specified name.

**Arguments**:

- `hook_extension` - Hook extension name

<a id="muninn.archive.Archive.hook_extensions"></a>

#### hook\_extensions

```python
def hook_extensions()
```

Return a list of supported hook extensions.

<a id="muninn.archive.Archive.attach"></a>

#### attach

```python
def attach(paths,
           product_type=None,
           use_symlinks=None,
           verify_hash=False,
           verify_hash_before=False,
           use_current_path=False,
           force=False)
```

Add a product to the archive using an existing metadata record in the database.

This function acts as the inverse of a strip(). A metadata record for this product should already exist in
the database and no product should exist for it in the archive.

The existing metadata record is found by performing a search based on product_type and physical_name.

**Arguments**:

- `paths` - List of paths pointing to product files.
- `product_type` - Product type of the product to ingest. If left unspecified, an attempt will be made to
  determine the product type automatically. By default, the product type will be determined
  automatically.
- `use_symlinks` - If set to True, symbolic links to the original product will be stored in the archive
  instead of a copy of the original product. If set to None, the value of the corresponding
  archive wide configuration option will be used. By default, the archive configuration will
  be used.
  This option is ignored if use_current_path=True.
- `verify_hash` - If set to True then, after the ingestion, the product in the archive will be matched against
  the hash from the metadata (only if the metadata contained a hash).
- `verify_hash_before` - If set to True then, before the product is attached to the archive, it will be matched
  against the metadata hash (if it exists).
- `use_current_path` - Ingest the product by keeping the file(s) at the current path (which must be inside the
  root directory of the archive).
  This option is ignored if ingest_product=False.
- `force` - If set to True, then skip default size check between product and existing metadata.
  

**Returns**:

  The attached product.

<a id="muninn.archive.Archive.auth_file"></a>

#### auth\_file

```python
def auth_file()
```

Return the path of the authentication file to download from remote locations.

<a id="muninn.archive.Archive.cleanup_derived_products"></a>

#### cleanup\_derived\_products

```python
def cleanup_derived_products()
```

Clean up all derived products for which the source products no
longer exist, as specified by the cascade rule configured in the
respective product type plugins.

Please see the Muninn documentation for more information on how
to configure cascade rules.

<a id="muninn.archive.Archive.close"></a>

#### close

```python
def close()
```

Close the archive immediately instead of when (and if) the archive
instance is collected.

Using the archive after calling this function results in undefined behavior.

<a id="muninn.archive.Archive.count"></a>

#### count

```python
def count(where="", parameters={})
```

Return the number of products matching the specified search expression.

**Arguments**:

- `where` - Search expression (default: "").
- `parameters` - Parameters referenced in the search expression (if any).

<a id="muninn.archive.Archive.create_properties"></a>

#### create\_properties

```python
def create_properties(properties, disable_hooks=False)
```

Create a record for the given product (as defined by the
provided dictionary of properties) in the product catalogue.
An important side effect of this operation is that it will
fail if:

1. The core.uuid is not unique within the product catalogue.
2. The combination of core.archive_path and core.physical_name is
not unique within the product catalogue.

**Arguments**:

- `properties` - The product properties.
- `disable_hooks` - Do not execute any hooks (default: False).

<a id="muninn.archive.Archive.delete_properties"></a>

#### delete\_properties

```python
def delete_properties(where="", parameters={})
```

Remove properties for one or more products from the catalogue.

This function will _not_ remove any product files from storage and
will _not_ trigger any of the specific cascade rules.

**Arguments**:

- `where` - Search expression or one or more product uuid(s) or properties (default: "").
- `parameters` - Parameters referenced in the search expression (default: empty dictionary).
  

**Returns**:

  The number of updated products

<a id="muninn.archive.Archive.derived_products"></a>

#### derived\_products

```python
def derived_products(uuid)
```

Return the UUIDs of the products that are linked to the given
product as derived products.

**Arguments**:

- `uuid` - Product UUID.

<a id="muninn.archive.Archive.destroy"></a>

#### destroy

```python
def destroy()
```

Completely remove the archive, including both the products and the
product catalogue.

Using the archive after calling this function results in undefined
behavior. The prepare() function can be used to bring the archive back
into a useable state.

<a id="muninn.archive.Archive.destroy_catalogue"></a>

#### destroy\_catalogue

```python
def destroy_catalogue()
```

Completely remove the catalogue database, but leave the datastore in storage untouched.

Using the archive after calling this function results in undefined behavior.
Using the prepare_catalogue() function and ingesting all products again, can bring the archive
back into a useable state.

<a id="muninn.archive.Archive.export"></a>

#### export

```python
def export(where="", parameters={}, target_path=os.path.curdir, format=None)
```

Export one or more products from the archive.

By default, a copy of the original product will be retrieved from the archive. This default behavior can be
customized by the product type plugin. For example, the custom implementation for a certain product type might
retrieve one or more derived products and bundle them together with the product itself.

**Arguments**:

- `where` - Search expression or one or more product uuid(s) or properties (default: "").
- `parameters` - Parameters referenced in the search expression (default: empty dictionary).
- `target_path` - Directory in which the retrieved products will be stored (default: current directory).
- `format` - Format in which the products will be exported (default: None).
  

**Returns**:

  Either a list containing the export paths for the
  exported products (when a search expression or multiple
  properties/UUIDs were passed), or a single export path.

<a id="muninn.archive.Archive.export_formats"></a>

#### export\_formats

```python
def export_formats()
```

Return a list of supported alternative export formats.

<a id="muninn.archive.Archive.generate_uuid"></a>

#### generate\_uuid

```python
@staticmethod
def generate_uuid()
```

Return a new generated UUID that can be used as UUID for a product metadata record.

<a id="muninn.archive.Archive.identify"></a>

#### identify

```python
def identify(paths)
```

Determine the product type of the product (specified as a single path, or a list of paths if it is a
multi-part product).

**Arguments**:

- `paths` - List of paths pointing to product files.
  

**Returns**:

  The determined product type.

<a id="muninn.archive.Archive.ingest"></a>

#### ingest

```python
def ingest(paths,
           product_type=None,
           properties=None,
           ingest_product=True,
           use_symlinks=None,
           verify_hash=False,
           use_current_path=False,
           force=False)
```

Ingest a product into the archive. Multiple paths can be specified, but the set of files and/or directories
these paths refer to is always ingested as a single logical product.

Product ingestion consists of two steps. First, product properties are extracted from the product and are used
to create an entry for the product in the product catalogue. Second, the product itself is ingested, either by
copying the product or by creating symbolic links to the product.

If the product to be ingested is already located at the target location within the archive (and there was not
already another catalogue entry pointing to it), muninn will leave the product at its location as-is, and won't
try to copy/symlink it.

**Arguments**:

- `paths` - List of paths pointing to product files.
- `product_type` - Product type of the product to ingest. If left unspecified, an attempt will be made to
  determine the product type automatically. By default, the product type will be determined
  automatically.
- `properties` - Used as product properties if specified. No properties will be extracted from the product
  in this case.
- `ingest_product` - If set to False, the product itself will not be ingested into the archive, only its
  properties. By default, the product will be ingested.
- `use_symlinks` - If set to True, symbolic links to the original product will be stored in the archive
  instead of a copy of the original product. If set to None, the value of the corresponding
  archive wide configuration option will be used. By default, the archive configuration will
  be used.
  This option is ignored if use_current_path=True.
- `verify_hash` - If set to True then, after the ingestion, the product in the archive will be matched against
  the hash from the metadata (only if the metadata contained a hash).
- `use_current_path` - Ingest the product by keeping the file(s) at the current path (which must be inside the
  root directory of the archive).
  This option is ignored if ingest_product=False.
- `force` - If set to True then any existing product with the same type and name (unique constraint)
  will be removed before ingestion, including partially ingested products.
  NB. Depending on product type specific cascade rules, removing a product can result in one
  or more derived products being removed (or stripped) along with it.
  

**Returns**:

  The ingested product.

<a id="muninn.archive.Archive.link"></a>

#### link

```python
def link(uuid, source_uuids)
```

Link a product to one or more source products.

**Arguments**:

- `uuid` - Product UUID.
- `source_uuids` - Source UUIDs.

<a id="muninn.archive.Archive.prepare"></a>

#### prepare

```python
def prepare(force=False)
```

Prepare the archive for (first) use.

The root path will be created and the product catalogue will be
initialized such that the archive is ready for use.

**Arguments**:

- `force` - If set to True then any existing products and / or product
  catalogue will be removed.

<a id="muninn.archive.Archive.prepare_catalogue"></a>

#### prepare\_catalogue

```python
def prepare_catalogue(dry_run=False)
```

Prepare the catalogue of the archive for (first) use.

**Arguments**:

- `dry_run` - Do not actually execute the preparation commands.
  

**Returns**:

  The list of SQL commands that (would) have been executed by this function.

<a id="muninn.archive.Archive.product_path"></a>

#### product\_path

```python
def product_path(uuid_or_properties)
```

Return the path in storage to the specified product.

**Arguments**:

- `uuid_or_properties` - UUID or dictionary of product properties.

<a id="muninn.archive.Archive.pull"></a>

#### pull

```python
def pull(where="",
         parameters={},
         verify_hash=False,
         verify_hash_download=False)
```

Pull one or more remote products into the archive.

Products should have a valid remote_url core metadata field and they should not yet exist in the local
archive (i.e. the archive_path core metadata field should not be set).

**Arguments**:

- `where` - Search expression or one or more product uuid(s) or properties.
- `parameters` - Parameters referenced in the search expression (if any).
- `verify_hash` - If set to True then, after the pull, the product in the archive will be matched against
  the hash from the metadata (only if the metadata contained a hash).
- `verify_hash_download` - If set to True then, before the product is stored in the archive, the pulled
  product will be matched against the metadata hash (if it exists).
  

**Returns**:

  The number of pulled products.

<a id="muninn.archive.Archive.rebuild_properties"></a>

#### rebuild\_properties

```python
def rebuild_properties(uuid, disable_hooks=False, use_current_path=False)
```

Rebuild product properties by re-extracting these properties (using product type plugins) from the
products stored in the archive.
Only properties and tags that are returned by the product type plugin will be updated. Other properties or
tags will remain as they were.

**Arguments**:

- `uuid` - Product UUID
- `disable_hooks` - Disable product type hooks (not meant for routine operation) (default: False).
- `use_current_path` - Do not attempt to relocate the product to the location specified in the product
  type plugin. Useful for read-only archives (default: False).

<a id="muninn.archive.Archive.rebuild_pull_properties"></a>

#### rebuild\_pull\_properties

```python
def rebuild_pull_properties(uuid,
                            verify_hash=False,
                            disable_hooks=False,
                            use_current_path=False)
```

Refresh products by re-running the pull, but using the existing products stored in the archive.

**Arguments**:

- `uuid` - Product UUID
- `verify_hash` - If set to True then the product in the archive will be matched against
  the hash from the metadata (only if the metadata contained a hash) (default: False).
- `disable_hooks` - Disable product type hooks (not meant for routine operation) (default: False).
- `use_current_path` - Do not attempt to relocate the product to the location specified in the product
  type plugin. Useful for read-only archives (default: False).

<a id="muninn.archive.Archive.remove"></a>

#### remove

```python
def remove(where="", parameters={}, force=False, cascade=True)
```

Remove one or more products from the archive, both from storage as well as from the product catalogue.
Return the number of products removed.

NB. Depending on product type specific cascade rules, removing a product can result in one or more derived
products being removed (or stripped) along with it. Such products are _not_ included in the returned count.

**Arguments**:

- `where` - Search expression or one or more product uuid(s) or properties (default: "").
- `parameters` - Parameters referenced in the search expression (default: empty dictionary).
- `force` - If set to True, also remove partially ingested products. This affects products for which a
  failure occured during ingestion, as well as products in the process of being ingested. Use
  this option with care (default: False).
- `cascade` - Apply cascade rules to strip/remove dependent products (default: True).
  

**Returns**:

  The number of removed products.

<a id="muninn.archive.Archive.retrieve"></a>

#### retrieve

```python
def retrieve(where="",
             parameters={},
             target_path=os.path.curdir,
             use_symlinks=False)
```

Retrieve one or more products from the archive.

**Arguments**:

- `where` - Search expression or one or more product uuid(s) or properties (default: "").
- `parameters` - Parameters referenced in the search expression (default: empty dictionary).
- `target_path` - Directory under which the retrieved products will be stored (default: current directory).
- `use_symlinks` - If set to True, products will be retrieved as symbolic links to the original products kept
  in the archive. If set to False, products will retrieved as copies of the original products.
  By default, products will be retrieved as copies (default: False).
  

**Returns**:

  Either a list containing the target paths for the
  retrieved products (when a search expression or multiple
  properties/uuids were passed), or a single target path.

<a id="muninn.archive.Archive.retrieve_properties"></a>

#### retrieve\_properties

```python
def retrieve_properties(uuid, namespaces=[], property_names=[])
```

Return properties for the specified product.

**Arguments**:

- `uuid` - Product UUID
- `namespaces` - List of namespaces of which the properties should be retrieved. By default, only properties
  defined in the "core" namespace will be retrieved.

<a id="muninn.archive.Archive.root"></a>

#### root

```python
def root()
```

Return the archive root path.

<a id="muninn.archive.Archive.search"></a>

#### search

```python
def search(where="",
           order_by=[],
           limit=None,
           parameters={},
           namespaces=[],
           property_names=[])
```

Search the product catalogue for products matching the specified search expression.

**Arguments**:

- `where` - Search expression (default: "").
- `order_by` - A list of property names that determines the ordering of the results. If the list is empty, the
  order of the results in undetermined and can very between calls to this function. Each property
  name in this list can be provided with a '+' or '-' prefix, or without a prefix. A '+' prefix,
  or no prefix denotes ascending sort order, a '-' prefix denotes decending sort order
- `(default` - empty list).
- `limit` - Limit the maximum number of results to the specified number (default: None).
- `parameters` - Parameters referenced in the search expression (default: empty dictionary).
- `namespaces` - List of namespaces of which the properties should be retrieved. By default, only properties
  defined in the "core" namespace will be retrieved.
  property_names
  --  List of property names that should be returned. By default all properties of the "core"
  namespace and those of the namespaces in the namespaces argument are included.
  If this parameter is a non-empty list then only the referenced properties will be returned.
  Properties are specified as '<namespace>.<identifier>'
  (the namespace can be omitted for the 'core' namespace).
  If the property_names parameter is provided then the namespaces parameter is ignored.
  

**Returns**:

  A list of matching products.

<a id="muninn.archive.Archive.source_products"></a>

#### source\_products

```python
def source_products(uuid)
```

Return the UUIDs of the products that are linked to the given product as source products.

**Arguments**:

- `uuid` - Product UUID

<a id="muninn.archive.Archive.strip"></a>

#### strip

```python
def strip(where="", parameters={}, force=False, cascade=True)
```

Remove one or more products from storage only (not from the product catalogue).

NB. Depending on product type specific cascade rules, stripping a product can result in one or more derived
products being stripped (or removed) along with it.

**Arguments**:

- `where` - Search expression or one or more product uuid(s) or properties (default: "").
- `parameters` - Parameters referenced in the search expression (default: empty dictionary).
- `force` - If set to True, also strip partially ingested products. This affects products for which a
  failure occured during ingestion, as well as products in the process of being ingested. Use
  this option with care (default: False).
- `cascade` - Apply cascade rules to strip/purge dependent products (default: True).
  

**Returns**:

  The number of stripped products.

<a id="muninn.archive.Archive.summary"></a>

#### summary

```python
def summary(where="",
            parameters=None,
            aggregates=None,
            group_by=None,
            group_by_tag=False,
            having=None,
            order_by=None)
```

Return a summary of the products matching the specified search expression.

**Arguments**:

- `where` - Search expression (default: "").
- `parameters` - Parameters referenced in the search expression (default: None).
- `aggregates` - A list of property aggregates defined as "<property_name>.<reduce_fn>".
  Properties need to be of type long, integer, real, text or timestamp.
  The reduce function can be 'min', 'max', 'sum', or 'avg'.
  'sum' and 'avg' are not possible for text and timestamp properties.
  A special property 'validity_duration' (defined as validity_stop - validity_start) can also
  be used. (default: None)
- `group_by` - A list of property names whose values are used for grouping the aggregation results.
  There will be a separate result row for each combination of group_by property values.
  Properties need to be of type long, integer, boolean, text or timestamp.
  Timestamps require a binning subscript which can be 'year', 'month', 'yearmonth', or 'date'
  (e.g. 'validity_start.yearmonth'). (default: None)
- `group_by_tag` - If set to True, results will also be grouped by available tag values.
  Note that products will be counted multiple times if they have multiple tags (default: False)
- `having` - A list of property aggregates defined as `<property_name>.<reduce_fn>`; properties need to be
  of type long, integer, real, text or timestamp; the reduce function can be 'min', 'max', 'sum',
  or 'avg'; 'sum' and 'avg' are not possible for text and timestamp properties; a special property
  'validity_duration' (defined as validity_stop - validity_start) can also be used (default: None).
- `order_by` - A list of result column names that determines the ordering of the results. If the list is
  empty, the order of the results is ordered by the `group_by` specification. Each name in the
  list can have a '+' (ascending) or '-' (descending) prefix, or no prefix (ascending). (default: None)
  

**Returns**:

  A list of row tuples matching the search expression created from the arguments.

<a id="muninn.archive.Archive.tag"></a>

#### tag

```python
def tag(where=None, tags=None, parameters={})
```

Set one or more tags on one or more product(s).

**Arguments**:

- `where` - Search expression or one or more product uuid(s) or properties (default: None).
- `tags` - One or more tags (default: None).
- `parameters` - Parameters referenced in the search expression (default: empty dictionary).

<a id="muninn.archive.Archive.tags"></a>

#### tags

```python
def tags(uuid)
```

Return the tags of a product.

**Arguments**:

- `uuid` - Product UUID.

<a id="muninn.archive.Archive.unlink"></a>

#### unlink

```python
def unlink(uuid, source_uuids=None)
```

Remove the link between a product and one or more of its source products.

**Arguments**:

- `uuid` - Product UUID
- `source_uuids` - Source product UUIDs

<a id="muninn.archive.Archive.untag"></a>

#### untag

```python
def untag(where=None, tags=None, parameters={})
```

Remove one or more tags from one or more product(s).

**Arguments**:

- `where` - Search expression or one or more product uuid(s) or properties (default: None).
- `tags` - One or more tags (default: None, meaning all existing tags)
- `parameters` - Parameters referenced in the search expression (default: empty dictionaryu).

<a id="muninn.archive.Archive.update_properties"></a>

#### update\_properties

```python
def update_properties(properties, uuid=None, create_namespaces=False)
```

Update product properties in the product catalogue. The UUID of the product to update will be taken from the
"core.uuid" property if it is present in the specified properties. Otherwise, the UUID should be provided
separately.

This function allows any property to be changed with the exception of the product UUID, and therefore needs to
be used with care. The recommended way to update product properties is to first retrieve them using either
retrieve_properties() or search(), change the properties, and then use this function to update the product
catalogue.

Argument:
properties         -- Product properties
uuid               -- UUID of the product to update. By default, the UUID will be taken from the "core.uuid"
                      property.
create_namespaces  -- Test if all namespaces are already defined for the product, and create them if needed
                      (default: False)

<a id="muninn.archive.Archive.verify_hash"></a>

#### verify\_hash

```python
def verify_hash(where="", parameters={})
```

Verify the hash for one or more products in the archive.

Products that are not active or are not in the archive will be skipped.
If there is no hash available in the metadata for a product then an
error will be raised.

**Arguments**:

- `where` - Search expression or one or more product uuid(s) or properties.
- `parameters` - Parameters referenced in the search expression (if any).
  

**Returns**:

  A list of UUIDs of products for which the verification failed.

