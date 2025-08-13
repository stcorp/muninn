---
layout: page
title: Configuration
permalink: /config/
---

# Archive configuration

An archive configuration file is a text file that describes an archive. The
configuration file for an archive with id ``foo`` should be called ``foo.cfg``.

The configuration file format resembles Windows INI files in that it consists
of named sections starting with a ``[section]`` header followed by
``name = value`` entries. Each section will be discussed in detail below.

# Section "archive"

This section contains general archive settings and may contain the following
settings:

- ``database``: The backend used for storing product properties. The currently
  supported backends are ``postgresql`` and ``sqlite``.

- ``storage``: The backend used for storing product data. The currently
  supported backends are ``none``, ``fs``, ``s3`` and ``swift``.

- ``cascade_grace_period``: Number of minutes after which a product may be
  considered for automatic removal. The default is 0 (immediately).

- ``max_cascade_cycles``: Maximum number of iterations of the automatic removal
  algorithm. The default is 25.

- ``namespace_extensions``: White space separated list of names of Python
  packages or modules that contain namespace definitions (see
  [Extensions](../extensions). The default is the empty list.

- ``product_type_extensions``: White space separated list of names of Python
  modules or packages that contain product type plug-ins (see
  [Extensions](../extensions). The default is the empty list.

- ``hook_extensions``: White space separated list of names of Python
  modules or packages that contain hook extensions (see
  [Extensions](../extensions). The default is the empty list.

- ``remote_backend_extensions``: White space separated list of names of Python
  modules or packages that contain remote backend plug-ins (see
  [Extensions](../extensions)). The default is the empty list.

- ``synchronizers``: White space separated list of names of catalogue
  synchronizers. Each synchronizer should have its own ``synchronizer:<name>``
  section in the archive configuration file.

- ``auth_file``: [Optional] JSON file containing the credentials to download
  using muninn-pull and/or for S3/Swift access credentials.

- ``tempdir``: [Optional] path where temporary data should be stored.


# Section "postgresql"

This section contains backend specific settings for the postgresql backend and
may contain the following settings:

- ``library``: Python library used to connect to postgresql. The default is
  ``psycopg2``. The only other library that is currently supported is ``pg8000``.

- ``connection_string``: Mandatory. A postgresql connection string of the database
  containing product properties. The default is the empty string, which will
  connect to the default database for the user invoking muninn. See psycopg
  and/or pg8000 documentation for the syntax.

- ``table_prefix``: Prefix that should be used for all table names, indices, and
  constraints. This is to allow multiple muninn catalogues inside a single
  database (or have a muninn catalogue together with other tables). The prefix
  will be prefixed without separation characters, so any underscores, etc. need
  to be included in the option value.


# Section "sqlite"

This section contains backend specific settings for the sqlite backend and may
contain the following settings:

- ``connection_string``: Mandatory. A full path to the sqlite database file
  containing the product properties. This file will be automatically created by
  muninn when it first tries to access the database.

- ``table_prefix``: Prefix that should be used for all table names, indices, and
  constraints. This is to allow multiple muninn catalogues inside a single
  database (or have a muninn catalogue together with other tables). The prefix
  will be prefixed without separation characters, so any underscores, etc. need
  to be included in the option value.

- ``mod_spatialite_path``: Path/name of the mod_spatialite library. Will be set
  to 'mod_spatialite' by default (which only works if library is on search path).
  Change this to e.g. /usr/local/lib/mod_spatialite to set an explicit path
  (no filename extension needed).


# Section "none"

This section contains backend specific settings for the ``none`` storage backend,
for which there are currently none.

When using this backend, muninn does not maintain an archive storage, and
instead uses the ``remote_url`` product property to point to products in local
storage.


# Section "fs"

This section contains backend specific settings for the filesystem storage
backend and may contain the following settings:

- ``root``: Mandatory. The root path on disk of the archive.

- ``use_symlinks``: If set to ``true``, an archived product will consist of
  symbolic links to the original product, instead of a copy of the product.
  The default is ``false``.


# Section "s3"

This section contains backend specific settings for the S3 storage
backend and may contain the following settings:

- ``bucket``: Mandatory. The bucket containing the archive.
- ``prefix``: [Optional] archive prefix within bucket.
- ``host``: Mandatory. S3 host name or URL.
- ``port``: Optional. S3 host port.
- ``region``: Optional. Name of the S3 region.
- ``access_key``: Mandatory*. S3 authentication access key.
- ``secret_access_key``: Mandatory*. S3 authentication secret access key.
- ``download_args``: [Optional] JSON representation of boto3 download_file ExtraArgs parameter.
- ``upload_args``: [Optional] JSON representation of boto3 upload_file ExtraArgs parameter.
- ``copy_args``: [Optional] JSON representation of boto3 copy ExtraArgs parameter.
- ``transfer_config``: [Optional] JSON representation of boto3.s3.transfer.TransferConfig parameters.

[*] ``access_key`` and ``secret_access_key`` can be taken from a credentials file pointed to by ``auth_file``. The entry in the credentials file should then have a key value equal to the ``host`` value in the archive configuration and it should contain fields for ``auth_type`` (set to ``S3``),  ``bucket`` (set equal to the ``bucket`` value in the archive configuration), and of course contain the ``access_key`` and ``secret_access_key`` fields.

# Section "swift"

This section contains backend specific settings for the Swift storage
backend and may contain the following settings:

- ``container``: Mandatory. The container containing the archive.
- ``user``: Mandatory*. Swift authentication user name.
- ``key``: Mandatory*. Swift authentication key.
- ``authurl``: Mandatory. Swift authentication auth URL.

[*] ``user`` and ``key`` can be taken from a credentials file pointed to by ``auth_file``. The entry in the credentials file should then have a key value equal to the ``authurl`` value in the archive configuration and it should contain a field for ``auth_type`` (set to ``Swift``), and of course contain the ``user`` and ``key`` fields.

# Section "extension:<product_type_extension>"

Sections starting with ``extension:`` provide additional parameters that can be passed to the given product type extension.
The documentation of the product type extension should define whether this section is applicable and what settings are possible.

# Section "synchronizer:<name>"

This section should exist for each synchronizer that is listed in the ``synchronizers`` setting of the ``archive`` section.
It should contain the following settings:

- ``module``: Mandatory. Python module or package that contains a synchronizer plug-in (see [Extensions](../extensions)).
- ...: any further settings are defined by the synchronizer plug-in.

# Example configuration file

```
[archive]
database = postgresql
storage = fs
namespace_extensions = muninn_sentinel5p
product_type_extensions = muninn_generic_products muninn_sentinel5p
auth_file = /home/alice/credentials.json

[fs]
root = /home/alice/archives/foo

[postgresql]
connection_string = dbname=foo user=alice password=wonderland host=192.168.0.1
```

# Example credentials file

```
{
  "server-one.com": {
     "username": "one",
     "password": "password_one"
  },
  "server-two.com": {
     "username": "two",
     "password": "password_two"
  },
  "https://server-two.com/specific/url/endpoint": {
     "username": "two",
     "password": "password_two"
  },
  "https://server-two.com/oauth/service/endpoint": {
     "auth_type": "oauth2",
     "grand_type": "ResourceOwnerPasswordCredentialsGrant",
     "username": "myuser",
     "password": "somepassword",
     "client_id": "thisclient",
     "client_secret": "somesecret",
     "token_url": "https://authentication-server.com/token/endpoint"
  },
  "s3://some-bucket": {
     "host": "https://s3-provider.com/",
     "access_key": "XXXXXXXXXXXXXXXXXXXX",
     "secret_access_key": "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
  },
  "https://s3-provider.com/": {
     "auth_type": "S3",
     "bucket": "my-bucket",
     "access_key": "XXXXXXXXXXXXXXXXXXXX",
     "secret_access_key": "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
  },
  "another-s3-provider.com": {
     "auth_type": "S3",
     "bucket": "another-bucket",
     "port": 443,
     "access_key": "XXXXXXXXXXXXXXXXXXXX",
     "secret_access_key": "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
  }
}
```
