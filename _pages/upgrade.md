---
layout: page
title: Upgrading
permalink: /upgrade/
---

# Upgrading to version 5.4

The keyword 'grand_type' for oauth2 entries in any auth_file credentials
configuration file should be renamed to the correctly spelled 'grant_type'.

# Upgrading to version 5.0

Although 5.0 is backwards compatible with 4.x, you will get deprecation
warnings. For each archive configuration file you should start migrating to
the new approach. For example, if you had:

```
[archive]
root = /home/alice/archives/foo
backend = postgresql
use_symlinks = true
```

then you should change this to:

```
[archive]
database = postgresql
storage = fs

[fs]
root = /home/alice/archives/foo
use_symlinks = true
```

# Upgrading to version 4.0

These upgrade steps are provided for Postgres only.
For each existing archive, please perform the following steps:

1. Perform step 1 from "Upgrading to version 2.0"_.

   Note that migration of postgres databases that used a schema_name is not
   supported.

2. Execute the following SQL statements.

```
    BEGIN;
    ALTER TABLE core ALTER COLUMN archive_date DROP NOT NULL;
    ALTER TABLE core ADD COLUMN metadata_date timestamp NOT NULL DEFAULT now();
    ALTER TABLE core ALTER COLUMN metadata_date DROP DEFAULT;
    ALTER TABLE core ADD COLUMN remote_url text;
    COMMIT;
```

    You might also want to create indices for the new fields:

```
    CREATE INDEX idx_core_metadata_date ON core (metadata_date);
    CREATE INDEX idx_core_remote_url ON core (remote_url);
```

# Upgrading to version 2.0

Previous versions of muninn imported all Python modules or packages found on
the extension search path. Each extension could contain both namespace
definitions and product type plug-ins.

Muninn 2.0 requires that an extension contains either namespace definitions
_or_ product type plug-ins, but not both. The extensions required by an archive
should be listed explicitly by name in the archive configuration file.

The ``PYTHONPATH`` environment variable should be set such that the listed
extensions can be imported by muninn. The ``MUNINN_EXTENSION_PATH`` environment
variable is no longer in use.

For each existing archive, please perform the following steps:

1. Login to the database used by the archive to be upgraded (e.g. using the
   psql command line tool included with Postgres). The connection details can
   be found in the archive configuration file (see section "Archive
   configuration files"_ if you are unfamiliar with these files).

   NB. Any occurence of "<schema name>" in any SQL statements found in this
   section should be substituted by the name schema name found in the archive
   configuration file (section "postgresql", entry "schema_name"). If no
   schema name is present in the archive configuration file, any occurence of
   "<schema name>" including the dot "." suffix should be removed from any
   SQL statements found in this section.

2. Muninn 2.0 requires the combination of product type and product name to be
   unique. In step 3, a constraint will be added to the database to enforce
   this requirement. Adding this constraint will only succeed if the archive
   does not contain any products that violate the constraint.

   To check an existing archive for duplicate combinations of product type and
   product name, please execute the following SQL statement:

     SELECT product_type, product_name, count(*) FROM <schema name>.core
         GROUP BY product_type, product_name HAVING count(*) > 1;

   You can use the result to locate any offending group of products and
   eleminate the duplicates (e.g. using muninn-search and muninn-remove).
   Proceed to the next step once the SQL statement above returns an empty
   result.

3. Execute the following SQL statements.

    ```
    BEGIN;
    ALTER TABLE <schema name>.core ADD COLUMN size bigint;
    ALTER TABLE <schema name>.core RENAME COLUMN logical_name TO product_name;
    ALTER TABLE <schema name>.core ADD CONSTRAINT core_product_name_uniq
        UNIQUE (product_type, product_name);
    COMMIT;
    ```

4. Update product type plug-ins to use ``core.product_name`` instead of
   ``core.logical_name``. Split extensions that contain both namespace
   definitions and product type plug-ins. In most cases, this will be taken
   care of by the extension developer and you only need to update each
   extension to the latest version.

5. Update the archive configuration file by adding the ``namespace_extensions``
   and ``product_type_extensions`` configuration options (see section
   "Archive configuration files"_).

# Upgrading to version 1.3

For each existing archive, please perform the following steps:

1. Login to the database used by the archive to be upgraded (e.g. using the
   psql command line tool included with Postgres). The connection details can
   be found in the archive configuration file (see section "Archive
   configuration file" if you are unfamiliar with these files).

   NB. Any occurence of "<schema name>" in any SQL statements found in this
   section should be substituted by the name schema name found in the archive
   configuration file (section "postgresql", entry "schema_name"). If no
   schema name is present in the archive configuration file, any occurence of
   "<schema name>" including the dot "." suffix should be removed from any
   SQL statements found in this section.

2. Execute the following SQL statements.

```
    BEGIN;
    CREATE TABLE <schema name>.tag (uuid UUID, tag TEXT);
    ALTER TABLE <schema name>.tag ADD PRIMARY KEY (uuid, tag);
    ALTER TABLE <schema name>.tag ADD CONSTRAINT tag_uuid_fkey FOREIGN KEY
        (uuid) REFERENCES <schema name>.core (uuid) ON DELETE CASCADE;
    COMMIT;
```
