---
layout: page
title: Usage
permalink: /usage/
---

# Using muninn

This section provides a brief overview of the available command-line tools and
describes how to create and remove a muninn archive.

## Command-line tools

Muninn comes with a number of command-line tools to interact with muninn
archives.

These tools are:
  - muninn-attach
  - muninn-destroy
  - muninn-export
  - muninn-ingest
  - muninn-list-tags
  - muninn-prepare
  - muninn-pull
  - muninn-remove
  - muninn-retrieve
  - muninn-search
  - muninn-strip
  - muninn-summary
  - muninn-tag
  - muninn-untag

Running any of these tools with the "-h" or "--help" option provides detailed
information on its purpose and usage.

For an overview of the expression language used by most of these tools to
select the products to operate on, see [Expression Language](../expr).

# Creating an archive

The steps to create an archive are:
  1. Perform backend specific initialization (if required).
  2. Create a configuration file describing the archive.
  3. Run muninn-prepare to initialize the archive for use.

When using the PostgreSQL database backend, you need to create a PostGIS
enabled database that muninn can use to store product properties. Multiple
archives can share the same database, as long as they use a different table
prefix.

Depending on your PostgreSQL installation, creating a database could be as
simple as:

```
$ createdb [database name]
$ psql -d [database name] -c "CREATE EXTENSION postgis;"
```

For Sqlite, muninn will automatically create the database file when it is first
accessed.

Next, you need to create a configuration file for the archive. See
[Configuration](../config) for details on the configuration file format.

Make sure the configuration file is stored somewhere on the configuration
search path (see [Installation](../install)). Move the file or update
the search path if this is not the case.

The final step is to run the ``muninn-prepare`` command-line tool to initialize
the archive for use:

```
$ muninn-prepare [archive id]
```

You should now be able to ingest, search for, retrieve, export, and remove
products using the corresponding command-line tools.

# Removing an archive

The steps to completely remove an archive are:
  1. Run muninn-destroy to remove all products and product properties
     contained in the archive.
  2. Remove the archive configuration file (optional).
  3. Perform backend specific clean-up (if required).

The first step is to run the "muninn-destroy" command-line tool to remove all
products and product properties contained in the archive:

```
$ muninn-destroy [archive id]
```

Next, you can optionally remove the archive configuration file.

If no other archives share the PostgreSQL database used by the archive you just
removed, you can proceed to remove the database: ::

```
$ dropdb [database name]
```
