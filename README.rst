Muninn
======

Muninn is a library and a set of command-line tools to create and manage
data product catalogues and archives. It can function as a pure product
metadata catalogue or it can manage a product archive together with its
metadata catalogue.

When using a product archive, muninn can automatically extract properties
from the products when products get added to the archive. Automatic property
extraction is handled through product type specific plug-ins
(see section "Extensions"_), which are *not* included in the muninn
distribution.

Muninn uses the concept of namespaces to group different sets of properties
for a product together within the catalogue. Muninn itself provides a 'core'
namespace that covers the most common properties for data products.
Support for additional namespaces are handled through external plug-ins
(see section "Extensions"_).

In Norse mythology, Muninn is a raven that, together with another raven called
Huggin, gather information for the god Odin. Muninn is Old Norse for "memory".


Installation instructions
=========================
To be able to use muninn, you will need:
  - A Unix-based operating system (e.g. Linux).
  - Python version 2.6 or higher, or Python 3.6 or higher.

For the postgresql backend:
  - psycopg2 version 2.2 or higher.
  - PostgreSQL version 8.4 or higher.
  - PostGIS version 2.0 or higher.

For the sqlite backend:
  - pysqlite >=2.8.3 or python built with sqlite3 loadable extension support
  - libspatialite 4.2.0 or higher

To be able to install muninn, you will need:
  - setuptools version 0.6 or compatible.

Optional dependencies:
  - argparse: mandatory when using Python 2.6
  - requests: to perform a muninn-pull on http/https urls
  - tabulate: provides more output format options for muninn-search
  - tqdm: to show a progress bar for muninn-update


Muninn is distributed as a source distribution created using setuptools version
0.6. It can be installed in several ways, for example using pip or by invoking
setup.py manually. Installation using setup.py requires super user privileges
in most cases.

Using pip: ::

  $ pip install muninn-4.3.tar.gz

Using setup.py: ::

  $ tar xvfz muninn-4.3.tar.gz
  $ cd muninn-4.3
  $ python setup.py install

The muninn distribution contains a generic archiving framework that cannot be
used in a meaningful way without product type plug-ins. Therefore, after
installing muninn, you will need to install (or implement) one or more muninn
extensions (see section "Extensions"_).

The extensions required by each archive should be added to the corresponding
archive configuration file (see section "Archive configuration files"_). If you
install extensions in a custom location, please update the ``PYTHONPATH``
environment variable accordingly.

Ensure the ``MUNINN_CONFIG_PATH`` environment variable, which should contain
the muninn configuration search path, is set. The configuration search path is
a colon (``:``) separated list of any combination of explicit paths to archive
configuration files and directories that will be scanned for archive
configuration files.

All muninn command-line tools, as well as the muninn.open() library function,
refer to an archive using its "archive id". This is a name that corresponds to
an archive configuration file (see section "Archive configuration files"_)
located in one of the directories on the muninn configuration search path.

For example, given an archive id ``foo``, muninn expects to find a
configuration file called ``foo.cfg`` on the search path.


Upgrade instructions
====================
This section contains instructions for upgrading existing archives that were
created with prior versions of muninn.

Upgrading to version 4.0
------------------------

These upgrade steps are provided for Postgres only.
For each existing archive, please perform the following steps:

1. Perform step 1 from "Upgrading to version 2.0"_.

   Note that migration of postgres databases that used a schema_name is not
   supported.

2. Execute the following SQL statements. ::

     BEGIN;
     ALTER TABLE <schema name>.core ALTER COLUMN archive_date DROP NOT NULL;
     ALTER TABLE <schema name>.core ADD COLUMN metadata_date timestamp NOT NULL DEFAULT now();
     ALTER TABLE <schema name>.core ALTER COLUMN metadata_date DROP DEFAULT;
     ALTER TABLE <schema name>.core ADD COLUMN remote_url text;
     COMMIT;

   You might also want to create indices for the new fields: ::

     CREATE INDEX idx_core_metadata_date ON core (metadata_date);
     CREATE INDEX idx_core_remote_url ON core (remote_url);

Upgrading to version 2.0
------------------------
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
   product name, please execute the following SQL statement: ::

     SELECT product_type, product_name, count(*) FROM <schema name>.core
         GROUP BY product_type, product_name HAVING count(*) > 1;

   You can use the result to locate any offending group of products and
   eleminate the duplicates (e.g. using muninn-search and muninn-remove).
   Proceed to the next step once the SQL statement above returns an empty
   result.

3. Execute the following SQL statements. ::

     BEGIN;
     ALTER TABLE <schema name>.core ADD COLUMN size bigint;
     ALTER TABLE <schema name>.core RENAME COLUMN logical_name TO product_name;
     ALTER TABLE <schema name>.core ADD CONSTRAINT core_product_name_uniq
         UNIQUE (product_type, product_name);
     COMMIT;

4. Update product type plug-ins to use ``core.product_name`` instead of
   ``core.logical_name``. Split extensions that contain both namespace
   definitions and product type plug-ins. In most cases, this will be taken
   care of by the extension developer and you only need to update each
   extension to the latest version.

5. Update the archive configuration file by adding the ``namespace_extensions``
   and ``product_type_extensions`` configuration options (see section
   "Archive configuration files"_).

Upgrading to version 1.3
------------------------
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

2. Execute the following SQL statements. ::

     BEGIN;
     CREATE TABLE <schema name>.tag (uuid UUID, tag TEXT);
     ALTER TABLE <schema name>.tag ADD PRIMARY KEY (uuid, tag);
     ALTER TABLE <schema name>.tag ADD CONSTRAINT tag_uuid_fkey FOREIGN KEY
         (uuid) REFERENCES <schema name>.core (uuid) ON DELETE CASCADE;
     COMMIT;


Using muninn
============
This section provides a brief overview of the available command-line tools and
describes how to create and remove a muninn archive.

Command-line tools
------------------
Muninn comes with a number of command-line tools to interact with muninn
archives.

These tools are:
  - muninn-destroy
  - muninn-export
  - muninn-ingest
  - muninn-pull
  - muninn-list-tags
  - muninn-prepare
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
select the products to operate on, see the section "Expression language".

Creating an archive
-------------------
The steps to create an archive are:
  1. Perform backend specific initialization (if required).
  2. Create a configuration file describing the archive.
  3. Run muninn-prepare to initialize the archive for use.

When using the PostgreSQL backend, you need to create a PostGIS enabled
database that muninn can use to store product properties. Multiple archives can
share the same database, as long as they use a different schema name.

Depending on your PostgreSQL installation, creating a database could be as
simple as: ::

  $ createdb [database name]
  $ psql -d [database name] -c "CREATE EXTENSION postgis;"

For Sqlite, muninn will automatically create the database file when it is first
accessed.

Next, you need to create a configuration file for the archive. See the section
"Archive configuration files"_ for details on the the configuration file
format.

Make sure the configuration file is stored somewhere on the configuration
search path (see section "Installation instructions"_). Move the file or update
the search path if this is not the case.

The final step is to run the ``muninn-prepare`` command-line tool to initialize
the archive for use: ::

  $ muninn-prepare [archive id]

You should now be able to ingest, search for, retrieve, export, and remove
products using the corresponding command-line tools.

Removing an archive
-------------------
The steps to completely remove an archive are:
  1. Run muninn-destroy to remove all products and product properties
     contained in the archive.
  2. Remove the archive configuration file (optional).
  3. Perform backend specific clean-up (if required).

The first step is to run the "muninn-destroy" command-line tool to remove all
products and product properties contained in the archive: ::

  $ muninn-destroy [archive id]

Next, you can optionally remove the archive configuration file. Note that if
you do not remove this file (and if can be found on the configuration search
path), other users can still try to access the non-existing archive.

If no other archives share the PostgreSQL database used by the archive you just
removed, you can proceed to remove the database: ::

  $ dropdb [database name]


Extensions
==========
Muninn is a generic archiving framework. To be able to use it to archive
specific (types of) products, it is necessary to install one or more
extensions.

A muninn extension is a Python module or package that implements the muninn
extension interface. Muninn defines two types of extensions: namespace
extensions (that contain namespace definitions) and product type extensions
(that contain product type plug-ins).

A namespace is a named set of product properties (see section "Namespaces"_).
Muninn defines a namespace called ``core`` that contains a small set of
properties that muninn needs to archive a product. For example, it contains the
name of the product, its SHA1 hash, UUID, and archive date.
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
in is to extract product properties and tags from products of the type that it
supports. At the minimum, this involves extracting all the required properties
defined in the "core" namespace. Without this information, muninn cannot
archive the product.

Product type plug-ins can also be used to tailor certain aspects of muninn. For
example, the plug-in controls what happens to a product (of the type it
supports) when all of the products it is linked to (see section "Links"_) have
been removed from the archive.


Archive configuration files
===========================
An archive configuration file is a text file that describes an archive. The
configuration file for an archive with id ``foo`` should be called ``foo.cfg``.

The configuration file format resembles Windows INI files in that it consists
of named sections starting with a ``[section]`` header followed by
``name = value`` entries. Each section will be discussed in detail below.

Section "archive"
-----------------
This section contains general archive settings and may contain the following
settings:

- ``root``: The root path on disk of the archive.

- ``backend``: The backend used for storing product properties. The currently
  supported backends are ``postgresql`` and ``sqlite``.

- ``use_symlinks``: If set to ``true``, an archived product will consist of
  symbolic links to the original product, instead of a copy of the product.
  The default is ``false``.

- ``cascade_grace_period``: Number of minutes after which a product may be
  considered for automatic removal. The default is 0 (immediately).

- ``max_cascade_cycles``: Maximum number of iterations of the automatic removal
  algorithm. The default is 25.

- ``external_archives``: White space separated list of archive ids of archives
  that may contain products linked to by products stored in this archive.
  The default is the empty list.

- ``namespace_extensions``: White space separated list of names of Python
  packages or modules that contain namespace definitions (see section
  "Extensions"_). The default is the empty list.

- ``product_type_extensions``: White space separated list of names of Python
  modules or packages that contain product type plug-ins (see section
  "Extensions"_). The default is the empty list.

- ``remote_backend_extensions``: White space separated list of names of Python
  modules or packages that contain remote backend plug-ins (see section
  "Extensions"_). The default is the empty list.

- ``auth_file``: [Optional] JSON file containing the credentials to download using
  muninn-pull


Section "postgresql"
--------------------
This sections contains backend specific settings for the postgresql backend and
may contain the following settings:

- connection_string: Mandatory. A postgresql connection string of the database
  containing product properties. The default is the empty string, which will
  connect to the default database for the user invoking muninn. See psycopg
  documentation for the syntax.

- table_prefix: Prefix that should be used for all table names, indices, and
  constraints. This is to allow multiple muninn catalogues inside a single
  database (or have a muninn catalogue together with other tables). The prefix
  will be prefixed without separation characters, so any underscores, etc. need
  to be included in the option value.

Section "sqlite"
----------------
This sections contains backend specific settings for the postgresql backend and
may contain the following settings:

- connection_string: Mandatory. A full path to the sqlite database file
  containing the product properties. This file will be automatically created by
  muninn when it first tries to access the database.

- table_prefix: Prefix that should be used for all table names, indices, and
  constraints. This is to allow multiple muninn catalogues inside a single
  database (or have a muninn catalogue together with other tables). The prefix
  will be prefixed without separation characters, so any underscores, etc. need
  to be included in the option value.

- mod_spatialite_path: Path/name of the mod_spatialite library. Will be set to
  'mod_spatialite' by default (which only works if library is on search path).
  Change this to e.g. /usr/local/lib/mod_spatialite to set an explicit path
  (no filename extension needed).

Example configuration file
--------------------------
::

  [archive]
  root = /home/alice/archives/foo
  backend = postgresql
  use_symlinks = true
  product_type_extensions = cryosat asar
  auth_file = /home/alice/credentials.json

  [postgresql]
  connection_string = dbname=foo user=alice password=wonderland host=192.168.0.1

Example credentials file
--------------------------
::

       {
          "server-one.com": {
             "username": "one",
             "password": "password_one"
          },
          "server-two.com": {
             "username": "two",
             "password": "password_two"
          }
       }

Data types
==========
Each product property can be of one of the following supported types: boolean,
integer, long, real, text, timestamp, uuid, and geometry. These types are
described in detail below.

The boolean type represents a truth value and has two possible states: ``true``
and ``false``.

The valid literal boolean values are:

  ``true``

  ``false``

The integer types (integer and long) represent whole numbers. The integer type
is a 32-bit signed integer and can be used to represent values in the range
-2147483648 to +2147483647 (inclusive). The long type is a 64-bit signed
integer and can be used to represent values in the range -9223372036854775808
to +9223372036854775807 (inclusive).

Some examples of literal integer values:

  ``-3``

  ``0``

  ``10``

  ``+99``

The floating point type (real) represents fractional numbers. The real type is
a double precision floating point number and has a typical range of around
1E-307 to 1E+308 with a precision of at least 15 digits.

Some examples of literal real values:

  ``1E-5``

  ``1.E+10``

  ``-3.1415E0``

  ``1.0``

The text type represents text. Literal values are enclosed in double quotes and
most common backslash escape sequence are recognized. To include a double quote
or a backslash inside a text literal, they must be escaped with a backslash,
i.e. ``\"`` and ``\\``.

Some examples of literal text values:

  ``"Hello world!\n"``

  ``"This is a so-called \"text\" literal."``

The timestamp type represents an instance in time with microsecond resolution.
Time zone information is not included. Although throughout muninn all
timestamps are expressed in UTC, users (and especially product type plug-in
developers) can choose a different convention (e.g. local time) for custom
product properties.

The minimum and maximum timestamp values are ``0001-01-01T00:00:00.000000`` and
``9999-12-31T23:59:59.999999`` respectively, which may also be written as
``0000-00-00T00:00:00.000000`` and ``9999-99-99T99:99:99.999999`` for
convenience.

Some examples of literal timestamp values:

  ``2000-01-01``

  ``2000-01-01T00:00:00``

  ``2000-01-01T00:00:00.``

  ``2000-01-01T00:00:00.3``

  ``1999-12-21T23:59:59.999999``

  ``0000-00-00``

  ``0000-00-00T00:00:00``

  ``9999-99-99T99:99:99.99``

The uuid type represents a universally unique identifier, a 128-bit number that
is used to uniquely identify products in a muninn archive.

Some examples of literal uuid values:

  ``32a61528-a712-427a-b28f-8ebd5b472b16``

  ``873dd103-2115-4bf8-9f05-d0eb4b3f71ea``

  ``bdc10916-d89f-416c-8987-a9c2af9b1ef7``

The geometry type represents two-dimensional geometric objects. The spatial
reference system used is WGS84 (SRID=4326). Longitude is measured in degrees
East, latitude is measured in degrees North. The coordinates of a point are
ordered as (longitude, latitude).

The geometric objects currently supported are: Point, LineString, Polygon,
MultiPoint, MultiLineString, and MultiPolygon.

The linear ring(s) that make up a polygon should be topologically closed. In
other words, the start and end point of any linear ring should be equal. A
polygon of which the exterior ring is ordered anti-clockwise is seen from the
"top". Any interior rings should be ordered in the direction opposite to the
exterior ring.

A sub-set of the Well Known Text (WKT) markup language is used to represent
literal geometry values. This sub-set is limited to the supported geometric
objects listed above. Only two-dimensional coordinates are supported. Empty
geometries are supported. An empty geometry is represented by the name of the
geometry type followed by the keyword ``EMPTY``.

Some examples of literal geometry values:

  ``POINT (3.0 55.0)``

  ``LINESTRING (3.0 55.0, 3.0 80.0, 5.0 75.0)``

  ``POLYGON ((5.0 52.0, 6.0 53.0, 3.0 52.5, 5.0 52.0))``

  ``POLYGON EMPTY``


Namespaces
==========
A namespace is a named set of product properties. The concept of a namespace is
used to group related product properties and to avoid name clashes. Any product
property can be defined to be either optional or mandatory.

For example, the definition of the ``core`` namespace includes the mandatory
property ``uuid``, and the optional properties ``validity_start`` and
``validity_stop``. The full name of these product properties is ``core.uuid``,
``core.validity_start``, and ``core.validity_stop``.


Links
=====
Product stored in a muninn archive can be linked to other products in the same
archive (or even to products stored in a different archive).

A link between a product A and a product B represents a relation between these
products where product A is considered to be the source of product B in some
sense (and consequently product B is considered to be derived from product A).

This information is useful for tracing the origin of a given product. Also, it
is possible to (for example) automatically remove a product whenever all of its
sources have been removed. Or to export certain derived products and / or
source products along with a product being exported.


Expression language
===================
To make it easy to search for products in an archive, muninn implements its own
expression language. The expression language is somewhat similar to the WHERE
clause in an SQL SELECT statement.

When a muninn extension includes namespace definitions, all product properties
defined in these namespaces can be used in expressions.

The details of the expression language are described below. See the section
"Data types"_ for more information about the data types supported by muninn.

Property references
-------------------
A product property ``x`` defined in namespace ``y`` is referred to using
``y.x``. If the namespace prefix ``y`` is omitted, it defaults to ``core``.
This means that any property from the ``core`` namespace may be referenced
directly.

Some examples of property references:

  ``uuid``

  ``validity_start``

  ``core.uuid``

  ``core.validity_start``


Parameter references
--------------------
A name preceded by an at sign ``@`` denotes the value of the parameter with
that name. This is primarily useful when calling library functions that take an
expression as an argument. These functions will also take a dictionary of
parameters that will be used to resolved any parameters references present in
the expression.

Some examples of parameter references:

  ``@uuid``

  ``@start``

Functions and operators
-----------------------
The supported logical operators are ``not``, ``and``, ``or``, in order of
decreasing precedence.

The comparison operators ``==`` (equal) and ``!=`` (not equal) are supported
for all types except geometry.

The comparison operators ``<`` (less than), ``>`` (greater than), ``<=`` (less
than or equal), ``>=`` (greater than or equal) are supported for all types
except boolean, uuid, and geometry.

The comparison operator ``~=`` (matches pattern) is supported only for text.
The syntax is:

    text ~= pattern

Any character in the pattern matches itself, except the percent sign ``%``, the
underscore ``_``, and the backslash ``\``.

The percent sign ``%`` matches any sequence of zero or more characters. The
underscore ``_`` matches any single characters. To match a literal percent sign
or underscore, it must be preceded by a backslash ``\``. To match a literal
backslash, write four backslashes ``\\\\``.

The result of the comparison is true only if the pattern matches the text value
on the left hand side. Therefore, to match a pattern anywhere it should be
preceded and followed by a percent sign.

Some examples of the ``~=`` operator:

    ``"foobarbaz" ~= "foobarbaz"``      (true)

    ``"foobarbaz" ~= "foo"``            (false)

    ``"foobarbaz" ~= "%bar%"``          (true)

    ``"foobarbaz" ~= "%ba_"``           (true)

The unary and binary arithmetic operators ``+`` and ``-`` are supported for all
numeric types. Furthermore, the binary operator ``-`` applied to a pair of
timestamps returns the length of the time interval between the timestamps as a
fractional number of seconds. Due to the way timestamps are represented in
sqlite, time intervals are limited to millisecond precision when using the
sqlite backend.

The unary function ``is_defined`` is supported for all data types and returns
true if its argument is defined. This can be used to check if optional
properties are defined or not.

The function ``covers(timestamp, timestamp, timestamp, timestamp)`` returns
true if the time range formed by the pair of timestamps covers the time range
formed by the second pair of timestamps. Both time ranges are closed.

The function ``intersects(timestamp, timestamp, timestamp, timestamp)`` returns
true if the time range formed by the pair of timestamps intersects the time
range formed by the second pair of timestamps. Both time ranges are closed.

The function ``covers(geometry, geometry)`` returns true if the first geometry
covers the second geometry.

The function ``intersects(geometry, geometry)`` returns true if the first
geometry intersects the second geometry.

The function ``is_source_of(uuid)`` returns true if the product under
consideration is a (direct) source of the product referred to by specified
uuid.

The function ``is_derived_from(uuid)`` returns true if the product under
consideration is (directly) derived from the product referred to by the
specified uuid.

The function ``has_tag(text)`` returns true if the product under consideration
is tagged with the specified tag.

The function ``now()`` returns a timestamp that represents the current time in
UTC.

Examples
--------

  ``is_defined(core.validity_start) and core.validity_start < now()``

  ``covers(core.validity_start, core.validity_stop, @start, @stop)``

  ``covers(core.footprint, POINT (5.0 52.0))``

  ``is_derived_from(32a61528-a712-427a-b28f-8ebd5b472b16)``

  ``validity_stop - validity_start > 300`` (timestamp differences are in seconds)
