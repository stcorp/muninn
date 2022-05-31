---
layout: page
title: Installation
permalink: /docs/install/
menu: docs
---

# Installation instructions

To be able to use muninn, you will need:
  - A Unix-based operating system (e.g. Linux).
  - Python2 version 2.7 or Python3 version 3.6 or higher.

For the postgresql database backend:
  - psycopg2 version 2.2 or higher (or pg8000 version 1.13 or higher)
  - PostgreSQL version 8.4 or higher.
  - PostGIS version 2.0 or higher.

For the sqlite database backend:
  - pysqlite >=2.8.3 or python built with sqlite3 loadable extension support
  - libspatialite 4.2.0 or higher

For the S3 storage backend:
  - boto3

For the Swift storage backend:
  - swiftclient

To be able to install muninn, you will need:
  - setuptools

Optional dependencies:
  - requests: to perform a muninn-pull on http/https urls
  - requests-oauthlib and oauthlib: to perform a muninn-pull using oauth2
    authentication
  - tabulate: provides more output format options for muninn-search
  - tqdm: to show a progress bar for muninn-update
  - paramiko: to enable the SFTP remote backend


Muninn is distributed as a source distribution created using setuptools.
It can be installed in several ways, for example using pip or by invoking
setup.py manually. Installation using setup.py with the default prefix will
often require super user privileges.

Using pip:

```
  $ pip install muninn-5.4.tar.gz
```

Using setup.py:

```
  $ tar xvfz muninn-5.4.tar.gz
  $ cd muninn-5.4
  $ python setup.py install
```

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
