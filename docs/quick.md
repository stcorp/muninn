---
layout: page
title: Quick Start
permalink: /quick/
---

Create and activate a Conda environment:

```
$ conda create -n muninn-test
$ conda activate muninn-test
$ conda install -c conda-forge muninn
```

Install a specific Muninn extension, for example:

```
$ pip install muninn-sentinel5p
```

Create a directory for the example archive:

```
$ mkdir /home/user/testarchive
$ cd /home/user/testarchive
```

Create a basic config file, `/home/user/testarchive/myarch.cfg`, referencing this extension:

```
[archive]
database = sqlite
storage = fs
product_type_extensions = muninn_sentinel5p
namespace_extensions = muninn_sentinel5p

[fs]
root = /home/user/testarchive/archive

[sqlite]
connection_string = /home/user/testarchive/myarch.db
```

Make sure Muninn can find the config file:

```
$ export MUNINN_CONFIG_PATH=/home/user/testarchive/myarch.cfg
```

Create a Muninn archive:

```
$ muninn-prepare myarch
```

Ingest a sentinel5p product into the archive:

```
$ curl -L --output S5P_PAL__L2__NO2____20210323T011843_20210323T030013_17829_01_020301_20211112T094255.nc \
https://data-portal.s5p-pal.com/cat/sentinel-5p/download/14491c3e-1e15-429a-b4e1-6bab267a1a83

$ muninn-ingest myarch S5P_PAL__L2__NO2____20210323T011843_20210323T030013_17829_01_020301_20211112T094255.nc
```

Search all products in the archive, showing all available metadata properties:

```
$ muninn-search -p '*' myarch ""
```

Get a summary of all products in the archive grouped by product type:

```
$ muninn-summary -g product_type myarch
```
