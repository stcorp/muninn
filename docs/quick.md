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
$ curl -OL https://atmospherevirtuallab.org/files/S5P_OFFL_L2__NO2____20200123T004109_20200123T022240_11799_01_010302_20200126T123552.nc

$ muninn-ingest myarch S5P_OFFL_L2__NO2____20200123T004109_20200123T022240_11799_01_010302_20200126T123552.nc
```

Search all products in the archive, showing all available metadata properties:

```
$ muninn-search -p '*' myarch ""
```

Get a summary of all products in the archive grouped by product type:

```
$ muninn-summary -g product_type myarch
```
