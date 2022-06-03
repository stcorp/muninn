---
layout: page
title: Quick Start
permalink: /quick/
---

Create and activate a Conda environment:

```
$ conda create -n muninn-test python=3.10
$ conda activate muninn-test
$ conda install muninn
```

Install a specific Muninn extension, for example:

```
$ git clone https://github.com/stcorp/muninn-sentinel5p.git
$ cd muninn-sentinel5p
$ python setup.py install
```

Create a basic config file, `my_arch.cfg`, referencing this extension:

```
[archive]
database = sqlite
storage = fs
product_type_extensions = muninn_sentinel5p
namespace_extensions = muninn_sentinel5p

[fs]
root = /tmp/my_arch

[sqlite]
connection_string = /tmp/my_arch.db
```

Make sure Muninn can find the config file:

```
$ export MUNINN_CONFIG_PATH=.
```

Create a Muninn archive:

```
$ muninn-prepare my_arch
```

Ingest a sentinel5p product into the archive:

```
$ wget https://data-portal.s5p-pal.com/cat/sentinel-5p/download/14491c3e-1e15-429a-b4e1-6bab267a1a83 \
       -O S5P_PAL__L2__NO2____20210323T011843_20210323T030013_17829_01_020301_20211112T094255.nc

$ muninn-ingest my_arch S5P_PAL__L2__NO2____20210323T011843_20210323T030013_17829_01_020301_20211112T094255.nc
```

Search all products in the archive:

```
$ muninn-search my_arch ""
```
