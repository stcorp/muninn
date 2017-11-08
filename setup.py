from setuptools import setup
import sys

pyversion = sys.version_info
if pyversion[0] == 2 and pyversion[1] >= 6:
    python_req = ">=2.6"
elif pyversion >= (3, 6):
    python_req = ">=3.6"
else:
    # python_requires is only availabe since setuptools 24.2.0 and pip 9.0.0
    sys.exit("Python 2.6 (or newer) or 3.6 (or newer) is required to use this package.")


requirements = []
if sys.version_info[0] == 2 and sys.version_info[1] == 6:
    requirements += ["argparse"]

setup(
    name="muninn",
    version="4.0",
    description="Configurable product archive",
    url="http://stcorp.nl/",
    author="S[&]T",
    author_email="info@stcorp.nl",
    license="BSD",
    packages=["muninn", "muninn.backends", "muninn.tools"],
    entry_points={"console_scripts": ["muninn-destroy = muninn.tools.destroy:main",
                                      "muninn-export = muninn.tools.export:main",
                                      "muninn-info = muninn.tools.info:main",
                                      "muninn-ingest = muninn.tools.ingest:main",
                                      "muninn-list-tags = muninn.tools.list_tags:main",
                                      "muninn-prepare = muninn.tools.prepare:main",
                                      "muninn-remove = muninn.tools.remove:main",
                                      "muninn-retrieve = muninn.tools.retrieve:main",
                                      "muninn-search = muninn.tools.search:main",
                                      "muninn-strip = muninn.tools.strip:main",
                                      "muninn-tag = muninn.tools.tag:main",
                                      "muninn-untag = muninn.tools.untag:main",
                                      "muninn-update = muninn.tools.update:main",
                                      "muninn-pull = muninn.tools.pull:main"]},
    python_requires=python_req,
    install_requires=requirements
)

# For the postgres backend you will need psycopg 2.2 or higher.
# psycopg 2.0.13 did not work when transfering geospatial information to/from
# the database, but version 2.4.5 did. It is likely that the 'string literal'
# bug fix from version 2.2 changed things.

# For the sqlite backend you will need pyspatialite>=3.0.1 or sqlite/pysqlite with
# the mod_spatialite extension.

# For muninn-pull using http requests you will need requests 2.13.0 or higher

# To get more output formatting options for muninn-search install 'tabulate'
