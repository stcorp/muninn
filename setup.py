from setuptools import setup
import sys

if sys.hexversion < 0x02060000:
    sys.exit("Python 2.6 or newer is required to use this package.")

requirements = []
if sys.version_info[0] == 2 and sys.version_info[1] == 6:
    requirements = ["argparse"]

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
                                      "muninn-ingest = muninn.tools.ingest:main",
                                      "muninn-list-tags = muninn.tools.list_tags:main",
                                      "muninn-prepare = muninn.tools.prepare:main",
                                      "muninn-remove = muninn.tools.remove:main",
                                      "muninn-retrieve = muninn.tools.retrieve:main",
                                      "muninn-search = muninn.tools.search:main",
                                      "muninn-strip = muninn.tools.strip:main",
                                      "muninn-tag = muninn.tools.tag:main",
                                      "muninn-untag = muninn.tools.untag:main",
                                      "muninn-pull = muninn.tools.pull:main"]},
    install_requires=requirements
)

# For the postgres backend you will need psycopg 2.2 or higher.
# psycopg 2.0.13 did not work when transfering geospatial information to/from
# the database, but version 2.4.5 did. It is likely that the 'string literal'
# bug fix from version 2.2 changed things.

# For the sqlite backend you will need pyspatialite 3.0.1 or higher.
