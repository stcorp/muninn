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
    version="6.0",
    description="Data product catalogue and archive system",
    url="https://github.com/stcorp/muninn",
    author="S[&]T",
    license="BSD",
    packages=["muninn", "muninn.tools", "muninn.database", "muninn.storage"],
    entry_points={"console_scripts": [
        "muninn-attach = muninn.tools.attach:main",
        "muninn-destroy = muninn.tools.destroy:main",
        "muninn-export = muninn.tools.export:main",
        "muninn-hash = muninn.tools.hash:main",
        "muninn-info = muninn.tools.info:main",
        "muninn-ingest = muninn.tools.ingest:main",
        "muninn-list-tags = muninn.tools.list_tags:main",
        "muninn-prepare = muninn.tools.prepare:main",
        "muninn-pull = muninn.tools.pull:main",
        "muninn-remove = muninn.tools.remove:main",
        "muninn-retrieve = muninn.tools.retrieve:main",
        "muninn-search = muninn.tools.search:main",
        "muninn-strip = muninn.tools.strip:main",
        "muninn-summary = muninn.tools.summary:main",
        "muninn-tag = muninn.tools.tag:main",
        "muninn-untag = muninn.tools.untag:main",
        "muninn-update = muninn.tools.update:main",
    ]},
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Topic :: Scientific/Engineering",
    ],
    python_requires=python_req,
    install_requires=requirements
)

# For the postgres backend you will need psycopg 2.2 or higher.

# For the sqlite backend you will need sqlite/pysqlite2 with the mod_spatialite
# extension.

# For muninn-pull using http requests you will need 'requests' 2.13.0 or higher
# For muninn-pull using sftp requests you will need 'paramiko'
# For muninn-pull with oauth2 authentication you weel need 'requests-oauthlib'
# and 'oauthlib'

# To get more output formatting options for muninn-search install 'tabulate'

# To see progress bars for muninn-update install 'tqdm'
