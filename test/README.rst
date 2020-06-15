Muninn tests
============

Steps to run the Muninn tests:
------------------------------

- Install the following Python dependencies:
  - pytest
  - boto3
  - swiftclient
  - psycopg2 or pg8000 (latter is used if former not available)
- Configure the following:
  - A Postgresql server
  - an S3 server (e.g., Minio)
  - a Swift server (e.g., https://hub.docker.com/r/morrisjobke/docker-swift-onlyone/)
- Update settings in test.cfg (DEFAULT section: desired combinations for which to run all tests)
- Run:

$ python3 -m pytest test.py

  Or:

$ python -m pytest test.py


Quick setup
-----------

These steps provide a quick setup to run the test for all cases.

Use docker to run postgres/minio/swift servers:
$ docker run -d -p 5432:5432 -e POSTGRES_HOST_AUTH_METHOD=trust postgis/postgis
$ docker run -d -p 9000:9000 minio/minio server /data
$ docker run -d -p 12345:8080 morrisjobke/docker-swift-onlyone

Create a conda environment for muninn and dependencies (requires mininconda/anaconda to be installed):
$ conda create -n muninntest
$ conda activate muninntest
$ conda install -c conda-forge request tabulate tqdm pytest psycopg2 libspatialite boto3 python-swiftclient
Update the path to mod_spatialite in the test.cfg file to point to the version in the conda environment
$ python -m pytest test.py


Steps to check test coverage:
-----------------------------

- Install the following Python dependencies:
  - coverage
- Run ./coverage.sh
- Look at htmlcov/index.html
