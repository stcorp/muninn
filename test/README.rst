Muninn tests
============

Steps to run the Muninn tests:
------------------------------

- Install the following Python dependencies:
  - pytest
  - boto3
  - swiftclient
  - psycopg2
  - pg8000
  - pyftpdlib
  - sftpserver
  - paramiko
- Configure the following:
  - A Postgresql server
  - an S3 server (e.g., Minio)
  - a Swift server (e.g., https://hub.docker.com/r/morrisjobke/docker-swift-onlyone/)
- Update settings in test.cfg (DEFAULT section: desired combinations for which to run all tests)
- Run:

$ pytest test.py


Quick setup
-----------

These steps provide a quick setup to run the test for all cases.

Use docker to run postgres/minio/swift servers:
$ docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres --name muninn-test-postgresql postgis/postgis
$ docker run -d -p 9000:9000 -p 9001:9001 -e MINIO_ACCESS_KEY=minio -e MINIO_SECRET_KEY=minio1300 --name muninn-test-minio minio/minio server /data --console-address ":9001"
$ docker run -d -p 12345:8080 -e SWIFT_USERNAME=test:tester -e SWIFT_KEY=testing --name muninn-test-swift fnndsc/docker-swift-onlyone

Create a conda environment for muninn and dependencies (requires mininconda/anaconda to be installed):
$ conda create -f environment.yml
$ conda activate muninn-test

Update the path to mod_spatialite in the test.cfg file to point to the version in the conda environment

Update settings in test.cfg (DEFAULT section: desired combinations for which to run all tests).

$ pytest test.py


Steps to check test coverage:
-----------------------------

- Install the following Python dependencies:
  - coverage
  - pytest-cov
- Run ./coverage.sh
- Look at htmlcov/index.html
