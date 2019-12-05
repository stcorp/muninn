Muninn tests
============

Steps to run the Muninn tests:

- Install the following Python dependencies:
  - pytest
  - boto3
  - swiftclient
- Configure the following:
  - A Postgresql server
  - an S3 server (e.g., Minio)
  - a Swift server (e.g., https://hub.docker.com/r/morrisjobke/docker-swift-onlyone/)
- Update settings in test.cfg.template
- Run pytest test.py
