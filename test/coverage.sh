#!/bin/sh
python3 -m coverage run --source ../muninn -m pytest test.py
python3 -m coverage html

