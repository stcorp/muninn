#!/bin/sh
python3 -m pytest --cov=muninn test.py
python3 -m coverage html

