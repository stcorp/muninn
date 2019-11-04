#!/bin/sh
coverage3 run --source ../muninn -m pytest test.py
coverage3 html

