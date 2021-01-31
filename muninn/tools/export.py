#
# Copyright (C) 2014-2021 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import argparse
import logging
import os
import re

import muninn

from .utils import create_parser, parse_args_and_run


def export(args):
    with muninn.open(args.archive) as archive:
        if args.list_formats:
            if not archive.export_formats():
                print("no alternative export formats available")
            else:
                print("alternative export formats: " + " ".join(archive.export_formats()))
            print("")
            return 0

        if args.expression is None:
            logging.error("no search expression specified")
            return 1

        target_path = os.getcwd() if args.directory is None else args.directory
        archive.export(where=args.expression, target_path=target_path, format=args.format)
    return 0


def export_format(text):
    if re.match("[a-zA-Z]\\w*$", text) is None:
        raise argparse.ArgumentTypeError("invalid export format: %r" % text)

    return text


def directory(text):
    if not os.path.isdir(text):
        raise argparse.ArgumentTypeError("no such directory: %r" % text)

    return text


def main():
    parser = create_parser(description="Export products from a muninn archive.")
    parser.add_argument("-d", "--directory", type=directory, help="directory in which retrieved products will be"
                        " stored; by default, retrieved products will be stored in the current working directory")
    parser.add_argument("-f", "--format", type=export_format, help="format in which to export the products; if left"
                        " unspecified, the default export format for the product type will be used")
    parser.add_argument("-l", "--list-formats", action="store_true", help="list alternative (non-default) export"
                        " formats supported by the archive and exit")
    parser.add_argument("archive", metavar="ARCHIVE", help="identifier of the archive to use")
    parser.add_argument("expression", metavar="EXPRESSION", help="expression used to search for products to export")
    return parse_args_and_run(parser, export)
