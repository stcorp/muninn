#
# Copyright (C) 2014-2021 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import os
import argparse

import muninn

from .utils import create_parser, parse_args_and_run


def retrieve(args):
    with muninn.open(args.archive) as archive:
        target_path = os.getcwd() if args.directory is None else args.directory
        archive.retrieve(where=args.expression, target_path=target_path, use_symlinks=args.link)
    return 0


def directory(text):
    if not os.path.isdir(text):
        raise argparse.ArgumentTypeError("no such directory: %r" % text)

    return text


def main():
    parser = create_parser(description="Retrieve products from a muninn archive.")
    parser.add_argument("-d", "--directory", type=directory, help="directory in which retrieved products will be"
                        " stored; by default, retrieved products will be stored in the current working directory")
    parser.add_argument("-l", "--link", action="store_true", help="retrieve using symbolic links instead of copy")
    parser.add_argument("archive", metavar="ARCHIVE", help="identifier of the archive to use")
    parser.add_argument("expression", metavar="EXPRESSION", help="expression used to search for products to retrieve")
    return parse_args_and_run(parser, retrieve)
