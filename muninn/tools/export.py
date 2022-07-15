#
# Copyright (C) 2014-2022 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import argparse
import logging
import os
import re

import muninn

from muninn.tools.utils import Processor, create_parser, parse_args_and_run


class ExportProcessor(Processor):
    def __init__(self, args, target_path):
        super(ExportProcessor, self).__init__(args)
        self.args = args
        self.target_path = target_path

    def perform_operation(self, archive, product):
        archive.export(where=product.core.uuid, target_path=self.target_path, format=self.args.format)
        return 1


def export(args):
    target_path = os.getcwd() if args.directory is None else args.directory
    processor = ExportProcessor(args, target_path)

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

        products = archive.search(where=args.expression, property_names=['uuid'])
        return processor.process(archive, args, products)
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
    parser = create_parser(description="Export products from a muninn archive.", parallel=True)
    parser.add_argument("-d", "--directory", type=directory, help="directory in which retrieved products will be"
                        " stored; by default, retrieved products will be stored in the current working directory")
    parser.add_argument("-f", "--format", type=export_format, help="format in which to export the products; if left"
                        " unspecified, the default export format for the product type will be used")
    parser.add_argument("-l", "--list-formats", action="store_true", help="list alternative (non-default) export"
                        " formats supported by the archive and exit")
    parser.add_argument("archive", metavar="ARCHIVE", help="identifier of the archive to use")
    parser.add_argument("expression", metavar="EXPRESSION", help="expression used to search for products to export")
    return parse_args_and_run(parser, export)


if __name__ == '__main__':
    main()
