#
# Copyright (C) 2014-2021 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import muninn

from .utils import create_parser, parse_args_and_run


def tag(args):
    with muninn.open(args.archive) as archive:
        for product in archive.search(where=args.expression, property_names=['uuid']):
            archive.tag(product.core.uuid, args.tag)

    return 0


def main():
    parser = create_parser(description="Set one or more tags on products contained in a muninn archive")
    parser.add_argument("archive", metavar="ARCHIVE", help="identifier of the archive to use")
    parser.add_argument("expression", metavar="EXPRESSION", help="expression used to search for products to tag")
    parser.add_argument("tag", metavar="TAG", nargs="+", help="tags to set")
    return parse_args_and_run(parser, tag)
