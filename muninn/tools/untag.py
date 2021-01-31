#
# Copyright (C) 2014-2021 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import muninn

from .utils import create_parser, parse_args_and_run


def untag(args):
    with muninn.open(args.archive) as archive:
        tags = None if args.all else args.tag
        for product in archive.search(where=args.expression, property_names=['uuid']):
            archive.untag(product.core.uuid, tags)

    return 0


def main():
    parser = create_parser(description="Remove one or more tags from products contained in a muninn archive")
    parser.add_argument("-a", "--all", action="store_true",
                        help="ignore tags supplied on the command line; instead remove all tags")
    parser.add_argument("archive", metavar="ARCHIVE", help="identifier of the archive to use")
    parser.add_argument("expression", metavar="EXPRESSION", help="expression used to search for products to untag")
    parser.add_argument("tag", metavar="TAG", nargs="*", help="tags to remove")
    return parse_args_and_run(parser, untag)
