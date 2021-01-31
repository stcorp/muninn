#
# Copyright (C) 2014-2021 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import muninn

from .utils import create_parser, parse_args_and_run


def remove(args):
    with muninn.open(args.archive) as archive:
        if args.catalogue_only:
            archive.delete_properties(args.expression)
        else:
            archive.remove(args.expression, force=args.force)
    return 0


def main():
    parser = create_parser(description="Remove products from a muninn archive.")
    parser.add_argument("-c", "--catalogue-only", action="store_true", help="remove the entry from the catalogue "
                        "without removing any product from the storage")
    parser.add_argument("-f", "--force", action="store_true", help="also remove partially ingested products; note "
                        "that this can cause products to be removed while in the process of being ingested")
    parser.add_argument("archive", metavar="ARCHIVE", help="identifier of the archive to use")
    parser.add_argument("expression", metavar="EXPRESSION", help="expression used to search for products to remove")
    return parse_args_and_run(parser, remove)
