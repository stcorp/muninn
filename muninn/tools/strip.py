#
# Copyright (C) 2014-2021 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import muninn

from .utils import create_parser, parse_args_and_run


def strip(args):
    with muninn.open(args.archive) as archive:
        archive.strip(args.expression, force=args.force)
    return 0


def main():
    parser = create_parser(description="Strip products contained in a muninn archive (i.e. remove "
                           "products from disk, but don't remove the corresponding entries from the product "
                           "catalogue)")
    parser.add_argument("-f", "--force", action="store_true", help="also strip partially ingested products; note"
                        " that this can cause product files to be removed while in the process of being ingested")
    parser.add_argument("archive", metavar="ARCHIVE", help="identifier of the archive to use")
    parser.add_argument("expression", metavar="EXPRESSION", help="expression used to search for products to remove")
    return parse_args_and_run(parser, strip)
