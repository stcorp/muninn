#
# Copyright (C) 2014-2017 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import muninn

from .utils import create_parser, parse_args_and_run


def rebuild(args):
    if args.force:
        expression = args.expression
    else:
        expression = "is_defined(core.archive_path)"
        if args.expression:
            expression += " and (%s)" % args.expression

    with muninn.open(args.archive) as archive:
        archive.rebuild_properties(expression)
    
    return 0


def main():
    parser = create_parser(description="Rebuild properties of existing products.")
    parser.add_argument("-f", "--force", action="store_true", help="also tries to rebuild the properties of products"
                        " that are not present in the filesystem")
    parser.add_argument("archive", metavar="ARCHIVE", help="identifier of the archive to use")
    parser.add_argument("expression", metavar="EXPRESSION", help="expression to filter products")
    return parse_args_and_run(parser, rebuild)
