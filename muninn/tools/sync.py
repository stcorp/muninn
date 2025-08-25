#
# Copyright (C) 2014-2025 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import argparse
import datetime

import muninn

from muninn.tools.utils import create_parser, parse_args_and_run


def valid_date(arg: str) -> datetime:
    for format_string in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
        try:
            return datetime.datetime.strptime(arg, format_string)
        except ValueError:
            pass
    raise argparse.ArgumentTypeError(f"invalid datetime: {arg} (expected 'YYYY-MM-DD(THH:MM:SS(.ffffff))')")


def run(args):
    with muninn.open(args.archive) as archive:
        archive.sync(args.synchronizer, args.product_types, args.start, args.end, args.force)


def main():
    parser = create_parser(description="Synchronize a muninn archive")
    parser.add_argument("archive", metavar="ARCHIVE", help="archive identifier")
    parser.add_argument("synchronizer", help="name of the synchronizer to use")
    parser.add_argument("-p", "--product-types", nargs="*",
                        help="the product types to be synchronized")
    parser.add_argument('--start', type=valid_date, help='start datetime of synchronization range')
    parser.add_argument('--end', type=valid_date, help='end datetime of synchronization range')
    parser.add_argument("-f", "--force", action="store_true", default=False,
                        help="force update, even if entries have not changed")
    return parse_args_and_run(parser, run)


if __name__ == '__main__':
    main()
