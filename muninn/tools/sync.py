#
# Copyright (C) 2014-2025 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import argparse
from datetime import datetime

import muninn

from muninn.tools.utils import create_parser, parse_args_and_run


def valid_date(date: str) -> datetime:
    date_fmt = '%Y-%m-%d'
    try:
        return datetime.strptime(date, date_fmt)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date: {date} (Expects '{date_fmt}')")


def run(args):
    with muninn.open(args.archive) as archive:
        archive.sync(args.synchronizer, args.product_types, args.start, args.end, args.force)


def main():
    parser = create_parser(description="Synchronize a muninn archive")
    parser.add_argument("archive", metavar="ARCHIVE", help="archive identifier")
    parser.add_argument("synchronizer", help="which synchronizer plugin to use")
    parser.add_argument("-p", "--product-types", nargs="+",
                        help="the product types to be synchronized.")
    parser.add_argument('--start', type=valid_date, help='start datetime of synchronization range (exclusive)')
    parser.add_argument('--end', type=valid_date,
                        help='end date of synchronization range (exclusive)')
    parser.add_argument(
        "-f", "--force", action="store_true", default=False, help="force update, even if archive date has not changed"
    )
    return parse_args_and_run(parser, run)


if __name__ == '__main__':
    main()
