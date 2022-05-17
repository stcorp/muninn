#
# Copyright (C) 2014-2022 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import sys

import muninn

from muninn.tools.utils import create_parser, parse_args_and_run
from muninn.tools.ingest import CheckProductListAction  # TODO to utils
from muninn.util import product_hash


def calc(args):
    if "-" in args.path:
        paths = [path.strip() for path in sys.stdin]
    else:
        paths = args.path
    for path in paths:
        print(path, product_hash([path], hash_type=args.hash_type))


def verify(args):
    total = 0
    failed = []
    with muninn.open(args.archive) as archive:
        for product in archive.search(args.expression):
            total += 1
            if len(archive.verify_hash(product)) > 0:
                failed.append(product)
                print(product.core.uuid, file=sys.stderr)
    if failed:
        print('%d out of %d products failed' % (len(failed), total), file=sys.stderr)
        sys.exit(1)
    else:
        print('verified hash for %d products' % total)


def command(args):
    if args.command == 'calc':
        calc(args)
    else:
        verify(args)


def main():
    parser = create_parser(description="Muninn product hashing utilities")
    sub_parsers = parser.add_subparsers(dest='command', help='sub-command help')

    # calc
    calc = sub_parsers.add_parser('calc', help='calculate hash for local products')
    calc.add_argument("path", metavar="PATH", nargs="+", action=CheckProductListAction,
                      help="list of paths, or \"-\" to read the list of paths from standard input")
    calc.add_argument("--hash-type", default='sha1', help="hash algorithm to use (default sha1)")

    # verify
    verify = sub_parsers.add_parser('verify', help='verify hash for given products')
    verify.add_argument("archive", metavar="ARCHIVE", help="identifier of the archive to use")
    verify.add_argument("expression", metavar="EXPRESSION", default="", help="expression to select products")

    return parse_args_and_run(parser, command)

if __name__ == '__main__':
    main()
