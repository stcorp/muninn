#
# Copyright (C) 2014-2022 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import logging
import sys

import muninn

from muninn.tools.utils import Processor, create_parser, parse_args_and_run
from muninn.tools.ingest import CheckProductListAction, expand_stem  # TODO to utils
from muninn.util import product_hash


class VerifyProcessor(Processor):
    def __init__(self, args):
        super(VerifyProcessor, self).__init__(args)
        self.args = args

    def perform_operation(self, archive, product):
        if len(archive.verify_hash(product)) == 0:
            return 1
        else:
            logging.error("%s: failed hash verification" % product.core.uuid)
            return 0


def calc(args):
    if "-" in args.path:
        paths = [path.strip() for path in sys.stdin]
    else:
        paths = args.path
    for path in paths:
        if args.path_is_stem:
            root_paths = expand_stem(path)
        else:
            root_paths = [path]
        print(path, product_hash(root_paths, hash_type=args.hash_type))


def verify(args):
    processor = VerifyProcessor(args)
    with muninn.open(args.archive) as archive:
        products = archive.search(where=args.expression, property_names=['uuid'])
        error = processor.process(archive, args, products)
        if error != 0:
            sys.exit(1)


def command(args):
    if args.command == 'calc':
        calc(args)
    else:
        verify(args)


def main():
    parser = create_parser(description="Muninn product hashing utilities")
    sub_parsers = parser.add_subparsers(dest='command', help='sub-command help')
    sub_parsers.required = True

    # calc
    calc = sub_parsers.add_parser('calc', help='calculate hash for local products')
    calc.add_argument("path", metavar="PATH", nargs="+", action=CheckProductListAction,
                      help="list of paths, or \"-\" to read the list of paths from standard input")
    calc.add_argument("--hash-type", default='sha1', help="hash algorithm to use (default sha1)")
    calc.add_argument("-s", "--path-is-stem", action="store_true", help="each product path is interpreted as a "
                      "stem; any file or directory of which the name starts with this stem is considered to be part "
                      "of the product")

    # verify
    verify = sub_parsers.add_parser('verify', help='verify hash for given products')
    verify.add_argument("archive", metavar="ARCHIVE", help="identifier of the archive to use")
    verify.add_argument("expression", metavar="EXPRESSION", default="", help="expression to select products")
    verify.add_argument("--parallel", action="store_true", help="use multi-processing to perform operation")
    verify.add_argument("--processes", type=int, help="use a specific amount of processes for --parallel")

    return parse_args_and_run(parser, command)


if __name__ == '__main__':
    main()
