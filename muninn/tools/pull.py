#
# Copyright (C) 2014-2021 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import logging

import muninn

from .utils import create_parser, parse_args_and_run


def pull(args):
    with muninn.open(args.archive) as archive:
        verify_hash = True if args.verify_hash else False

        # find all remote products that satisfy filter
        expression = "active and is_defined(remote_url) and not is_defined(archive_path)"
        if args.expression:
            expression = "%s and (%s)" % (expression, args.expression)

        logging.debug('Going to pull products that match: %s', expression)
        num_products = archive.pull(expression, verify_hash=verify_hash)
        logging.debug('Pulled %d product(s)', num_products)

    return 0


def main():
    parser = create_parser(description="Pull remote files into the archive.")
    parser.add_argument("--verify-hash", action="store_true",
                        help="verify the hash of the product after it has been put in the archive")
    parser.add_argument("archive", metavar="ARCHIVE", help="identifier of the archive to use")
    parser.add_argument("expression", metavar="EXPRESSION", help="expression to filter products to pull")
    return parse_args_and_run(parser, pull)
