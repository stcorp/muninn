#
# Copyright (C) 2014-2022 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import logging

import muninn

from muninn.tools.utils import Processor, create_parser, parse_args_and_run


class PullProcessor(Processor):
    def __init__(self, args):
        super(PullProcessor, self).__init__(args)
        self.args = args
        self.verify_hash = True if args.verify_hash else False
        self.verify_hash_download = True if args.verify_hash_download else False

    def perform_operation(self, archive, product):
        archive.pull(
            product.core.uuid,
            verify_hash=self.verify_hash,
            verify_hash_download=self.verify_hash_download
        )
        return 1


def pull(args):
    processor = PullProcessor(args)
    with muninn.open(args.archive) as archive:
        # find all remote products that satisfy filter
        expression = "active and is_defined(remote_url) and not is_defined(archive_path)"
        if args.expression:
            expression = "%s and (%s)" % (expression, args.expression)
        logging.debug('Going to pull products that match: %s', expression)
        products = archive.search(where=expression, property_names=['uuid'])
        returncode = processor.process(archive, args, products)
        if returncode == 0:
            logging.debug('Pulled %d product(s)', len(products))
        return returncode


def main():
    parser = create_parser(description="Pull remote files into the archive.", parallel=True)
    parser.add_argument("--verify-hash", action="store_true",
                        help="verify the hash of the product after it has been put in the archive")
    parser.add_argument("--verify-hash-download", action="store_true",
                        help="verify the hash of the pulled product before it has been put in the archive")
    parser.add_argument("archive", metavar="ARCHIVE", help="identifier of the archive to use")
    parser.add_argument("expression", metavar="EXPRESSION", help="expression to filter products to pull")
    return parse_args_and_run(parser, pull)


if __name__ == '__main__':
    main()
