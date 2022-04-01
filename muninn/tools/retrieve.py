#
# Copyright (C) 2014-2022 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import multiprocessing
import os
import argparse

import muninn

from muninn.tools.utils import Processor, create_parser, parse_args_and_run

try:
    from tqdm import tqdm as bar
except ImportError:
    def bar(range, total=None):
        return range


def directory(text):
    if not os.path.isdir(text):
        raise argparse.ArgumentTypeError("no such directory: %r" % text)

    return text


class RetrieveProcessor(Processor):
    def __init__(self, args, target_path):
        super(RetrieveProcessor, self).__init__(args.archive)
        self.args = args
        self.target_path = target_path

    def perform_operation(self, archive, product):
        archive.retrieve_by_uuid(product.core.uuid, target_path=self.target_path, use_symlinks=self.args.link)
        return 1


def retrieve(args):
    target_path = os.getcwd() if args.directory is None else args.directory
    processor = RetrieveProcessor(args, target_path)

    with muninn.open(args.archive) as archive:
        num_success = 0
        products = archive.search(where=args.expression, property_names=['uuid'])
        total = len(products)
        if args.parallel:
            if args.processes is not None:
                pool = multiprocessing.Pool(args.processes)
            else:
                pool = multiprocessing.Pool()
            num_success = sum(list(bar(pool.imap(processor, products), total=total)))
            pool.close()
            pool.join()
        else:
            for product in products:
                processor.perform_operation(archive, product)
                num_success += 1

    return 0 if num_success == total else 1


def main():
    parser = create_parser(description="Retrieve products from a muninn archive.")
    parser.add_argument("-d", "--directory", type=directory, help="directory in which retrieved products will be"
                        " stored; by default, retrieved products will be stored in the current working directory")
    parser.add_argument("-l", "--link", action="store_true", help="retrieve using symbolic links instead of copy")
    parser.add_argument("archive", metavar="ARCHIVE", help="identifier of the archive to use")
    parser.add_argument("expression", metavar="EXPRESSION", help="expression used to search for products to retrieve")
    parser.add_argument("--parallel", action="store_true", help="use multi-processing to perform ingestion")
    parser.add_argument("--processes", type=int, help="use a specific amount of processes for --parallel")
    return parse_args_and_run(parser, retrieve)


if __name__ == '__main__':
    main()
