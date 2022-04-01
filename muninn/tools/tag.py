#
# Copyright (C) 2014-2022 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import multiprocessing

import muninn

from muninn.tools.utils import Processor, create_parser, parse_args_and_run

try:
    from tqdm import tqdm as bar
except ImportError:
    def bar(range, total=None):
        return range

def tag(args):
    with muninn.open(args.archive) as archive:
        for product in archive.search(where=args.expression, property_names=['uuid']):
            archive.tag(product.core.uuid, args.tag)

    return 0


class TagProcessor(Processor):
    def __init__(self, args):
        super(TagProcessor, self).__init__(args.archive)
        self.args = args

    def perform_operation(self, archive, product):
        archive.tag(product.core.uuid, self.args.tag)
        return 1


def retrieve(args):
    processor = TagProcessor(args)

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
    parser = create_parser(description="Set one or more tags on products contained in a muninn archive", parallel=True)
    parser.add_argument("archive", metavar="ARCHIVE", help="identifier of the archive to use")
    parser.add_argument("expression", metavar="EXPRESSION", help="expression used to search for products to tag")
    parser.add_argument("tag", metavar="TAG", nargs="+", help="tags to set")
    return parse_args_and_run(parser, tag)


if __name__ == '__main__':
    main()
