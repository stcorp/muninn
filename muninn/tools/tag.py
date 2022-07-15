#
# Copyright (C) 2014-2022 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import muninn

from muninn.tools.utils import Processor, create_parser, parse_args_and_run


class TagProcessor(Processor):
    def __init__(self, args):
        super(TagProcessor, self).__init__(args)
        self.args = args

    def perform_operation(self, archive, product):
        archive.tag(product.core.uuid, self.args.tag)
        return 1


def tag(args):
    processor = TagProcessor(args)
    with muninn.open(args.archive) as archive:
        products = archive.search(where=args.expression, property_names=['uuid'])
        return processor.process(archive, args, products)


def main():
    parser = create_parser(description="Set one or more tags on products contained in a muninn archive", parallel=True)
    parser.add_argument("archive", metavar="ARCHIVE", help="identifier of the archive to use")
    parser.add_argument("expression", metavar="EXPRESSION", help="expression used to search for products to tag")
    parser.add_argument("tag", metavar="TAG", nargs="+", help="tags to set")
    return parse_args_and_run(parser, tag)


if __name__ == '__main__':
    main()
