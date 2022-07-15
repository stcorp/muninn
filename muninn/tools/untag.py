#
# Copyright (C) 2014-2022 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import muninn

from muninn.tools.utils import Processor, create_parser, parse_args_and_run


class UntagProcessor(Processor):
    def __init__(self, args):
        super(UntagProcessor, self).__init__(args)
        self.args = args

    def perform_operation(self, archive, product):
        tags = None if self.args.all else self.args.tag
        archive.untag(product.core.uuid, tags)
        return 1


def untag(args):
    processor = UntagProcessor(args)
    with muninn.open(args.archive) as archive:
        products = archive.search(where=args.expression, property_names=['uuid'])
        return processor.process(archive, args, products)


def main():
    parser = create_parser(description="Remove one or more tags from products contained in a muninn archive",
                           parallel=True)
    parser.add_argument("-a", "--all", action="store_true",
                        help="ignore tags supplied on the command line; instead remove all tags")
    parser.add_argument("archive", metavar="ARCHIVE", help="identifier of the archive to use")
    parser.add_argument("expression", metavar="EXPRESSION", help="expression used to search for products to untag")
    parser.add_argument("tag", metavar="TAG", nargs="*", help="tags to remove")
    return parse_args_and_run(parser, untag)


if __name__ == '__main__':
    main()
