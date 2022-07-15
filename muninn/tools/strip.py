#
# Copyright (C) 2014-2022 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import muninn

from muninn.tools.utils import Processor, create_parser, parse_args_and_run


class StripProcessor(Processor):
    def __init__(self, args):
        super(StripProcessor, self).__init__(args)
        self.args = args

    def perform_operation(self, archive, product):
        archive.strip(product.core.uuid, force=self.args.force, cascade=False)
        return 1


def strip(args):
    processor = StripProcessor(args)
    with muninn.open(args.archive) as archive:
        products = archive.search(where=args.expression, property_names=['uuid'])
        returncode = processor.process(archive, args, products)
        archive.cleanup_derived_products()
        return returncode


def main():
    parser = create_parser(description="Strip products contained in a muninn archive (i.e. remove "
                           "products from disk, but don't remove the corresponding entries from the product "
                           "catalogue)", parallel=True)
    parser.add_argument("-f", "--force", action="store_true", help="also strip partially ingested products; note"
                        " that this can cause product files to be removed while in the process of being ingested")
    parser.add_argument("archive", metavar="ARCHIVE", help="identifier of the archive to use")
    parser.add_argument("expression", metavar="EXPRESSION", help="expression used to search for products to remove")
    return parse_args_and_run(parser, strip)


if __name__ == '__main__':
    main()
