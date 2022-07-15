#
# Copyright (C) 2014-2022 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import muninn

from muninn.tools.utils import Processor, create_parser, parse_args_and_run


class RemoveProcessor(Processor):
    def __init__(self, args):
        super(RemoveProcessor, self).__init__(args)
        self.args = args

    def perform_operation(self, archive, product):
        if self.args.catalogue_only:
            archive.delete_properties(product.core.uuid)
        else:
            archive.remove(product.core.uuid, force=self.args.force, cascade=False)
        return 1


def remove(args):
    processor = RemoveProcessor(args)
    with muninn.open(args.archive) as archive:
        products = archive.search(where=args.expression, property_names=['uuid'])
        returncode = processor.process(archive, args, products)
        if not args.catalogue_only:
            archive.cleanup_derived_products()
        return returncode


def main():
    parser = create_parser(description="Remove products from a muninn archive.", parallel=True)
    parser.add_argument("-c", "--catalogue-only", action="store_true", help="remove the entry from the catalogue "
                        "without removing any product from the storage")
    parser.add_argument("-f", "--force", action="store_true", help="also remove partially ingested products; note "
                        "that this can cause products to be removed while in the process of being ingested")
    parser.add_argument("archive", metavar="ARCHIVE", help="identifier of the archive to use")
    parser.add_argument("expression", metavar="EXPRESSION", help="expression used to search for products to remove")
    return parse_args_and_run(parser, remove)


if __name__ == '__main__':
    main()
