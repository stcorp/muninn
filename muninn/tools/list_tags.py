#
# Copyright (C) 2014-2022 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import muninn

from muninn.tools.utils import create_parser, parse_args_and_run


def list_tags(args):
    with muninn.open(args.archive) as archive:
        for product in archive.search(where=args.expression, property_names=['uuid', 'product_name']):
            tags = archive.tags(product.core.uuid)
            print("%s (%s): %s" % (product.core.product_name, product.core.uuid, ", ".join(tags)))

    return 0


def main():
    parser = create_parser(description="List tags of products contained in a muninn archive")
    parser.add_argument("archive", metavar="ARCHIVE", help="identifier of the archive to use")
    parser.add_argument("expression", metavar="EXPRESSION",
                        help="expression used to search for products to list tags of")
    return parse_args_and_run(parser, list_tags)


if __name__ == '__main__':
    main()
