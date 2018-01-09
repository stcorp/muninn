#
# Copyright (C) 2014-2018 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import logging

import muninn

from .utils import create_parser, parse_args_and_run

logger = logging.getLogger(__name__)

ACTIONS = [
    'ingest',
    'post_ingest',
    'pull',
    'post_pull',
]


def update(args):
    expression = "is_defined(core.archive_path)"
    if args.expression:
        expression += " and (%s)" % args.expression

    namespaces = []
    if args.namespaces:
        for namespace in args.namespaces:
            namespaces += namespace.split(' ')

    if args.action == 'ingest':
        with muninn.open(args.archive) as archive:
            products = [(product.core.uuid, product.core.product_name) for product in archive.search(expression)]
            for uuid, product_name in products:
                logger.debug('running update:ingest on %s ' % product_name)
                archive.rebuild_properties(uuid, disable_hooks=args.disable_hooks, use_current_path=args.keep)
        logger.debug('update:ingest was run on %d product(s)' % len(products))

    elif args.action == 'post_ingest':
        with muninn.open(args.archive) as archive:
            products = [(product.core.uuid, product.core.product_type) for product in archive.search(expression, namespaces=namespaces)]
            count = 0
            for uuid, product_type in products:
                plugin = archive.product_type_plugin(product_type)
                if hasattr(plugin, "post_ingest_hook"):
                    count += 1
                    product = archive._get_product(uuid)
                    logger.debug('running update:post_ingest on %s ' % product.core.product_name)
                    plugin.post_ingest_hook(archive, product)
        logger.debug('update:post_ingest was run on %d product(s)' % count)

    elif args.action == 'pull':
        # only get products with a remote_url
        if expression:
            expression = "is_defined(remote_url) and (%s)" % expression
        else:
            expression = "is_defined(remote_url)"
        with muninn.open(args.archive) as archive:
            products = [(product.core.uuid, product.core.product_name) for product in archive.search(expression)]
            for uuid, product_name in products:
                logger.debug('running update:pull on %s ' % product_name)
                archive.rebuild_pull_properties(uuid, verify_hash=args.verify_hash, disable_hooks=args.disable_hooks, use_current_path=args.keep)
        logger.debug('update:pull was run on %d product(s)' % len(products))

    elif args.action == 'post_pull':
        with muninn.open(args.archive) as archive:
            products = [(product.core.uuid, product.core.product_type) for product in archive.search(expression)]
            count = 0
            for uuid, product_type in products:
                plugin = archive.product_type_plugin(product_type)
                if hasattr(plugin, "post_pull_hook"):
                    count += 1
                    product = archive._get_product(uuid)
                    logger.debug('running update:post_pull on %s ' % product.core.product_name)
                    plugin.post_pull_hook(archive, product)
        logger.debug('update:post_pull was run on %d product(s)' % count)

    else:
        return 1

    return 0


def main():
    parser = create_parser(description="""Updates properties of existing products.
        This is an archive maintenance tool, meant to be used when the archive structure has changed.
        Use with care!""")
    parser.add_argument("-a", "--action", choices=ACTIONS, required=True, help="action name")
    parser.add_argument("--disable-hooks", action="store_true",
                        help="do not run the hooks associated with the action")
    parser.add_argument("--namespaces", action="append",
                        help="white space separated list of namespaces to make available for `post_ingest`")
    parser.add_argument("--verify-hash", action="store_true",
                        help="verify the hash of the product after a `pull` update")
    parser.add_argument("-k", "--keep", action="store_true",
                        help="do not attempt to relocate the product to the location specified in the "
                             "product type plug-in (useful for read-only archives)")
    parser.add_argument("archive", metavar="ARCHIVE", help="identifier of the archive to use")
    parser.add_argument("expression", metavar="EXPRESSION", default="", help="expression to select products")
    return parse_args_and_run(parser, update)
