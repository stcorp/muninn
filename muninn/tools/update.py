#
# Copyright (C) 2014-2017 S[&]T, The Netherlands.
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
    if args.force:
        expression = args.expression
    else:
        expression = "core.active and is_defined(core.archive_path)"
        if args.expression:
            expression += " and (%s)" % args.expression

    namespaces = []
    if args.namespaces:
        for namespace in args.namespaces:
            namespaces += namespace.split(' ')

    if args.action == 'ingest':
        with muninn.open(args.archive) as archive:
            count = archive.rebuild_properties(expression, disable_hooks=args.disable_hooks)
        logger.debug('`ingest` was run on %d product(s)' % count)

    elif args.action == 'post_ingest':
        with muninn.open(args.archive) as archive:
            products = archive.search(expression, namespaces=namespaces)
            count = 0
            for product in products:
                plugin = archive.product_type_plugin(product.core.product_type)
                if hasattr(plugin, "post_ingest_hook"):
                    count += 1
                    plugin.post_ingest_hook(archive, product)
        logger.debug('`post_ingest` was run on %d product(s)' % count)

    elif args.action == 'pull':
        # only get products with a remote_url
        if expression:
            expression = "is_defined(remote_url) and (%s)" % expression
        else:
            expression = "is_defined(remote_url)"
        # NB: existing files will be overwritten
        with muninn.open(args.archive) as archive:
            count = archive.pull(expression, verify_hash=args.verify_hash, force=True, disable_hooks=args.disable_hooks)
        logger.debug('`pull` was run on %d product(s)' % count)

    elif args.action == 'post_pull':
        with muninn.open(args.archive) as archive:
            products = archive.search(expression)
            count = 0
            for product in products:
                plugin = archive.product_type_plugin(product.core.product_type)
                if hasattr(plugin, "post_pull_hook"):
                    count += 1
                    plugin.post_pull_hook(archive, product)
        logger.debug('`post_pull` was run on %d product(s)' % count)

    else:
        return 1

    return 0


def main():
    parser = create_parser(description="""Updates properties of existing products.
        This is a archive maintenance tool, meant to be used when the archive structure has changed.
        Use with care!""")
    parser.add_argument("-a", "--action", choices=ACTIONS, required=True, help="action name")
    parser.add_argument("-f", "--force", action="store_true", help="""also tries to rebuild the properties of products
                        that are not present in the filesystem""")
    parser.add_argument("--disable-hooks", action="store_true",
                        help="do not run the hooks associated with the action")
    parser.add_argument("--namespaces", action="append",
                        help="white space separated list of namespaces to make available for `post_ingest`")
    parser.add_argument("--verify-hash", action="store_true",
                        help="verify the hash of the product after it has been put in the archive by `pull`")
    parser.add_argument("archive", metavar="ARCHIVE", help="identifier of the archive to use")
    parser.add_argument("expression", metavar="EXPRESSION", default="", help="expression to select products")
    return parse_args_and_run(parser, update)
