#
# Copyright (C) 2014-2021 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import logging
import multiprocessing
import sys

try:
    from tqdm import tqdm as bar
except ImportError:
    def bar(range, total=None):
        return range

import muninn
from muninn.struct import Struct

from .utils import Processor, create_parser, parse_args_and_run

logger = logging.getLogger(__name__)

ACTIONS = [
    'ingest',
    'post_ingest',
    'pull',
    'post_pull',
    'retype',
]


class UpdateProcessor(Processor):

    def __init__(self, args):
        super(UpdateProcessor, self).__init__(args.archive)
        self.action = args.action
        self.argument = args.argument
        self.disable_hooks = args.disable_hooks
        self.use_current_path = args.keep
        self.verify_hash = args.verify_hash

    def perform_operation(self, archive, product):
        if self.action == 'ingest':
            logger.debug('running update:ingest on %s ' % product.core.product_name)
            archive.rebuild_properties(product.core.uuid, disable_hooks=self.disable_hooks,
                                       use_current_path=self.use_current_path)

        elif self.action == 'post_ingest':
            plugin = archive.product_type_plugin(product.core.product_type)
            if hasattr(plugin, "post_ingest_hook"):
                logger.debug('running update:post_ingest on %s ' % product.core.product_name)
                plugin.post_ingest_hook(archive, product)

        elif self.action == 'pull':
            logger.debug('running update:pull on %s ' % product.core.product_name)
            archive.rebuild_pull_properties(product.core.uuid, verify_hash=self.verify_hash,
                                            disable_hooks=self.disable_hooks,
                                            use_current_path=self.use_current_path)

        elif self.action == 'post_pull':
            plugin = archive.product_type_plugin(product.core.product_type)
            if hasattr(plugin, "post_pull_hook"):
                logger.debug('running update:post_pull on %s ' % product.core.product_name)
                plugin.post_pull_hook(archive, product)

        elif self.action == 'retype':
            if self.argument is not None:
                archive.update_properties(Struct({'core': {'product_type': self.argument}}), product.core.uuid)
            else:
                print('missing argument for retype action')
                sys.exit(1)


def update(args):
    expression = "is_defined(core.archive_path)"
    if args.expression:
        expression += " and (%s)" % args.expression

    if args.action == 'pull':
        # only get products with a remote_url
        if expression:
            expression = "is_defined(remote_url) and (%s)" % expression
        else:
            expression = "is_defined(remote_url)"

    processor = UpdateProcessor(args)
    with muninn.open(args.archive) as archive:
        if args.action in ['ingest', 'pull']:
            # we only need the uuid and the product_name
            products = archive.search(expression, property_names=['uuid', 'product_name'])
        else:
            products = archive.search(expression, namespaces=archive.namespaces())
        if args.parallel:
            if args.processes is not None:
                pool = multiprocessing.Pool(args.processes)
            else:
                pool = multiprocessing.Pool()
            list(bar(pool.imap(processor, products), total=len(products)))
            pool.close()
            pool.join()
        else:
            for product in bar(products):
                processor.perform_operation(archive, product)

    return 0


def main():
    parser = create_parser(description="""Updates properties of existing products.
        This is an archive maintenance tool, meant to be used when the archive structure has changed.
        Use with care!""")
    parser.add_argument("--disable-hooks", action="store_true",
                        help="do not run the hooks associated with the action")
    parser.add_argument("--parallel", action="store_true", help="use multi-processing to perform update")
    parser.add_argument("--processes", type=int, help="use a specific amount of processes for --parallel")
    parser.add_argument("--verify-hash", action="store_true",
                        help="verify the hash of the product after a `pull` update")
    parser.add_argument("-k", "--keep", action="store_true",
                        help="do not attempt to relocate the product to the location specified in the "
                             "product type plug-in (useful for read-only archives)")
    parser.add_argument("action", metavar="ACTION", choices=ACTIONS, help="action name (%s)" % ', '.join(ACTIONS))
    parser.add_argument("archive", metavar="ARCHIVE", help="identifier of the archive to use")
    parser.add_argument("expression", metavar="EXPRESSION", default="", help="expression to select products")
    parser.add_argument("argument", metavar="ARGUMENT", nargs='?', help="product type for retype action")
    return parse_args_and_run(parser, update)
