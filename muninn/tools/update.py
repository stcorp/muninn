#
# Copyright (C) 2014-2019 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import logging
import multiprocessing

try:
    from tqdm import tqdm as bar
except:
    def bar(range):
        return range

import muninn

from .utils import create_parser, parse_args_and_run

logger = logging.getLogger(__name__)

ACTIONS = [
    'ingest',
    'post_ingest',
    'pull',
    'post_pull',
]

class Processor(object):

    def __init__(self, args, archive=None):
        self.archive_name = args.archive
        self.action = args.action
        self.disable_hooks = args.disable_hooks
        self.use_current_path = args.keep
        self.verify_hash = args.verify_hash
        self.archive = archive
        self.ignore_keyboard_interrupt = archive is None  # archive is None -> we are using multiprocessing

    def __call__(self, product):
        try:
            if self.archive is None:
                self.archive = muninn.open(self.archive_name)

            if self.action == 'ingest':
                logger.debug('running update:ingest on %s ' % product.core.product_name)
                self.archive.rebuild_properties(product.core.uuid, disable_hooks=self.disable_hooks,
                                                use_current_path=self.use_current_path)

            elif self.action == 'post_ingest':
                plugin = self.archive.product_type_plugin(product.core.product_type)
                if hasattr(plugin, "post_ingest_hook"):
                    logger.debug('running update:post_ingest on %s ' % product.core.product_name)
                    plugin.post_ingest_hook(self.archive, product)

            elif self.action == 'pull':
                logger.debug('running update:pull on %s ' % product.core.product_name)
                self.archive.rebuild_pull_properties(product.core.uuid, verify_hash=self.verify_hash,
                                                     disable_hooks=self.disable_hooks,
                                                     use_current_path=self.use_current_path)

            elif self.action == 'post_pull':
                plugin = self.archive.product_type_plugin(product.core.product_type)
                if hasattr(plugin, "post_pull_hook"):
                    logger.debug('running update:post_pull on %s ' % product.core.product_name)
                    plugin.post_pull_hook(self.archive, product)

        except KeyboardInterrupt:
            # don't capture keyboard interrupts inside sub-processes (only the main process should handle it)
            if not self.ignore_keyboard_interrupt:
                raise


def update(args):
    expression = "is_defined(core.archive_path)"
    if args.expression:
        expression += " and (%s)" % args.expression

    namespaces = []
    if args.namespaces:
        for namespace in args.namespaces:
            namespaces += namespace.split(' ')

    if args.action == 'pull':
        # only get products with a remote_url
        if expression:
            expression = "is_defined(remote_url) and (%s)" % expression
        else:
            expression = "is_defined(remote_url)"

    with muninn.open(args.archive) as archive:
        products = archive.search(expression, namespaces=namespaces)
        if args.parallel:
            pool = multiprocessing.Pool()
            list(bar(pool.imap(Processor(args), products), total=len(products)))
            pool.close()
            pool.join()
        else:
            update_func = Processor(args, archive)
            for product in bar(products):
                update_func(product)

    return 0


def main():
    parser = create_parser(description="""Updates properties of existing products.
        This is an archive maintenance tool, meant to be used when the archive structure has changed.
        Use with care!""")
    parser.add_argument("-a", "--action", choices=ACTIONS, required=True, help="action name")
    parser.add_argument("--disable-hooks", action="store_true",
                        help="do not run the hooks associated with the action")
    parser.add_argument("--namespaces", action="append",
                        help="white space separated list of namespaces to make available "
                             "(for post_ingest and post_pull actions)")
    parser.add_argument("--parallel", action="store_true",
                        help="use multi-processing to perform update")
    parser.add_argument("--verify-hash", action="store_true",
                        help="verify the hash of the product after a `pull` update")
    parser.add_argument("-k", "--keep", action="store_true",
                        help="do not attempt to relocate the product to the location specified in the "
                             "product type plug-in (useful for read-only archives)")
    parser.add_argument("archive", metavar="ARCHIVE", help="identifier of the archive to use")
    parser.add_argument("expression", metavar="EXPRESSION", default="", help="expression to select products")
    return parse_args_and_run(parser, update)
