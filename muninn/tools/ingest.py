#
# Copyright (C) 2014-2022 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import argparse
import fnmatch
import glob
import logging
import os
import sys

import muninn

from muninn.tools.utils import Processor, create_parser, parse_args_and_run


class Error(muninn.Error):
    pass


class CheckProductListAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        if "-" in values and len(values) > 1:
            raise argparse.ArgumentError(self, "use either a single \"-\" on the command line (to read product paths "
                                               "from standard input), or specify one or more product paths, but not "
                                               "both.")
        setattr(namespace, self.dest, values)


def expand_stem(stem):
    try:
        return glob.glob(stem + "*")
    except EnvironmentError as error:
        raise Error("unable to expand stem \"%s\" [%s]" % (stem, error))


def expand_enclosing_directory(path):
    try:
        return [os.path.join(path, basename) for basename in os.listdir(path)]
    except EnvironmentError as error:
        raise Error("unable to expand enclosing directory \"%s\" [%s]" % (path, error))


def expand_identity(path):
    return [path]


def filter_paths(paths, patterns):
    for pattern in patterns:
        paths = filter(lambda path: not fnmatch.fnmatch(os.path.basename(path), pattern), paths)
        # paths = [path for path in path if not fnmatch.fnmatch(os.path.basename(path), pattern)]
    return paths


def get_path_expansion_function(is_stem=False, is_enclosing_directory=False):
    assert not (is_stem and is_enclosing_directory)
    if is_stem:
        return expand_stem
    if is_enclosing_directory:
        return expand_enclosing_directory
    return expand_identity


class IngestProcessor(Processor):

    def __init__(self, args):
        super(IngestProcessor, self).__init__(args)
        assert not args.link or not args.keep
        self.path_expansion_function = get_path_expansion_function(args.path_is_stem, args.path_is_enclosing_directory)
        self.use_symlinks = args.link
        self.verify_hash = args.verify_hash
        self.ingest_product = not args.catalogue_only
        self.exclude = args.exclude
        self.product_type = args.product_type
        self.keep = args.keep
        self.force = args.force
        self.tag = args.tag

    def perform_operation(self, archive, path):
        path = os.path.abspath(path.strip())

        # Expand path into multiple files and/or directories that belong to the same product.
        try:
            product_paths = self.path_expansion_function(path)
        except Error as error:
            logging.error("%s: unable to determine which files or directories belong to product [%s]" % (path, error))
            return 0

        # Discard paths matching any of the user supplied exclude patterns.
        if self.exclude:
            product_paths = filter_paths(product_paths, self.exclude)

        if not product_paths:
            logging.error("%s: path does not match any files or directories" % path)
            return 0

        try:
            properties = archive.ingest(product_paths, self.product_type, use_symlinks=self.use_symlinks,
                                        verify_hash=self.verify_hash, use_current_path=self.keep,
                                        ingest_product=self.ingest_product, force=self.force)
        except muninn.Error as error:
            logging.error("%s: unable to ingest product [%s]" % (path, error))
            return 0
        if self.tag:
            try:
                archive.tag(properties.core.uuid, self.tag)
            except muninn.Error as error:
                logging.error("%s: unable to tag product [%s]" % (path, error))
                return 0

        return 1


def ingest(args):
    processor = IngestProcessor(args)
    with muninn.open(args.archive) as archive:
        if "-" in args.path:
            paths = [path for path in sys.stdin]
        else:
            paths = args.path
        return processor.process(archive, args, paths)


def main():
    parser = create_parser(description="Ingest products into a muninn archive.", parallel=True)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-s", "--path-is-stem", action="store_true", help="each product path is interpreted as a "
                       "stem; any file or directory of which the name starts with this stem is considered to be part "
                       "of the product")
    group.add_argument("-d", "--path-is-enclosing-directory", action="store_true", help="each product path is "
                       "interpreted as an enclosing directory; the actual product consists of any file or directory "
                       "found inside this enclosing directory; the enclosing directory itself is not considered part "
                       "of the product")
    parser.add_argument("-e", "--exclude", metavar="PATTERN", action="append", help="exclude any files or "
                        "directories whose basename matches PATTERN; *, ?, and [] can be used as wildcards; to match "
                        "a wildcard character literally, include it within brackets, e.g. [?] to match the character ? "
                        "literally")
    parser.add_argument("-t", "--product-type", help="force the product type of products to ingest")
    parser.add_argument("-T", "--tag", action="append", default=[], help="tag to set on the product")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-l", "--link", action="store_true", help="ingest symbolic links to each product")
    group.add_argument("-c", "--catalogue-only", action="store_true", help="only ingest product properties")
    group.add_argument("-k", "--keep", action="store_true", help="ingest product, using the current product path if it "
                                                                 "is in the muninn path, otherwise throws an error")
    parser.add_argument("-f", "--force", action="store_true", help="remove any existing product with the same type and "
                                                                   "name before ingesting the new product")
    parser.add_argument("--verify-hash", action="store_true",
                        help="verify the hash of the product after it has been put in the archive")
    parser.add_argument("archive", metavar="ARCHIVE", help="identifier of the archive to use")
    parser.add_argument("path", metavar="PATH", nargs="+", action=CheckProductListAction,
                        help="products to ingest, or \"-\" to read the list of products from standard input")
    return parse_args_and_run(parser, ingest)


if __name__ == '__main__':
    main()
