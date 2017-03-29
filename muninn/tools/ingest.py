#
# Copyright (C) 2014-2017 S[&]T, The Netherlands.
#

from __future__ import absolute_import, print_function

import argparse
import fnmatch
import glob
import logging
import muninn
import os
import sys


class Error(Exception):
    pass


class CheckProductListAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        if "-" in values and len(values) > 1:
            raise argparse.ArgumentError(self, "use either a single \"-\" on the command line (to read product paths "
                                               "from standard input), or specify one or more product paths, but not "
                                               "both.")
        setattr(namespace, self.dest, values)


def log_internal_error():
    import traceback

    logging.error("terminated due to an internal error")
    for message in traceback.format_exc().splitlines():
        logging.error("| " + message)


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
    return paths


def get_path_expansion_function(is_stem=False, is_enclosing_directory=False):
    assert(not (is_stem and is_enclosing_directory))
    if is_stem:
        return expand_stem
    if is_enclosing_directory:
        return expand_enclosing_directory
    return expand_identity


def ingest(args):
    archive = muninn.open(args.archive)
    path_expansion_function = get_path_expansion_function(args.path_is_stem, args.path_is_enclosing_directory)
    assert(not args.link or not args.copy)
    use_symlinks = True if args.link else False if args.copy else None
    verify_hash = True if args.verify_hash else False

    errors_encountered = False
    paths = sys.stdin if "-" in args.path else args.path
    for path in paths:
        path = os.path.abspath(path.strip())

        # Expand path into multiple files and/or directories that belong to the same product.
        try:
            product_paths = path_expansion_function(path)
        except Error as error:
            logging.error("%s: unable to determine which files or directories belong to product [%s]" % (path, error))
            errors_encountered = True
            continue

        # Discard paths matching any of the user supplied exclude patterns.
        if args.exclude:
            product_paths = filter_paths(product_paths, args.exclude)

        if not product_paths:
            logging.error("%s: path does not match any files or directories" % path)
            errors_encountered = True
            continue

        try:
            properties = archive.ingest(product_paths, args.product_type, use_symlinks=use_symlinks,
                                        verify_hash=verify_hash)
        except muninn.Error as error:
            logging.error("%s: unable to ingest product [%s]" % (path, error))
            errors_encountered = True
            continue

        if args.tag:
            try:
                archive.tag(properties.core.uuid, args.tag)
            except muninn.Error as error:
                logging.error("%s: unable to tag product [%s]" % (path, error))
                errors_encountered = True

    return 0 if not errors_encountered else 1


def version(program_name):
    print("%s %s" % (program_name, muninn.__version__))
    print(muninn.__copyright__)
    print("")


def main():
    # This parser is used in combination with the parse_known_args() function as a way to implement a "--version"
    # option that prints version information and exits, and is included in the help message.
    #
    # The "--version" option should have the same semantics as the "--help" option in that if it is present on the
    # command line, the corresponding action should be invoked directly, without checking any other arguments.
    # However, the argparse module does not support user defined options with such semantics.
    version_parser = argparse.ArgumentParser(add_help=False)
    version_parser.add_argument("--version", action="store_true", help="output version information and exit")

    parser = argparse.ArgumentParser(description="Ingest products into a muninn archive.", parents=[version_parser])
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
    group.add_argument("-c", "--copy", action="store_true", help="ingest a copy of each product")
    parser.add_argument("--verify-hash", action="store_true",
                        help="verify the hash of the product after it has been put in the archive")
    parser.add_argument("archive", metavar="ARCHIVE", help="identifier of the archive to use")
    parser.add_argument("path", metavar="PATH", nargs="+", action=CheckProductListAction,
                        help="products to ingest, or \"-\" to read the list of products from standard input")

    args, unused_args = version_parser.parse_known_args()
    if args.version:
        version(os.path.basename(sys.argv[0]))
        sys.exit(0)

    args = parser.parse_args(unused_args)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    try:
        return ingest(args)
    except SystemExit:
        raise
    except KeyboardInterrupt:
        sys.exit(1)
    except (Error, muninn.Error) as error:
        logging.error(error)
        sys.exit(1)
    except:
        log_internal_error()
        sys.exit(1)
    finally:
        logging.shutdown()
