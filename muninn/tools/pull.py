#
# Copyright (C) 2014-2017 S[&]T, The Netherlands.
#

from __future__ import absolute_import, print_function

import argparse
import datetime
import logging
import muninn
import os
import sys


def log_internal_error():
    import traceback

    logging.error("terminated due to an internal error")
    for message in traceback.format_exc().splitlines():
        logging.error("| " + message)


def pull(args):
    archive = muninn.open(args.archive)
    verify_hash = True if args.verify_hash else False

    # find all remote products that satisfy filter
    expression = "(is_defined(remote_url) and not is_defined(archive_path))"
    if args.expression:
        expression = "%s and %s" % (expression, args.expression)

    num_products = archive.pull(expression, verify_hash=verify_hash)

    return 0


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

    parser = argparse.ArgumentParser(description="Pull remote files into the archive.",
                                     parents=[version_parser])
    parser.add_argument("--verify-hash", action="store_true",
                        help="verify the hash of the product after it has been put in the archive")
    parser.add_argument("archive", metavar="ARCHIVE", help="identifier of the archive to use")
    parser.add_argument("expression", metavar="EXPRESSION", help="expression to filter products to pull")

    args, unused_args = version_parser.parse_known_args()
    if args.version:
        version(os.path.basename(sys.argv[0]))
        sys.exit(0)

    args = parser.parse_args(unused_args)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    try:
        return pull(args)
    except SystemExit:
        raise
    except KeyboardInterrupt:
        sys.exit(1)
    except muninn.Error as error:
        logging.error(error)
        sys.exit(1)
    except:
        log_internal_error()
        sys.exit(1)
    finally:
        logging.shutdown()
