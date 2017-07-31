#
# Copyright (C) 2014-2017 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import argparse
import logging
import muninn
import os
import re
import sys


def log_internal_error():
    import traceback

    logging.error("terminated due to an internal error")
    for message in traceback.format_exc().splitlines():
        logging.error("| " + message)


def export(args):
    with muninn.open(args.archive) as archive:
        if args.list_formats:
            if not archive.export_formats():
                print("no alternative export formats available")
            else:
                print("alternative export formats: " + " ".join(archive.export_formats()))
            print("")
            return 0

        if args.expression is None:
            logging.error("no search expression specified")
            return 1

        target_path = os.getcwd() if args.directory is None else args.directory
        archive.export(where=args.expression, target_path=target_path, format=args.format)
    return 0


def export_format(text):
    if re.match("[a-zA-Z]\\w*$", text) is None:
        raise argparse.ArgumentTypeError("invalid export format: %r" % text)

    return text


def directory(text):
    if not os.path.isdir(text):
        raise argparse.ArgumentTypeError("no such directory: %r" % text)

    return text


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

    parser = argparse.ArgumentParser(description="Export products from a muninn archive.", parents=[version_parser])
    parser.add_argument("-d", "--directory", type=directory, help="directory in which retrieved products will be"
                        " stored; by default, retrieved products will be stored in the current working directory")
    parser.add_argument("-f", "--format", type=export_format, help="format in which to export the products; if left"
                        " unspecified, the default export format for the product type will be used")
    parser.add_argument("-l", "--list-formats", action="store_true", help="list alternative (non-default) export"
                        " formats supported by the archive and exit")
    parser.add_argument("archive", metavar="ARCHIVE", help="identifier of the archive to use")
    parser.add_argument("expression", metavar="EXPRESSION", help="expression used to search for products to export")

    args, unused_args = version_parser.parse_known_args()
    if args.version:
        version(os.path.basename(sys.argv[0]))
        sys.exit(0)

    args = parser.parse_args(unused_args)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    try:
        return export(args)
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
