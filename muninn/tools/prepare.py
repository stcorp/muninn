#
# Copyright (C) 2014-2017 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import argparse
import logging
import muninn
import os
import sys


def log_internal_error():
    import traceback

    logging.error("terminated due to an internal error")
    for message in traceback.format_exc().splitlines():
        logging.error("| " + message)


def prepare(args):
    with muninn.open(args.archive) as archive:
        if args.dry_run:
            print("The following SQL statements would be executed:")
            for sql in archive.prepare_catalogue(dry_run=True):
                print("  " + sql)
        elif args.catalogue_only:
            archive.prepare_catalogue()
        else:
            archive.prepare(force=args.force)
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

    parser = argparse.ArgumentParser(description="Prepare a muninn archive for first use.", parents=[version_parser])
    parser.add_argument("-c", "--catalogue-only", action="store_true", help="only prepare the catalogue database, "
                        "without creating (or removing anything from) the archive root path on disk")
    parser.add_argument("-f", "--force", action="store_true",
                        help="force preparation of an existing archive, completely removing its contents")
    parser.add_argument("--dry-run", action="store_true", help="dump the SQL statements without executing them")
    parser.add_argument("archive", metavar="ARCHIVE", help="identifier of the archive to use")

    args, unused_args = version_parser.parse_known_args()
    if args.version:
        version(os.path.basename(sys.argv[0]))
        sys.exit(0)

    args = parser.parse_args(unused_args)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    try:
        return prepare(args)
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
