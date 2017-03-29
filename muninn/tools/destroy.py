#
# Copyright (C) 2014-2017 S[&]T, The Netherlands.
#

from __future__ import absolute_import, print_function

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


def ask_yes_no(question, default=True):
    prompt = "[y/n]" if default is None else "[Y/n]" if default else "[y/N]"
    while True:
        print(question + " " + prompt, end="")
        answer = raw_input().lower()

        if default is not None and not answer:
            return default

        if answer in ("y", "yes", "n", "no"):
            return answer.startswith("y")

        print("")
        print("Please respond with \"yes\" or \"no\" (or \"y\" or \"n\").")


def destroy(args):
    archive = muninn.open(args.archive)
    if not args.yes:
        if args.catalogue_only:
            print(("You are about to remove the catalogue database for the archive \"%s\". " +
                   "This operation cannot be undone!") % args.archive)
        else:
            print(("You are about to completely remove the archive \"%s\". " +
                   "This operation cannot be undone!") % args.archive)
        if not ask_yes_no("Do you want to continue?", False):
            return 1

    if args.catalogue_only:
        archive.destroy_catalogue()
    else:
        archive.destroy()
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

    parser = argparse.ArgumentParser(description="Remove a muninn archive and its contents",
                                     parents=[version_parser])
    parser.add_argument("-c", "--catalogue-only", action="store_true", help="only remove the catalogue database, "
                        "without touching (or removing anything from) the archive root path on disk")
    parser.add_argument("-y", "--yes", action="store_true", help="assume yes and do not prompt for confirmation")
    parser.add_argument("archive", metavar="ARCHIVE", help="identifier of the archive to use")

    args, unused_args = version_parser.parse_known_args()
    if args.version:
        version(os.path.basename(sys.argv[0]))
        sys.exit(0)

    args = parser.parse_args(unused_args)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    try:
        return destroy(args)
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
