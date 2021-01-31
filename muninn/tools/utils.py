#
# Copyright (C) 2014-2021 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import argparse
import logging
import muninn
import os
import sys


# This is a base class for operations on a list of items that can be performed in parallel using multiprocessing.
# If you use the processor object as a callable then it is assumed that the operation is performed using subprocesses.
# It will then create its own muninn archive instance per sub-process and prevents KeyboardInterrupt handling.
# For non-parallel execution just invoke the perform_operation method directly on the processor object using a valid
# archive handle.
class Processor(object):

    def __init__(self, archive_name):
        self._archive_name = archive_name
        self._archive = None

    def perform_operation(self, archive, item):
        pass

    def __call__(self, item):
        try:
            if self._archive is None:
                self._archive = muninn.open(self._archive_name)
            return self.perform_operation(self._archive, item)
        except KeyboardInterrupt:
            # don't capture keyboard interrupts inside sub-processes (only the main process should handle it)
            pass


# This parser is used in combination with the parse_known_args() function as a way to implement a "--version"
# option that prints version information and exits, and is included in the help message.
#
# The "--version" option should have the same semantics as the "--help" option in that if it is present on the
# command line, the corresponding action should be invoked directly, without checking any other arguments.
# However, the argparse module does not support user defined options with such semantics.
version_parser = argparse.ArgumentParser(add_help=False)
version_parser.add_argument("--version", action="store_true", help="output version information and exit")


def create_parser(*args, **kwargs):
    parser = argparse.ArgumentParser(*args, parents=[version_parser], **kwargs)
    parser.add_argument("--verbose", action="store_true", help="display debug information")
    return parser


def version(program_name):
    print("%s %s" % (program_name, muninn.__version__))
    print(muninn.__copyright__)
    print("")


def log_internal_error():
    import traceback

    logging.error("terminated due to an internal error")
    for message in traceback.format_exc().splitlines():
        logging.error("| " + message)


def parse_args_and_run(parser, func):
    args, unused_args = version_parser.parse_known_args()
    if args.version:
        version(os.path.basename(sys.argv[0]))
        sys.exit(0)

    args = parser.parse_args(unused_args)

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level, format="%(levelname)s: %(message)s")

    try:
        return func(args)
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
