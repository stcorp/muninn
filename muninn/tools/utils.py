#
# Copyright (C) 2014-2025 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import argparse
import logging
import multiprocessing
import os
import sys

try:
    from tqdm import tqdm as bar
    from multiprocessing import RLock
    RLock()  # fork off resource tracker before tqdm does it (with multiple threads running)
except ImportError:
    def bar(range, total=None, disable=None):
        return range

import muninn


def ceil(size):
    integer_size = int(size)
    return integer_size + 1 if size > integer_size else integer_size


def human_readable_size(size, base=1024, powers=["Bi", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]):
    if len(powers) == 0:
        return str(size)

    power = 0
    unit = 1
    while power < len(powers) - 1 and unit * base <= size:
        unit *= base
        power += 1

    size = size / unit
    ceil_size = ceil(size)
    ceil_size_10 = ceil(size * 10.0) / 10.0

    if power > 0 and size < 10.0 and ceil_size_10 < 10.0:
        result = "%.1f" % ceil_size_10
    elif ceil_size == base and power < len(powers) - 1:
        power += 1
        result = "1.0"
    else:
        result = str(ceil_size)

    return result + powers[power]


def format_duration(duration):
    if duration is None:
        return ''
        # return "<unknown>"

    try:
        duration = timedelta(seconds=round(duration))
    except OverflowError:
        return "<overflow>"
    return str(duration)


def format_size(size, human_readable=False):
    if size is None:
        return ''
        # return "<unknown>"

    if not human_readable:
        return str(size)

    return human_readable_size(float(size))


# This is a base class for operations on a list of items that can be performed in parallel using multiprocessing.
# If you use the processor object as a callable then it is assumed that the operation is performed using subprocesses.
# It will then create its own muninn archive instance per sub-process and prevents KeyboardInterrupt handling.
# For non-parallel execution just invoke the perform_operation method directly on the processor object using a valid
# archive handle.
class Processor(object):

    def __init__(self, args):
        global _POOL  # TODO what about multiple processors..

        self._archive_name = args.archive
        self._archive = None

        if args.parallel:
            if args.processes is not None:
                _POOL = multiprocessing.Pool(args.processes)
            else:
                _POOL = multiprocessing.Pool()

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

    def process(self, archive, args, items):
        total = len(items)
        num_success = 0

        if args.parallel:
            num_success = sum(list(bar(_POOL.imap(self, items), total=total, disable=None)))
            _POOL.close()
            _POOL.join()

        elif total > 1:
            for item in bar(items, disable=None):
                num_success += self.perform_operation(archive, item)

        elif total == 1:
            # don't show progress bar if we ingest just one item
            num_success = self.perform_operation(archive, items[0])

        return 0 if num_success == total else 1


# This parser is used in combination with the parse_known_args() function as a way to implement a "--version"
# option that prints version information and exits, and is included in the help message.
#
# The "--version" option should have the same semantics as the "--help" option in that if it is present on the
# command line, the corresponding action should be invoked directly, without checking any other arguments.
# However, the argparse module does not support user defined options with such semantics.
version_parser = argparse.ArgumentParser(add_help=False)
version_parser.add_argument("--version", action="store_true", help="output version information and exit")


def create_parser(*args, **kwargs):
    parallel = False
    if 'parallel' in kwargs:
        parallel = kwargs.pop('parallel')

    parser = argparse.ArgumentParser(*args, parents=[version_parser], **kwargs)
    parser.add_argument("--verbose", action="store_true", help="display debug information")

    if parallel:
        parser.add_argument("--parallel", action="store_true", help="use multi-processing to perform operation")
        parser.add_argument("--processes", type=int, help="use a specific amount of processes for --parallel")

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
    except Exception:
        log_internal_error()
        sys.exit(1)
    finally:
        logging.shutdown()
