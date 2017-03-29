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


class PlainWriter(object):
    def __init__(self, properties):
        self._properties = properties

    def header(self):
        header = [namespace + "." + name for namespace, name in self._properties]
        print("|", " | ".join(header), "|")

    def properties(self, properties):
        values = []
        for namespace, name in self._properties:
            try:
                values.append(str(properties[namespace][name]))
            except KeyError:
                values.append("")
        print("|", " | ".join(values), "|")

    def footer(self):
        pass


class CSVWriter(PlainWriter):
    def __init__(self, properties):
        super(CSVWriter, self).__init__(properties)

    def header(self):
        header = [namespace + "." + name for namespace, name in self._properties]
        print(",".join(["\"" + name.replace("\"", "\"\"") + "\"" for name in header]))

    def properties(self, properties):
        values = []
        for namespace, name in self._properties:
            try:
                values.append("\"" + str(properties[namespace][name]).replace("\"", "\"\"") + "\"")
            except KeyError:
                values.append("\"\"")
        print(",".join(values))


def parse_property_name(name):
    split_name = name.split(".")

    if len(split_name) == 1:
        return ("core", split_name[0])
    elif len(split_name) == 2:
        return tuple(split_name)

    raise ValueError("invalid property name: %r" % name)


def log_internal_error():
    import traceback

    logging.error("terminated due to an internal error")
    for message in traceback.format_exc().splitlines():
        logging.error("| " + message)


def format_attribute(value):
    return "<unknown>" if value is None else str(value)


def ceil(size):
    integer_size = int(size)
    return integer_size + 1 if size > integer_size else integer_size


def human_readable_size(size, base=1024, powers=["", "K", "M", "G", "T", "E"]):
    if len(powers) == 0:
        return str(size)

    power = 0
    unit = 1
    while power < len(powers) - 1 and unit * base <= size:
        unit *= base
        power += 1

    size = float(size) / unit
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


def format_size(size, human_readable=False):
    if size is None:
        return "<unknown>"

    if not human_readable:
        return str(size)

    return human_readable_size(size)


def count(args):
    archive = muninn.open(args.archive)
    print(archive.count(args.expression))
    return 0


def summary(args):
    archive = muninn.open(args.archive)
    summary = archive.summary(args.expression)

    if summary.count == 0:
        print("no products found")
        return 0

    print("count:         ", summary.count)
    if summary.size is None:
        print("size:           N/A")
    else:
        print("size:          ", format_size(long(summary.size), args.human_readable))
    print("validity start:", format_attribute(summary.validity_start))
    print("validity stop: ", format_attribute(summary.validity_stop))
    print("duration:      ", format_attribute(summary.duration))
    return 0


def uuid(args):
    archive = muninn.open(args.archive)

    # Collect possibly multiple sort order specifier lists into a single list.
    order_by = [] if args.order_by is None else sum(args.order_by, [])

    # Find products using the search expression and print the UUIDs of the products found.
    for product in archive.search(args.expression, order_by, args.limit):
        print(product.core.uuid)

    return 0


def search(args):
    archive = muninn.open(args.archive)

    # Collect possibly multiple sort order specifier lists into a single list.
    order_by = [] if args.order_by is None else sum(args.order_by, [])

    # Use default properties if no properties were explicitly requested.
    if args.properties is None:
        properties = [("core", "uuid"), ("core", "active"), ("core", "hash"), ("core", "size"),
                      ("core", "metadata_date"), ("core", "archive_date"), ("core", "archive_path"),
                      ("core", "product_type"), ("core", "product_name"), ("core", "physical_name"),
                      ("core", "validity_start"), ("core", "validity_stop"), ("core", "creation_date"),
                      ("core", "footprint"), ("core", "remote_url")]
    else:
        # Expand wildcards.
        properties = []
        for (namespace, name) in sum(args.properties, []):
            if name == "*":
                properties.extend([(namespace, name) for name in archive.namespace_schema(namespace)])
            else:
                properties.append((namespace, name))

    # Check property names against namespace schemas.
    for (namespace, name) in properties:
        schema = archive.namespace_schema(namespace)
        if name not in schema:
            logging.error("no property: %r defined within namespace: %r" % (name, namespace))
            return 1

    # Construct a list of the namespaces requested.
    namespaces = set([namespace for namespace, _ in properties])
    namespaces.discard("core")

    # Find products using the search expression.
    products = archive.search(args.expression, order_by, args.limit, namespaces=namespaces)

    # Output the requested properties of all products matching the search expression in the requested output format.
    if args.output_format == "plain":
        writer = PlainWriter(properties)
    else:
        writer = CSVWriter(properties)

    writer.header()
    for product in products:
        writer.properties(product)
    writer.footer()
    return 0


def property_list(text):
    property_list = []
    for property_name in text.split():
        try:
            namespace, name = parse_property_name(property_name)
        except ValueError as _error:
            raise argparse.ArgumentTypeError(*_error.args)

        property_list.append((namespace, name))

    return property_list


def order_by_list(text):
    # An order specifier without a "+" (ascending) prefix is interpreted as descending. Otherwise, it would be
    # impossible to specify descending order on the command line, because a "-" prefix is interpreted as an option by
    # argparse.
    #
    order_by_list = []
    for order_by in text.split():
        if order_by.startswith("+") or order_by.startswith("-"):
            sort_order, property_name = order_by[0], order_by[1:]
        else:
            sort_order, property_name = "-", order_by

        try:
            namespace, name = parse_property_name(property_name)
        except ValueError as _error:
            raise argparse.ArgumentTypeError(*_error.args)

        order_by_list.append(sort_order + namespace + "." + name)

    return order_by_list


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

    parser = argparse.ArgumentParser(description="Search a muninn archive for products.", parents=[version_parser])
    parser.add_argument("-f", "--output-format", choices=["plain", "csv"], default="plain", help="output format")
    parser.add_argument("-l", "--limit", type=int, help="limit the maximum number of products")
    parser.add_argument("-o", "--order-by", action="append", type=order_by_list, default=[], help="white space "
                        "separated list of sort order specifiers; a \"+\" prefix denotes ascending order; no prefix "
                        "denotes descending order")
    parser.add_argument("-p", "--property", action="append", type=property_list, dest="properties",
                        help="white space separated list of properties to output; use <namespace>.* to include all "
                        " properties of a namespace, e.g. core.*")
    parser.add_argument("-H", "--human-readable", action="store_true", help="output human readable size in product "
                        "summary (see -s, --summary option)")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-c", "--count", action="store_true", help="supress normal output; instead print the "
                       "number of products matching the search expression")
    group.add_argument("-s", "--summary", action="store_true", help="supress normal output; instead print a short "
                       "summary of the products matching the search expression")
    group.add_argument("-u", "--uuid", action="store_true", help="supress normal output; instead print the uuid "
                       "of each product found")
    parser.add_argument("archive", metavar="ARCHIVE", help="identifier of the archive to use")
    parser.add_argument("expression", metavar="EXPRESSION", help="expression used to search for products")

    args, unused_args = version_parser.parse_known_args()
    if args.version:
        version(os.path.basename(sys.argv[0]))
        sys.exit(0)

    args = parser.parse_args(unused_args)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    try:
        if args.count:
            return count(args)
        elif args.summary:
            return summary(args)
        elif args.uuid:
            return uuid(args)
        else:
            return search(args)
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
