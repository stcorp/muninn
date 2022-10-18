#
# Copyright (C) 2014-2022 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import argparse
import logging

import muninn
try:
    import tabulate
except ImportError:
    tabulate = None

from muninn.tools.utils import create_parser, parse_args_and_run

OWN_SUPPORTED_FORMATS = ['psv', 'csv']
if tabulate is None:
    default_format = 'psv'
    SUPPORTED_FORMATS = OWN_SUPPORTED_FORMATS
else:
    default_format = 'orgtbl'
    SUPPORTED_FORMATS = set(tabulate.tabulate_formats + OWN_SUPPORTED_FORMATS)


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


class TabulateWriter(PlainWriter):
    def __init__(self, properties, fmt='orgtbl'):
        super(TabulateWriter, self).__init__(properties)
        self._header = []
        self._data = []
        self._format = fmt

    def header(self):
        self._header = [namespace + "." + name for namespace, name in self._properties]

    def properties(self, properties):
        values = []
        for namespace, name in self._properties:
            try:
                values.append(str(properties[namespace][name]))
            except KeyError:
                values.append("")
        self._data.append(values)

    def footer(self):
        print(tabulate.tabulate(self._data, headers=self._header, tablefmt=self._format, disable_numparse=True))


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
        if name == '*':
            return ('*', '*')
        else:
            return ('core', name)
    elif len(split_name) == 2:
        return tuple(split_name)

    raise ValueError("invalid property name: %r" % name)


def count(args):
    with muninn.open(args.archive) as archive:
        print(archive.count(args.expression))
    return 0


def uuid(args):
    with muninn.open(args.archive) as archive:
        # Collect possibly multiple sort order specifier lists into a single list.
        order_by = [] if args.order_by is None else sum(args.order_by, [])

        # Find products using the search expression and print the UUIDs of the products found.
        for product in archive.search(args.expression, order_by, args.limit, property_names=['uuid']):
            print(product.core.uuid)

    return 0


def paths(args):
    with muninn.open(args.archive) as archive:
        # Collect possibly multiple sort order specifier lists into a single list.
        order_by_default = ['+core.archive_path', '+core.physical_name']
        order_by = order_by_default if not args.order_by else sum(args.order_by, []) + order_by_default

        # Find products using the search expression and print the paths of the products found.
        products = archive.search(args.expression, order_by, args.limit,
                                  property_names=['archive_path', 'physical_name'])
        for product in products:
            product_path = archive.product_path(product)
            if product_path is not None:
                print(product_path)

    return 0


def _extend_properties(properties, namespace, name, archive):
    if namespace == '*':
        # get all namespaces; make sure 'core' is the first one
        namespaces = archive.namespaces()
        if namespaces[0] != 'core':
            namespaces.remove('core')
            namespaces.insert(0, 'core')
        for namespace in namespaces:
            _extend_properties(properties, namespace, name, archive)
    elif name == "*":
        # get all fields
        properties.extend([(namespace, name) for name in archive.namespace_schema(namespace)])
    else:
        properties.append((namespace, name))


def search(args):
    with muninn.open(args.archive) as archive:
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
                _extend_properties(properties, namespace, name, archive)

        # Check property names against namespace schemas.
        for (namespace, name) in properties:
            schema = archive.namespace_schema(namespace)
            if name not in schema:
                logging.error("no property: %r defined within namespace: %r" % (name, namespace))
                return 1

        # Find products using the search expression.
        products = archive.search(args.expression, order_by, args.limit,
                                  property_names=[".".join(item) for item in properties])

        # Output the requested properties of all products matching the search expression in the requested output format.
        if args.output_format == "psv":  # PSV = Pipe Separated Values
            writer = PlainWriter(properties)
        elif args.output_format == "csv":
            writer = CSVWriter(properties)
        elif tabulate is not None:
            writer = TabulateWriter(properties, args.output_format)
        else:
            writer = PlainWriter(properties)

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


def run(args):
    if args.count:
        return count(args)
    elif args.uuid:
        return uuid(args)
    elif args.paths:
        return paths(args)
    else:
        return search(args)


def main():
    parser = create_parser(description="Search a muninn archive for products.")
    parser.add_argument("-f", "--output-format", choices=SUPPORTED_FORMATS, default=default_format,
                        help="output format")
    parser.add_argument("-l", "--limit", type=int, help="limit the maximum number of products")
    parser.add_argument("-o", "--order-by", action="append", type=order_by_list, default=[], help="white space "
                        "separated list of sort order specifiers; a \"+\" prefix denotes ascending order; no prefix "
                        "denotes descending order")
    parser.add_argument("-p", "--property", action="append", type=property_list, dest="properties",
                        help="white space separated list of properties to output; use `<namespace>.*` to include all "
                        "properties of a namespace, e.g. core.*; use `*` to include all namespaces")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-c", "--count", action="store_true", help="suppress normal output; instead print the "
                       "number of products matching the search expression")
    group.add_argument("-u", "--uuid", action="store_true", help="suppress normal output; instead print the uuid "
                       "of each product found")
    group.add_argument("--paths", action="store_true", help="suppress normal output; instead print the physical "
                       "path of each product found")
    parser.add_argument("archive", metavar="ARCHIVE", help="identifier of the archive to use")
    parser.add_argument("expression", metavar="EXPRESSION", help="expression used to search for products")

    return parse_args_and_run(parser, run)


if __name__ == '__main__':
    main()
