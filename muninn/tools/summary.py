#
# Copyright (C) 2014-2022 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import re
from datetime import timedelta
import argparse

import muninn
from muninn.tools.utils import create_parser, parse_args_and_run


try:
    import tabulate
except ImportError:
    tabulate = None

OWN_OUTPUT_FORMATS = ['psv', 'csv']
if tabulate is None:
    DEFAULT_FORMAT = 'psv'
    OUTPUT_FORMATS = OWN_OUTPUT_FORMATS
else:
    DEFAULT_FORMAT = 'orgtbl'
    OUTPUT_FORMATS = set(tabulate.tabulate_formats + OWN_OUTPUT_FORMATS)


DEFAULT_STATS = ['size.sum', 'validity_start.min', 'validity_stop.max']


def ceil(size):
    integer_size = int(size)
    return integer_size + 1 if size > integer_size else integer_size


def human_readable_size(size, base=1024, powers=["Bi", "Ki", "Mi", "Gi", "Ti", "Ei"]):
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


# Support multiple table output formats

class PlainWriter(object):
    def __init__(self, header, human_readable=False):
        self._header = [re.sub('^core.', '', item) for item in header]
        self._duration_fields = []
        self._size_fields = []
        self.human_readable = human_readable
        for i, name in enumerate(header):
            if name.startswith('core.validity_duration'):
                self._duration_fields.append(i)
            elif name.startswith('core.size'):
                self._size_fields.append(i)

    def header(self):
        print("|", " | ".join(self._header), "|")

    def _format_items(self, values):
        result = []
        for i, value in enumerate(values):
            if i in self._duration_fields:
                result.append(format_duration(value))
            elif i in self._size_fields:
                result.append(format_size(value, self.human_readable))
            else:
                result.append(value)
        return result

    def row(self, values):
        print("|", " | ".join(str(item) for item in self._format_items(values)), "|")

    def footer(self):
        pass


class TabulateWriter(PlainWriter):
    def __init__(self, header, human_readable=False, fmt='orgtbl'):
        super(TabulateWriter, self).__init__(header, human_readable)
        self._data = []
        self._format = fmt

    def header(self):
        pass

    def row(self, values):
        self._data.append(self._format_items(values))

    def footer(self):
        # align core.size fields to the right (only available in tabulate > 0.8.2)
        # colalign = ['right' if i in self._size_fields else None for i in range(len(self._header))]
        # print(tabulate.tabulate(self._data, headers=self._header, tablefmt=self._format), colalign=colalign)
        print(tabulate.tabulate(self._data, headers=self._header, tablefmt=self._format))


class CSVWriter(PlainWriter):
    def header(self):
        print(",".join(["\"" + name.replace("\"", "\"\"") + "\"" for name in self._header]))

    def row(self, values):
        print(",".join("\"" + str(item).replace("\"", "\"\"") + "\"" for item in self._format_items(values)))


def get_writer(header, args):
    if args.output_format == "psv":  # PSV = Pipe Separated Values
        writer = PlainWriter(header, args.human_readable)
    elif args.output_format == "csv":
        writer = CSVWriter(header, args.human_readable)
    elif tabulate is not None:
        writer = TabulateWriter(header, args.human_readable, args.output_format)
    else:
        writer = PlainWriter(header, args.human_readable)
    return writer


def canonical_property(archive, name):
    '''
    Make sure "core" is added to properties without namespace.
    '''
    if name in ['count', 'tag']:
        return name

    metadata = archive._namespace_schemas

    if name == '*':
        name = '*.*'
    else:
        split_name = name.split('.')
        if split_name[0] not in metadata and (split_name[0] in metadata['core'] or
                                              split_name[0] == 'validity_duration'):
            name = '%s.%s' % ('core', name)

    return name


def coalesce_identifier_arguments(arg, archive):
    if arg:
        names = re.split('[ ,]+', ' '.join(arg))
        result = [canonical_property(archive, name) for name in names if name]
    else:
        result = []
    return result


def coalesce_order_by_args(arg, archive):
    # An order specifier without a "+" (ascending) prefix is interpreted as descending. Otherwise, it would be
    # impossible to specify descending order on the command line, because a "-" prefix is interpreted as an option by
    result = []
    if arg:
        names = re.split('[ ,]+', ' '.join(arg))
        for name in names:
            if name:
                if name.startswith('+') or name.startswith('-'):
                    order = name[0]
                    name = name[1:]
                else:
                    order = '-'
                name = canonical_property(archive, name)
                result.append(order + name)
    return result


def run(args):
    with muninn.open(args.archive) as archive:
        group_by = coalesce_identifier_arguments(args.group_by, archive)
        if args.stats:
            stats = coalesce_identifier_arguments(args.stats, archive)
        else:
            stats = coalesce_identifier_arguments(DEFAULT_STATS, archive)
        order_by = coalesce_order_by_args(args.order_by, archive)

        result, header = archive.summary(
            args.expression,
            aggregates=stats,
            group_by=group_by,
            group_by_tag=args.group_by_tag,
            order_by=order_by,
            having=args.stats_filter
        )

        # Output summary in the requested output format.
        writer = get_writer(header, args)
        writer.header()
        for product in result:
            writer.row(product)
        writer.footer()

    return 0


def main():
    parser = create_parser(
        description="Summary of the products matching the search expression.",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("-f", "--output-format", choices=OUTPUT_FORMATS, default=DEFAULT_FORMAT,
                        metavar="FORMAT", help="output format; choices (depending on installation of "
                        "python-tabulate): {%(choices)s}; default: %(default)s")
    parser.add_argument("-o", "--order-by", action="append", dest="order_by", help="white space "
                        "separated list of sort order specifiers; a \"+\" prefix denotes ascending order; no prefix "
                        "denotes descending order")
    parser.add_argument("-g", "--group-by", action="append", dest="group_by",
                        help="list of property names whose values are used for grouping the aggregation results; "
                        "there will be a separate result row for each combination of group_by property values; "
                        "properties need to be of type long, integer, boolean, text or timestamp; "
                        "timestamps require a binning subscript which can be 'year', 'month', 'yearmonth', 'day', "
                        "'date', 'hour', 'minute', 'second' or 'time' (e.g. `validity_start.yearmonth`)")
    parser.add_argument("-t", "--group-by-tag", action="store_true", help="group result by tag value; "
                        "note that products will be counted multiple times if they have multiple tags")
    parser.add_argument("-s", "--stats", action="append", dest="stats", help="list of property aggregates defined as "
                        "`<property_name>.<reduce_fn>`; "
                        "properties need to be of type long, integer, real, text or timestamp; "
                        "the reduce function can be 'min', 'max', 'sum', or 'avg'; "
                        "'sum' and 'avg' are not possible for text and timestamp properties; "
                        "a special property 'validity_duration' (defined as validity_stop - validity_start) can also "
                        "be used; default stats: %r" % ' '.join(DEFAULT_STATS))
    parser.add_argument("--stats-filter", metavar='EXPRESSION', help="expression used to filter on aggregates")
    parser.add_argument("-H", "--human-readable", action="store_true", help="output human readable core.size")
    parser.add_argument("archive", metavar="ARCHIVE", help="identifier of the archive to use")
    parser.add_argument("expression", metavar="EXPRESSION", nargs='?', help="expression used to search for products")

    return parse_args_and_run(parser, run)


if __name__ == '__main__':
    main()
