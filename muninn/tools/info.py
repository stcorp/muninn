#
# Copyright (C) 2014-2022 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import muninn

from .utils import create_parser, parse_args_and_run


def run(args):
    with muninn.open(args.archive) as archive:
        print("NAMESPACES")
        for namespace in sorted(archive.namespaces()):
            print("  %s" % namespace)
            namespace_schema = archive.namespace_schema(namespace)
            for name in sorted(namespace_schema):
                field = namespace_schema[name]
                field_name = field.name()
                if field.__module__ != 'muninn.schema':
                    field_name = '%s.%s' % (field.__module__, field.name())
                optional = namespace_schema.is_optional(name)
                print("    %s: %s%s" % (name, field_name, " (optional)" if optional else "", ))

        print("\nPRODUCT TYPES")
        for product_type in sorted(archive.product_types()):
            print("  %s" % product_type)

        if archive.remote_backends():
            print("\nREMOTE BACKENDS")
            for remote_backend in sorted(archive.remote_backends()):
                print("  %s" % remote_backend)

    return 0


def main():
    parser = create_parser(description="Display generic information about the archive.")
    parser.add_argument("archive", metavar="ARCHIVE", help="identifier of the archive to use")
    return parse_args_and_run(parser, run)
