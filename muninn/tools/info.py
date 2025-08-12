#
# Copyright (C) 2014-2025 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import muninn

from muninn.tools.utils import create_parser, parse_args_and_run


def run(args):
    archive_names = args.archive or muninn.list_archives()

    print("ARCHIVES")
    for index, archive_name in enumerate(sorted(archive_names)):
        if index > 0 and not args.name_only:
            print()
        print("  " + archive_name)
        try:
            with muninn.open(archive_name) as archive:
                if args.name_only:
                    continue
                print("    NAMESPACES")
                for namespace in sorted(archive.namespaces()):
                    print("      %s" % namespace)
                    namespace_schema = archive.namespace_schema(namespace)
                    for name in sorted(namespace_schema):
                        field = namespace_schema[name]
                        field_name = field.name()
                        if field.__module__ != 'muninn.schema':
                            field_name = '%s.%s' % (field.__module__, field.name())
                        optional = namespace_schema.is_optional(name)
                        print("        %s: %s%s" % (name, field_name, " (optional)" if optional else "", ))

                print("\n    PRODUCT TYPES")
                for product_type in sorted(archive.product_types()):
                    print("      %s" % product_type)

                if archive.remote_backends():
                    print("\n    REMOTE BACKENDS")
                    for remote_backend in sorted(archive.remote_backends()):
                        print("      %s" % remote_backend)

                if archive.synchronizers():
                    print("\n    SYNCHRONIZERS")
                    for synchronizer in sorted(archive.synchronizers()):
                        print("      %s" % synchronizer)
        except Exception:
            print('    (could not open archive)')

    return 0


def main():
    parser = create_parser(description="Display generic archive information")
    parser.add_argument("-n", "--name-only", action="store_true",
                        help="only show archive name")
    parser.add_argument("archive", nargs='*', metavar="ARCHIVE",
                        help="archive identifier (default: search and show all archives)")
    return parse_args_and_run(parser, run)


if __name__ == '__main__':
    main()
