#
# Copyright (C) 2014-2022 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import muninn

from muninn.tools.utils import create_parser, parse_args_and_run


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


def main():
    parser = create_parser(description="Prepare a muninn archive for first use.")
    parser.add_argument("-c", "--catalogue-only", action="store_true", help="only prepare the catalogue database, "
                        "without creating (or removing anything from) the archive storage")
    parser.add_argument("-f", "--force", action="store_true",
                        help="force preparation of an existing archive, completely removing all content")
    parser.add_argument("--dry-run", action="store_true", help="dump the SQL statements without executing them")
    parser.add_argument("archive", metavar="ARCHIVE", help="identifier of the archive to use")
    return parse_args_and_run(parser, prepare)


if __name__ == '__main__':
    main()
