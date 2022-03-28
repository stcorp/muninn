#
# Copyright (C) 2014-2022 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

from muninn._compat import input
import muninn

from muninn.tools.utils import create_parser, parse_args_and_run


def ask_yes_no(question, default=True):
    prompt = "[y/n]" if default is None else "[Y/n]" if default else "[y/N]"
    while True:
        print(question + " " + prompt, end="")
        answer = input().lower()

        if default is not None and not answer:
            return default

        if answer in ("y", "yes", "n", "no"):
            return answer.startswith("y")

        print("")
        print("Please respond with \"yes\" or \"no\" (or \"y\" or \"n\").")


def destroy(args):
    with muninn.open(args.archive) as archive:
        if not args.yes:
            if args.catalogue_only:
                print(("You are about to remove the catalogue database for the archive \"%s\". "
                       "This operation cannot be undone!") % args.archive)
            else:
                print(("You are about to completely remove the archive \"%s\". "
                       "This operation cannot be undone!") % args.archive)
            if not ask_yes_no("Do you want to continue?", False):
                return 1

        if args.catalogue_only:
            archive.destroy_catalogue()
        else:
            archive.destroy()
    return 0


def main():
    parser = create_parser(description="Remove a muninn archive and its contents")
    parser.add_argument("-c", "--catalogue-only", action="store_true", help="only remove the catalogue database, "
                        "without touching (or removing anything from) the archive root path on disk")
    parser.add_argument("-y", "--yes", action="store_true", help="assume yes and do not prompt for confirmation")
    parser.add_argument("archive", metavar="ARCHIVE", help="identifier of the archive to use")
    return parse_args_and_run(parser, destroy)


if __name__ == '__main__':
    main()
