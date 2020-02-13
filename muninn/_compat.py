from __future__ import absolute_import, division, print_function
'''
A tiny python 2/3 compability layer implementing only a minimal subset as needed by muninn
Inspired by six, future, and jinja2
'''
import sys
import operator


PY3 = sys.version_info[0] == 3

if PY3:

    long = int

    string_types = (str, )

    def is_python2_unicode(x):
        return False

    def dictkeys(d):
        return list(d.keys())

    def dictvalues(d):
        return list(d.values())

    def path_utf8(path):
        return path.encode('utf-8')

    def encode(s):
        return s.encode('utf-8')

    def decode(s):
        return s.decode('utf-8')

    itervalues = operator.methodcaller("values")

    imap = map
    izip = zip

    from urllib.parse import urlparse as urlparse_mod
    urlparse = urlparse_mod

    input = input

else:

    long = long

    string_types = (basestring, )

    def is_python2_unicode(x):
        return type(x) is unicode

    def path_utf8(path):
        return path.decode(sys.getfilesystemencoding()).encode('utf-8')

    def decode(s):
        return s

    def encode(s):
        return s

    dictkeys = operator.methodcaller("keys")
    dictvalues = operator.methodcaller("values")
    itervalues = operator.methodcaller("itervalues")

    import itertools
    imap = itertools.imap
    izip = itertools.izip

    from urlparse import urlparse as urlparse_mod
    urlparse = urlparse_mod

    input = raw_input
