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

    dictkeys = operator.methodcaller("keys")
    dictvalues = operator.methodcaller("values")
    itervalues = operator.methodcaller("itervalues")

    import itertools
    imap = itertools.imap
    izip = itertools.izip

    from urlparse import urlparse as urlparse_mod
    urlparse = urlparse_mod

    input = raw_input


def with_metaclass(meta, *bases):
    """
    Function from jinja2/_compat.py. License: BSD.
    Use it like this::
        class BaseForm(object):
            pass
        class FormType(type):
            pass
        class Form(with_metaclass(FormType, BaseForm)):
            pass
    This requires a bit of explanation: the basic idea is to make a
    dummy metaclass for one level of class instantiation that replaces
    itself with the actual metaclass.  Because of internal type checks
    we also need to make sure that we downgrade the custom metaclass
    for one level to something closer to type (that's why __call__ and
    __init__ comes back from type etc.).
    This has the advantage over six.with_metaclass of not introducing
    dummy classes into the final MRO.
    """

    class metaclass(meta):
        __call__ = type.__call__
        __init__ = type.__init__

        def __new__(cls, name, this_bases, d):
            if this_bases is None:
                return type.__new__(cls, name, (), d)
            return meta(name, bases, d)

    return metaclass('temporary_class', None, {})
