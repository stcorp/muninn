#
# Copyright (C) 2014-2022 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import inspect


class TypeVisitor(object):
    def visit(self, visitable, *args, **kwargs):
        for type in inspect.getmro(visitable):
            try:
                visit_func = getattr(self, "visit_%s" % type.__name__)
            except AttributeError:
                pass
            else:
                return visit_func(visitable, *args, **kwargs)

        try:
            visit_func = getattr(self, "default")
        except AttributeError:
            pass
        else:
            return visit_func(visitable, *args, **kwargs)


class Visitor(object):
    def visit(self, visitable, *args, **kwargs):
        for type_ in inspect.getmro(type(visitable)):
            try:
                visit_func = getattr(self, "visit_%s" % type_.__name__)
            except AttributeError:
                pass
            else:
                return visit_func(visitable, *args, **kwargs)

        try:
            visit_func = getattr(self, "default")
        except AttributeError:
            pass
        else:
            return visit_func(visitable, *args, **kwargs)
