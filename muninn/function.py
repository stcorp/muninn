#
# Copyright (C) 2014-2017 S[&]T, The Netherlands.
#

from __future__ import absolute_import

import collections
import itertools


class Prototype(object):
    def __init__(self, name, argument_types=(), return_type=None):
        self.name = name
        self.argument_types = argument_types
        self.return_type = return_type

        self._id = "%s(%s)" % (self.name, ",".join(itertools.imap(lambda type: type.name(), self.argument_types)))
        if self.return_type is not None:
            self._id += " " + self.return_type.name()

    @property
    def id(self):
        return self._id

    @property
    def arity(self):
        return len(self.argument_types)

    def _argument_types_equal(self, other):
        if self.arity != other.arity:
            return False

        for type_self, type_other in itertools.izip(self.argument_types, other.argument_types):
            if type_self is not type_other:
                return False
        return True

    def __eq__(self, other):
        return self.name == other.name and self._argument_types_equal(other) and self.return_type is other.return_type

    def __ne__(self, other):
        return not self == other

    def __str__(self):
        return self.id

    def __repr__(self):
        return "Prototype(name=%r, argument_types=%r, return_type=%r)" % (self.name, self.argument_types,
                                                                          self.return_type)

    def __hash__(self):
        return hash(self.id)


class FunctionTable(collections.MutableSet):
    def __init__(self, prototypes=[]):
        self._prototypes = {}
        for prototype in prototypes:
            self.add(prototype)

    def __contains__(self, prototype):
        try:
            prototypes = self._prototypes[prototype.name]
        except KeyError:
            return False
        else:
            return prototype in prototypes

    def __iter__(self):
        for prototypes in self._prototypes.itervalues():
            for prototype in prototypes:
                yield prototype

    def __len__(self):
        return sum(itertools.imap(len, self._prototypes.itervalues()))

    def add(self, prototype):
        try:
            self._prototypes[prototype.name].add(prototype)
        except KeyError:
            self._prototypes[prototype.name] = set((prototype,))

    def discard(self, prototype):
        try:
            self._prototypes[prototype.name].discard(prototype)
        except KeyError:
            pass

    def resolve(self, prototype):
        top, top_equal = [], 0
        for candidate in self._prototypes[prototype.name]:
            if candidate.arity != prototype.arity:
                continue

            equal, compatible = 0, 0
            for candidate_type, type in itertools.izip(candidate.argument_types, prototype.argument_types):
                if type is candidate_type:
                    equal += 1
                elif issubclass(type, candidate_type):
                    compatible += 1
                else:
                    break

            if equal + compatible != prototype.arity:
                continue

            if equal > top_equal:
                top = [candidate]
                top_equal = equal
            elif equal == top_equal:
                top.append(candidate)

        return top
