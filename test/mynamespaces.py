from muninn.schema import Mapping, optional, Text, Integer, JSON


class MyNamespace(Mapping):
    hello = optional(Text)
    myjson = optional(JSON)


class MyNamespace2(Mapping):
    counter = optional(Integer)

NAMESPACES = {
        'mynamespace': MyNamespace,
        'mynamespace2': MyNamespace2,
}


def namespaces():
    return list(NAMESPACES)


def namespace(key):
    return NAMESPACES[key]
