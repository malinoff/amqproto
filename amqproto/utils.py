"""
amqproto.utils
~~~~~~~~~~~~~~

Various utility functions.
"""

import inspect
from itertools import zip_longest

import attr
import construct as c


def grouper(iterable, n, fillvalue=None):  # pylint: disable=invalid-name
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') -->
    #   [('A', 'B', 'C'), ('D', 'E', 'F'), ('G', 'x', 'x')]
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)


def chunker(iterable, n):  # pylint: disable=invalid-name
    """Group data into n-sized chunks or blocks."""
    # chunker('ABCDEFG', 3) --> ['ABC', 'DEF', 'G']
    for idx in range(0, len(iterable), n):
        yield iterable[idx:idx+n]


def make_struct(cls, exclude_attrs=(), struct=c.Struct):
    """Helper function to make C-like structs from attrs-powered classes.
    Attributes become fields, annotations define C types.
    """
    # The whole thing is highly dependent on the internals of attrs :(
    # pylint: disable=protected-access
    struct_fields = []
    ignore = set()
    for name, attrib in _attribs_from(cls):
        if name in exclude_attrs:
            continue

        attrib = getattr(cls, name)
        type_ = cls.__annotations__[name]

        if attrib._default is None:
            type_ = c.Default(type_, c.Pass)
        elif attrib._default is not attr.NOTHING and not attrib.init:
            ignore.add(name)
            type_ = c.Const(type_, attrib._default)

        struct_fields.append(name / type_)

    return c.ExprAdapter(
        subcon=cls.__name__ / struct(*struct_fields),
        encoder=None,
        decoder=lambda obj, ctx: cls(**{
            key: value for key, value in obj.items()
            if key not in ignore
        })
    )


def _attribs_from(cls):
    # pylint: disable=protected-access
    members = inspect.getmembers(
        cls, lambda obj: isinstance(obj, attr._make._CountingAttr)
    )
    return sorted(members, key=lambda attr: attr[1].counter)
