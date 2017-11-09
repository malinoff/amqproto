import inspect
from itertools import zip_longest

import attr
import construct as c

from . import domains as d


def chunker(iterable, n):  # pylint: disable=invalid-name
    """Group data into n-sized chunks or blocks."""
    # chunker('ABCDEFG', 3) --> ['ABC', 'DEF', 'G']
    for idx in range(0, len(iterable), n):
        yield iterable[idx:idx+n]


def grouper(iterable, n, fillvalue=None):  # pylint: disable=invalid-name
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') -->
    #   [('A', 'B', 'C'), ('D', 'E', 'F'), ('G', 'x', 'x')]
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)


def make_struct(cls, exclude_attrs=(), struct=c.Struct):
    """Helper function to make C-like structs from attrs-powered classes.
    Attributes become fields, annotations define C types.
    """
    # The whole thing is highly dependent on the internals of attrs :(
    # pylint: disable=protected-access
    struct_fields = []
    # Bits are accumulated into octets.
    bits = []
    type_ = None
    for name, attrib in _attribs_from(cls):
        if name in exclude_attrs:
            continue

        attrib = getattr(cls, name)
        type_ = cls.__annotations__[name]

        if type_ is d.Bit:
            bits.append(name / c.Flag)
            continue
        # In AMQP 0.9.1, all methods (that have bit fields) have sequential
        # bit fields that should (and technically can) be accumulated
        # into a single octet.
        if type_ is not d.Bit and bits:
            if len(bits) < 8:
                bits += [c.Padding(8 - len(bits))]
            bit_struct = c.Embedded(c.BitStruct(*reversed(bits)))
            struct_fields.append(bit_struct)
            bits = []
        if attrib._default is None:
            type_ = c.Default(type_, c.Pass)
        elif attrib._default is not attr.NOTHING and not attrib.init:
            type_ = c.Const(type_, attrib._default)

        struct_fields.append(name / type_)

    # Handle the case when a method ends with a bit field.
    if type_ is d.Bit:
        if len(bits) < 8:
            bits += [c.Padding(8 - len(bits))]
        bit_struct = c.Embedded(c.BitStruct(*reversed(bits)))
        struct_fields.append(bit_struct)

    return cls.__name__ / struct(*struct_fields)


def _attribs_from(cls):
    # pylint: disable=protected-access
    members = inspect.getmembers(
        cls, lambda obj: isinstance(obj, attr._make._CountingAttr)
    )
    return sorted(members, key=lambda attr: attr[1].counter)
