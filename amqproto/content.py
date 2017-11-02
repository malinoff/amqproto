"""
amqproto.content
~~~~~~~~~~~~~~~~

AMQP content.
"""

# flake8: noqa=E701
# pylint: disable=too-few-public-methods

import inspect
from itertools import zip_longest

import attr
import construct as c

from . import domains as d


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
            bit_struct = c.Embedded(c.BitStruct(*bits))
            struct_fields.append(bit_struct)
        if attrib._default is None:
            type_ = c.Default(type_, c.Pass)
        elif attrib._default is not attr.NOTHING and not attrib.init:
            type_ = c.Const(type_, attrib._default)

        struct_fields.append(name / type_)

    # Handle the case when a method ends with a bit field.
    if type_ is d.Bit:
        if len(bits) < 8:
            bits += [c.Padding(8 - len(bits))]
        bit_struct = c.Embedded(c.BitStruct(*bits))
        struct_fields.append(bit_struct)

    return cls.__name__ / struct(*struct_fields)


def _attribs_from(cls):
    # pylint: disable=protected-access
    members = inspect.getmembers(
        cls, lambda obj: isinstance(obj, attr._make._CountingAttr)
    )
    return sorted(members, key=lambda attr: attr[1].counter)


class PropertiesStruct(c.Construct):
    """Content header properties have the following format:

    0                2
    +----------------+-------------- ---
    | Property flags | Property list ...
    +----------------+-------------- ---
          Short

    * The property flags are an array of bits that indicate
      the presence or absence of each property value in sequence.
      The bits are ordered from most high to low - bit 15 indicates
      the first property.

    * The property flags can specify more than 16 properties.
      If the last bit (0) is set, this indicates that a further property
      flags field follows. There are many property flags fields as needed.

    * The property values are class-specific AMQP data fields.

    * Bit properties are indicated ONLY by their respective property flag
      (1 or 0) and are never present in the property list.
    """

    # pylint: disable=protected-access

    def __init__(self, *subcons):
        super().__init__()
        # Group all properties into groups of 15 properties,
        # 16th bit indicates the contiuation of properties.
        flags = [prop.name / c.Default(c.Flag, 0)
                 for group in grouper(subcons, 15, c.Padding(1))
                 for prop in list(group) + [c.Const(c.Flag, 1)]]
        # Set the last bit to 0, as there's no more contiuation.
        if flags:
            flags[-1].value = 0
        self.flags = c.BitStruct(*flags)

        def _is_bit(subcon):
            while True:
                if isinstance(subcon, c.BitsInteger) and subcon.length == 1:
                    return True
                subcon = getattr(subcon, 'subcon', None)
                if subcon is None:
                    break
            return False

        self.subcons = [subcon for subcon in subcons if not _is_bit(subcon)]

    def _parse(self, stream, context, path):
        flags = self.flags._parse(stream, context, path)
        obj = c.Container()
        for subcon in self.subcons:
            if flags[subcon.name]:
                value = subcon._parse(stream, context, path)
                obj[subcon.name] = value
        return obj

    def _build(self, obj, stream, context, path):
        self.flags._build(obj, stream, context, path)
        for subcon in self.subcons:
            value = obj.get(subcon.name)
            if value is not None:
                subcon._build(value, stream, context, path)

    def _sizeof(self, context, path):
        return self.flags.sizeof() + sum(
            subcon.sizeof() for subcon in self.subcons
        )


class Properties:
    """Base class for content properties."""

    struct = None

    BY_ID = {}

    def __init_subclass__(cls, class_id, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.class_id = attr.ib(default=class_id, init=False)
        cls.struct = make_struct(
            cls, exclude_attrs={'class_id'}, struct=PropertiesStruct
        )
        cls.BY_ID[class_id] = cls


@attr.s(slots=True)
class Content:
    """Describes an AMQP content."""

    class_id: int = None

    body: bytes = attr.ib()
    body_size: int = attr.ib()
    properties: Properties = attr.ib(default=attr.Factory(
        takes_self=True,
        factory=lambda self: Properties.BY_ID[self.class_id](),
    ))
    delivery_info = attr.ib(default=None)

    def complete(self):
        """AMQP allows to break content into multiple chunks for sending.
        This method tells if the content is received completely.
        """
        return len(self.body) == self.body_size


@attr.s()
class BasicProperties(Properties, class_id=60):
    """Basic properties."""

    content_type: d.ShortStr = attr.ib(None)
    content_encoding: d.ShortStr = attr.ib(None)
    headers: d.Table = attr.ib(None)
    delivery_mode: d.Octet = attr.ib(None)
    priority: d.Octet = attr.ib(None)
    correlation_id: d.ShortStr = attr.ib(None)
    reply_to: d.ShortStr = attr.ib(None)
    expiration: d.ShortStr = attr.ib(None)
    message_id: d.ShortStr = attr.ib(None)
    timestamp: d.Timestamp = attr.ib(None)
    type: d.ShortStr = attr.ib(None)
    user_id: d.ShortStr = attr.ib(None)
    app_id: d.ShortStr = attr.ib(None)
    # Deprecated
    cluster_id: d.ShortStr = attr.ib(default=None, init=False, repr=False)


class BasicContent(Content):
    """Basic content."""

    class_id = 60
