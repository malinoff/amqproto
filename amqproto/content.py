"""
amqproto.content
~~~~~~~~~~~~~~~~

AMQP content.
"""

from datetime import datetime

import attr


class Properties:
    """
    Base class for content properties.
    """
    __slots__ = ()

    BY_ID = {}

    @classmethod
    def register(cls, spec, class_id):
        def decorator(properties):
            properties.spec = spec
            properties.class_id = class_id
            properties._field_names = attr.fields_dict(properties).keys()
            cls.BY_ID[class_id] = properties
            return properties
        return decorator

    @classmethod
    def read(cls, reader):
        property_flags = []
        while True:
            flags = bin(reader.read_short())[2:]
            property_flags.extend(flag == '1' for flag in flags[:-1])
            if flags[-1] == '0':
                break
        spec = [
            char
            for char, present in zip(cls.spec, property_flags)
            if present
        ]
        return cls(*reader.load(spec))

    def write(self, writer):
        # TODO support for the 16th bit set
        properties = [getattr(self, prop) for prop in self._field_names]
        spec = [
            char
            for char, prop in zip(self.spec, properties)
            if prop is not None
        ]
        flags = ''.join('0' if prop is None else '1' for prop in properties)
        flags = int(flags, 2)
        writer.write_short(flags)
        writer.dump(spec, *(prop for prop in properties if prop is not None))


@attr.s(slots=True)
class Content:
    """
    Describes an AMQP content.
    """

    body: bytes = attr.ib()
    body_size: int = attr.ib()
    properties: Properties = attr.ib(default=None)
    delivery_info = attr.ib(default=None, repr=False, cmp=False, hash=False)

    BY_ID = {}

    def __attrs_post_init__(self):
        self.properties = Properties.BY_ID[self.class_id]()

    def complete(self):
        """AMQP allows to break content into multiple chunks for sending.
        This method tells if the content is received completely.
        """
        return len(self.body) == self.body_size

    @classmethod
    def register(cls, class_id):
        def decorator(content):
            content.class_id = class_id
            cls.BY_ID[class_id] = content
            return content
        return decorator

    @classmethod
    def read(cls, reader):
        class_id = reader.read_short()
        weight = reader.read_short()  # noqa: F841
        assert weight == 0
        body_size = reader.read_long_long()
        properties = Properties.BY_ID[class_id].read(reader)
        return cls.BY_ID[class_id](b'', body_size, properties)

    def write(self, writer):
        writer.write_short(self.class_id)
        writer.write_short(0)  # weight
        writer.write_long_long(self.body_size)
        self.properties.write(writer)


@Properties.register(spec='ssTBBsssstssss', class_id=60)
@attr.s(slots=True)
class BasicProperties(Properties):
    """Basic properties."""

    content_type: str = attr.ib(None)
    content_encoding: str = attr.ib(None)
    headers: dict = attr.ib(None)
    delivery_mode: int = attr.ib(None)
    priority: int = attr.ib(None)
    correlation_id: str = attr.ib(None)
    reply_to: str = attr.ib(None)
    expiration: str = attr.ib(None)
    message_id: str = attr.ib(None)
    timestamp: datetime = attr.ib(None)
    type: str = attr.ib(None)
    user_id: str = attr.ib(None)
    app_id: str = attr.ib(None)
    # Deprecated
    cluster_id: str = attr.ib(default=None, init=False, repr=False)


@Content.register(class_id=60)
@attr.s(slots=True)
class BasicContent(Content):
    """
    Basic content.
    """
