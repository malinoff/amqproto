"""
amqproto.content
~~~~~~~~~~~~~~~~

AMQP content.
"""

import attr

from .serialization import load, dump


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
    def load(cls, stream):
        # TODO support for the 16th bit set
        prop_names = []
        spec = []
        flags = load('H', stream)[0]
        bit = 1 << 15
        for char, name in zip(cls.spec, cls._field_names):
            if flags & bit:
                spec.append(char)
                prop_names.append(name)
            bit >>= 1
        props = load(spec, stream)
        return cls(**dict(zip(prop_names, props)))

    def dump(self):
        # TODO support for the 16th bit set
        spec = ['H']
        flags = 0
        props = []
        for idx, (char, prop_name) in enumerate(
                zip(self.spec, self._field_names)):
            prop = getattr(self, prop_name)
            if prop is not None:
                spec.append(char)
                props.append(prop)
                flags |= 1 << (15 - idx)
        return dump(spec, flags, *props)


@attr.s(slots=True)
class Content:
    """
    Describes an AMQP content.
    """

    body = attr.ib()
    body_size = attr.ib(default=None)
    properties = attr.ib(default=None)
    delivery_info = attr.ib(default=None, repr=False, cmp=False, hash=False)

    BY_ID = {}

    def __attrs_post_init__(self):
        if self.body_size is None:
            self.body_size = len(self.body)
        if self.properties is None:
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
    def load(cls, stream):
        class_id, weight, body_size = load('HHQ', stream)
        assert weight == 0
        properties = Properties.BY_ID[class_id].load(stream)
        return cls.BY_ID[class_id](b'', body_size, properties)

    def dump(self):
        header = dump('HHQ', self.class_id, 0, self.body_size)
        properties = self.properties.dump()
        return header + properties


@Properties.register(spec='ssTBBsssstsssss', class_id=60)
@attr.s(slots=True)
class BasicProperties(Properties):
    """Basic properties."""

    content_type = attr.ib(None)
    content_encoding = attr.ib(None)
    headers = attr.ib(None)
    delivery_mode = attr.ib(None)
    priority = attr.ib(None)
    correlation_id = attr.ib(None)
    reply_to = attr.ib(None)
    expiration = attr.ib(None)
    message_id = attr.ib(None)
    timestamp = attr.ib(None)
    type = attr.ib(None)
    user_id = attr.ib(None)
    app_id = attr.ib(None)
    # Deprecated
    cluster_id = attr.ib(default=None, init=False, repr=False)
    _reserved = attr.ib(default=None, init=False, repr=False)


@Content.register(class_id=60)
@attr.s(slots=True)
class BasicContent(Content):
    """
    Basic content.
    """
