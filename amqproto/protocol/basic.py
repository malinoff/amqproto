"""
amqproto.protocol.basic
~~~~~~~~~~~~~~~~~~~~~~~

Implementation of AMQP Basic class.
"""

import enum
import datetime
import collections

import attr

from . import types

__all__ = ['BasicMessage']

PROPERTIES = collections.OrderedDict((
    ('content_type', types.Shortstr),
    ('content_encoding', types.Shortstr),
    ('headers', types.Table),
    ('delivery_mode', types.Octet),
    ('priority', types.Octet),
    ('correlation_id', types.Shortstr),
    ('reply_to', types.Shortstr),
    ('expiration', types.Shortstr),
    ('message_id', types.Shortstr),
    ('timestamp', types.Timestamp),
    ('type', types.Shortstr),
    ('user_id', types.Shortstr),
    ('app_id', types.Shortstr),
))


class DeliveryMode(enum.Enum):
    """Delivery modes for Basic message."""
    NonPersistent = 1
    Persistent = 2


@attr.s(slots=True)
class BasicMessage:
    DeliveryMode = DeliveryMode
    PROPERTIES = PROPERTIES

    body = attr.ib()
    body_size = attr.ib(default=None)
    # Special attribute containing information
    # received by BasicDeliver/BasicGetOk/etc
    delivery_info = attr.ib(default=None)
    content_type = attr.ib(default='application/octet-stream')
    content_encoding = attr.ib(default='utf-8')
    headers = attr.ib(default=None)
    delivery_mode = attr.ib(default=None)
    priority = attr.ib(default=None)
    correlation_id = attr.ib(default=None)
    reply_to = attr.ib(default=None)
    expiration = attr.ib(default=None)
    message_id = attr.ib(default=None)
    timestamp = attr.ib(default=attr.Factory(datetime.datetime.utcnow))
    type = attr.ib(default=None)
    user_id = attr.ib(default=None)
    app_id = attr.ib(default=None)

    # A convenient attribute to gather all properties in a single place
    properties = attr.ib(default=None, repr=False, init=False)

    def __attrs_post_init__(self):
        self.body_size = len(self.body)

        self.properties = collections.OrderedDict((
            ('content_type', self.content_type),
            ('content_encoding', self.content_encoding),
            ('headers', self.headers),
            ('delivery_mode', self.delivery_mode),
            ('priority', self.priority),
            ('correlation_id', self.correlation_id),
            ('reply_to', self.reply_to),
            ('expiration', self.expiration),
            ('message_id', self.message_id),
            ('timestamp', self.timestamp),
            ('type', self.type),
            ('user_id', self.user_id),
            ('app_id', self.app_id),
        ))
        for key in self.properties:
            value = self.properties[key]
            if not isinstance(value, types.BaseType) and value is not None:
                value = self.PROPERTIES[key](value)
                self.properties[key] = value

        self.__dict__.update(**self.properties)

    @property
    def decoded_body(self):
        body = self.body
        if isinstance(body, bytes):
            # pylint: disable=no-member
            body = body.decode(self.content_encoding)
        return body
