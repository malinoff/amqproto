"""
amqproto.protocol.basic
~~~~~~~~~~~~~~~~~~~~~~~

Implementation of AMQP Basic class.
"""

import enum
import datetime
import collections

from . import types

__all__ = ['Message']

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


class Message:
    """Basic message."""

    DeliveryMode = DeliveryMode
    PROPERTIES = PROPERTIES

    # pylint: disable=unused-variable,too-many-locals,redefined-builtin
    def __init__(self, body=b'', *,
                 delivery_info: dict=None,
                 body_size: int=None,
                 content_type: str='application/octet-stream',
                 content_encoding: str='utf-8',
                 headers: dict=None,
                 delivery_mode: DeliveryMode=None,
                 priority: int=None,
                 correlation_id: str=None,
                 reply_to: str=None,
                 expiration: str=None,
                 message_id: str=None,
                 timestamp: datetime.datetime=None,
                 type: str=None,
                 user_id: str=None,
                 app_id: str=None):

        if not isinstance(body, bytes):
            body = body.encode(content_encoding)
        if timestamp is None:
            timestamp = datetime.datetime.utcnow()

        # Special attribute containing information
        # received by BasicDeliver/BasicGetOk/etc
        self.delivery_info = delivery_info

        self.body = body
        if body_size is None:
            body_size = len(body)
        self.body_size = body_size

        self.properties = collections.OrderedDict((
            ('content_type', content_type),
            ('content_encoding', content_encoding),
            ('headers', headers),
            ('delivery_mode', delivery_mode),
            ('priority', priority),
            ('correlation_id', correlation_id),
            ('reply_to', reply_to),
            ('expiration', expiration),
            ('message_id', message_id),
            ('timestamp', timestamp),
            ('type', type),
            ('user_id', user_id),
            ('app_id', app_id),
        ))
        for key in self.properties:
            value = self.properties[key]
            if not isinstance(value, types.BaseType) and value is not None:
                value = self.PROPERTIES[key](value)
                self.properties[key] = value

        self.__dict__.update(**self.properties)
    # pylint: enable=unused-variable,too-many-locals

    @property
    def decoded_body(self):
        body = self.body
        if isinstance(body, bytes):
            # pylint: disable=no-member
            body = body.decode(self.content_encoding)
        return body
