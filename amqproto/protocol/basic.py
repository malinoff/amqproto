"""
amqproto.protocol.basic
~~~~~~~~~~~~~~~~~~~~~~~

Implementation of AMQP Basic class.
"""

import enum
import datetime
import functools
import collections

import attr

from . import types

__all__ = ['BasicMessage', 'DeliveryMode']

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


@functools.singledispatch
def _py_type_to_amqp_type(value):
    return value


@_py_type_to_amqp_type.register(str)
@_py_type_to_amqp_type.register(bytes)
def _py_type_to_amqp_str_bytes(value):
    return types.Shortstr(value)


@_py_type_to_amqp_type.register(int)
def _py_type_to_amqp_int(value):
    return types.Octet(value)


@_py_type_to_amqp_type.register(dict)
def _py_type_to_amqp_dict(value):
    return types.Table(value)


@_py_type_to_amqp_type.register(datetime.datetime)
def _py_type_to_amqp_datetime(value):
    return types.Timestamp(value)


@_py_type_to_amqp_type.register(type(None))
def _py_type_to_amqp_none(value):
    return None


@_py_type_to_amqp_type.register(enum.Enum)
def _py_type_to_amqp_enum(value):
    return _py_type_to_amqp_type(value.value)


class DeliveryMode(enum.Enum):
    """Delivery modes for Basic message."""
    NonPersistent = 1
    Persistent = 2


@attr.s(slots=True)
class BasicMessage:
    body = attr.ib(default=b'')
    body_size = attr.ib(default=None)
    # Special attribute containing information
    # received by BasicDeliver/BasicGetOk/etc
    delivery_info = attr.ib(default=None, convert=_py_type_to_amqp_type)
    # Basic properties
    content_type = attr.ib(default='application/octet-stream',
                           convert=_py_type_to_amqp_type)
    content_encoding = attr.ib(default='utf-8',
                               convert=_py_type_to_amqp_type)
    headers = attr.ib(default=None, convert=_py_type_to_amqp_type)
    delivery_mode = attr.ib(default=None, convert=_py_type_to_amqp_type)
    priority = attr.ib(default=None, convert=_py_type_to_amqp_type)
    correlation_id = attr.ib(default=None, convert=_py_type_to_amqp_type)
    reply_to = attr.ib(default=None, convert=_py_type_to_amqp_type)
    expiration = attr.ib(default=None, convert=_py_type_to_amqp_type)
    message_id = attr.ib(default=None, convert=_py_type_to_amqp_type)
    timestamp = attr.ib(default=attr.Factory(datetime.datetime.utcnow),
                        convert=_py_type_to_amqp_type)
    type = attr.ib(default=None, convert=_py_type_to_amqp_type)
    user_id = attr.ib(default=None, convert=_py_type_to_amqp_type)
    app_id = attr.ib(default=None, convert=_py_type_to_amqp_type)

    # A convenient attribute to gather all properties in a single place
    @property
    def properties(self):
        def basic_property(attrib, value):
            return attrib.name not in ['body', 'body_size', 'delivery_info']
        return attr.asdict(
            self, filter=basic_property, dict_factory=collections.OrderedDict
        )

    def __attrs_post_init__(self):
        if isinstance(self.body, str):
            _py_encoding = self.content_encoding.decode('utf-8')
            self.body = self.body.encode(_py_encoding)
        self.body_size = len(self.body)

    @property
    def decoded_body(self):
        body = self.body
        if isinstance(body, bytes):
            _py_encoding = self.content_encoding.decode('utf-8')
            body = body.decode(_py_encoding)
        return body
