"""
amqproto.frames
~~~~~~~~~~~~~~~

AMQP frames.
"""

# pylint: disable=invalid-name

import inspect

import attr
import construct as c

from . import methods
from . import content
from . import domains as d

# Constants
FRAME_METHOD = 1
FRAME_HEADER = 2
FRAME_BODY = 3
FRAME_HEARTBEAT = 8
FRAME_END = 206


def _decode_method(obj, _):
    cls = methods.Method.BY_ID[(obj.class_id, obj.method_id)]
    sig = inspect.signature(cls)
    return cls(**{key: value for key, value in obj.items()
                  if key in sig.parameters})


Method = 'Method' / c.ExprAdapter(
    subcon=c.Struct(
        'class_id' / c.Short,
        'method_id' / c.Short,
        'method' / c.Embedded(
            c.Switch(lambda ctx: (ctx.class_id, ctx.method_id), cases={
                ids: method.struct
                for ids, method in methods.Method.BY_ID.items()
            })
        )
    ),
    encoder=lambda obj, ctx: attr.asdict(obj),
    decoder=_decode_method,
)

ContentHeader = 'ContentHeader' / c.ExprAdapter(
    subcon=c.Struct(
        'class_id' / d.Short,
        'weight' / c.Const(d.Short, value=0),
        'body_size' / d.Longlong,
        'properties' / c.Switch(c.this.class_id, cases={
            class_id: properties.struct
            for class_id, properties in content.Properties.BY_ID.items()
        })
    ),
    encoder=lambda obj, ctx: {
        'class_id': obj.class_id,
        'body_size': obj.body_size,
        'properties': attr.asdict(obj.properties),
    },
    decoder=lambda obj, ctx: content.Content.BY_ID[obj.class_id](
        body=b'', body_size=obj.body_size, properties=obj.properties,
    ),
)

ContentBody = 'ContentBody' / c.GreedyBytes

Heartbeat = 'Heartbeat' / c.Pass

ProtocolHeader = 'ProtocolHeader' / c.Struct(
    'frame_type' / c.Computed('protocol_header'),
    'prefix' / c.Const(c.Bytes(5), b'AMQP\x00'),
    'payload' / c.Struct(
        'protocol_major' / c.Byte,
        'protocol_minor' / c.Byte,
        'protocol_revision' / c.Byte,
    )
)

Frame = 'Frame' / c.Struct(
    'frame_type' / c.Enum(
        d.UnsignedByte,
        method=FRAME_METHOD,
        content_header=FRAME_HEADER,
        content_body=FRAME_BODY,
        heartbeat=FRAME_HEARTBEAT,
    ),
    'channel_id' / d.UnsignedShort,
    'payload' / c.Prefixed(
        d.UnsignedLong,  # payload_size
        c.Switch(c.this.frame_type, cases={
            'method': Method,
            'content_header': ContentHeader,
            'content_body': ContentBody,
            'heartbeat': Heartbeat,
        }),
    ),
    'frame_end' / c.Const(d.UnsignedByte, FRAME_END),
)
