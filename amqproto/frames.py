"""
amqproto.frames
~~~~~~~~~~~~~~~

AMQP frames.
"""
# pylint: disable=invalid-name
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

Method = 'Method' / c.Struct(
    'class_id' / c.Short,
    'method_id' / c.Short,
    'method' / c.Switch(lambda ctx: (ctx.class_id, ctx.method_id), cases={
        ids: method.struct
        for ids, method in methods.Method.BY_ID.items()
    })
)

ContentHeader = 'ContentHeader' / c.Struct(
    'class_id' / d.Short,
    'weight' / c.Const(d.Short, value=0),
    'body_size' / d.Longlong,
    'properties' / c.Switch(c.this.class_id, cases={
        class_id: properties.struct
        for class_id, properties in content.Properties.BY_ID.items()
    }),
)

ContentBody = 'ContentBody' / c.Struct(
    'content' / c.GreedyBytes,
)

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
        d.Long,  # payload_size
        c.Switch(c.this.frame_type, cases={
            'method': Method,
            'content_header': ContentHeader,
            'content_body': ContentBody,
            'heartbeat': Heartbeat,
        }),
    ),
    'frame_end' / c.Const(d.UnsignedByte, FRAME_END),
)
