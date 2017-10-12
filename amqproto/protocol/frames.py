import itertools

import construct as c

from . import methods
from . import domains as d

# Constants
FRAME_METHOD = 1
FRAME_HEADER = 2
FRAME_BODY = 3
FRAME_HEARTBEAT = 8
FRAME_MIN_SIZE = 4096
FRAME_END = 206


def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return itertools.zip_longest(*args, fillvalue=fillvalue)


class Properties(c.Construct):
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
        self.flags = c.Struct(*flags)

        def is_bit(subcon):
            while True:
                if isinstance(subcon, c.BitsInteger) and subcon.length == 1:
                    return True
                subcon = getattr(subcon, 'subcon', None)
                if subcon is None:
                    break
            return False

        self.subcons = [subcon for subcon in subcons if not is_bit(subcon)]

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


BasicProperties = 'BasicProperties' / Properties(
    'content_type' / d.Shortstr,
    'content_encoding' / d.Shortstr,
    'headers' / d.Table,
    'delivery_mode' / c.Enum(d.Octet, transient=1, persistent=2),
    'priority' / d.Octet,
    'correlation_id' / d.Shortstr,
    'reply_to' / d.Shortstr,
    'expiration' / d.Shortstr,
    'message_id' / d.Shortstr,
    'timestamp' / d.Timestamp,
    'type' / d.Shortstr,
    'user_id' / d.Shortstr,
    'app_id' / d.Shortstr,
    # Deprecated
    'cluster_id' / c.Const(d.Shortstr, ''),
)

ContentHeaderPayload = 'ContentHeaderPayload' / c.Struct(
    'class_id' / d.Short,
    'weight' / c.Const(d.Short, value=0),
    'body_size' / d.Longlong,
    'properties' / c.Switch(c.this.class_id, cases={
        60: BasicProperties,
    }, default=Properties()),
)

ContentBodyPayload = 'ContentBodyPayload' / c.Struct(
    'content' / c.GreedyBytes,
)

MethodPayload = 'MethodPayload' / c.Struct(
    'class_id' / d.Short,
    'method_id' / d.Short,
    'arguments' / c.Embedded(c.Switch(
        lambda ctx: (ctx.class_id, ctx.method_id),
        cases=methods.IDS_TO_METHODS,
    ))
)

HeartbeatPayload = 'HeartbeatPayload' / c.Struct()

Frame = 'Frame' / c.Struct(
    'frame_type' / c.Enum(
        d.UnsignedByte,
        method=FRAME_METHOD,
        header=FRAME_HEADER,
        body=FRAME_BODY,
        heartbeat=FRAME_HEARTBEAT,
    ),
    'channel_id' / d.UnsignedShort,
    'payload' / c.Prefixed(
        d.Longlong,  # payload_size
        c.Switch(c.this.frame_type, cases={
            'method': MethodPayload,
            'header': ContentHeaderPayload,
            'body': ContentBodyPayload,
            'heartbeat': HeartbeatPayload,
        }),
    ),
    'frame_end' / c.Const(d.UnsignedByte, FRAME_END),
)
