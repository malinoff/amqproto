"""
amqproto.protocol.frames
~~~~~~~~~~~~~~~~~~~~~~~~

Implementation of AMQP frames.

Frames are the core of the protocol.
Each frame is bound to the specific channel
and have a payload according to the specification, 2.3.5.
"""

import io

from . import basic
from . import types
from . import methods

# Various frames related constants defined in the specification.
FRAME_METHOD = 1
FRAME_HEADER = 2
FRAME_BODY = 3
FRAME_HEARTBEAT = 8
FRAME_MIN_SIZE = 4096
FRAME_END = 206


class Frame:
    """Base class for all AMQP frames."""

    frame_type = None
    """Frame type according to the spec, subclasses must provide this."""

    payload_cls = None
    """Payload implementation, subclasses must provide this."""

    size = None
    """Frame size in bytes."""

    METADATA_SIZE = (
        # frame_type
        types.UnsignedByte._STRUCT_SIZE +
        # channel_id
        types.UnsignedShort._STRUCT_SIZE +
        # payload_size
        types.UnsignedLong._STRUCT_SIZE +
        # frame_end
        types.UnsignedByte._STRUCT_SIZE)
    """How many bytes from frame_max size are taken by the metadata:
    frame type, channel id, payload size and frame end byte.
    """

    def __init__(self, channel_id, payload):
        self.channel_id = channel_id
        self.payload = payload

        self.size = self.METADATA_SIZE + self.payload.size

    @classmethod
    def from_bytestream(cls, stream: io.BytesIO, body_chunk_size=None):
        """Instantiates a `Frame` subclass from the byte stream according to
        the specification, 2.3.5.

        Deserializing some frames requires to read an unspecified amount
        of data (possibly discovered within a previous frame), in such
        cases `body_chunk_size` parameter is useful.

        """
        frame_type = types.UnsignedByte.from_bytestream(stream)
        if frame_type == 65:
            # Not a real frame type - just b'A' from b'AMQP'
            stream.seek(stream.tell() - 1)
            return ProtocolHeaderFrame.from_bytestream(stream)

        channel_id = types.UnsignedShort.from_bytestream(stream)
        # UnsignedLong payload size + 'size' of octets makes ByteArray useful
        payload_bytes = types.ByteArray.from_bytestream(stream)
        end = types.UnsignedByte.from_bytestream(stream)
        # Fail fast if `end` is not what we expect
        if end != FRAME_END:
            raise ValueError('Frame end, got {!r} expected {!r}'.format(
                end, FRAME_END
            ))

        frame_cls = FRAMES[frame_type]
        payload_stream = io.BytesIO(payload_bytes)
        payload = frame_cls.payload_cls.from_bytestream(
            payload_stream, body_chunk_size=body_chunk_size
        )

        return frame_cls(channel_id, payload)

    def to_bytestream(self, stream: io.BytesIO):
        """Serializes the frame into the byte stream according to the
        specification, 2.3.5."""
        buf = io.BytesIO()
        self.payload.to_bytestream(buf)

        types.UnsignedByte(self.frame_type).to_bytestream(stream)
        types.UnsignedShort(self.channel_id).to_bytestream(stream)
        types.UnsignedLong(buf.tell()).to_bytestream(stream)
        stream.write(buf.getvalue())
        types.UnsignedByte(FRAME_END).to_bytestream(stream)

    def __eq__(self, other):
        return (self.size == other.size and
                self.channel_id == other.channel_id and
                self.payload == other.payload)


class Payload:
    """Base class for all payload classes."""

    size = None
    """Payload size in bytes. Subclasses must provide this attribute."""

    @classmethod
    def from_bytestream(cls, stream: io.BytesIO, body_chunk_size=None):
        """Deserialize the payload from the byte stream."""
        raise NotImplementedError

    def to_bytestream(self, stream: io.BytesIO):
        """Serialize the payload to the byte stream."""
        raise NotImplementedError

    def __eq__(self, other):
        raise NotImplementedError


class ContentHeaderPayload(Payload):
    """Specification 4.2.6.1.

    0          2        4           12               14.
    +----------+--------+-----------+----------------+------------- - -
    | class-id | weight | body size | property flags | property list...
    +----------+--------+-----------+----------------+------------- - -
        short    short    long long       short          remainder...

    """

    def __init__(self, class_id, body_size, properties, weight=0):
        self.class_id = class_id
        self.body_size = body_size
        self.weight = weight

        class_properties = PROPERTIES_BY_CLASS_ID[self.class_id]
        for name, value in properties.items():
            if value is not None:
                if not isinstance(value, types.BaseType):
                    amqptype = class_properties[name]
                    properties[name] = amqptype(value)
        self.properties = properties

        properties_size = sum(value.size
                              for value in self.properties.values()
                              if value is not None)
        self.size = (types.UnsignedShort._STRUCT_SIZE +  # class_id
                     types.UnsignedShort._STRUCT_SIZE +  # weight
                     types.UnsignedLongLong._STRUCT_SIZE +  # body_size
                     types.UnsignedShort._STRUCT_SIZE +  # property_flags
                     properties_size)

    def __eq__(self, other):
        return (self.size == other.size and
                self.class_id == other.class_id and
                self.body_size == other.body_size and
                self.weight == other.weigth and
                self.properties == other.properties)

    @classmethod
    def from_bytestream(cls, stream: io.BytesIO, body_chunk_size=None):
        class_id = types.UnsignedShort.from_bytestream(stream)
        weight = types.UnsignedShort.from_bytestream(stream)
        assert weight == 0
        body_size = types.UnsignedLongLong.from_bytestream(stream)
        property_flags = types.UnsignedShort.from_bytestream(stream)

        class_properties = PROPERTIES_BY_CLASS_ID[class_id]
        number_of_properties = 15

        properties = {}

        for i, (name, amqptype) in enumerate(class_properties.items()):
            pos = number_of_properties - i
            value = None
            if property_flags & (1 << pos):
                value = amqptype.from_bytestream(stream)
            properties[name] = value

        return cls(class_id, body_size, properties, weight)

    def to_bytestream(self, stream: io.BytesIO):
        class_properties = PROPERTIES_BY_CLASS_ID[self.class_id]

        types.UnsignedShort(self.class_id).to_bytestream(stream)
        types.UnsignedShort(self.weight).to_bytestream(stream)
        types.UnsignedLongLong(self.body_size).to_bytestream(stream)

        properties = bytearray()
        property_flags = 0
        bitshift = 15

        for name, value in self.properties.items():
            if value is not None:
                property_flags |= (1 << bitshift)
                properties += value.pack()
            bitshift -= 1

        types.UnsignedShort(property_flags).to_bytestream(stream)
        stream.write(properties)


class ContentBodyPayload(Payload):
    """Specification 4.2.6.2.

    +-----------------------+ +-----------+
    | Opaque binary payload | | frame-end |
    +-----------------------+ +-----------+

    """

    def __init__(self, data):
        self.data = data

        self.size = len(data)

    def __eq__(self, other):
        return self.size == other.size and self.data == other.data

    @classmethod
    def from_bytestream(cls, stream, body_chunk_size):
        return cls(stream.read(body_chunk_size))

    def to_bytestream(self, stream):
        stream.write(self.data)


class HeartbeatPayload(Payload):
    """Specification 4.2.7.

    0     1     2     3           4.
    +-----+-----+-----+-----------+
    | 0x8 | 0x0 | 0x0 | frame-end |
    +-----+-----+-----+-----------+

    """

    def __init__(self):
        self.data = b''

        self.size = 0

    def __eq__(self, other):
        return self.size == other.size and self.data == other.data

    @classmethod
    def from_bytestream(cls, stream: io.BytesIO, body_chunk_size=None):
        return cls()

    def to_bytestream(self, stream: io.BytesIO):
        types.ByteArray(self.data).to_bytestream(stream)


class ProtocolHeaderPayload(Payload):
    """Specification 4.2.2.

    0   1   2   3   4   5   6   7   8.
    +---+---+---+---+---+---+---+---+
    |'A'|'M'|'Q'|'P'| 0 | 0 | 9 | 1 |
    +---+---+---+---+---+---+---+---+

    """

    PREFIX = b'AMQP\x00'

    def __init__(self,
                 protocol_major=0,
                 protocol_minor=9,
                 protocol_revision=1):
        self.protocol_major = protocol_major
        self.protocol_minor = protocol_minor
        self.protocol_revision = protocol_revision

        self.size = 8

    def __eq__(self, other):
        return all(
            getattr(self, attr) == getattr(other, attr)
            for attr in
            ['protocol_major', 'protocol_minor', 'protocol_revision']
        )

    @classmethod
    def from_bytestream(cls, stream: io.BytesIO, body_chunk_size=None):
        protocol = stream.read(5)
        if protocol != b'AMQP\x00':
            raise TypeError
        protocol_major = types.UnsignedByte.from_bytestream(stream)
        protocol_minor = types.UnsignedByte.from_bytestream(stream)
        protocol_revision = types.UnsignedByte.from_bytestream(stream)
        return cls(protocol_major, protocol_minor, protocol_revision)

    def to_bytestream(self, stream: io.BytesIO):
        stream.write(self.PREFIX)
        types.UnsignedByte(self.protocol_major).to_bytestream(stream)
        types.UnsignedByte(self.protocol_minor).to_bytestream(stream)
        types.UnsignedByte(self.protocol_revision).to_bytestream(stream)


class MethodFrame(Frame):
    """Method frames carry the high-level protocol commands (which are called
    "methods"). One method frame carries one command.

    Specification 2.3.5.1, 4.2.4.

    """

    frame_type = FRAME_METHOD
    payload_cls = methods.Method


class ContentHeaderFrame(Frame):
    """Content is the application data we carry from client-to-client via the
    AMQP server. Content is, roughly speaking, a set of properties plus a
    binary data part. The set of allowed properties are defined by the Basic
    class, and these form the "content header frame". The data can be any size,
    and MAY be broken into several (or many) chunks, each forming a "content
    body frame".

    Speficiation 2.3.5.2, 4.2.6.

    """

    frame_type = FRAME_HEADER
    payload_cls = ContentHeaderPayload


class ContentBodyFrame(Frame):
    """The content body payload is an opaque binary block followed by a frame
    end octet. The content body can be split into as many frames as needed. The
    maximum size of the frame payload is agreed upon by both peers during
    connection negotiation.

    Specification 2.3.5.2, 4.2.6.2.

    """

    frame_type = FRAME_BODY
    payload_cls = ContentBodyPayload


class HeartbeatFrame(Frame):
    """Heartbeat frames tell the recipient that the sender is still alive. The
    rate and timing of heartbeat frames is negotiated during connection tuning.

    Specification 2.3.5.3, 4.2.7.

    """

    frame_type = FRAME_HEARTBEAT
    payload_cls = HeartbeatPayload
    expected_channel_id = 0

    def __init__(self, channel_id, payload):
        assert channel_id == self.expected_channel_id
        super().__init__(channel_id, payload)


class ProtocolHeaderFrame(Frame):
    """This is actually not a frame according to the protocol - just bytes,
    but it's quite handy to pretend it is a frame to unify
    `input bytes -> frames -> output bytes` sequence.

    Specification 4.2.2.
    """

    payload_cls = ProtocolHeaderPayload

    @classmethod
    def from_bytestream(cls, stream: io.BytesIO, body_chunk_size=None):
        payload = cls.payload_cls.from_bytestream(stream, body_chunk_size)
        return cls(None, payload)

    def to_bytestream(self, stream: io.BytesIO):
        self.payload.to_bytestream(stream)


# pylint: disable=no-member
FRAMES = {cls.frame_type: cls for cls in Frame.__subclasses__()}

# Just in case somebody will want to add non-basic stuff...
PROPERTIES_BY_CLASS_ID = {
    60: basic.PROPERTIES,
}

__all__ = list(locals())
