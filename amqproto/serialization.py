from enum import Enum
from io import BytesIO
from decimal import Decimal
from calendar import timegm
from datetime import datetime
from struct import pack, unpack, error
from typing import Iterable, Tuple, Union

__all__ = ['FrameType', 'load', 'parse_protocol_header', 'parse_frames',
           'dump', 'dump_protocol_header', 'dump_frame_method',
           'dump_frame_content', 'dump_frame_heartbeat']


class FrameType(Enum):
    METHOD = 1
    CONTENT_HEADER = 2
    CONTENT_BODY = 3
    HEARTBEAT = 8


# I'd love to use typing.NamedTuple here, but we support py 3.5
# because of pypy3.
_Frame = Tuple[
    int,  # channel_id
    FrameType,
    Union['Method', 'Content', bytes],
]


class IncompleteData(Exception):
    """
    Raises when the parser can't read enough bytes from the stream.
    """


def parse_protocol_header(stream: BytesIO) -> Tuple[int, int, int]:
    """
    Parse protocol header from the stream.
    """
    prefix, *version = unpack('>5sBBB', _read(stream, 8))
    if prefix != b'AMQP\x00':
        raise ValueError("wrong protocol, expected b'AMQP\x00', got {}".format(
            prefix
        ))
    return version


def parse_frames(stream: BytesIO) -> Iterable[_Frame]:
    """
    Parse and yield frames from the stream.
    This function properly handles situations when the data is incomplete,
    and can raise an exception when parsing fails.
    """
    while True:
        old = stream.tell()
        try:
            yield _parse_frame(stream)
        except IncompleteData as exc:
            stream.seek(old)
            break


def dump_protocol_header(major: int, minor: int, revision: int) -> bytes:
    """
    Serialize a protocol header.
    """
    return pack('>5sBBB', b'AMQP\x00', major, minor, revision)


def dump_frame_method(channel_id: int, method: 'Method') -> bytes:
    """
    Serialize a method frame.
    """
    return _dump_frame(FrameType.METHOD.value, channel_id, method.dump())


def dump_frame_content(channel_id: int, content: 'Content',
                       max_frame_size: int) -> bytes:
    """
    Serialize a content (content header frame + content body frame).
    """
    buf = bytearray()
    data = content.dump()
    buf += _dump_frame(FrameType.CONTENT_HEADER.value, channel_id, data)
    body = content.body
    for idx in range(0, content.body_size, max_frame_size):
        chunk = body[idx:idx + max_frame_size]
        buf += _dump_frame(FrameType.CONTENT_BODY.value, channel_id, chunk)
    return bytes(buf)


def dump_frame_heartbeat(channel_id: int) -> bytes:
    """
    Serialize a heartbeat frame into bytes.
    """
    return _dump_frame(FrameType.HEARTBEAT.value, channel_id, b'')


def load(fmt: str, stream: BytesIO, _unpack=unpack):
    """
    Parse stream according to the provided format, this function is similar
    to ``struct.unpack``.

    Valid format chars are:
        '?' - bool
        'B' - octet
        'H' - short
        'L' - long
        'Q' - long long
        's' - short string
        'S' - long string
        't' - timestamp
        'T' - table
    """
    values = []
    bitcount = bits = 0
    for char in fmt:
        if char == '?':
            if not bitcount:
                bits = _unpack('>B', _read(stream, 1))[0]
                bitcount = 8
            value = (bits & 1) == 1
            bits >>= 1
            bitcount -= 1
        else:
            bitcount = bits = 0
        if char == 'B':
            value = _unpack('>B', _read(stream, 1))[0]
        elif char == 'H':
            value = _unpack('>H', _read(stream, 2))[0]
        elif char == 'L':
            value = _unpack('>L', _read(stream, 4))[0]
        elif char == 'Q':
            value = _unpack('>Q', _read(stream, 8))[0]
        elif char == 's':
            length = _unpack('>B', _read(stream, 1))[0]
            value = _unpack('>%ss' % length, _read(stream, length))[0]
            value = value.decode('utf-8', 'surrogatepass')
        elif char == 'S':
            length = _unpack('>L', _read(stream, 4))[0]
            value = _unpack('>%ss' % length, _read(stream, length))[0]
            value = value.decode('utf-8', 'surrogatepass')
        elif char == 't':
            timestamp = _unpack('>Q', _read(stream, 8))[0]
            value = datetime.utcfromtimestamp(timestamp)
        elif char == 'T':
            value = {}
            length = _unpack('>L', _read(stream, 4))[0]
            stream2 = BytesIO(_read(stream, length))
            while stream2.tell() < length:
                key = load('s', stream2)[0]
                value[key] = _load_item(stream2)
        elif char != '?':
            raise ValueError('wrong format char', char)
        values.append(value)
    return values


def dump(fmt: str, *values, _pack=pack) -> bytes:
    """
    Serialize values to bytes according to the provided format,
    this function is similar to ``struct.pack``.

    Valid format chars are:
        '?' - bool
        'B' - octet
        'H' - short
        'L' - long
        'Q' - long long
        's' - short string
        'S' - long string
        't' - timestamp
        'T' - table
    """
    if len(fmt) != len(values):
        raise ValueError('required to dump {} values, got {}'.format(
            len(fmt), len(values)
        ))
    buf = bytearray()
    bitcount, bits = 0, []
    for char, value in zip(fmt, values):
        data = b''
        if char == '?':
            value = bool(value)
            shift = bitcount % 8
            if shift == 0:
                bits.append(0)
            bits[-1] |= (value << shift)
            bitcount += 1
            continue
        elif bits:
            buf += _pack('>%sB' % len(bits), *bits)
            bitcount, bits = 0, []
        if char == 'B':
            data = _pack('>B', value)
        elif char == 'H':
            data = _pack('>H', value)
        elif char == 'L':
            data = _pack('>L', value)
        elif char == 'Q':
            data = _pack('>Q', value)
        elif char == 's':
            if isinstance(value, str):
                value = value.encode('utf-8', 'surrogatepass')
            data = _pack('>B', len(value)) + value
        elif char == 'S':
            if isinstance(value, str):
                value = value.encode('utf-8', 'surrogatepass')
            data = _pack('>L', len(value)) + value
        elif char == 't':
            data = _pack('>Q', timegm(value.utctimetuple()))
        elif char == 'T':
            stream2 = BytesIO()
            for key, val in value.items():
                stream2.write(dump('s', key) + _dump_item(val))
            payload = stream2.getvalue()
            data = _pack('>L', len(payload)) + payload
        elif char != '?':
            raise RuntimeError('should not get there', char)
        buf += data
    if bits:
        buf += _pack('>%sB' % len(bits), *bits)
    return buf


def _read(stream: BytesIO, length: int):
    data = stream.read(length)
    if len(data) != length:
        raise IncompleteData('tried to read {} bytes, got {} bytes'.format(
            length, len(data)
        ))
    return data


def _parse_frame(stream: BytesIO, _unpack=unpack):
    frame_type, channel_id, length = _unpack('>BHL', _read(stream, 7))
    frame_type = FrameType(frame_type)
    payload = None
    if frame_type is FrameType.METHOD:
        payload = Method.load(stream)
    elif frame_type is FrameType.CONTENT_HEADER:
        payload = Content.load(stream)
    elif frame_type is FrameType.CONTENT_BODY:
        payload = _read(stream, length)
    elif frame_type is FrameType.HEARTBEAT:
        pass
    end = _read(stream, 1)
    if end != b'\xCE':
        raise error('wrong frame end %r' % end)
    return channel_id, frame_type, payload


def _load_item(stream: BytesIO, _unpack=unpack):
    kind = _read(stream, 1)
    if kind == b't':
        return _unpack('>?', _read(stream, 1))[0]
    elif kind == b'b':
        return _unpack('>b', _read(stream, 1))[0]
    elif kind == b'B':
        return _unpack('>B', _read(stream, 1))[0]
    elif kind == b's':
        return _unpack('>h', _read(stream, 2))[0]
    elif kind == b'u':
        return _unpack('>H', _read(stream, 2))[0]
    elif kind == b'I':
        return _unpack('>l', _read(stream, 4))[0]
    elif kind == b'i':
        return _unpack('>L', _read(stream, 4))[0]
    elif kind == b'l':
        return _unpack('>q', _read(stream, 8))[0]
    elif kind == b'f':
        return _unpack('>f', _read(stream, 4))[0]
    elif kind == b'd':
        return _unpack('>d', _read(stream, 8))[0]
    elif kind == b'D':
        exponent, value = _unpack('>Bl', _read(stream, 5))
        return Decimal(value) / Decimal(10 ** exponent)
    elif kind == b'S':
        return load('S', stream)[0]
    elif kind == b'A':
        array = []
        length = _unpack('>L', _read(stream, 4))[0]
        start = stream.tell()
        while stream.tell() - start < length:
            array.append(_load_item(stream))
        return array
    elif kind == b'T':
        return load('t', stream)[0]
    elif kind == b'F':
        return load('T', stream)[0]
    elif kind == b'V':
        return None
    elif kind == b'x':
        length = _unpack('>L', _read(stream, 4))[0]
        return _read(stream, length)
    else:
        raise RuntimeError('should not get there', kind)


def _dump_frame(frame_type: FrameType, channel_id: int, data: bytes) -> bytes:
    return dump('BHL', frame_type, channel_id, len(data)) + data + b'\xCE'


def _dump_item(value, _pack=pack) -> bytes:
    if isinstance(value, bool):
        data = b't' + b'\x01' if value else b'\x00'
    elif isinstance(value, int):
        for kind, spec in zip(
                ('b', 'B', 's', 'u', 'I', 'i', 'l'),
                ('>B', '>b', '>h', '>H', '>l', '>L', '>q')):
            try:
                data = kind + _pack(spec, value)
                break
            except error:
                continue
        else:
            raise ValueError(
                'cannot _pack {} into amqp integer types'.format(value)
            )
    elif isinstance(value, float):
        for kind, spec in zip(('f', 'd'), ('>f', '>d')):
            try:
                data = kind + _pack(spec, value)
                break
            except error:
                continue
        else:
            raise ValueError(
                'cannot _pack {} into amqp float types'.format(value)
            )
    elif isinstance(value, Decimal):
        sign, digits, exponent = value.as_tuple()
        v = 0
        for digit in digits:
            v = (v * 10) + digit
        if sign:
            v = -v
        data = _pack('>cBl', b'D', -exponent, v)
    elif isinstance(value, str):
        data = b'S' + dump('S', value)
    elif isinstance(value, (list, tuple)):
        stream2 = BytesIO
        for item in value:
            stream2.write(_dump_item(item))
        payload = stream2.getvalue()
        data = b'A' + _pack('>L', len(payload)) + payload
    elif isinstance(value, datetime):
        data = b't' + dump('t', value)
    elif isinstance(value, dict):
        data = b'F' + dump('T', value)
    elif value is None:
        data = b'V'
    elif isinstance(value, bytes):
        data = _pack('>L', len(value)) + value
    else:
        raise RuntimeError('should not get there', value)
    return data


# Avoid circular imports problem.
from .methods import Method  # noqa
from .content import Content  # noqa
