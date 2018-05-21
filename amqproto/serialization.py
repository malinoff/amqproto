from io import BytesIO
from decimal import Decimal
from calendar import timegm
from datetime import datetime
from struct import pack, unpack, error

__all__ = ['load', 'parse_protocol_header', 'parse_frames',
           'dump', 'dump_protocol_header', 'dump_frame_method',
           'dump_frame_content', 'dump_frame_heartbeat']

FRAME_METHOD = 1
FRAME_CONTENT_HEADER = 2
FRAME_CONTENT_BODY = 3
FRAME_HEARTBEAT = 8


def load(fmt: str, stream: BytesIO, _unpack=unpack):
    values = []
    bitcount = bits = 0
    read = stream.read
    for char in fmt:
        if char == '?':
            if not bitcount:
                bits = _unpack('>B', read(1))[0]
                bitcount = 8
            value = (bits & 1) == 1
            bits >>= 1
            bitcount -= 1
        else:
            bitcount = bits = 0
        if char == 'B':
            value = _unpack('>B', read(1))[0]
        elif char == 'H':
            value = _unpack('>H', read(2))[0]
        elif char == 'L':
            value = _unpack('>L', read(4))[0]
        elif char == 'Q':
            value = _unpack('>Q', read(8))[0]
        elif char == 's':
            length = _unpack('>B', read(1))[0]
            value = _unpack('>%ss' % length, read(length))[0]
            value = value.decode('utf-8', 'surrogatepass')
        elif char == 'S':
            length = _unpack('>L', read(4))[0]
            value = _unpack('>%ss' % length, read(length))[0]
            value = value.decode('utf-8', 'surrogatepass')
        elif char == 't':
            timestamp = _unpack('>Q', read(8))[0]
            value = datetime.utcfromtimestamp(timestamp)
        elif char == 'T':
            value = {}
            length = _unpack('>L', read(4))[0]
            start = stream.tell()
            while stream.tell() - start < length:
                key = load('s', stream)[0]
                value[key] = _load_item(stream)
        elif char != '?':
            raise RuntimeError('should not get there', char)
        values.append(value)
    return values


def parse_protocol_header(stream):
    prefix, *version = unpack('>5sBBB', stream.read(8))
    assert prefix == b'AMQP\x00'
    return version


def parse_frames(stream):
    while True:
        old = stream.tell()
        try:
            yield _parse_frame(stream)
        except error:
            stream.seek(old)
            break


def _parse_frame(stream):
    frame_type, channel_id, length = load('BHL', stream)  # noqa: F841
    kind = payload = None
    if frame_type == FRAME_METHOD:
        kind = 'method'
        payload = Method.load(stream)
    elif frame_type == FRAME_CONTENT_HEADER:
        kind = 'content_header'
        payload = Content.load(stream)
    elif frame_type == FRAME_CONTENT_BODY:
        kind = 'content_body'
        payload = stream.read(length)
    elif frame_type == FRAME_HEARTBEAT:
        kind = 'heartbeat'
    else:
        raise error('could not parse frame')
    end = stream.read(1)
    if end != b'\xCE':
        raise error('could not parse frame end')
    return channel_id, kind, payload


def _load_item(stream, _unpack=unpack):
    read = stream.read
    kind = read(1)
    if kind == b't':
        return _unpack('>?', read(1))[0]
    elif kind == b'b':
        return _unpack('>b', read(1))[0]
    elif kind == b'B':
        return _unpack('>B', read(1))[0]
    elif kind == b's':
        return _unpack('>h', read(2))[0]
    elif kind == b'u':
        return _unpack('>H', read(2))[0]
    elif kind == b'I':
        return _unpack('>l', read(4))[0]
    elif kind == b'i':
        return _unpack('>L', read(4))[0]
    elif kind == b'l':
        return _unpack('>q', read(8))[0]
    elif kind == b'f':
        return _unpack('>f', read(4))[0]
    elif kind == b'd':
        return _unpack('>d', read(8))[0]
    elif kind == b'D':
        exponent, value = _unpack('>Bl', read(5))
        return Decimal(value) / Decimal(10 ** exponent)
    elif kind == b'S':
        return load('S', stream)[0]
    elif kind == b'A':
        array = []
        length = _unpack('>L', stream.read(4))[0]
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
        length = _unpack('>L', read(4))[0]
        return read(length)
    else:
        raise RuntimeError('should not get there', kind)


def dump(fmt, *values, _pack=pack):
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


def dump_protocol_header(major, minor, revision):
    return pack('>5sBBB', b'AMQP\x00', major, minor, revision)


def dump_frame_method(channel_id, method):
    return _dump_frame(FRAME_METHOD, channel_id, method.dump())


def dump_frame_content(channel_id, content, max_frame_size):
    buf = bytearray()
    data = content.dump()
    buf += _dump_frame(FRAME_CONTENT_HEADER, channel_id, data)
    body = content.body
    for idx in range(0, content.body_size, max_frame_size):
        chunk = body[idx:idx + max_frame_size]
        buf += _dump_frame(FRAME_CONTENT_BODY, channel_id, chunk)
    return bytes(buf)


def dump_frame_heartbeat(channel_id):
    return _dump_frame(FRAME_HEARTBEAT, channel_id, b'')


def _dump_frame(frame_type, channel_id, data):
    return dump('BHL', frame_type, channel_id, len(data)) + data + b'\xCE'


def _dump_item(value, _pack=pack):
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


from .methods import Method
from .content import Content
