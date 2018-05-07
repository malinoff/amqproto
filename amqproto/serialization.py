from io import BytesIO
from decimal import Decimal
from calendar import timegm
from datetime import datetime
from functools import singledispatch
from struct import pack, unpack, error

FRAME_METHOD = 1
FRAME_CONTENT_HEADER = 2
FRAME_CONTENT_BODY = 3
FRAME_HEARTBEAT = 8


class Reader:

    def __init__(self, data: bytes):
        self.stream = BytesIO(data)
        self.bitcount = 0
        self.bits = 0

    def read_bit(self) -> bool:
        if not self.bitcount:
            self.bits = ord(self.stream.read(1))
        self.bitcount = 8
        value = (self.bits & 1) == 1
        self.bits >>= 1
        self.bitcount -= 1
        return value

    def read_octet(self) -> int:
        self.bitcount = self.bits = 0
        return unpack('>B', self.stream.read(1))[0]

    def read_short(self) -> int:
        self.bitcount = self.bits = 0
        return unpack('>H', self.stream.read(2))[0]

    def read_long(self) -> int:
        self.bitcount = self.bits = 0
        return unpack('>I', self.stream.read(2))[0]

    def read_long_long(self) -> int:
        self.bitcount = self.bits = 0
        return unpack('>Q', self.stream.read(2))[0]

    def read_short_str(self) -> str:
        self.bitcount = self.bits = 0
        length, = unpack('>B', self.stream.read(1))
        return self.stream.read(length).decode('utf-8', 'surrogatepass')

    def read_long_str(self) -> str:
        self.bitcount = self.bits = 0
        length, = unpack('>L', self.stream.read(4))
        return self.stream.read(length).decode('utf-8', 'surrogatepass')

    def read_timestamp(self) -> datetime:
        self.bitcount = self.bits = 0
        timestamp, = unpack('>Q', self.stream.read(8))
        return datetime.utcfromtimestamp(timestamp)

    def read_table(self) -> dict:
        self.bitcount = self.bits = 0
        table = {}
        stream = self.stream
        length, = unpack('>I', stream.read(4))
        start = stream.tell()
        while stream.tell() - start < length:
            key = self.read_short_str()
            table[key] = self._read_item()
        return table

    def read_frame(self):
        frame_type = self.read_octet()
        channel_id = self.read_short()
        length = self.read_long()  # noqa: F841
        if frame_type == FRAME_METHOD:
            return channel_id, 'method', Method.read(self)
        elif frame_type == FRAME_CONTENT_HEADER:
            return channel_id, 'content_header', Content.read(self)
        elif frame_type == FRAME_CONTENT_BODY:
            return channel_id, 'content_body', self.stream.read()
        elif frame_type == FRAME_HEARTBEAT:
            return channel_id, 'heartbeat', None

    def _read_item(self):
        stream = self.stream
        kind = stream.read(1)
        if kind == b't':
            return stream.read(1) != 0
        elif kind == b'b':
            return unpack('>b', stream.read(1))[0]
        elif kind == b'B':
            return unpack('>B', stream.read(1))[0]
        elif kind == b's':
            return unpack('>h', stream.read(2))[0]
        elif kind == b'u':
            return unpack('>H', stream.read(2))[0]
        elif kind == b'I':
            return unpack('>l', stream.read(4))[0]
        elif kind == b'i':
            return unpack('>L', stream.read(4))[0]
        elif kind == b'l':
            return unpack('>q', stream.read(8))[0]
        elif kind == b'f':
            return unpack('>f', stream.read(4))[0]
        elif kind == b'd':
            return unpack('>d', stream.read(4))[0]
        elif kind == b'D':
            exponent, = unpack('>B', stream.read(1))
            value, = unpack('>l', stream.read(4))
            return Decimal(value) / Decimal(10 ** exponent)
        elif kind == b'S':
            return self.read_long_str()
        elif kind == b'A':
            array = []
            length, = unpack('>L', stream.read(4))
            start = stream.tell()
            while stream.tell() - start < length:
                array.append(self._read_item())
            return array
        elif kind == b'T':
            return self.read_timestamp()
        elif kind == b'F':
            return self.read_table()
        elif kind == b'V':
            return None
        elif kind == b'x':
            length, = unpack('>L', stream.read(4))
            return stream.read(length)


class Writer:

    def __init__(self):
        self.stream = BytesIO()
        self.bitcount = 0
        self.bits = []

    def write_bit(self, value):
        value = bool(value)
        shift = self.bitcount % 8
        if shift == 0:
            self.bits.append(0)
        self.bits[-1] |= (value << shift)
        self.bitcount += 1

    def write_octet(self, value: int):
        self._flush_bits()
        self.stream.write(pack('>B', value))

    def write_short(self, value: int):
        self._flush_bits()
        self.stream.write(pack('>H', value))

    def write_long(self, value: int):
        self._flush_bits()
        self.stream.write(pack('>I', value))

    def write_long_long(self, value: int):
        self._flush_bits()
        self.stream.write(pack('>Q', value))

    def write_short_str(self, value: str):
        self._flush_bits()
        if isinstance(value, str):
            value = value.encode('utf-8', 'surrogatepass')
        self.stream.write('>B', len(value))
        self.stream.write(value)

    def write_long_str(self, value: str):
        self._flush_bits()
        if isinstance(value, str):
            value = value.encode('utf-8', 'surrogatepass')
        self.stream.write('>I', len(value))
        self.stream.write(value)

    def write_timestamp(self, value: datetime):
        self._flush_bits()
        self.stream.write(timegm(value.utctimetuple()))

    def write_table(self, value: dict):
        self._flush_bits()
        start = self.stream.tell()
        # Prepare the space for the length
        self.stream.write(b'\x00\x00\x00\x00')
        for key, value in value.items():
            self.write_short_str(key)
            self._write_item(value)
        length = self.stream.tell() - start - 4
        # write the length
        self.stream.seek(start)
        self.stream.write(pack('>L', length))
        self.stream.seek(0, 2)

    def write_protocol_header(self, major, minor, revision):
        self.stream.write(b'AMQP\x00' + pack('>BBB', major, minor, revision))

    def write_frame_method(self, channel_id, method):
        writer = Writer()
        method.write(writer)
        payload = writer.getvalue()
        self.write_frame(FRAME_METHOD, channel_id, payload)

    def write_frame_content_header(self, channel_id, content):
        writer = Writer()
        content.write(writer)
        payload = writer.getvalue()
        self.write_frame(FRAME_CONTENT_HEADER, channel_id, payload)

    def write_frame_content_body(self, channel_id, chunk):
        self.write_frame(FRAME_CONTENT_BODY, channel_id, chunk)

    def write_frame_heartbeat(self, channel_id):
        self.write_frame(FRAME_HEARTBEAT, channel_id, b'')

    def write_frame(self, frame_type, channel_id, payload):
        self.write_octet(frame_type)
        self.write_short(channel_id)
        self.write_long(len(payload))
        self.stream.write(payload)

    def getvalue(self):
        self._flush_bits()
        return self.stream.getvalue()

    def _flush_bits(self):
        if self.bits:
            self.stream.write(pack('>B' * len(self.bits), *self.bits))
            self.bits = []

    @singledispatch
    def _write_item(self, value):
        raise ValueError('unsupported type {} of value {}'.format(
            type(value), value
        ))

    @_write_item.register(bool)
    def _write_bool(self, value):
        self.stream.write(b't' + b'\x01' if value else b'\x00')

    @_write_item.register(int)
    def _write_int(self, value):
        for kind, spec in zip(
                ('b', 'B', 's', 'u', 'I', 'i', 'l'),
                ('>B', '>b', '>h', '>H', '>l', '>L', '>q')):
            try:
                data = pack(spec, value)
                self.stream.write(kind + data)
                break
            except error:
                continue
        else:
            raise ValueError(
                'cannot pack {} into amqp integer types'.format(value)
            )

    @_write_item.register(float)
    def _write_float(self, value):
        for kind, spec in zip(('f', 'd'), ('>f', '>d')):
            try:
                data = pack(spec, value)
                self.stream.write(kind + data)
                break
            except error:
                continue
        else:
            raise ValueError(
                'cannot pack {} into amqp float types'.format(value)
            )

    @_write_item.register(Decimal)
    def _write_decimal(self, value):
        sign, digits, exponent = value.as_tuple()
        v = 0
        for digit in digits:
            v = (v * 10) + digit
        if sign:
            v = -v
        self.stream.write(pack('>cBl', b'D', -exponent, v))

    @_write_item.register(str)
    def _write_str(self, value):
        self.stream.write(b'S')
        self.write_long_str(value)

    @_write_item.register((list, tuple))
    def _write_list(self, value):
        self.stream.write(b'A')
        start = self.stream.tell()
        # Prepare the space for the length
        self.stream.write(b'\x00\x00\x00\x00')
        for item in value:
            self._write_item(item)
        length = self.stream.tell() - start - 4
        # write the length
        self.stream.seek(start)
        self.stream.write(pack('>L', length))
        self.stream.seek(0, 2)

    @_write_item.register(datetime)
    def _write_datetime(self, value):
        self.stream.write(
            timegm(value.utctimetuple())
        )

    @_write_item.register(dict)
    def _write_dict(self, value):
        self.stream.write(b'F')
        return self.write_table(value)

    @_write_item.register(type(None))
    def _write_none(self, value):
        self.stream.write(b'V')

    @_write_item.register(bytes)
    def _write_bytes(self, value):
        self.stream.write(pack('>L', len(value)))
        self.stream.write(value)


def load(fmt, reader: Reader):
    result = []
    value = None
    for char in fmt:
        if char == '?':
            value = reader.read_bit()
        elif char == 'B':
            value = reader.read_octet()
        elif char == 'H':
            value = reader.read_short()
        elif char == 'L':
            value = reader.read_long()
        elif char == 'Q':
            value = reader.read_long_long()
        elif char == 's':
            value = reader.read_short_str()
        elif char == 'S':
            value = reader.read_long_str()
        elif char == 't':
            value = reader.read_timestamp()
        elif char == 'T':
            value = reader.read_table()
        else:
            raise RuntimeError('unknown format char {!r}'.format(char))
        result.append(value)
    return result


def dump(fmt, writer: Writer, *values):
    assert len(fmt) == len(values)
    for char, value in zip(fmt, values):
        if char == '?':
            writer.write_bit(value)
        elif char == 'B':
            writer.write_octet(value)
        elif char == 'H':
            writer.write_short(value)
        elif char == 'L':
            writer.write_long(value)
        elif char == 'Q':
            writer.write_long_long(value)
        elif char == 's':
            writer.write_short_str(value)
        elif char == 'S':
            writer.write_long_str(value)
        elif char == 't':
            writer.write_timestamp(value)
        elif char == 'T':
            writer.write_table(value)
        else:
            raise RuntimeError('unknown format char {!r}'.format(char))


# Avoid circular imports
from .methods import Method  # noqa: E402
from .content import Content  # noqa: E402
