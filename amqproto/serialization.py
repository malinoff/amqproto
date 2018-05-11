from decimal import Decimal
from calendar import timegm
from datetime import datetime
from struct import pack, unpack_from, error, calcsize

from .methods import Method
from .content import Content

FRAME_METHOD = 1
FRAME_CONTENT_HEADER = 2
FRAME_CONTENT_BODY = 3
FRAME_HEARTBEAT = 8


def chunker(iterable, n):  # pylint: disable=invalid-name
    """Group data into n-sized chunks or blocks."""
    # chunker('ABCDEFG', 3) --> ['ABC', 'DEF', 'G']
    for idx in range(0, len(iterable), n):
        yield iterable[idx:idx+n]


class Reader:
    __slots__ = ('buffer', '_view', 'offset',
                 '_bitcount', '_bits', '_handlers')

    def __init__(self):
        self.buffer = bytearray()
        self._view = memoryview(self.buffer)
        self.offset = 0

        self._bitcount = 0
        self._bits = 0

        self._handlers = {
            '?': self.read_bit,
            'B': self.read_octet,
            'H': self.read_short,
            'L': self.read_long,
            'Q': self.read_long_long,
            's': self.read_short_str,
            'S': self.read_long_str,
            't': self.read_timestamp,
            'T': self.read_table,
        }

    def feed(self, data):
        del self._view
        self.buffer += data
        self._view = memoryview(self.buffer)

    def _read(self, fmt):
        size = calcsize(fmt)
        value = unpack_from(fmt, self._view, self.offset)
        self.offset += size
        return value

    def read_bit(self) -> bool:
        if not self._bitcount:
            self._bits = self._read('>B')
        self._bitcount = 8
        value = (self._bits & 1) == 1
        self._bits >>= 1
        self._bitcount -= 1
        return value

    def read_octet(self) -> int:
        self._bitcount = self._bits = 0
        return self._read('>B')[0]

    def read_short(self) -> int:
        self._bitcount = self._bits = 0
        return self._read('>H')[0]

    def read_long(self) -> int:
        self._bitcount = self._bits = 0
        return self._read('>L')[0]

    def read_long_long(self) -> int:
        self._bitcount = self._bits = 0
        return self._read('>Q')[0]

    def read_short_str(self) -> str:
        self._bitcount = self._bits = 0
        length = self._read('>B')[0]
        return self._read('>%ss' % length)[0].decode('utf-8', 'surrogatepass')

    def read_long_str(self) -> str:
        self._bitcount = self._bits = 0
        length = self._read('>L')[0]
        return self._read('>%ss' % length)[0].decode('utf-8', 'surrogatepass')

    def read_timestamp(self) -> datetime:
        self._bitcount = self._bits = 0
        timestamp = self._read('>Q')[0]
        return datetime.utcfromtimestamp(timestamp)

    def read_table(self) -> dict:
        self._bitcount = self._bits = 0
        table = {}
        length = self._read('>L')[0]
        start = self.offset
        while self.offset - start < length:
            key = self.read_short_str()
            table[key] = self._read_item()
        return table

    def read_protocol_header(self):
        prefix = self._read('>5s')[0]
        assert prefix == b'AMQP\x00'
        return self._read('>BBB')

    def read_frames(self):
        read_frame = self.read_frame
        while True:
            old = self.offset
            try:
                yield read_frame()
            except error:
                self.offset = old
                break
        del self._view
        del self.buffer[:self.offset]
        self._view = memoryview(self.buffer)
        self.offset = 0

    def read_frame(self):
        frame_type = self.read_octet()
        channel_id = self.read_short()
        length = self.read_long()  # noqa: F841
        kind = payload = None
        if frame_type == FRAME_METHOD:
            kind = 'method'
            payload = Method.read(self)
        elif frame_type == FRAME_CONTENT_HEADER:
            kind = 'content_header'
            payload = Content.read(self)
        elif frame_type == FRAME_CONTENT_BODY:
            kind = 'content_body'
            payload = self.read(length)
        elif frame_type == FRAME_HEARTBEAT:
            kind = 'heartbeat'
        end = self._read('>c')[0]
        assert end == b'\xCE'
        return channel_id, kind, payload

    def _read_item(self):
        kind = self._read('>c')[0]
        if kind == b't':
            return self._read('>B')[0] != 0
        elif kind == b'b':
            return self._read('>b')[0]
        elif kind == b'B':
            return self._read('>B')[0]
        elif kind == b's':
            return self._read('>h')[0]
        elif kind == b'u':
            return self._read('>H')[0]
        elif kind == b'I':
            return self._read('>l')[0]
        elif kind == b'i':
            return self._read('>L')[0]
        elif kind == b'l':
            return self._read('>q')[0]
        elif kind == b'f':
            return self._read('>f')[0]
        elif kind == b'd':
            return self._read('>d')[0]
        elif kind == b'D':
            exponent, value = self._read('>Bl')
            return Decimal(value) / Decimal(10 ** exponent)
        elif kind == b'S':
            return self.read_long_str()
        elif kind == b'A':
            array = []
            length = self._read('>L')[0]
            start = self.offset
            while self.offset - start < length:
                array.append(self._read_item())
            return array
        elif kind == b'T':
            return self.read_timestamp()
        elif kind == b'F':
            return self.read_table()
        elif kind == b'V':
            return None
        elif kind == b'x':
            length = self._read('>L')
            return self._read('>%ss' % length)[0]

    def load(self, fmt):
        handlers = self._handlers
        return [handlers[char]() for char in fmt]


class Writer:
    __slots__ = ('buffer', '_bitcount', '_bits', '_handlers', '_pack')

    def __init__(self):
        self.buffer = bytearray()

        self._bitcount = 0
        self._bits = []

        self._handlers = {
            '?': self.write_bit,
            'B': self.write_octet,
            'H': self.write_short,
            'L': self.write_long,
            'Q': self.write_long_long,
            's': self.write_short_str,
            'S': self.write_long_str,
            't': self.write_timestamp,
            'T': self.write_table,
        }

    def write_bit(self, value):
        value = bool(value)
        shift = self._bitcount % 8
        if shift == 0:
            self._bits.append(0)
        self._bits[-1] |= (value << shift)
        self._bitcount += 1

    def write_octet(self, value: int):
        if self._bits:
            self._flush_bits()
        self.buffer += pack('>B', value)

    def write_short(self, value: int):
        if self._bits:
            self._flush_bits()
        self.buffer += pack('>H', value)

    def write_long(self, value: int):
        if self._bits:
            self._flush_bits()
        self.buffer += pack('>L', value)

    def write_long_long(self, value: int):
        if self._bits:
            self._flush_bits()
        self.buffer += pack('>Q', value)

    def write_short_str(self, value: str):
        if self._bits:
            self._flush_bits()
        if isinstance(value, str):
            value = value.encode('utf-8', 'surrogatepass')
        self.buffer += pack('>B', len(value)) + value

    def write_long_str(self, value: str):
        if self._bits:
            self._flush_bits()
        if isinstance(value, str):
            value = value.encode('utf-8', 'surrogatepass')
        self.buffer += pack('>L', len(value)) + value

    def write_timestamp(self, value: datetime):
        if self._bits:
            self._flush_bits()
        self.buffer += timegm(value.utctimetuple())

    def write_table(self, value: dict):
        if self._bits:
            self._flush_bits()
        writer = Writer()
        for key, value in value.items():
            writer.write_short_str(key)
            writer._write_item(value)
        payload = writer.getvalue()
        self.buffer += pack('>L', len(payload)) + payload

    def write_protocol_header(self, major, minor, revision):
        self.buffer += b'AMQP\x00' + pack('>BBB', major, minor, revision)

    def write_frame_method(self, channel_id, method):
        writer = Writer()
        method.write(writer)
        payload = writer.getvalue()
        self.write_frame(FRAME_METHOD, channel_id, payload)

    def write_frame_content(self, channel_id, content, max_frame_size):
        writer = Writer()
        content.write(writer)
        payload = writer.getvalue()
        self.write_frame(FRAME_CONTENT_HEADER, channel_id, payload)
        for chunk in chunker(content.body, max_frame_size):
            self.write_frame(FRAME_CONTENT_BODY, channel_id, chunk)

    def write_frame_heartbeat(self, channel_id):
        self.write_frame(FRAME_HEARTBEAT, channel_id, b'')

    def write_frame(self, frame_type, channel_id, payload):
        self.write_octet(frame_type)
        self.write_short(channel_id)
        self.write_long(len(payload))
        self.buffer += payload + b'\xCE'

    def getvalue(self):
        if self._bits:
            self._flush_bits()
        return self.buffer

    def _flush_bits(self):
        self.buffer += pack('>%sB' % len(self._bits), *self._bits)
        self._bits = []

    def _write_item(self, value):
        if isinstance(value, bool):
            self.buffer += b't' + b'\x01' if value else b'\x00'
        elif isinstance(value, int):
            for kind, spec in zip(
                    ('b', 'B', 's', 'u', 'I', 'i', 'l'),
                    ('>B', '>b', '>h', '>H', '>l', '>L', '>q')):
                try:
                    data = pack(spec, value)
                    self.buffer += kind + data
                    break
                except error:
                    continue
            else:
                raise ValueError(
                    'cannot pack {} into amqp integer types'.format(value)
                )
        elif isinstance(value, float):
            for kind, spec in zip(('f', 'd'), ('>f', '>d')):
                try:
                    data = pack(spec, value)
                    self.buffer += kind + data
                    break
                except error:
                    continue
            else:
                raise ValueError(
                    'cannot pack {} into amqp float types'.format(value)
                )
        elif isinstance(value, Decimal):
            sign, digits, exponent = value.as_tuple()
            v = 0
            for digit in digits:
                v = (v * 10) + digit
            if sign:
                v = -v
            self.buffer += pack('>cBl', b'D', -exponent, v)
        elif isinstance(value, str):
            self.buffer += b'S'
            self.write_long_str(value)
        elif isinstance(value, (list, tuple)):
            self.buffer += b'A'
            writer = Writer()
            for item in value:
                writer._write_item(item)
            payload = writer.getvalue()
            self.buffer += pack('>L', len(payload)) + payload
        elif isinstance(value, datetime):
            self.buffer += timegm(value.utctimetuple())
        elif isinstance(value, dict):
            self.buffer += b'F'
            self.write_table(value)
        elif value is None:
            self.buffer += b'V'
        elif isinstance(value, bytes):
            self.buffer += pack('>L', len(value)) + value

    def dump(self, fmt, *values):
        handlers = self._handlers
        for char, value in zip(fmt, values):
            handlers[char](value)
