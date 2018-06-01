from io import BytesIO
from struct import error
from datetime import datetime

import pytest

from amqproto import methods
from amqproto.serialization import (
    FrameType,
    parse_protocol_header, dump_protocol_header,
    parse_frames, dump_frame_method, dump_frame_content, dump_frame_heartbeat,
    load, dump, IncompleteData,
)


class TestProtocolHeader:

    def test_parse_good(self):
        version = parse_protocol_header(BytesIO(b'AMQP\x00\xff\x00\x10'))
        assert version == [255, 0, 16]

    def test_parse_bad(self):
        with pytest.raises(ValueError) as exc:
            parse_protocol_header(BytesIO(b'NOT A AMQP'))
        err = "ValueError: wrong protocol, expected b'AMQP\x00', got b'NOT A'"
        assert err in str(exc)

    def test_dump_good(self):
        data = dump_protocol_header(0, 10, 255)
        assert data == b'AMQP\x00\x00\x0a\xff'

    def test_dump_bad(self):
        with pytest.raises(error):
            dump_protocol_header(256, 256, 256)


class TestFrameMethod:

    @pytest.fixture(scope='session')
    def method_bytes(self):
        return b''.join([
            b'\x01',  # frame type
            b'\x00\x00',  # channel_id
            b'\x00\x00\x00\x12',  # length
            b''.join([  # ConnectionStart method
                b'\x00\x0a',  # class_id
                b'\x00\x0a',  # method_id
                b'\x00',  # version_major
                b'\x09',  # version_minor
                b'\x00\x00\x00\x00',  # server properties
                b'\x00\x00\x00\x00',  # mechanisms
                b'\x00\x00\x00\x00',  # locales
            ]),
            b'\xce',
        ])

    def test_parse_good(self, method_bytes):
        frames = list(parse_frames(BytesIO(method_bytes)))
        assert len(frames) == 1
        channel_id, frame_type, method = frames[0]
        assert channel_id == 0
        assert frame_type is FrameType.METHOD
        assert method == methods.ConnectionStart(
            version_major=0,
            version_minor=9,
            server_properties={},
            mechanisms='',
            locales='',
        )


@pytest.mark.parametrize('fmt,data,expected', [
    # bool
    ('?', b'\x00', [False]),
    ('?', b'\x01', [True]),
    ('?', b'', IncompleteData('tried to read 1 bytes, got 0 bytes')),

    # octet
    ('B', b'\x00', [0]),
    ('B', b'\x10', [0x10]),
    ('B', b'\xff', [0xff]),
    ('B', b'', IncompleteData('tried to read 1 bytes, got 0 bytes')),

    # short
    ('H', b'\x00\x00', [0]),
    ('H', b'\x12\x34', [0x1234]),
    ('H', b'\xff\xff', [0xffff]),
    ('H', b'', IncompleteData('tried to read 2 bytes, got 0 bytes')),
    ('H', b'\x00', IncompleteData('tried to read 2 bytes, got 1 bytes')),

    # long
    ('L', b'\x00\x00\x00\x00', [0]),
    ('L', b'\x11\x22\x33\x44', [0x11223344]),
    ('L', b'\xff\xff\xff\xff', [0xffffffff]),
    ('L', b'', IncompleteData('tried to read 4 bytes, got 0 bytes')),
    ('L', b'\x00', IncompleteData('tried to read 4 bytes, got 1 bytes')),
    ('L', b'\x00\x00\x00',
        IncompleteData('tried to read 4 bytes, got 3 bytes')),

    # long long
    ('Q', b'\x00\x00\x00\x00\x00\x00\x00\x00', [0]),
    ('Q', b'\x11\x22\x33\x44\x55\x66\x77\x88', [0x1122334455667788]),
    ('Q', b'\xff\xff\xff\xff\xff\xff\xff\xff', [0xffffffffffffffff]),
    ('Q', b'', IncompleteData('tried to read 8 bytes, got 0 bytes')),
    ('Q', b'\x00\x01', IncompleteData('tried to read 8 bytes, got 2 bytes')),
    ('Q', b'\x00\x00\x00\x01\x01\x01',
        IncompleteData('tried to read 8 bytes, got 6 bytes')),

    # short str
    ('s', b'\x00', ['']),
    ('s', b'\x03foo', ['foo']),
    ('s', b'\x08\xd0\x9e\xd0\xbc\xd1\x81\xd0\xba', ['Омск']),
    ('s', b'', IncompleteData('tried to read 1 bytes, got 0 bytes')),
    ('s', b'\x04bar', IncompleteData('tried to read 4 bytes, got 3 bytes')),

    # long str
    ('S', b'\x00\x00\x00\x00', ['']),
    ('S', b'\x00\x00\x00\x03foo', ['foo']),
    ('S', b'\x00\x00\x00\x08\xd0\x9e\xd0\xbc\xd1\x81\xd0\xba', ['Омск']),
    ('S', b'', IncompleteData('tried to read 4 bytes, got 0 bytes')),
    ('S', b'\x00\x00\x00\x04bar',
        IncompleteData('tried to read 4 bytes, got 3 bytes')),

    # timestamp
    ('t', b'\x00\x00\x00\x00\x00\x00\x00\x00', [datetime(1970, 1, 1)]),
    ('t', b'\x00\x00\x00\x00\x2d\x38\x83\x80', [datetime(1994, 1, 16)]),
    ('t', b'\x00\x00\x00\x00',
        IncompleteData('tried to read 8 bytes, got 4 bytes')),
    ('t', b'\xff\xff\xff\xff\xff\xff\xff\xff',
        OverflowError('timestamp out of range for platform time_t')),

    # table
    ('T', b'\x00\x00\x00\x00', [{}]),
    ('T', b'\x00\x00\x00\x12\x03foot\x01\x03barS\x00\x00\x00\x03baz',
        [{'foo': True, 'bar': 'baz'}]),
    ('T', b'', IncompleteData('tried to read 4 bytes, got 0 bytes')),
    ('T', b'\x00\x00\x00\x11\x03foo',
        IncompleteData('tried to read 17 bytes, got 4 bytes')),

    # multiple values
    ('?BHtSTssQL',
     b''.join([
         b'\x00', b'\x00', b'\x00\x00', b'\x00\x00\x00\x00\x00\x00\x00\x00',
         b'\x00\x00\x00\x00', b'\x00\x00\x00\x00', b'\x00', b'\x00',
         b'\x00\x00\x00\x00\x00\x00\x00\x00', b'\x00\x00\x00\x00',
     ]),
     [False, 0, 0, datetime(1970, 1, 1), '', {}, '', '', 0, 0]),
])
def test_load(fmt, data, expected):
    stream = BytesIO(data)
    if isinstance(expected, Exception):
        with pytest.raises(expected.__class__) as exc:
            load(fmt, stream)
        assert str(expected) in str(exc)
    else:
        result = load(fmt, stream)
        assert result == expected


@pytest.mark.parametrize('fmt,values,expected', [
    # bool
    ('?', [False], b'\x00'),
    ('?', [True], b'\x01'),
    ('?', [], ValueError('required to dump 1 values, got 0')),

    # octet
    ('B', [0], b'\x00'),
    ('B', [0x10], b'\x10'),
    ('B', [0xff], b'\xff'),
    ('B', [], ValueError('required to dump 1 values, got 0')),

    # short
    ('H', [0], b'\x00\x00'),
    ('H', [0x1234], b'\x12\x34'),
    ('H', [0xffff], b'\xff\xff'),

    # long
    ('L', [0], b'\x00\x00\x00\x00'),
    ('L', [0x11223344], b'\x11\x22\x33\x44'),
    ('L',  [0xffffffff], b'\xff\xff\xff\xff'),

    # long long
    ('Q', [0], b'\x00\x00\x00\x00\x00\x00\x00\x00'),
    ('Q', [0x1122334455667788], b'\x11\x22\x33\x44\x55\x66\x77\x88'),
    ('Q', [0xffffffffffffffff], b'\xff\xff\xff\xff\xff\xff\xff\xff'),

    # short str
    ('s', [''], b'\x00'),
    ('s', ['foo'], b'\x03foo'),
    ('s', ['Омск'], b'\x08\xd0\x9e\xd0\xbc\xd1\x81\xd0\xba'),

    # long str
    ('S', [''], b'\x00\x00\x00\x00'),
    ('S', ['foo'], b'\x00\x00\x00\x03foo'),
    ('S', ['Омск'], b'\x00\x00\x00\x08\xd0\x9e\xd0\xbc\xd1\x81\xd0\xba'),

    # timestamp
    ('t', [datetime(1970, 1, 1)], b'\x00\x00\x00\x00\x00\x00\x00\x00'),
    ('t', [datetime(1994, 1, 16)], b'\x00\x00\x00\x00\x2d\x38\x83\x80'),

    # table
    ('T', [{}], b'\x00\x00\x00\x00'),
    ('T', [{'foo': True, 'bar': 'baz'}],
        b'\x00\x00\x00\x12\x03foot\x01\x03barS\x00\x00\x00\x03baz'),

    # multiple values
    ('?BHtSTssQL',
     [False, 0, 0, datetime(1970, 1, 1), '', {}, '', '', 0, 0],
     b''.join([
         b'\x00', b'\x00', b'\x00\x00', b'\x00\x00\x00\x00\x00\x00\x00\x00',
         b'\x00\x00\x00\x00', b'\x00\x00\x00\x00', b'\x00', b'\x00',
         b'\x00\x00\x00\x00\x00\x00\x00\x00', b'\x00\x00\x00\x00',
     ])),
])
def test_dump(fmt, values, expected):
    if isinstance(expected, Exception):
        with pytest.raises(expected.__class__) as exc:
            dump(fmt, *values)
        assert str(expected) in str(exc)
    else:
        result = dump(fmt, *values)
        assert result == expected
