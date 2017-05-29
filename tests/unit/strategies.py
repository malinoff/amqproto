import functools

import hypothesis.strategies as hs
import hypothesis.extra.datetime as hed

from amqproto.protocol import errors
import amqproto.protocol.types as at
import amqproto.protocol.methods as am

utf8encode = functools.partial(str.encode, encoding='utf-8')


def validate(cls):
    def inner(value):
        try:
            cls(value)
            return True
        except ValueError:
            return False
    return inner


def st(strategy, cls):
    return strategy.filter(validate(cls))


bools = hs.booleans()
signed_bytes = hs.integers(at.SignedByte.MIN, at.SignedByte.MAX)
not_signed_bytes = hs.integers(min_value=at.SignedByte.MAX + 1) | \
    hs.integers(max_value=at.SignedByte.MIN - 1)

unsigned_bytes = hs.integers(at.UnsignedByte.MIN, at.UnsignedByte.MAX)
not_unsigned_bytes = hs.integers(min_value=at.UnsignedByte.MAX + 1) | \
    hs.integers(max_value=at.UnsignedByte.MIN - 1)

signed_shorts = hs.integers(at.SignedShort.MIN, at.SignedShort.MAX)
not_signed_shorts = hs.integers(min_value=at.SignedShort.MAX + 1) | \
    hs.integers(max_value=at.SignedShort.MIN - 1)

unsigned_shorts = hs.integers(at.UnsignedShort.MIN, at.UnsignedShort.MAX)
not_unsigned_shorts = hs.integers(min_value=at.UnsignedShort.MAX + 1) | \
    hs.integers(max_value=at.UnsignedShort.MIN - 1)

signed_longs = hs.integers(at.SignedLong.MIN, at.SignedLong.MAX)
not_signed_longs = hs.integers(min_value=at.SignedLong.MAX + 1) | \
    hs.integers(max_value=at.SignedLong.MIN - 1)

unsigned_longs = hs.integers(at.UnsignedLong.MIN, at.UnsignedLong.MAX)
not_unsigned_longs = hs.integers(min_value=at.UnsignedLong.MAX + 1) | \
    hs.integers(max_value=at.UnsignedLong.MIN - 1)

signed_long_longs = hs.integers(at.SignedLongLong.MIN, at.SignedLongLong.MAX)
not_signed_long_longs = hs.integers(min_value=at.SignedLongLong.MAX + 1) | \
    hs.integers(max_value=at.SignedLongLong.MIN - 1)

unsigned_long_longs = hs.integers(at.UnsignedLongLong.MIN,
                                  at.UnsignedLongLong.MAX)
not_unsigned_long_longs = hs.integers(min_value=at.UnsignedLongLong.MAX + 1) | \
    hs.integers(max_value=at.UnsignedLongLong.MIN - 1)

floats = st(hs.floats(), at.Float)
doubles = st(hs.floats(), at.Double)
decimals = st(hs.decimals(), at.Decimal)
short_strs = st(hs.text().map(utf8encode), at.ShortStr)
long_strs = st(hs.text().map(utf8encode), at.LongStr)
voids = hs.just(None)
bytearrays = st(hs.binary(), at.ByteArray)
timestamps = st(hed.datetimes(timezones=[]), at.Timestamp)

types = (bools | signed_bytes | unsigned_bytes |
         signed_shorts | unsigned_shorts |
         signed_longs | unsigned_longs |
         signed_long_longs | floats | doubles | decimals | long_strs | voids |
         bytearrays | timestamps)

tables = hs.dictionaries(short_strs, hs.recursive(
    types, lambda c: c | hs.dictionaries(short_strs, c) | hs.lists(c)
))

arrays = hs.lists(hs.recursive(
    types, lambda c: c | hs.dictionaries(short_strs, c) | hs.lists(c)
))


type_to_strategy = {
    at.Bool: bools,
    at.SignedByte: signed_bytes,
    at.UnsignedByte: unsigned_bytes,
    at.SignedShort: signed_shorts,
    at.UnsignedShort: unsigned_shorts,
    at.SignedLong: signed_longs,
    at.UnsignedLong: unsigned_longs,
    at.SignedLongLong: signed_long_longs,
    at.UnsignedLongLong: unsigned_long_longs,
    at.Shortstr: short_strs,
    at.Longstr:  long_strs,
    at.Float: floats,
    at.Double: doubles,
    at.Decimal: decimals,
    at.Void: voids,
    at.Timestamp: timestamps,
    at.ByteArray: bytearrays,
    at.Table: tables,
    at.Array: arrays,
}

ERRORS_CLASSES = errors.SoftError.__subclasses__() + errors.HardError.__subclasses__()


@hs.composite
def methods(draw, method_cls):
    close_method = method_cls in (am.ConnectionClose, am.ChannelClose)
    kwargs = {}
    for name, amqptype in method_cls.field_info:
        if name.startswith('reserved'):
            continue
        if name == 'global':
            name = 'global_'

        if close_method and name == 'reply_code':
            kwargs[name] = draw(hs.sampled_from([cls.reply_code for cls in ERRORS_CLASSES]))
        else:
            kwargs[name] = draw(type_to_strategy[amqptype])
    return method_cls(**kwargs)


def draw_method_example(method_cls):
    method = methods(method_cls).example()
    args = {}
    for name, _ in method.field_info:
        if name.startswith('reserved'):
            continue
        if name == 'global':
            name = 'global_'

        args[name] = getattr(method, name)
    return method, args
