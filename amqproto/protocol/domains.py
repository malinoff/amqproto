"""
amqproto.protocol.domains
~~~~~~~~~~~~~~~~~~~~~~~~~

Implementation of AMQP domains (types).

Instances of type classes behave like standard python types. For example,
all numeric types can be compared with built-in numeric types like
`int`, `float` and `decimal.Decimal`.

A type class instance can be created in two ways:

    * directly via constructor,
    * via classmethod `from_bytestring` which accepts a `io.BytesIO` instance.

A type class instance can be serialized into a `io.BytesIO` instance via
`to_bytestring` method.

The specification also defines the set of types that can be used within
`Table`s and `Array`s. Such types define `TABLE_LABEL` class attribute
according to the following link:
https://www.rabbitmq.com/amqp-0-9-1-errata.html#section_3
Type classes that cannot be used within `Table`s and `Arrays` have
`TABLE_LABEL` set to `None`.

According to the specification, 4.2.5.2 bits are accumulated into whole octets.
This doesn't happen within `Table`s and `Array`s.
To serialize multiple accumulated bits, method `Bool.many_to_bytestream`
should be used.  To deserialize multiple accumulated bits, method
`Bool.many_from_bytestream` should be used.

In order to add a custom type class, one should subclass `BaseType` and
override the following methods:

    * `pack`,
    * `unpack`,

`BaseType` provides a handy shortcuts for simple types (types that can be
serialized/deserialized via a single `struct.pack` call, for example numeric
types), in that case one only need to override `validate` method.

"""
# There isn't much to document except for the module docstring...
# pylint: disable=missing-docstring

import io
import re
import math
import struct
import decimal
import datetime
import functools
import collections
import collections.abc


class BaseType:

    __slots__ = ()

    TABLE_LABEL = None

    _STRUCT_FMT = None
    _STRUCT_SIZE = None

    @property
    def size(self):
        return self._STRUCT_SIZE

    def pack(self):
        return struct.pack('!' + self._STRUCT_FMT, self)

    @classmethod
    def unpack(cls, stream: io.BytesIO):
        raw = stream.read(cls._STRUCT_SIZE)
        try:
            value = struct.unpack('!' + cls._STRUCT_FMT, raw)[0]
        except struct.error:
            raise ValueError('Cannot unpack value of type {} from {!r}'.format(
                cls.__name__, raw,
            ))
        return value, cls._STRUCT_SIZE

    def to_bytestream(self, stream: io.BytesIO):
        stream.write(self.pack())

    @classmethod
    def from_bytestream(cls, stream: io.BytesIO):
        value, _ = cls.unpack(stream)
        return cls(value)

    def __repr__(self):
        return '<{}: {}>'.format(self.__class__.__name__, self)


# Primitive domains
class Bool(BaseType):

    __slots__ = ('_value', )

    TABLE_LABEL = b't'

    _STRUCT_FMT = '?'
    _STRUCT_SIZE = 1

    def __init__(self, value=False):
        self._value = bool(value)

    # pack and unpack used when packing/unpacking happens within tables/arrays
    # pack_many/many_to_bytestream and unpack_many/many_from_bytestream
    # used when packing/unpacking happens elsewhere
    # damn you AMQP creators who decided to save a couple of bytes ;(
    def pack(self):
        return struct.pack('!?', self._value)

    @classmethod
    def pack_many(cls, values):
        if not values:
            return b''
        result = 0
        bitshift = 0
        for val in values:
            if val:
                result |= (1 << bitshift)
            bitshift += 1
        bytes_count = len(values) // 8
        if len(values) % 8 != 0:
            bytes_count += 1
        return result.to_bytes(bytes_count, 'big')

    @classmethod
    def unpack_many(cls, stream, number_of_bits):
        if not number_of_bits:
            return []
        bytes_count = number_of_bits // 8
        if number_of_bits % 8 != 0:
            bytes_count += 1
        bits = int.from_bytes(stream.read(bytes_count), 'big')
        result = []
        for i in range(number_of_bits):
            value = bits & (1 << i)
            result.append(cls(value))
        return result

    @classmethod
    def many_to_bytestream(cls, bools, stream):
        packed = cls.pack_many(bools)
        stream.write(packed)

    @classmethod
    def many_from_bytestream(cls, stream, number_of_bits):
        return cls.unpack_many(stream, number_of_bits)

    def __bool__(self):
        return self._value

    def __eq__(self, other):
        return self._value == bool(other)

    def __str__(self):
        return str(self._value)


Bit = Bool


class SignedByte(BaseType, int):
    TABLE_LABEL = b'b'

    MIN = -(1 << 7)
    MAX = (1 << 7) - 1

    _STRUCT_FMT = 'b'
    _STRUCT_SIZE = 1

    def __new__(cls, *args, **kwargs):
        value = super().__new__(cls, *args, **kwargs)
        if not cls.MIN <= value <= cls.MAX:
            raise ValueError('value {} must be in range: {}, {}'.format(
                value, cls.MIN, cls.MAX
            ))
        return value


class UnsignedByte(BaseType, int):
    TABLE_LABEL = b'B'

    MIN = 0
    MAX = (1 << 8) - 1

    _STRUCT_FMT = 'B'
    _STRUCT_SIZE = 1

    def __new__(cls, *args, **kwargs):
        value = super().__new__(cls, *args, **kwargs)
        if not cls.MIN <= value <= cls.MAX:
            raise ValueError('value {} must be in range: {}, {}'.format(
                value, cls.MIN, cls.MAX
            ))
        return value


Octet = UnsignedByte


class SignedShort(BaseType, int):
    TABLE_LABEL = b's'

    MIN = -(1 << 15)
    MAX = (1 << 15) - 1

    _STRUCT_FMT = 'h'
    _STRUCT_SIZE = 2

    def __new__(cls, *args, **kwargs):
        value = super().__new__(cls, *args, **kwargs)
        if not cls.MIN <= value <= cls.MAX:
            raise ValueError('value {} must be in range: {}, {}'.format(
                value, cls.MIN, cls.MAX
            ))
        return value


class UnsignedShort(BaseType, int):
    TABLE_LABEL = b'u'

    MIN = 0
    MAX = (1 << 16) - 1

    _STRUCT_FMT = 'H'
    _STRUCT_SIZE = 2

    def __new__(cls, *args, **kwargs):
        value = super().__new__(cls, *args, **kwargs)
        if not cls.MIN <= value <= cls.MAX:
            raise ValueError('value {} must be in range: {}, {}'.format(
                value, cls.MIN, cls.MAX
            ))
        return value


Short = UnsignedShort


class SignedLong(BaseType, int):
    TABLE_LABEL = b'I'

    MIN = -(1 << 31)
    MAX = (1 << 31) - 1

    _STRUCT_FMT = 'l'
    _STRUCT_SIZE = 4

    def __new__(cls, *args, **kwargs):
        value = super().__new__(cls, *args, **kwargs)
        if not cls.MIN <= value <= cls.MAX:
            raise ValueError('value {} must be in range: {}, {}'.format(
                value, cls.MIN, cls.MAX
            ))
        return value


class UnsignedLong(BaseType, int):
    TABLE_LABEL = b'i'

    MIN = 0
    MAX = (1 << 32) - 1

    _STRUCT_FMT = 'L'
    _STRUCT_SIZE = 4

    def __new__(cls, *args, **kwargs):
        value = super().__new__(cls, *args, **kwargs)
        if not cls.MIN <= value <= cls.MAX:
            raise ValueError('value {} must be in range: {}, {}'.format(
                value, cls.MIN, cls.MAX
            ))
        return value


Long = UnsignedLong


class SignedLongLong(BaseType, int):
    TABLE_LABEL = b'l'

    MIN = -(1 << 63)
    MAX = (1 << 63) - 1

    _STRUCT_FMT = 'q'
    _STRUCT_SIZE = 8

    def __new__(cls, *args, **kwargs):
        value = super().__new__(cls, *args, **kwargs)
        if not cls.MIN <= value <= cls.MAX:
            raise ValueError('value {} must be in range: {}, {}'.format(
                value, cls.MIN, cls.MAX
            ))
        return value


class UnsignedLongLong(BaseType, int):
    # Missing in rabbitmq/qpid table types
    TABLE_LABEL = None

    MIN = 0
    MAX = (1 << 64) - 1

    _STRUCT_FMT = 'Q'
    _STRUCT_SIZE = 8

    def __new__(cls, *args, **kwargs):
        value = super().__new__(cls, *args, **kwargs)
        if not cls.MIN <= value <= cls.MAX:
            raise ValueError('value {} must be in range: {}, {}'.format(
                value, cls.MIN, cls.MAX
            ))
        return value


Longlong = UnsignedLongLong


class Float(BaseType, float):
    TABLE_LABEL = b'f'

    _STRUCT_FMT = 'f'
    _STRUCT_SIZE = 4

    def __new__(cls, *args, **kwargs):
        value = super().__new__(cls, *args, **kwargs)
        if math.isnan(value):
            raise ValueError
        try:
            value = struct.unpack('!f', struct.pack('!f', value))[0]
            return super().__new__(cls, value)
        except OverflowError:
            raise ValueError


class Double(BaseType, float):
    TABLE_LABEL = b'd'

    _STRUCT_FMT = 'd'
    _STRUCT_SIZE = 8

    def __new__(cls, *args, **kwargs):
        value = super().__new__(cls, *args, **kwargs)
        if math.isnan(value):
            raise ValueError
        try:
            value = struct.unpack('!d', struct.pack('!d', value))[0]
            return super().__new__(cls, value)
        except OverflowError:
            raise ValueError


class Decimal(BaseType, decimal.Decimal):
    TABLE_LABEL = b'D'

    MIN_EXP = UnsignedByte.MIN
    MAX_EXP = UnsignedByte.MAX
    MIN_VALUE = UnsignedLong.MIN
    MAX_VALUE = UnsignedLong.MAX

    def __new__(cls, *args, **kwargs):
        value = super().__new__(cls, *args, **kwargs)
        sign, _, exponent = value.as_tuple()
        if (sign == 0 and
                exponent != 'F' and  # Decimal('Infinity')
                exponent != 'N' and  # Decimal('sNaN')
                exponent != 'n' and  # Decimal('NaN')
                cls.MIN_EXP <= exponent <= cls.MAX_EXP and
                cls.MIN_VALUE <= value <= cls.MAX_VALUE):
            return value
        raise ValueError('bad decimal value: {!r}'.format(value))

    @property
    def size(self):
        return UnsignedByte._STRUCT_SIZE + UnsignedLong._STRUCT_SIZE

    def pack(self):
        sign, digits, exponent = self.as_tuple()
        value = 0
        for digit in digits:
            value = value * 10 + digit
        if sign:
            value = -value
        return UnsignedByte(-exponent).pack() + UnsignedLong(value).pack()

    @classmethod
    def unpack(cls, stream: io.BytesIO):
        total = 0
        exponent, consumed = UnsignedByte.unpack(stream)
        total += consumed
        value, consumed = UnsignedLong.unpack(stream)
        total += consumed
        value = decimal.Decimal(value) / decimal.Decimal(10 ** exponent)
        return cls(value), total


class ShortStr(BaseType, bytes):
    # Missing in rabbitmq/qpid
    TABLE_LABEL = None
    MAX = UnsignedByte.MAX

    def __new__(cls, value=b''):
        if isinstance(value, str):
            value = value.encode('utf-8')
        value = super().__new__(cls, value)

        try:
            value.decode('utf-8')
        except UnicodeDecodeError:
            raise ValueError('value must be utf-8 encoded bytes')
        if len(value) > cls.MAX:
            raise ValueError(
                'value {!r} is too long for ShortStr'.format(value)
            )
        return value

    @property
    def size(self):
        return UnsignedByte._STRUCT_SIZE + len(self)

    def pack(self):
        return UnsignedByte(len(self)).pack() + self

    @classmethod
    def unpack(cls, stream: io.BytesIO):
        str_len, consumed = UnsignedByte.unpack(stream)
        value = stream.read(str_len)
        if len(value) != str_len:
            raise ValueError('ShortStr length does not equal to read length')
        return cls(value), consumed + str_len


Shortstr = ShortStr


class LongStr(BaseType, bytes):
    TABLE_LABEL = b'S'
    MAX = UnsignedLong.MAX

    def __new__(cls, value=b''):
        if isinstance(value, str):
            value = value.encode('utf-8')
        value = super().__new__(cls, value)

        try:
            value.decode('utf-8')
        except UnicodeDecodeError:
            raise ValueError('value must be utf-8 encoded bytes')
        if len(value) > cls.MAX:
            raise ValueError(
                'value {!r} is too long for LongStr'.format(value)
            )
        return value

    @property
    def size(self):
        return UnsignedLong._STRUCT_SIZE + len(self)

    def pack(self):
        return UnsignedLong(len(self)).pack() + self

    @classmethod
    def unpack(cls, stream: io.BytesIO):
        str_len, consumed = UnsignedLong.unpack(stream)
        value = stream.read(str_len)
        if len(value) != str_len:
            raise ValueError('LongStr length does not equal to read length')
        return cls(value), consumed + str_len


Longstr = LongStr


class Void(BaseType):

    __slots__ = ('_value', )

    TABLE_LABEL = b'V'

    size = 0

    def __init__(self, value=None):
        assert value is None
        self._value = value

    def pack(self):
        # No bytes to pack - it's Void!
        return b''

    @classmethod
    def unpack(cls, stream: io.BytesIO):
        return None, 0

    def __str__(self):
        return 'Void'

    def __eq__(self, other):
        return self._value == other


class ByteArray(BaseType, bytes):
    # According to http://bit.ly/librabbitmq_amqp_table_c
    # bytearrays behave like `LongStr`ings but have different semantics.

    TABLE_LABEL = b'x'
    MAX = UnsignedLong.MAX

    def __new__(cls, *args, **kwargs):
        value = super().__new__(cls, *args, **kwargs)
        if len(value) > cls.MAX:
            raise ValueError('value {!r} is too long for ByteArray'.format(
                value
            ))
        return value

    @property
    def size(self):
        return UnsignedLong._STRUCT_SIZE + len(self)

    def pack(self):
        return UnsignedLong(len(self)).pack() + self

    @classmethod
    def unpack(cls, stream: io.BytesIO):
        str_len, consumed = UnsignedLong.unpack(stream)
        value = stream.read(str_len)
        if len(value) != str_len:
            raise ValueError('ByteArray length does not equal to read length')
        return cls(value), consumed + str_len


class Timestamp(datetime.datetime, BaseType):
    TABLE_LABEL = b'T'

    size = UnsignedLongLong._STRUCT_SIZE

    def __new__(cls, *args, **kwargs):
        if len(args) == 1 and isinstance(args[0], datetime.datetime):
            value = args[0]
            args = (value.year, value.month, value.day,
                    value.hour, value.minute, value.second,
                    value.microsecond, value.tzinfo)
        value = super().__new__(cls, *args, **kwargs)
        if value.year < 1970:
            raise ValueError('timestamps must be after 1970')
        return value

    def pack(self):
        stamp = self.timestamp()
        return UnsignedLongLong(stamp).pack()

    @classmethod
    def unpack(cls, stream: io.BytesIO):
        value, consumed = UnsignedLongLong.unpack(stream)
        date = datetime.datetime.fromtimestamp(value)
        return cls(date), consumed

    def __eq__(self, other):
        diff = abs(self - other)
        # Spec 4.2.5.4 Timestamps, accuracy: 1 second
        return diff <= datetime.timedelta(seconds=1)


class Table(BaseType, collections.abc.MutableMapping):

    __slots__ = ('_value', )

    TABLE_LABEL = b'F'

    def __init__(self, *args, **kwargs):
        value = collections.OrderedDict(*args, **kwargs)

        validated = collections.OrderedDict()
        for key, value in value.items():
            if not isinstance(key, ShortStr):
                key = ShortStr(key)
            if not isinstance(value, BaseType):
                # pylint: disable=E1111
                value = _py_type_to_amqp_type(value)
            validated[key] = value
        self._value = validated

    @property
    def size(self):
        values_size = sum(1 +  # label
                          key.size +
                          value.size
                          for key, value in self._value.items())
        return UnsignedLong._STRUCT_SIZE + values_size

    def pack(self):
        stream = io.BytesIO()
        for key, value in self._value.items():
            key.to_bytestream(stream)
            stream.write(value.TABLE_LABEL)
            value.to_bytestream(stream)
        buffer = stream.getvalue()
        return UnsignedLong(len(buffer)).pack() + buffer

    @classmethod
    def unpack(cls, stream: io.BytesIO):
        result = collections.OrderedDict()
        table_len, initial = UnsignedLong.unpack(stream)
        consumed = initial

        # pylint: disable=C0103
        while consumed < initial + table_len:
            key, x = ShortStr.unpack(stream)
            consumed += x

            label = stream.read(1)
            consumed += 1

            amqptype = TABLE_LABEL_TO_CLS[label]
            value, x = amqptype.unpack(stream)
            consumed += x

            result[key] = amqptype(value)
        return result, consumed

    def __getitem__(self, key):
        if isinstance(key, (str, bytes)):
            key = ShortStr(key)
        return self._value[key]

    def __setitem__(self, key, value):
        if isinstance(key, (str, bytes)):
            key = ShortStr(key)
        if not isinstance(value, BaseType):
            # pylint: disable=E1111
            value = _py_type_to_amqp_type(value)
        self._value[key] = value

    def __delitem__(self, key):
        if isinstance(key, (str, bytes)):
            key = ShortStr(key)
        del self._value[key]

    def __iter__(self):
        return iter(self._value)

    def __len__(self):
        return len(self._value)

    def __str__(self):
        return str(self._value)


class Array(BaseType, collections.abc.MutableSequence):

    __slots__ = ('_value', )

    TABLE_LABEL = b'A'

    def __init__(self, *args, **kwargs):
        value = list(*args, **kwargs)

        validated = []
        for item in value:
            if not isinstance(item, BaseType):
                # pylint: disable=E1111
                item = _py_type_to_amqp_type(item)
            validated.append(item)
        self._value = validated

    @property
    def size(self):
        values_size = sum(1 + value.size for value in self._value)
        return UnsignedLong._STRUCT_SIZE + values_size

    def pack(self):
        stream = io.BytesIO()
        for value in self._value:
            stream.write(value.TABLE_LABEL)
            value.to_bytestream(stream)
        buffer = stream.getvalue()
        return UnsignedLong(len(buffer)).pack() + buffer

    @classmethod
    def unpack(cls, stream: io.BytesIO):
        result = []
        array_len, consumed = UnsignedLong.unpack(stream)
        initial = consumed

        # pylint: disable=C0103
        while consumed < (array_len + initial):
            label = stream.read(1)
            consumed += 1

            amqptype = TABLE_LABEL_TO_CLS[label]
            value, x = amqptype.unpack(stream)
            consumed += x

            result.append(amqptype(value))
        return result, consumed

    def __getitem__(self, key):
        return self._value[key]

    def __setitem__(self, key, value):
        if not isinstance(value, BaseType):
            # pylint: disable=E1111
            value = _py_type_to_amqp_type(value)
        self._value[key] = value

    def __delitem__(self, key):
        del self._value[key]

    def __len__(self):
        return len(self._value)

    def __eq__(self, other):
        return self._value == other

    def insert(self, index, value):
        if not isinstance(value, BaseType):
            # pylint: disable=E1111
            value = _py_type_to_amqp_type(value)
        self._value.insert(index, value)

    def __hash__(self):
        raise NotImplementedError

    def __str__(self):
        return str(self._value)


@functools.singledispatch
def _py_type_to_amqp_type(value):
    raise ValueError(value)


@_py_type_to_amqp_type.register(bool)
def _py_type_to_amqp_bool(value):
    return Bool(value)


@_py_type_to_amqp_type.register(str)
@_py_type_to_amqp_type.register(bytes)
def _py_type_to_amqp_str_bytes(value):
    if isinstance(value, bytes):
        try:
            # If we can decode UTF-8, it's LongStr
            # No ShortStr support in rabbitmq/qpid
            value.decode('utf-8')
            cls = LongStr
        except UnicodeDecodeError:
            # If we can't decode UTF-8, it's ByteArray
            cls = ByteArray
    else:
        cls = LongStr
    return cls(value)


@_py_type_to_amqp_type.register(float)
def _py_type_to_amqp_float(value):
    # pylint: disable=R0204
    try:
        value = Float(value)
    except ValueError:
        value = Double(value)
    return value


@_py_type_to_amqp_type.register(int)
def _py_type_to_amqp_int(value):
    last_error = None
    for cls in (SignedByte, UnsignedByte,
                SignedShort, UnsignedShort,
                SignedLong, UnsignedLong,
                SignedLongLong):
        try:
            value = cls(value)
            last_error = None
            break
        except ValueError as exc:
            last_error = exc
    if last_error is not None:  # pragma: no cover
        # pylint: disable=E0702
        raise last_error
    return value


@_py_type_to_amqp_type.register(decimal.Decimal)
def _py_type_to_amqp_decimal(value):
    return Decimal(value)


@_py_type_to_amqp_type.register(datetime.datetime)
def _py_type_to_amqp_datetime(value):
    return Timestamp(value)


@_py_type_to_amqp_type.register(dict)
def _py_type_to_amqp_dict(value):
    return Table(value)


@_py_type_to_amqp_type.register(list)
@_py_type_to_amqp_type.register(tuple)
def _py_type_to_amqp_list_tuple(value):
    return Array(value)


@_py_type_to_amqp_type.register(type(None))
def _py_type_to_amqp_none(value):
    return Void(None)


# pylint: disable=E1101
TABLE_LABEL_TO_CLS = {cls.TABLE_LABEL: cls
                      for cls in BaseType.__subclasses__()
                      if cls.TABLE_LABEL is not None}


# Non-primitive domains
class ClassId(Short):
    """Class id.

    Identifier of an AMQP class.
    """

    @staticmethod
    def validate(value):
        pass


class ConsumerTag(Shortstr):
    """Consumer tag.

    Identifier for the consumer, valid within the current channel.
    """

    @staticmethod
    def validate(value):
        pass


class DeliveryTag(Longlong):
    """Server-assigned delivery tag.

    The server-assigned and channel-specific delivery tag.

    Rules:

      * channel-local:
        The delivery tag is valid only within the channel from which the
        message was received. I.e. a client MUST NOT receive a message on
        one channel and then acknowledge it on another.

      * non-zero:
        The server MUST NOT use a zero value for delivery tags. Zero is
        reserved for client use, meaning "all messages so far received".
    """

    @staticmethod
    def validate(value):
        pass


class ExchangeName(Shortstr):
    """Exchange name.

    The exchange name is a client-selected string that identifies the exchange
    for publish methods.
    """

    @staticmethod
    def validate(value):
        if len(value) > 127:
            raise ValueError('len(value) > 127')
        if re.match(r'^[a-zA-Z0-9-_.:]*$', value) is None:
            raise ValueError(
                'value does not match the following regex: {}'.format(
                    r'^[a-zA-Z0-9-_.:]*$'
                )
            )


class MethodId(Short):
    """Method id.

    Identifier of an AMQP method.
    """

    @staticmethod
    def validate(value):
        pass


class NoAck(Bit):
    """No acknowledgement needed.

    If this field is set the server does not expect acknowledgements for
    messages. That is, when a message is delivered to the client the server
    assumes the delivery will succeed and immediately dequeues it. This
    functionality may increase performance but at the cost of reliability.
    Messages can get lost if a client dies before they are delivered to the
    application.
    """

    @staticmethod
    def validate(value):
        pass


class NoLocal(Bit):
    """Do not deliver own messages.

    If the no-local field is set the server will not send messages to the
    connection that published them.
    """

    @staticmethod
    def validate(value):
        pass


class NoWait(Bit):
    """Do not send reply method.

    If set, the server will not respond to the method. The client should not
    wait for a reply method. If the server could not complete the method it
    will raise a channel or connection exception.
    """

    @staticmethod
    def validate(value):
        pass


class Path(Shortstr):
    """Path.

    Unconstrained.
    """

    @staticmethod
    def validate(value):
        if not value:
            raise ValueError('value is Falsey')
        if len(value) > 127:
            raise ValueError('len(value) > 127')


class PeerProperties(Table):
    """Peer properties.

    This table provides a set of peer properties, used for identification,
    debugging, and general information.
    """

    @staticmethod
    def validate(value):
        pass


class QueueName(Shortstr):
    """Queue name.

    The queue name identifies the queue within the vhost.  In methods where the
    queue name may be blank, and that has no specific significance, this refers
    to the 'current' queue for the channel, meaning the last queue that the
    client declared on the channel.  If the client did not declare a queue, and
    the method needs a queue name, this will result in a 502 (syntax error)
    channel exception.
    """

    @staticmethod
    def validate(value):
        if len(value) > 127:
            raise ValueError('len(value) > 127')
        if re.match(r'^[a-zA-Z0-9-_.:]*$', value) is None:
            raise ValueError(
                'value does not match the following regex: {}'.format(
                    r'^[a-zA-Z0-9-_.:]*$'
                )
            )


class Redelivered(Bit):
    """Message is being redelivered.

    This indicates that the message has been previously delivered to this or
    another client.

    Rules:

      * implementation:
        The server SHOULD try to signal redelivered messages when it can.
        When redelivering a message that was not successfully acknowledged,
        the server SHOULD deliver it to the original client if possible.

      * hinting:
        The client MUST NOT rely on the redelivered field but should take it
        as a hint that the message may already have been processed. A fully
        robust client must be able to track duplicate received messages on
        non-transacted, and locally-transacted channels.
    """

    @staticmethod
    def validate(value):
        pass


class MessageCount(Long):
    """Number of messages in queue.

    The number of messages in the queue, which will be zero for newly-declared
    queues. This is the number of messages present in the queue, and committed
    if the channel on which they were published is transacted, that are not
    waiting acknowledgement.
    """

    @staticmethod
    def validate(value):
        pass


class ReplyCode(Short):
    """Reply code from server.

    The reply code. The AMQ reply codes are defined as constants at the start
    of this formal specification.
    """

    @staticmethod
    def validate(value):
        if not value:
            raise ValueError('value is Falsey')


class ReplyText(Shortstr):
    """Localised reply text.

    The localised reply text. This text can be logged as an aid to resolving
    issues.
    """

    @staticmethod
    def validate(value):
        if not value:
            raise ValueError('value is Falsey')


__all__ = [name for name in locals() if not name.startswith('_')]