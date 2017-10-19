"""
amqproto.domains
~~~~~~~~~~~~~~~~

AMQP domains (types).
"""
# pylint: disable=missing-docstring
# pylint: disable=invalid-name
import re
import decimal
import functools
from datetime import datetime
from collections.abc import Sequence, Mapping

import construct as c


class DecimalAdapter(c.Adapter):

    def _encode(self, obj, context):
        _, digits, exponent = obj.as_tuple()
        value = int(''.join(map(str, digits)))
        return {'exponent': -exponent, 'value': value}

    def _decode(self, obj, context):
        digits = list(map(int, str(obj.value)))
        return decimal.Decimal((0, digits, -obj.exponent))


class TimestampAdapter(c.Adapter):

    def _encode(self, obj, context):
        return int(obj.timestamp())

    def _decode(self, obj, context):
        return datetime.fromtimestamp(obj)


@functools.singledispatch
def _py_type_to_amqp_domain(value):
    raise ValueError(value)


@_py_type_to_amqp_domain.register(bool)
def _py_type_to_amqp_bool(value):
    return Bool


@_py_type_to_amqp_domain.register(str)
def _py_type_to_amqp_str(value):
    # ShortStr isn't supported by RabbitMQ/Qpid
    return LongStr


@_py_type_to_amqp_domain.register(bytes)
def _py_type_to_amqp_bytearray(value):
    return ByteArray


@_py_type_to_amqp_domain.register(float)
def _py_type_to_amqp_float(value):
    return c.Select(Float, Double)


@_py_type_to_amqp_domain.register(int)
def _py_type_to_amqp_int(value):
    domains = (
        SignedByte, UnsignedByte,
        SignedShort, UnsignedShort,
        SignedLong, UnsignedLong,
    )
    for domain in domains:
        try:
            domain.build(value)
            return domain
        except c.ConstructError:
            continue
    return SignedLongLong


@_py_type_to_amqp_domain.register(decimal.Decimal)
def _py_type_to_amqp_decimal(value):
    return Decimal


@_py_type_to_amqp_domain.register(datetime)
def _py_type_to_amqp_datetime(value):
    return Timestamp


@_py_type_to_amqp_domain.register(Mapping)
def _py_type_to_amqp_dict(value):
    return Table


@_py_type_to_amqp_domain.register(Sequence)
def _py_type_to_amqp_sequence(value):
    return Array


@_py_type_to_amqp_domain.register(type(None))
def _py_type_to_amqp_none(value):
    return Void


class TableAdapter(c.Adapter):

    def _encode(self, obj, context):
        if obj is None:
            return {}
        entries = []
        for key, value in obj.items():
            # pylint: disable=assignment-from-no-return
            type_ = _py_type_to_amqp_domain(value)
            table_value = {'label': DOMAIN_TO_LABEL[type_], 'value': value}
            table_entry = {'key': key, 'value': table_value}
            entries.append(table_entry)
        return entries

    def _decode(self, obj, context):
        return {
            table_entry.key: table_entry.value.value
            for table_entry in obj
        }


class ArrayAdapter(c.Adapter):

    def _encode(self, obj, context):
        entries = []
        for value in obj:
            # pylint: disable=assignment-from-no-return
            type_, _ = _py_type_to_amqp_domain(value)
            table_value = {'label': DOMAIN_TO_LABEL[type_], 'value': value}
            entries.append(table_value)
        return entries

    def _decode(self, obj, context):
        return [obj.value for table_value in obj]


# Primitive domains
Bit = c.Bit
Bool = c.Int8ub
SignedByte = c.Int8sb
Octet = UnsignedByte = c.Int8ub
SignedShort = c.Int16sb
Short = UnsignedShort = c.Int16ub
SignedLong = c.Int32sb
Long = UnsignedLong = c.Int32ub
SignedLongLong = c.Int64sb
Longlong = UnsignedLongLong = c.Int64ub
Float = c.Float32b
Double = c.Float64b
Decimal = DecimalAdapter(c.Struct(
    'exponent' / UnsignedByte,
    'value' / UnsignedLong,
))
Shortstr = ShortStr = c.PascalString(UnsignedByte, encoding='utf-8')
LongStr = c.PascalString(UnsignedLong, encoding='utf-8')
Void = c.Bytes(0)
ByteArray = c.PascalString(UnsignedLong)
Timestamp = TimestampAdapter(UnsignedLongLong)

# https://www.rabbitmq.com/amqp-0-9-1-errata.html#section_3
LABEL_TO_DOMAIN = {
    b't': Bool,
    b'b': SignedByte,
    b'B': UnsignedByte,
    b's': SignedShort,
    b'u': UnsignedShort,
    b'I': SignedLong,
    b'i': UnsignedLong,
    b'l': SignedLongLong,
    # UnsignedLongLong isn't supported by RabbitMQ/Qpid
    b'f': Float,
    b'd': Double,
    b'D': Decimal,
    # ShortStr isn't supported by RabbitMQ/Qpid
    b'S': LongStr,
    b'A': c.LazyBound(lambda ctx: Array),
    b'T': Timestamp,
    b'F': c.LazyBound(lambda ctx: Table),
    b'V': Void,
    b'x': ByteArray,
}

TableValue = c.Struct(
    'label' / c.String(1),
    'value' / c.Switch(c.this.label, LABEL_TO_DOMAIN)
)
TableEntry = c.Struct(
    'key' / ShortStr,
    'value' / TableValue,
)
Table = TableAdapter(TableEntry[:])
Array = TableValue[:]

DOMAIN_TO_LABEL = {value: key for key, value in LABEL_TO_DOMAIN.items()}
# LazyBound can't be looked up, let's fix this
DOMAIN_TO_LABEL[Table] = b'F'
DOMAIN_TO_LABEL[Array] = b'A'


def _regex_check(regex, value):
    if re.match(regex, value) is None:
        raise ValueError('{!r} does not match the following regex: {}'.format(
            value, regex
        ))


def _emptiness_check(value):
    if not value:
        raise ValueError('cannot be empty')


# Non-primitive domains
ClassId = Short
"""Class id.

Identifier of an AMQP class.
"""

ConsumerTag = Shortstr
"""Consumer tag.

Identifier for the consumer, valid within the current channel.
"""

DeliveryTag = Longlong
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

ExchangeName = c.ExprValidator(
    Shortstr,
    validator=lambda obj, ctx: _regex_check(r'^[a-zA-Z0-9-_.:]*$', obj)
)
"""Exchange name.

The exchange name is a client-selected string that identifies the exchange
for publish methods.
"""

MethodId = Short
"""Method id.

Identifier of an AMQP method.
"""

NoAck = Bit
"""No acknowledgement needed.

If this field is set the server does not expect acknowledgements for
messages. That is, when a message is delivered to the client the server
assumes the delivery will succeed and immediately dequeues it. This
functionality may increase performance but at the cost of reliability.
Messages can get lost if a client dies before they are delivered to the
application.
"""


NoLocal = Bit
"""Do not deliver own messages.

If the no-local field is set the server will not send messages to the
connection that published them.
"""


NoWait = Bit
"""Do not send reply method.

If set, the server will not respond to the method. The client should not
wait for a reply method. If the server could not complete the method it
will raise a channel or connection exception.
"""

Path = c.ExprValidator(
    Shortstr, lambda obj, ctx: _emptiness_check(obj)
)
"""Path.

Unconstrained.
"""


PeerProperties = Table
"""Peer properties.

This table provides a set of peer properties, used for identification,
debugging, and general information.
"""

QueueName = c.ExprValidator(
    Shortstr, lambda obj, ctx: _regex_check(r'^[a-zA-Z0-9-_.:]*$', obj)
)
"""Queue name.

The queue name identifies the queue within the vhost.  In methods where the
queue name may be blank, and that has no specific significance, this refers
to the 'current' queue for the channel, meaning the last queue that the
client declared on the channel.  If the client did not declare a queue, and
the method needs a queue name, this will result in a 502 (syntax error)
channel exception.
"""

Redelivered = Bit
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


MessageCount = Long
"""Number of messages in queue.

The number of messages in the queue, which will be zero for newly-declared
queues. This is the number of messages present in the queue, and committed
if the channel on which they were published is transacted, that are not
waiting acknowledgement.
"""

ReplyCode = c.ExprValidator(
    Short, lambda obj, ctx: _emptiness_check(obj)
)
"""Reply code from server.

The reply code. The AMQ reply codes are defined as constants at the start
of this formal specification.
"""

ReplyText = c.ExprValidator(
    Shortstr, lambda obj, ctx: _emptiness_check(obj)
)
"""Localised reply text.

The localised reply text. This text can be logged as an aid to resolving
issues.
"""
