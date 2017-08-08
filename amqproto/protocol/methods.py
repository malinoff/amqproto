"""
amqproto.protocol.methods
~~~~~~~~~~~~~~~~~~~~~~~~~

Implementation of AMQP methods.

This file was generated 2017-08-08 from
/codegen/amqp0-9-1.extended.xml.

"""
# There are a lot of reserved_* attributes, they are unused intentionally.
# Disable "too many lines" too, for obvious reasons.
# Some arguments may shadow builtins, we don't care - methods are data classes.
# pylint: disable=unused-variable,too-many-lines,redefined-builtin

import io
import collections

from . import types


class Method:
    """Base class for all AMQP methods."""

    content = False

    def __init__(self, *values):
        assert len(values) == len(self.field_info)

        self.values = collections.OrderedDict()
        for (name, amqptype), value in zip(self.field_info, values):
            if name == 'global':
                name = 'global_'
            if value is None:
                value = amqptype()
            else:
                value = amqptype(value)
            self.values[name] = value

        values_size = sum(value.size for value in self.values.values())
        self.size = (types.UnsignedShort._STRUCT_SIZE +  # class_id
                     types.UnsignedShort._STRUCT_SIZE +  # method_id
                     values_size)

    @classmethod
    def from_bytestream(cls, stream: io.BytesIO, body_chunk_size=None):
        """Instantiates a `Method` subclass from the byte stream.

        Which subclass' instance should be instantiated is determined
        according to the specification, 2.3.5.1.
        """

        class_id = types.UnsignedShort.from_bytestream(stream)
        method_id = types.UnsignedShort.from_bytestream(stream)
        method_cls = METHODS[(class_id, method_id)]

        kwargs = {}
        bit_names = []
        number_of_bits = 0
        for name, amqptype in method_cls.field_info:
            if name == 'global':
                name = 'global_'

            if amqptype is types.Bool:
                number_of_bits += 1
                bit_names.append(name)
                continue
            elif number_of_bits:
                # We have some bools but this next field is not a bool
                bits = types.Bool.many_from_bytestream(stream, number_of_bits)
                for bit_name, bit in zip(bit_names, bits):
                    kwargs[bit_name] = bit
                number_of_bits = 0
                bit_names.clear()

            kwargs[name] = amqptype.from_bytestream(stream)

        if number_of_bits:
            bits = types.Bool.many_from_bytestream(stream, number_of_bits)
            for name, bit in zip(bit_names, bits):
                kwargs[name] = bit
            number_of_bits = 0
            bit_names.clear()
        return method_cls(**kwargs)

    def to_bytestream(self, stream: io.BytesIO):
        """Serialize the method into the byte stream according to
        the specification, 2.3.5.1.
        """

        types.UnsignedShort(self.method_type[0]).to_bytestream(stream)
        types.UnsignedShort(self.method_type[1]).to_bytestream(stream)
        bits = []
        for value in self.values.values():
            if isinstance(value, types.Bool):
                bits.append(value)
            else:
                if bits:
                    types.Bool.many_to_bytestream(bits, stream)
                    bits = []
                value.to_bytestream(stream)

        if bits:
            types.Bool.many_to_bytestream(bits, stream)

    def __getattr__(self, name):
        try:
            return self.values[name]
        except KeyError:
            raise AttributeError('{} object has no attribute {}'.format(
                type(self).__name__, name
            )) from None

    def __eq__(self, other):
        return (self.method_type == other.method_type and
                self.values == other.values)

    def __repr__(self):
        return '<{}: {}>'.format(self.__class__.__name__,
                                 ' '.join('{}={}'.format(k, v)
                                          for k, v in self.values.items()))


class ConnectionStart(Method):
    """This method starts the connection negotiation process by telling the
    client the protocol version that the server proposes, along with a list of
    security mechanisms which the client can use for authentication.

    Arguments:
        version_major: Octet
        version_minor: Octet
        server_properties: Table
        mechanisms: Longstr
        locales: Longstr
    """
    method_type = (10, 10)

    field_info = (
        ('version_major', types.Octet),
        ('version_minor', types.Octet),
        ('server_properties', types.Table),
        ('mechanisms', types.Longstr),
        ('locales', types.Longstr),
    )

    synchronous = True
    content = False

    def __init__(self, *,
                 version_major,
                 version_minor,
                 server_properties,
                 mechanisms,
                 locales):
        super().__init__(
            version_major,
            version_minor,
            server_properties,
            mechanisms,
            locales,
        )


class ConnectionStartOK(Method):
    """This method selects a SASL security mechanism.

    Arguments:
        client_properties: Table
        mechanism: Shortstr
        response: Longstr
        locale: Shortstr
    """
    method_type = (10, 11)

    field_info = (
        ('client_properties', types.Table),
        ('mechanism', types.Shortstr),
        ('response', types.Longstr),
        ('locale', types.Shortstr),
    )

    synchronous = True
    content = False

    def __init__(self, *,
                 client_properties,
                 mechanism,
                 response,
                 locale):
        super().__init__(
            client_properties,
            mechanism,
            response,
            locale,
        )


class ConnectionSecure(Method):
    """The SASL protocol works by exchanging challenges and responses until
    both peers have received sufficient information to authenticate each other.
    This method challenges the client to provide more information.

    Arguments:
        challenge: Longstr
    """
    method_type = (10, 20)

    field_info = (
        ('challenge', types.Longstr),
    )

    synchronous = True
    content = False

    def __init__(self, *,
                 challenge):
        super().__init__(
            challenge,
        )


class ConnectionSecureOK(Method):
    """This method attempts to authenticate, passing a block of SASL data for
    the security mechanism at the server side.

    Arguments:
        response: Longstr
    """
    method_type = (10, 21)

    field_info = (
        ('response', types.Longstr),
    )

    synchronous = True
    content = False

    def __init__(self, *,
                 response):
        super().__init__(
            response,
        )


class ConnectionTune(Method):
    """This method proposes a set of connection configuration values to the
    client. The client can accept and/or adjust these.

    Arguments:
        channel_max: Short
        frame_max: Long
        heartbeat: Short
    """
    method_type = (10, 30)

    field_info = (
        ('channel_max', types.Short),
        ('frame_max', types.Long),
        ('heartbeat', types.Short),
    )

    synchronous = True
    content = False

    def __init__(self, *,
                 channel_max,
                 frame_max,
                 heartbeat):
        super().__init__(
            channel_max,
            frame_max,
            heartbeat,
        )


class ConnectionTuneOK(Method):
    """This method sends the client's connection tuning parameters to the
    server. Certain fields are negotiated, others provide capability
    information.

    Arguments:
        channel_max: Short
        frame_max: Long
        heartbeat: Short
    """
    method_type = (10, 31)

    field_info = (
        ('channel_max', types.Short),
        ('frame_max', types.Long),
        ('heartbeat', types.Short),
    )

    synchronous = True
    content = False

    def __init__(self, *,
                 channel_max,
                 frame_max,
                 heartbeat):
        super().__init__(
            channel_max,
            frame_max,
            heartbeat,
        )


class ConnectionOpen(Method):
    """This method opens a connection to a virtual host, which is a collection
    of resources, and acts to separate multiple application domains within a
    server. The server may apply arbitrary limits per virtual host, such as the
    number of each type of entity that may be used, per connection and/or in
    total.

    Arguments:
        virtual_host: Shortstr
        reserved_1: Shortstr
        reserved_2: Bit
    """
    method_type = (10, 40)

    field_info = (
        ('virtual_host', types.Shortstr),
        ('reserved_1', types.Shortstr),
        ('reserved_2', types.Bit),
    )

    synchronous = True
    content = False

    def __init__(self, *,
                 virtual_host,
                 reserved_1=None,
                 reserved_2=None):
        super().__init__(
            virtual_host,
            types.Shortstr(),
            types.Bit(),
        )


class ConnectionOpenOK(Method):
    """This method signals to the client that the connection is ready for use.

    Arguments:
        reserved_1: Shortstr
    """
    method_type = (10, 41)

    field_info = (
        ('reserved_1', types.Shortstr),
    )

    synchronous = True
    content = False

    def __init__(self, *,
                 reserved_1=None):
        super().__init__(
            types.Shortstr(),
        )


class ConnectionClose(Method):
    """This method indicates that the sender wants to close the connection.
    This may be due to internal conditions (e.g. a forced shut-down) or due to
    an error handling a specific method, i.e. an exception. When a close is due
    to an exception, the sender provides the class and method id of the method
    which caused the exception.

    Arguments:
        reply_code: Short
        reply_text: Shortstr
        class_id: Short
        method_id: Short
    """
    method_type = (10, 50)

    field_info = (
        ('reply_code', types.Short),
        ('reply_text', types.Shortstr),
        ('class_id', types.Short),
        ('method_id', types.Short),
    )

    synchronous = True
    content = False

    def __init__(self, *,
                 reply_code,
                 reply_text,
                 class_id,
                 method_id):
        super().__init__(
            reply_code,
            reply_text,
            class_id,
            method_id,
        )


class ConnectionCloseOK(Method):
    """This method confirms a Connection.Close method and tells the recipient
    that it is safe to release resources for the connection and close the
    socket.
    """
    method_type = (10, 51)

    field_info = ()

    synchronous = True
    content = False


class ChannelOpen(Method):
    """This method opens a channel to the server.

    Arguments:
        reserved_1: Shortstr
    """
    method_type = (20, 10)

    field_info = (
        ('reserved_1', types.Shortstr),
    )

    synchronous = True
    content = False

    def __init__(self, *,
                 reserved_1=None):
        super().__init__(
            types.Shortstr(),
        )


class ChannelOpenOK(Method):
    """This method signals to the client that the channel is ready for use.

    Arguments:
        reserved_1: Longstr
    """
    method_type = (20, 11)

    field_info = (
        ('reserved_1', types.Longstr),
    )

    synchronous = True
    content = False

    def __init__(self, *,
                 reserved_1=None):
        super().__init__(
            types.Longstr(),
        )


class ChannelFlow(Method):
    """This method asks the peer to pause or restart the flow of content data
    sent by a consumer. This is a simple flow-control mechanism that a peer can
    use to avoid overflowing its queues or otherwise finding itself receiving
    more messages than it can process. Note that this method is not intended
    for window control. It does not affect contents returned by Basic.Get-Ok
    methods.

    Arguments:
        active: Bit
    """
    method_type = (20, 20)

    field_info = (
        ('active', types.Bit),
    )

    synchronous = True
    content = False

    def __init__(self, *,
                 active):
        super().__init__(
            active,
        )


class ChannelFlowOK(Method):
    """Confirms to the peer that a flow command was received and processed.

    Arguments:
        active: Bit
    """
    method_type = (20, 21)

    field_info = (
        ('active', types.Bit),
    )

    synchronous = False
    content = False

    def __init__(self, *,
                 active):
        super().__init__(
            active,
        )


class ChannelClose(Method):
    """This method indicates that the sender wants to close the channel. This
    may be due to internal conditions (e.g. a forced shut-down) or due to an
    error handling a specific method, i.e. an exception. When a close is due to
    an exception, the sender provides the class and method id of the method
    which caused the exception.

    Arguments:
        reply_code: Short
        reply_text: Shortstr
        class_id: Short
        method_id: Short
    """
    method_type = (20, 40)

    field_info = (
        ('reply_code', types.Short),
        ('reply_text', types.Shortstr),
        ('class_id', types.Short),
        ('method_id', types.Short),
    )

    synchronous = True
    content = False

    def __init__(self, *,
                 reply_code,
                 reply_text,
                 class_id,
                 method_id):
        super().__init__(
            reply_code,
            reply_text,
            class_id,
            method_id,
        )


class ChannelCloseOK(Method):
    """This method confirms a Channel.Close method and tells the recipient
    that it is safe to release resources for the channel.
    """
    method_type = (20, 41)

    field_info = ()

    synchronous = True
    content = False


class ExchangeDeclare(Method):
    """This method creates an exchange if it does not already exist, and if
    the exchange exists, verifies that it is of the correct and expected class.

    Arguments:
        reserved_1: Short
        exchange: Shortstr
        type: Shortstr
        passive: Bit
        durable: Bit
        auto_delete: Bit
        internal: Bit
        no_wait: Bit
        arguments: Table
    """
    method_type = (40, 10)

    field_info = (
        ('reserved_1', types.Short),
        ('exchange', types.Shortstr),
        ('type', types.Shortstr),
        ('passive', types.Bit),
        ('durable', types.Bit),
        ('auto_delete', types.Bit),
        ('internal', types.Bit),
        ('no_wait', types.Bit),
        ('arguments', types.Table),
    )

    synchronous = True
    content = False

    def __init__(self, *,
                 reserved_1=None,
                 exchange,
                 type,
                 passive,
                 durable,
                 auto_delete,
                 internal,
                 no_wait,
                 arguments):
        super().__init__(
            types.Short(),
            exchange,
            type,
            passive,
            durable,
            auto_delete,
            internal,
            no_wait,
            arguments,
        )


class ExchangeDeclareOK(Method):
    """This method confirms a Declare method and confirms the name of the
    exchange, essential for automatically-named exchanges.
    """
    method_type = (40, 11)

    field_info = ()

    synchronous = True
    content = False


class ExchangeDelete(Method):
    """This method deletes an exchange. When an exchange is deleted all queue
    bindings on the exchange are cancelled.

    Arguments:
        reserved_1: Short
        exchange: Shortstr
        if_unused: Bit
        no_wait: Bit
    """
    method_type = (40, 20)

    field_info = (
        ('reserved_1', types.Short),
        ('exchange', types.Shortstr),
        ('if_unused', types.Bit),
        ('no_wait', types.Bit),
    )

    synchronous = True
    content = False

    def __init__(self, *,
                 reserved_1=None,
                 exchange,
                 if_unused,
                 no_wait):
        super().__init__(
            types.Short(),
            exchange,
            if_unused,
            no_wait,
        )


class ExchangeDeleteOK(Method):
    """This method confirms the deletion of an exchange.
    """
    method_type = (40, 21)

    field_info = ()

    synchronous = True
    content = False


class ExchangeBind(Method):
    """This method binds an exchange to an exchange.

    Arguments:
        reserved_1: Short
        destination: Shortstr
        source: Shortstr
        routing_key: Shortstr
        no_wait: Bit
        arguments: Table
    """
    method_type = (40, 30)

    field_info = (
        ('reserved_1', types.Short),
        ('destination', types.Shortstr),
        ('source', types.Shortstr),
        ('routing_key', types.Shortstr),
        ('no_wait', types.Bit),
        ('arguments', types.Table),
    )

    synchronous = True
    content = False

    def __init__(self, *,
                 reserved_1=None,
                 destination,
                 source,
                 routing_key,
                 no_wait,
                 arguments):
        super().__init__(
            types.Short(),
            destination,
            source,
            routing_key,
            no_wait,
            arguments,
        )


class ExchangeBindOK(Method):
    """This method confirms that the bind was successful.
    """
    method_type = (40, 31)

    field_info = ()

    synchronous = True
    content = False


class ExchangeUnbind(Method):
    """This method unbinds an exchange from an exchange.

    Arguments:
        reserved_1: Short
        destination: Shortstr
        source: Shortstr
        routing_key: Shortstr
        no_wait: Bit
        arguments: Table
    """
    method_type = (40, 40)

    field_info = (
        ('reserved_1', types.Short),
        ('destination', types.Shortstr),
        ('source', types.Shortstr),
        ('routing_key', types.Shortstr),
        ('no_wait', types.Bit),
        ('arguments', types.Table),
    )

    synchronous = True
    content = False

    def __init__(self, *,
                 reserved_1=None,
                 destination,
                 source,
                 routing_key,
                 no_wait,
                 arguments):
        super().__init__(
            types.Short(),
            destination,
            source,
            routing_key,
            no_wait,
            arguments,
        )


class ExchangeUnbindOK(Method):
    """This method confirms that the unbind was successful.
    """
    method_type = (40, 51)

    field_info = ()

    synchronous = True
    content = False


class QueueDeclare(Method):
    """This method creates or checks a queue. When creating a new queue the
    client can specify various properties that control the durability of the
    queue and its contents, and the level of sharing for the queue.

    Arguments:
        reserved_1: Short
        queue: Shortstr
        passive: Bit
        durable: Bit
        exclusive: Bit
        auto_delete: Bit
        no_wait: Bit
        arguments: Table
    """
    method_type = (50, 10)

    field_info = (
        ('reserved_1', types.Short),
        ('queue', types.Shortstr),
        ('passive', types.Bit),
        ('durable', types.Bit),
        ('exclusive', types.Bit),
        ('auto_delete', types.Bit),
        ('no_wait', types.Bit),
        ('arguments', types.Table),
    )

    synchronous = True
    content = False

    def __init__(self, *,
                 reserved_1=None,
                 queue,
                 passive,
                 durable,
                 exclusive,
                 auto_delete,
                 no_wait,
                 arguments):
        super().__init__(
            types.Short(),
            queue,
            passive,
            durable,
            exclusive,
            auto_delete,
            no_wait,
            arguments,
        )


class QueueDeclareOK(Method):
    """This method confirms a Declare method and confirms the name of the
    queue, essential for automatically-named queues.

    Arguments:
        queue: Shortstr
        message_count: Long
        consumer_count: Long
    """
    method_type = (50, 11)

    field_info = (
        ('queue', types.Shortstr),
        ('message_count', types.Long),
        ('consumer_count', types.Long),
    )

    synchronous = True
    content = False

    def __init__(self, *,
                 queue,
                 message_count,
                 consumer_count):
        super().__init__(
            queue,
            message_count,
            consumer_count,
        )


class QueueBind(Method):
    """This method binds a queue to an exchange. Until a queue is bound it
    will not receive any messages. In a classic messaging model,
    store-and-forward queues are bound to a direct exchange and subscription
    queues are bound to a topic exchange.

    Arguments:
        reserved_1: Short
        queue: Shortstr
        exchange: Shortstr
        routing_key: Shortstr
        no_wait: Bit
        arguments: Table
    """
    method_type = (50, 20)

    field_info = (
        ('reserved_1', types.Short),
        ('queue', types.Shortstr),
        ('exchange', types.Shortstr),
        ('routing_key', types.Shortstr),
        ('no_wait', types.Bit),
        ('arguments', types.Table),
    )

    synchronous = True
    content = False

    def __init__(self, *,
                 reserved_1=None,
                 queue,
                 exchange,
                 routing_key,
                 no_wait,
                 arguments):
        super().__init__(
            types.Short(),
            queue,
            exchange,
            routing_key,
            no_wait,
            arguments,
        )


class QueueBindOK(Method):
    """This method confirms that the bind was successful.
    """
    method_type = (50, 21)

    field_info = ()

    synchronous = True
    content = False


class QueueUnbind(Method):
    """This method unbinds a queue from an exchange.

    Arguments:
        reserved_1: Short
        queue: Shortstr
        exchange: Shortstr
        routing_key: Shortstr
        arguments: Table
    """
    method_type = (50, 50)

    field_info = (
        ('reserved_1', types.Short),
        ('queue', types.Shortstr),
        ('exchange', types.Shortstr),
        ('routing_key', types.Shortstr),
        ('arguments', types.Table),
    )

    synchronous = True
    content = False

    def __init__(self, *,
                 reserved_1=None,
                 queue,
                 exchange,
                 routing_key,
                 arguments):
        super().__init__(
            types.Short(),
            queue,
            exchange,
            routing_key,
            arguments,
        )


class QueueUnbindOK(Method):
    """This method confirms that the unbind was successful.
    """
    method_type = (50, 51)

    field_info = ()

    synchronous = True
    content = False


class QueuePurge(Method):
    """This method removes all messages from a queue which are not awaiting
    acknowledgment.

    Arguments:
        reserved_1: Short
        queue: Shortstr
        no_wait: Bit
    """
    method_type = (50, 30)

    field_info = (
        ('reserved_1', types.Short),
        ('queue', types.Shortstr),
        ('no_wait', types.Bit),
    )

    synchronous = True
    content = False

    def __init__(self, *,
                 reserved_1=None,
                 queue,
                 no_wait):
        super().__init__(
            types.Short(),
            queue,
            no_wait,
        )


class QueuePurgeOK(Method):
    """This method confirms the purge of a queue.

    Arguments:
        message_count: Long
    """
    method_type = (50, 31)

    field_info = (
        ('message_count', types.Long),
    )

    synchronous = True
    content = False

    def __init__(self, *,
                 message_count):
        super().__init__(
            message_count,
        )


class QueueDelete(Method):
    """This method deletes a queue. When a queue is deleted any pending
    messages are sent to a dead-letter queue if this is defined in the server
    configuration, and all consumers on the queue are cancelled.

    Arguments:
        reserved_1: Short
        queue: Shortstr
        if_unused: Bit
        if_empty: Bit
        no_wait: Bit
    """
    method_type = (50, 40)

    field_info = (
        ('reserved_1', types.Short),
        ('queue', types.Shortstr),
        ('if_unused', types.Bit),
        ('if_empty', types.Bit),
        ('no_wait', types.Bit),
    )

    synchronous = True
    content = False

    def __init__(self, *,
                 reserved_1=None,
                 queue,
                 if_unused,
                 if_empty,
                 no_wait):
        super().__init__(
            types.Short(),
            queue,
            if_unused,
            if_empty,
            no_wait,
        )


class QueueDeleteOK(Method):
    """This method confirms the deletion of a queue.

    Arguments:
        message_count: Long
    """
    method_type = (50, 41)

    field_info = (
        ('message_count', types.Long),
    )

    synchronous = True
    content = False

    def __init__(self, *,
                 message_count):
        super().__init__(
            message_count,
        )


class BasicQos(Method):
    """This method requests a specific quality of service. The QoS can be
    specified for the current channel or for all channels on the connection.
    The particular properties and semantics of a qos method always depend on
    the content class semantics. Though the qos method could in principle apply
    to both peers, it is currently meaningful only for the server.

    Arguments:
        prefetch_size: Long
        prefetch_count: Short
        global: Bit
    """
    method_type = (60, 10)

    field_info = (
        ('prefetch_size', types.Long),
        ('prefetch_count', types.Short),
        ('global', types.Bit),
    )

    synchronous = True
    content = False

    def __init__(self, *,
                 prefetch_size,
                 prefetch_count,
                 global_):
        super().__init__(
            prefetch_size,
            prefetch_count,
            global_,
        )


class BasicQosOK(Method):
    """This method tells the client that the requested QoS levels could be
    handled by the server. The requested QoS applies to all active consumers
    until a new QoS is defined.
    """
    method_type = (60, 11)

    field_info = ()

    synchronous = True
    content = False


class BasicConsume(Method):
    """This method asks the server to start a "consumer", which is a transient
    request for messages from a specific queue. Consumers last as long as the
    channel they were declared on, or until the client cancels them.

    Arguments:
        reserved_1: Short
        queue: Shortstr
        consumer_tag: Shortstr
        no_local: Bit
        no_ack: Bit
        exclusive: Bit
        no_wait: Bit
        arguments: Table
    """
    method_type = (60, 20)

    field_info = (
        ('reserved_1', types.Short),
        ('queue', types.Shortstr),
        ('consumer_tag', types.Shortstr),
        ('no_local', types.Bit),
        ('no_ack', types.Bit),
        ('exclusive', types.Bit),
        ('no_wait', types.Bit),
        ('arguments', types.Table),
    )

    synchronous = True
    content = False

    def __init__(self, *,
                 reserved_1=None,
                 queue,
                 consumer_tag,
                 no_local,
                 no_ack,
                 exclusive,
                 no_wait,
                 arguments):
        super().__init__(
            types.Short(),
            queue,
            consumer_tag,
            no_local,
            no_ack,
            exclusive,
            no_wait,
            arguments,
        )


class BasicConsumeOK(Method):
    """The server provides the client with a consumer tag, which is used by
    the client for methods called on the consumer at a later stage.

    Arguments:
        consumer_tag: Shortstr
    """
    method_type = (60, 21)

    field_info = (
        ('consumer_tag', types.Shortstr),
    )

    synchronous = True
    content = False

    def __init__(self, *,
                 consumer_tag):
        super().__init__(
            consumer_tag,
        )


class BasicCancel(Method):
    """This method cancels a consumer. This does not affect already delivered
    messages, but it does mean the server will not send any more messages for
    that consumer. The client may receive an arbitrary number of messages in
    between sending the cancel method and receiving the cancel-ok reply. It may
    also be sent from the server to the client in the event of the consumer
    being unexpectedly cancelled (i.e. cancelled for any reason other than the
    server receiving the corresponding basic.cancel from the client). This
    allows clients to be notified of the loss of consumers due to events such
    as queue deletion. Note that as it is not a MUST for clients to accept this
    method from the client, it is advisable for the broker to be able to
    identify those clients that are capable of accepting the method, through
    some means of capability negotiation.

    Arguments:
        consumer_tag: Shortstr
        no_wait: Bit
    """
    method_type = (60, 30)

    field_info = (
        ('consumer_tag', types.Shortstr),
        ('no_wait', types.Bit),
    )

    synchronous = True
    content = False

    def __init__(self, *,
                 consumer_tag,
                 no_wait):
        super().__init__(
            consumer_tag,
            no_wait,
        )


class BasicCancelOK(Method):
    """This method confirms that the cancellation was completed.

    Arguments:
        consumer_tag: Shortstr
    """
    method_type = (60, 31)

    field_info = (
        ('consumer_tag', types.Shortstr),
    )

    synchronous = True
    content = False

    def __init__(self, *,
                 consumer_tag):
        super().__init__(
            consumer_tag,
        )


class BasicPublish(Method):
    """This method publishes a message to a specific exchange. The message
    will be routed to queues as defined by the exchange configuration and
    distributed to any active consumers when the transaction, if any, is
    committed.

    Arguments:
        reserved_1: Short
        exchange: Shortstr
        routing_key: Shortstr
        mandatory: Bit
        immediate: Bit
    """
    method_type = (60, 40)

    field_info = (
        ('reserved_1', types.Short),
        ('exchange', types.Shortstr),
        ('routing_key', types.Shortstr),
        ('mandatory', types.Bit),
        ('immediate', types.Bit),
    )

    synchronous = False
    content = True

    def __init__(self, *,
                 reserved_1=None,
                 exchange,
                 routing_key,
                 mandatory,
                 immediate):
        super().__init__(
            types.Short(),
            exchange,
            routing_key,
            mandatory,
            immediate,
        )


class BasicReturn(Method):
    """This method returns an undeliverable message that was published with
    the "immediate" flag set, or an unroutable message published with the
    "mandatory" flag set. The reply code and text provide information about the
    reason that the message was undeliverable.

    Arguments:
        reply_code: Short
        reply_text: Shortstr
        exchange: Shortstr
        routing_key: Shortstr
    """
    method_type = (60, 50)

    field_info = (
        ('reply_code', types.Short),
        ('reply_text', types.Shortstr),
        ('exchange', types.Shortstr),
        ('routing_key', types.Shortstr),
    )

    synchronous = False
    content = True

    def __init__(self, *,
                 reply_code,
                 reply_text,
                 exchange,
                 routing_key):
        super().__init__(
            reply_code,
            reply_text,
            exchange,
            routing_key,
        )


class BasicDeliver(Method):
    """This method delivers a message to the client, via a consumer. In the
    asynchronous message delivery model, the client starts a consumer using the
    Consume method, then the server responds with Deliver methods as and when
    messages arrive for that consumer.

    Arguments:
        consumer_tag: Shortstr
        delivery_tag: Longlong
        redelivered: Bit
        exchange: Shortstr
        routing_key: Shortstr
    """
    method_type = (60, 60)

    field_info = (
        ('consumer_tag', types.Shortstr),
        ('delivery_tag', types.Longlong),
        ('redelivered', types.Bit),
        ('exchange', types.Shortstr),
        ('routing_key', types.Shortstr),
    )

    synchronous = False
    content = True

    def __init__(self, *,
                 consumer_tag,
                 delivery_tag,
                 redelivered,
                 exchange,
                 routing_key):
        super().__init__(
            consumer_tag,
            delivery_tag,
            redelivered,
            exchange,
            routing_key,
        )


class BasicGet(Method):
    """This method provides a direct access to the messages in a queue using a
    synchronous dialogue that is designed for specific types of application
    where synchronous functionality is more important than performance.

    Arguments:
        reserved_1: Short
        queue: Shortstr
        no_ack: Bit
    """
    method_type = (60, 70)

    field_info = (
        ('reserved_1', types.Short),
        ('queue', types.Shortstr),
        ('no_ack', types.Bit),
    )

    synchronous = True
    content = False

    def __init__(self, *,
                 reserved_1=None,
                 queue,
                 no_ack):
        super().__init__(
            types.Short(),
            queue,
            no_ack,
        )


class BasicGetOK(Method):
    """This method delivers a message to the client following a get method. A
    message delivered by 'get-ok' must be acknowledged unless the no-ack option
    was set in the get method.

    Arguments:
        delivery_tag: Longlong
        redelivered: Bit
        exchange: Shortstr
        routing_key: Shortstr
        message_count: Long
    """
    method_type = (60, 71)

    field_info = (
        ('delivery_tag', types.Longlong),
        ('redelivered', types.Bit),
        ('exchange', types.Shortstr),
        ('routing_key', types.Shortstr),
        ('message_count', types.Long),
    )

    synchronous = True
    content = True

    def __init__(self, *,
                 delivery_tag,
                 redelivered,
                 exchange,
                 routing_key,
                 message_count):
        super().__init__(
            delivery_tag,
            redelivered,
            exchange,
            routing_key,
            message_count,
        )


class BasicGetEmpty(Method):
    """This method tells the client that the queue has no messages available
    for the client.

    Arguments:
        reserved_1: Shortstr
    """
    method_type = (60, 72)

    field_info = (
        ('reserved_1', types.Shortstr),
    )

    synchronous = True
    content = False

    def __init__(self, *,
                 reserved_1=None):
        super().__init__(
            types.Shortstr(),
        )


class BasicAck(Method):
    """When sent by the client, this method acknowledges one or more messages
    delivered via the Deliver or Get-Ok methods. When sent by server, this
    method acknowledges one or more messages published with the Publish method
    on a channel in confirm mode. The acknowledgement can be for a single
    message or a set of messages up to and including a specific message.

    Arguments:
        delivery_tag: Longlong
        multiple: Bit
    """
    method_type = (60, 80)

    field_info = (
        ('delivery_tag', types.Longlong),
        ('multiple', types.Bit),
    )

    synchronous = False
    content = False

    def __init__(self, *,
                 delivery_tag,
                 multiple):
        super().__init__(
            delivery_tag,
            multiple,
        )


class BasicReject(Method):
    """This method allows a client to reject a message. It can be used to
    interrupt and cancel large incoming messages, or return untreatable
    messages to their original queue.

    Arguments:
        delivery_tag: Longlong
        requeue: Bit
    """
    method_type = (60, 90)

    field_info = (
        ('delivery_tag', types.Longlong),
        ('requeue', types.Bit),
    )

    synchronous = False
    content = False

    def __init__(self, *,
                 delivery_tag,
                 requeue):
        super().__init__(
            delivery_tag,
            requeue,
        )


class BasicRecoverAsync(Method):
    """This method asks the server to redeliver all unacknowledged messages on
    a specified channel. Zero or more messages may be redelivered. This method
    is deprecated in favour of the synchronous Recover/Recover-Ok.

    Arguments:
        requeue: Bit
    """
    method_type = (60, 100)

    field_info = (
        ('requeue', types.Bit),
    )

    synchronous = False
    content = False

    def __init__(self, *,
                 requeue):
        super().__init__(
            requeue,
        )


class BasicRecover(Method):
    """This method asks the server to redeliver all unacknowledged messages on
    a specified channel. Zero or more messages may be redelivered. This method
    replaces the asynchronous Recover.

    Arguments:
        requeue: Bit
    """
    method_type = (60, 110)

    field_info = (
        ('requeue', types.Bit),
    )

    synchronous = False
    content = False

    def __init__(self, *,
                 requeue):
        super().__init__(
            requeue,
        )


class BasicRecoverOK(Method):
    """This method acknowledges a Basic.Recover method.
    """
    method_type = (60, 111)

    field_info = ()

    synchronous = True
    content = False


class BasicNack(Method):
    """This method allows a client to reject one or more incoming messages. It
    can be used to interrupt and cancel large incoming messages, or return
    untreatable messages to their original queue. This method is also used by
    the server to inform publishers on channels in confirm mode of unhandled
    messages. If a publisher receives this method, it probably needs to
    republish the offending messages.

    Arguments:
        delivery_tag: Longlong
        multiple: Bit
        requeue: Bit
    """
    method_type = (60, 120)

    field_info = (
        ('delivery_tag', types.Longlong),
        ('multiple', types.Bit),
        ('requeue', types.Bit),
    )

    synchronous = False
    content = False

    def __init__(self, *,
                 delivery_tag,
                 multiple,
                 requeue):
        super().__init__(
            delivery_tag,
            multiple,
            requeue,
        )


class TxSelect(Method):
    """This method sets the channel to use standard transactions. The client
    must use this method at least once on a channel before using the Commit or
    Rollback methods.
    """
    method_type = (90, 10)

    field_info = ()

    synchronous = True
    content = False


class TxSelectOK(Method):
    """This method confirms to the client that the channel was successfully
    set to use standard transactions.
    """
    method_type = (90, 11)

    field_info = ()

    synchronous = True
    content = False


class TxCommit(Method):
    """This method commits all message publications and acknowledgments
    performed in the current transaction. A new transaction starts immediately
    after a commit.
    """
    method_type = (90, 20)

    field_info = ()

    synchronous = True
    content = False


class TxCommitOK(Method):
    """This method confirms to the client that the commit succeeded. Note that
    if a commit fails, the server raises a channel exception.
    """
    method_type = (90, 21)

    field_info = ()

    synchronous = True
    content = False


class TxRollback(Method):
    """This method abandons all message publications and acknowledgments
    performed in the current transaction. A new transaction starts immediately
    after a rollback. Note that unacked messages will not be automatically
    redelivered by rollback; if that is required an explicit recover call
    should be issued.
    """
    method_type = (90, 30)

    field_info = ()

    synchronous = True
    content = False


class TxRollbackOK(Method):
    """This method confirms to the client that the rollback succeeded. Note
    that if an rollback fails, the server raises a channel exception.
    """
    method_type = (90, 31)

    field_info = ()

    synchronous = True
    content = False


class ConfirmSelect(Method):
    """This method sets the channel to use publisher acknowledgements. The
    client can only use this method on a non-transactional channel.

    Arguments:
        nowait: Bit
    """
    method_type = (85, 10)

    field_info = (
        ('nowait', types.Bit),
    )

    synchronous = True
    content = False

    def __init__(self, *,
                 nowait):
        super().__init__(
            nowait,
        )


class ConfirmSelectOK(Method):
    """This method confirms to the client that the channel was successfully
    set to use publisher acknowledgements.
    """
    method_type = (85, 11)

    field_info = ()

    synchronous = True
    content = False


# Method type -> class dispatch table
METHODS = {cls.method_type: cls for cls in Method.__subclasses__()}

__all__ = list(locals())