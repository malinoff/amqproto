"""
amqproto.channel
~~~~~~~~~~~~~~~~

AMQP channels.
"""

# Pylint can't handle attrs magic.
# pylint: disable=attribute-defined-outside-init,no-member

import io
import uuid
import warnings
from collections import deque

import attr

from . import methods
from . import replies
from .frames import Frame
from .settings import Settings
from .content import BasicContent


def chunker(iterable, n):  # pylint: disable=invalid-name
    """Group data into n-sized chunks or blocks."""
    # chunker('ABCDEFG', 3) --> ['ABC', 'DEF', 'G']
    for idx in range(0, len(iterable), n):
        yield iterable[idx:idx+n]


@attr.s()
class BaseChannel:
    """In AMQP, a connection is actually a channel (channel_id == 0),
    so it's natural they share some logic. This class implements such
    shared logic.
    """

    channel_id: int = attr.ib(default=0)

    server_settings: Settings = attr.ib(
        default=attr.Factory(lambda: Settings(type='server')),
        init=False,
    )

    negotiated_settings: Settings = attr.ib(
        default=attr.Factory(lambda: Settings(type='negotiated')),
        init=False,
    )

    protocol_version = attr.ib(default=(0, 9, 1), init=False)

    closed = attr.ib(default=True, init=False)

    def __attrs_post_init__(self):
        # The buffer to accumulate all bytes required to be sent.
        self._outbound_buffer = io.BytesIO()
        # Reference to the last received method waiting for its content.
        self._content_waiter = None

    def capable_of(self, method: methods.Method, capability: str):
        """Check if the server is capable of the method.
        Can be used to prevent client implementations from sending
        unsupported methods.
        """
        capabilities = self.server_settings.properties['capabilities']
        if not capabilities.get(capability, False):
            raise replies.NotImplemented(
                'the server does not support {}'.format(capability),
                method.class_id, method.method_id,
            )

    def data_to_send(self) -> bytes:
        """Returns some data to send."""
        data = self._outbound_buffer.getvalue()
        if data:
            # Avoid unnecessary reallocations if there is nothing to send
            self._outbound_buffer = io.BytesIO()
        return data

    def _handle_method(self, method):
        # We assume that the server won't send us methods we don't explicitly
        # declare as supported via client properties, so there's no
        # additional check here.
        method.channel_id = self.channel_id
        to_return = []
        if self._content_waiter is not None:
            # The server decided to stop sending the content
            waiter, self._content_waiter = self._content_waiter, None
            to_return.append(waiter)
        if not method.followed_by_content:
            to_return.append(method)
        else:
            self._content_waiter = method
        return to_return

    def _handle_content_header(self, payload):
        content = BasicContent(
            body=b'', delivery_info=self._content_waiter, **payload
        )
        self._content_waiter.content = content
        if content.complete():
            # An empty body
            waiter, self._content_waiter = self._content_waiter, None
            return [waiter]
        return []

    def _handle_content_body(self, payload):
        waiter = self._content_waiter
        waiter.content.body += payload
        if waiter.content.complete():
            waiter, self._content_waiter = self._content_waiter, None
            return [waiter]
        return []

    def _prepare_for_sending(self, method):
        """Prepare the method for sending (including the content,
        if there is one).
        """
        Frame.build_stream({
            'frame_type': 'method',
            'channel_id': self.channel_id,
            'payload': method,
        }, self._outbound_buffer)
        if not method.followed_by_content:
            return
        Frame.build_stream({
            'frame_type': 'content_header',
            'channel_id': self.channel_id,
            'payload': {
                'class_id': method.content.class_id,
                'body_size': method.content.body_size,
                'properties': attr.asdict(method.content.properties),
            }
        }, self._outbound_buffer)
        # 8 is the frame metadata size:
        # frame_type is UnsignedByte (1),
        # channel_id is UnsignedShort (1 + 2)
        # payload length is UnsignedLong (3 + 4)
        # frame_end is UnsignedByte (7 + 1)
        max_frame_size = self.negotiated_settings.frame_max - 8
        for chunk in chunker(method.content.body, max_frame_size):
            Frame.build_stream({
                'frame_type': 'content_body',
                'channel_id': self.channel_id,
                'payload': {'content': chunk},
            }, self._outbound_buffer)


@attr.s()
class Channel(BaseChannel):
    """Sans-I/O implementation of AMQP channels.
    Maintains per-channel state.

    :param channel_id: channel id, unique within a single connection.
    """

    flow_active: bool = attr.ib(default=True, init=False)
    transaction_active: bool = attr.ib(default=False, init=False)
    publisher_confirms_active: bool = attr.ib(default=False, init=False)

    def __attrs_post_init__(self):
        super().__attrs_post_init__()
        # A list of delivered messages, either via
        # BasicDeliver or BasicReturn.
        self._delivered_messages = deque()
        # Set of all active consumer tags.
        self._consumers = set()
        # Mapping (delivery tag -> content) of unconfirmed messages.
        self._unconfirmed_messages = {}
        self._next_delivery_tag = 1
        # A list of messages that have been explicitly nacked by the server.
        self._nacked_messages = deque()

        self._method_handlers = {
            methods.ChannelOpenOK: self._handle_channel_open_ok,
            methods.ChannelCloseOK: self._handle_channel_close_ok,
            methods.ChannelClose: self._handle_channel_close,
            methods.ChannelFlow: self._handle_channel_flow,
            methods.ChannelFlowOK: self._handle_channel_flow_ok,
            methods.BasicConsumeOK: self._handle_basic_consume_ok,
            methods.BasicCancel: self._handle_basic_cancel,
            methods.BasicCancelOK: self._handle_basic_cancel_ok,
            methods.BasicReturn: self._handle_basic_return,
            methods.BasicDeliver: self._handle_basic_deliver,
            methods.BasicAck: self._handle_basic_ack,
            methods.BasicNack: self._handle_basic_nack,
            methods.TxSelectOK: self._handle_tx_select_ok,
            methods.ConfirmSelectOK: self._handle_confirm_select_ok,
        }

    def _channel_open(self):
        method = methods.ChannelOpen()
        return self._prepare_for_sending(method)

    def _handle_channel_open_ok(self, method):
        # pylint: disable=unused-argument
        self.closed = False

    def _channel_close(self, reply_code, reply_text, class_id, method_id):
        method = methods.ChannelClose(
            reply_code, reply_text, class_id, method_id
        )
        self.closed = True
        return self._prepare_for_sending(method)

    def _handle_channel_close_ok(self, method):
        # pylint: disable=unused-argument
        self.closed = True

    def _handle_channel_close(self, method):
        method = methods.ChannelCloseOK()
        self.closed = True
        return self._prepare_for_sending(method)

    def channel_flow(self, active: bool):
        """This method asks the peer to pause or restart the flow of content
        data sent by a consumer. This is a simple flow-control mechanism that
        a peer can use to avoid overflowing its queues or otherwise finding
        itself receiving more messages than it can process. Note that this
        method is not intended for window control. It does not affect contents
        returned by BasicGetOk methods.

        :param active: If ``True``, the peer starts sending content frames.
            If ``False, the peer stops sending content frames.
        """
        method = methods.ChannelFlow(active)
        return self._prepare_for_sending(method)

    def _handle_channel_flow(self, method):
        self.flow_active = method.active

    def _handle_channel_flow_ok(self, method):
        self.flow_active = method.active

    def exchange_declare(self, exchange,
                         type='direct',  # pylint: disable=redefined-builtin
                         passive=False, durable=False, auto_delete=False,
                         internal=False, no_wait=False, arguments=None):
        """This method creates an exchange if it does not already exist,
        and if the exchange exists, verifies that it is of the correct
        and expected class.

        :param exchange: Specifies the name of the exchange to declare.
            Note that exchange names starting with "amq." are reserved for
            pre-declared and standardised exchanges.

        :param type: Each exchange belongs to one of a set of exchange types
            implemented by the server. The exchange types define
            the functionality of the exchange - i.e. how messages are routed
            through it. It is not valid or meaningful to attempt to change
            the type of an existing exchange.

        :param passive: If set, the server will reply with ExchangeDeclareOk
            if the exchange already exists with the same name, and raise
            an error if not. The client can use this to check whether
            an exchange exists without modifying the server state. When set,
            all other method fields except name and no-wait are ignored.
            A declare with both passive and no-wait has no effect. Arguments
            are compared for semantic equivalence.

        :param durable: If set when creating a new exchange, the exchange
            will be marked as durable. Durable exchanges remain active
            when a server restarts. Non-durable exchanges (transient exchanges)
            are purged if/when a server restarts.

        :param auto_delete: If set, the exchange is deleted when all queues
            have finished using it.

        :param internal: If set, the exchange may not be used directly by
            publishers, but only when bound to other exchanges.
            Internal exchanges are used to construct wiring that is not visible
            to applications.

        :param no_wait: If set, the server will not respond to the method.
            If the server could not complete the method it will raise
            a channel or connection exception.

        :param arguments: A dict of arguments for the declaration. The syntax
            and semantics of these arguments depends on the server
            implementation.
        """
        method = methods.ExchangeDeclare(
            exchange, type, passive, durable, auto_delete,
            internal, no_wait, arguments,
        )
        return self._prepare_for_sending(method)

    def exchange_delete(self, exchange, if_unused=False, no_wait=False):
        """This method deletes an exchange. When an exchange is deleted
        all queue bindings on the exchange are cancelled.

        :param exchange: Specifies the name of the exchange to delete.

        :param if_unused: If set, the server will only delete the exchange
            if it has no queue bindings. If the exchange has queue bindings
            the server does not delete it but raises a channel exception
            instead.

        :param no_wait: If set, the server will not respond to the method.
            If the server could not complete the method it will raise
            a channel or connection exception.
        """
        method = methods.ExchangeDelete(exchange, if_unused, no_wait)
        return self._prepare_for_sending(method)

    def exchange_bind(self, source, destination, routing_key='',
                      no_wait=False, arguments=None):
        """This method binds an exchange to an exchange.

        :param source: Specifies the name of the source exchange to bind.

        :param destination: Specifies the name of the destination exchange
            to bind.

        :param routing_key: Specifies the routing key for the binding.
            The routing key is used for routing messages depending on
            the exchange configuration. Not all exchanges use a routing
            key - refer to the specific exchange documentation.

        :param no_wait: If set, the server will not respond to the method.
            If the server could not complete the method it will raise
            a channel or connection exception.

        :param arguments: A dict of arguments for the binding. The syntax and
            semantics of these arguments depends on the server implementation.
        """
        self.capable_of(methods.ExchangeBind, 'exchange_exchange_bindings')
        method = methods.ExchangeBind(
            destination, source, routing_key, no_wait, arguments,
        )
        return self._prepare_for_sending(method)

    def exchange_unbind(self, source, destination, routing_key='',
                        no_wait=False, arguments=None):
        """This method unbinds an exchange from an exchange.

        :param source: Specifies the name of the source exchange to unbind.

        :param destination: Specifies the name of the destination exchange
            to unbind.

        :param routing_key: Specifies the routing key of the binding to unbind.

        :param no_wait: If set, the server will not respond to the method.
            If the server could not complete the method it will raise
            a channel or connection exception.

        :param arguments: Specifies the arguments of the binding to unbind.
        """
        self.capable_of(methods.ExchangeUnbind, 'exchange_exchange_bindings')
        method = methods.ExchangeUnbind(
            destination, source, routing_key, no_wait, arguments,
        )
        return self._prepare_for_sending(method)

    def queue_declare(self, queue, passive=False, durable=False,
                      exclusive=False, auto_delete=False, no_wait=False,
                      arguments=None):
        """This method creates or checks a queue. When creating a new queue
        the client can specify various properties that control the durability
        of the queue and its contents, and the level of sharing for the queue.

        :param queue: Specifies the name of the Queue to declare.

        :param passive: If set, the server will reply with QeueuDeclareOk
            if the queue already exists with the same name, and raise an error
            if not. The client can use this to check whether a queue exists
            without modifying the server state. When set, all other method
            fields except name and no_wait are ignored. A declare with both
            passive and no-wait has no effect. Arguments are compared for
            semantic equivalence.

        :param durable: If set when creating a new queue, the queue will be
            marked as durable. Durable queues remain active when a server
            restarts. Non-durable queues (transient queues) are purged
            if/when a server restarts. Note that durable queues do not
            necessarily hold persistent messages, although it does not
            make sense to send persistent messages to a transient queue.

        :param exclusive: Exclusive queues may only be accessed by the current
            connection, and are deleted when that connection closes.
            Passive declaration of an exclusive queue by other connections
            are not allowed.

        :param auto_delete: If set, the queue is deleted when all consumers
            have finished using it. The last consumer can be cancelled either
            explicitly or because its channel is closed. If there was no
            consumer ever on the queue, it won't be deleted. Applications can
            explicitly delete auto-delete queues using the QueueDelete method
            as normal.

        :param no_wait: If set, the server will not respond to the method.
            If the server could not complete the method it will raise
            a channel or connection exception.

        :param arguments: A dict of arguments for the declaration. The syntax
            and semantics of these arguments depends on the server
            implementation.
        """
        method = methods.QueueDeclare(
            queue, passive, durable, exclusive, auto_delete, no_wait, arguments
        )
        return self._prepare_for_sending(method)

    def queue_bind(self, queue, exchange='', routing_key='', no_wait=False,
                   arguments=None):
        """This method binds a queue to an exchange. Until a queue is bound
        it will not receive any messages. In a classic messaging model,
        store-and-forward queues are bound to a direct exchange and
        subscription queues are bound to a topic exchange.

        :param queue: Specifies the name of the queue to bind.

        :param exchange: Specifies the name of the exchange to bind
            the queue to. Empty string means the default exchange.

        :param routing_key: Specifies the routing key for the binding.
            The routing key is used for routing messages depending on
            the exchange configuration. Not all exchanges use a routing
            key - refer to the specific exchange documentation.
            If the queue name is empty, the server uses the last queue
            declared on the channel. If the routing key is also empty,
            the server uses this queue name for the routing key as well.
            If the queue name is provided but the routing key is empty,
            the server does the binding with that empty routing key.
            The meaning of empty routing keys depends on the exchange
            implementation.

        :param no_wait: If set, the server will not respond to the method.
            If the server could not complete the method it will raise
            a channel or connection exception.

        :param arguments: A dict of arguments for the binding. The syntax
            and semantics of these arguments depends on the server
            implementation.
        """
        method = methods.QueueBind(
            queue, exchange, routing_key, no_wait, arguments,
        )
        return self._prepare_for_sending(method)

    def queue_unbind(self, queue, exchange='', routing_key='', arguments=None):
        """This method unbinds a queue from an exchange.

        :param queue: Specifies the name of the queue to unbind.

        :param exchange: The name of the exchange to unbind the queue from.

        :param routing_key: Specifies the routing key of the binding to unbind.

        :param arguments: Specifies the arguments of the binding to unbind.
        """
        method = methods.QueueUnbind(queue, exchange, routing_key, arguments)
        return self._prepare_for_sending(method)

    def queue_purge(self, queue, no_wait=False):
        """This method removes all messages from a queue which are not awaiting
        acknowledgment.

        :param queue: Specifies the name of the queue to purge.

        :param no_wait: If set, the server will not respond to the method.
            If the server could not complete the method it will raise
            a channel or connection exception.
        """
        method = methods.QueuePurge(queue, no_wait)
        return self._prepare_for_sending(method)

    def queue_delete(self, queue, if_unused=False, if_empty=False,
                     no_wait=False):
        """This method deletes a queue. When a queue is deleted any pending
        messages are sent to a dead-letter queue if this is defined
        in the server configuration, and all consumers on the queue are
        cancelled.

        :param queue: Specifies the name of the queue to delete.

        :param if_unused: If set, the server will only delete the queue
            if it has no consumers. If the queue has consumers the server
            does does not delete it but raises a channel exception instead.

        :param if_empty: If set, the server will only delete the queue
            if it has no messages.

        :param no_wait: If set, the server will not respond to the method.
            If the server could not complete the method it will raise
            a channel or connection exception.
        """
        method = methods.QueueDelete(queue, if_unused, if_empty, no_wait)
        return self._prepare_for_sending(method)

    def basic_qos(self, prefetch_size=0, prefetch_count=0, global_=False):
        """This method requests a specific quality of service. The QoS can be
        specified for the current channel or for all channels
        on the connection. The particular properties and semantics of a qos
        method always depend on the content class semantics. Though the qos
        method could in principle apply to both peers, it is currently
        meaningful only for the server.

        :param prefetch_size: The client can request that messages be sent in
            advance so that when the client finishes processing a message,
            the following message is already held locally, rather than needing
            to be sent down the channel. Prefetching gives a performance
            improvement. This field specifies the prefetch window size
            in octets. The server will send a message in advance if it is equal
            to or smaller in size than the available prefetch size (and also
            falls into other prefetch limits). May be set to zero, meaning
            "no specific limit", although other prefetch limits may still
            apply. The prefetch_size is ignored if the no_ack option is set.

        :param prefetch_count: Specifies a prefetch window in terms of whole
            messages. This field may be used in combination with
            the prefetch_size field; a message will only be sent in advance
            if both prefetch windows (and those at the channel and connection
            level) allow it. The prefetch-count is ignored if the no-ack option
            is set.

        :param global_: RabbitMQ has reinterpreted this field. The original
            specification said: "By default the QoS settings apply to
            the current channel only. If this field is set, they are
            applied to the entire connection." Instead, RabbitMQ takes
            global_=False to mean that the QoS settings should apply
            per-consumer (for new consumers on the channel; existing
            ones being unaffected) and global_=True to mean that the QoS
            settings should apply per-channel.
        """
        method = methods.BasicQos(prefetch_size, prefetch_count, global_)
        return self._prepare_for_sending(method)

    def basic_consume(self, queue, consumer_tag=None, no_local=False,
                      no_ack=False, exclusive=False,
                      no_wait=False, arguments=None):
        """This method asks the server to start a "consumer", which is a
        transient request for messages from a specific queue. Consumers
        last as long as the channel they were declared on, or until
        the client cancels them.

        :param queue: Specifies the name of the queue to consume from.

        :param consumer_tag: Specifies the identifier for the consumer.
            The consumer tag is local to a channel, so two clients can use
            the same consumer tags. If this field is empty the server
            will generate a unique tag.

        :param no_local: If the no-local field is set the server will not
            send messages to the connection that published them.

        :param no_ack: If this field is set the server does not expect
            acknowledgements for messages. That is, when a message is
            delivered to the client the server assumes the delivery will
            succeed and immediately dequeues it. This functionality may
            increase performance but at the cost of reliability. Messages
            can get lost if a client dies before they are delivered
            to the application.

        :param exclusive: Request exclusive consumer access, meaning only
            this consumer can access the queue.

        :param no_wait: If set, the server will not respond to the method.
            If the server could not complete the method it will raise
            a channel or connection exception.

        :param arguments: A dict of arguments for the consume. The syntax
            and semantics of these arguments depends on the server
            implementation.
        """
        if consumer_tag == '' and no_wait:
            warnings.warn('creating consumer that cannot be cancelled, '
                          'consider passing a non-empty consumer_tag '
                          'or do not pass no_wait=True')
        if consumer_tag is None:
            consumer_tag = str(uuid.uuid4())
        method = methods.BasicConsume(
            queue, consumer_tag, no_local, no_ack,
            exclusive, no_wait, arguments,
        )
        if not method.has_response():
            self._consumers.add(consumer_tag)
        return self._prepare_for_sending(method)

    def _handle_basic_consume_ok(self, method):
        self._consumers.add(method.consumer_tag)

    def basic_cancel(self, consumer_tag, no_wait=False):
        """This method cancels a consumer. This does not affect already
        delivered messages, but it does mean the server will not send any
        more messages for that consumer. The client may receive an arbitrary
        number of messages in between sending the cancel method and receiving
        the BasicCancelOK reply.

        It may also be sent from the server to the client in the event
        of the consumer being unexpectedly cancelled (i.e. cancelled
        for any reason other than the server receiving the
        corresponding BasicCancel from the client). This allows
        clients to be notified of the loss of consumers due to events
        such as queue deletion. Note that as it is not a MUST for
        clients to accept this method from the server, it is advisable
        for the broker to be able to identify those clients that are
        capable of accepting the method, through some means of
        capability negotiation.

        :param consumer_tag: Specifies the identifier for the consumer
            to cancel.

        :param no_wait: If set, the server will not respond to the method.
            If the server could not complete the method it will raise
            a channel or connection exception.
        """
        method = methods.BasicCancel(consumer_tag, no_wait)
        if not method.has_response():
            self._consumers.remove(consumer_tag)
        return self._prepare_for_sending(method)

    def _handle_basic_cancel_ok(self, method):
        self._consumers.remove(method.consumer_tag)

    def _handle_basic_cancel(self, method):
        self._consumers.remove(method.consumer_tag)

    def basic_publish(self, content, exchange='', routing_key='',
                      mandatory=False, immediate=False):
        """This method publishes a message to a specific exchange.
        The message will be routed to queues as defined by the exchange
        configuration and distributed to any active consumers when
        the transaction, if any, is committed.

        :param content: Specifies the content to send.

        :param exchange: Specifies the name of the exchange to publish to.
            The exchange name can be empty, meaning the default exchange.
            If the exchange name is specified, and that exchange does not
            exist, the server will raise a channel exception.

        :param routing_key: Specifies the routing key for the message.
            The routing key is used for routing messages depending on
            the exchange configuration.

        :param mandatory: This flag tells the server how to react if
            the message cannot be routed to a queue. If this flag is set,
            the server will return an unroutable message with a Return method.
            If this flag is zero, the server silently drops the message.

        :param immediate: This flag tells the server how to react if
            the message cannot be routed to a queue consumer immediately.
            If this flag is set, the server will return an undeliverable
            message with a Return method. If this flag is zero, the server
            will queue the message, but with no guarantee that it will ever
            be consumed.
        """
        # Handle the simplest case of content being a (byte)string.
        if isinstance(content, str):
            content = content.encode('utf-8')
        if isinstance(content, bytes):
            content = BasicContent(body=content, body_size=len(content))
        method = methods.BasicPublish(
            exchange, routing_key, mandatory, immediate, content
        )
        if self.publisher_confirms_active:
            self._unconfirmed_messages[self._next_delivery_tag] = content
            self._next_delivery_tag += 1
        return self._prepare_for_sending(method)

    def _handle_basic_return(self, method):
        self._delivered_messages.append(method.content)

    def _handle_basic_deliver(self, method):
        self._delivered_messages.append(method.content)

    def basic_get(self, queue, no_ack=False):
        """This method provides a direct access to the messages in a queue
        using a synchronous dialogue that is designed for specific types of
        application where synchronous functionality is more important
        than performance.

        :param queue: Specifies the name of the queue to get a message from.

        :param no_ack: If this field is set the server does not expect
            acknowledgements for messages. That is, when a message is
            delivered to the client the server assumes the delivery will
            succeed and immediately dequeues it. This functionality may
            increase performance but at the cost of reliability. Messages
            can get lost if a client dies before they are delivered
            to the application.
        """
        method = methods.BasicGet(queue, no_ack)
        return self._prepare_for_sending(method)

    def basic_ack(self, delivery_tag, multiple=False):
        """This method acknowledges one or more messages delivered via
        the BasicDeliver or BasicGetOK methods.

        The acknowledgement can be for a single message or a set of
        messages up to and including a specific message.

        :param delivery_tag: The server-assigned and channel-specific
            delivery tag.

        :param multiple: If set, the delivery tag is treated as "up to and
            including", so that multiple messages can be acknowledged
            with a single method. If set to zero, the delivery tag
            refers to a single message. If the multiple field is set, and
            the delivery tag is zero, this indicates acknowledgement of
            all outstanding messages.
        """
        method = methods.BasicAck(delivery_tag, multiple)
        return self._prepare_for_sending(method)

    def _handle_basic_ack(self, method):
        if method.multiple:
            self._unconfirmed_messages = {
                tag: content
                for tag, content in self._unconfirmed_messages.items()
                if tag not in range(1, method.delivery_tag + 1)
            }
        else:
            self._unconfirmed_messages.pop(method.delivery_tag)

    def basic_reject(self, delivery_tag, requeue=False):
        """This method allows a client to reject a message.
        It can be used to interrupt and cancel large incoming messages,
        or return untreatable messages to their original queue.

        :param delivery_tag: The server-assigned and channel-specific
            delivery tag.

        :param requeue: If requeue is ``True``, the server will attempt to
            requeue the message. If requeue is ``False`` or the requeue
            attempt fails the messages are discarded or dead-lettered.
        """
        method = methods.BasicReject(delivery_tag, requeue)
        return self._prepare_for_sending(method)

    def basic_recover_async(self, requeue=False):
        """This method asks the server to redeliver all unacknowledged
        messages on a specified channel. Zero or more messages may be
        redelivered. This method is deprecated in favour of
        the synchronous BasicRecover.

        :param requeue: If this field is ``False``, the message will be
            redelivered to the original recipient. If this bit is ``True``,
            the server will attempt to requeue the message, potentially
            then delivering it to an alternative subscriber.
        """
        method = methods.BasicRecoverAsync(requeue)
        return self._prepare_for_sending(method)

    def basic_recover(self, requeue=False):
        """This method asks the server to redeliver all unacknowledged messages
        on a specified channel. Zero or more messages may be redelivered.
        This method replaces the BasicRecoverAsync.

        :param requeue: If this field is ``False``, the message will be
            redelivered to the original recipient. If this bit is ``True``,
            the server will attempt to requeue the message, potentially
            then delivering it to an alternative subscriber.
        """
        method = methods.BasicRecover(requeue)
        return self._prepare_for_sending(method)

    def basic_nack(self, delivery_tag, multiple=False, requeue=False):
        """This method allows a client to reject one or more incoming
        messages. It can be used to interrupt and cancel large incoming
        messages, or return untreatable messages to their original queue.

        This method is also used by the server to inform publishers on
        channels in confirm mode of unhandled messages. If a publisher
        receives this method, it probably needs to republish
        the offending messages.

        :param delivery_tag: The server-assigned and channel-specific
            delivery tag.

        :param multiple: If set, the delivery tag is treated as "up to and
            including", so that multiple messages can be acknowledged
            with a single method. If set to zero, the delivery tag
            refers to a single message. If the multiple field is set, and
            the delivery tag is zero, this indicates acknowledgement of
            all outstanding messages.

        :param requeue: If requeue is ``True``, the server will attempt to
            requeue the message. If requeue is ``False`` or the requeue
            attempt fails the messages are discarded or dead-lettered.
        """
        method = methods.BasicNack(delivery_tag, multiple, requeue)
        return self._prepare_for_sending(method)

    def _handle_basic_nack(self, method):
        self._nacked_messages.append(method.content)

    def tx_select(self):
        """This method sets the channel to use standard transactions.
        The client must use this method at least once on a channel
        before using the TxCommit or TxRollback methods.
        """
        assert not self.trasaction_active
        method = methods.TxSelect()
        return self._prepare_for_sending(method)

    def _handle_tx_select_ok(self):
        self.trasaction_active = True

    def tx_commit(self):
        """This method commits all message publications and acknowledgments
        performed in the current transaction. A new transaction starts
        immediately after a commit.
        """
        assert self.transaction_active
        method = methods.TxCommit()
        return self._prepare_for_sending(method)

    def tx_rollback(self):
        """This method abandons all message publications and acknowledgments
        performed in the current transaction. A new transaction starts
        immediately after a rollback. Note that unacked messages will not be
        automatically redelivered by rollback; if that is required
        an explicit recover call should be issued.
        """
        assert self.transaction_active
        method = methods.TxRollback()
        return self._prepare_for_sending(method)

    def confirm_select(self, no_wait=False):
        """This method sets the channel to use publisher acknowledgements.
        The client can only use this method on a non-transactional channel.
        """
        assert not self.publisher_confirms_active
        method = methods.ConfirmSelect(no_wait)
        if not method.has_response():
            self.publisher_confirms_active = True
        return self._prepare_for_sending(method)

    def _handle_confirm_select_ok(self, method):
        # pylint: disable=unused-argument
        self.publisher_confirms_active = True
