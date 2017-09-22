from .domains import *
from .frames import MethodPayload


class Connection:
    """Work with socket connections.

    The connection class provides methods for a client to establish a
    network connection to a server, and for both peers to operate the
    connection thereafter.

    Grammar:

      connection          = open-connection *use-connection close-connection
      open-connection     = C:protocol-header
                            S:START C:START-OK
                            *challenge
                            S:TUNE C:TUNE-OK
                            C:OPEN S:OPEN-OK
      challenge           = S:SECURE C:SECURE-OK
      use-connection      = *channel
      close-connection    = C:CLOSE S:CLOSE-OK
                          / S:CLOSE C:CLOSE-OK
    """

    class_id = 10


    def on_start(self,
                 version_major: Octet,
                 version_minor: Octet,
                 server_properties: PeerProperties,
                 mechanisms: Longstr,
                 locales: Longstr):
        """Start connection negotiation.

        This method starts the connection negotiation process by telling the
        client the protocol version that the server proposes, along with a list
        of security mechanisms which the client can use for authentication.
        """

    def start_ok(self,
                 client_properties: PeerProperties,
                 mechanism: Shortstr,
                 response: Longstr,
                 locale: Shortstr):
        """Select security mechanism and locale.

        This method selects a SASL security mechanism.
        """
        return MethodPayload(
            class_id=10,
            method_id=11,
            PeerProperties(client_properties),
            Shortstr(mechanism),
            Longstr(response),
            Shortstr(locale),
        )

    def on_secure(self,
                  challenge: Longstr):
        """Security mechanism challenge.

        The SASL protocol works by exchanging challenges and responses until
        both peers have received sufficient information to authenticate each
        other. This method challenges the client to provide more information.
        """

    def secure_ok(self,
                  response: Longstr):
        """Security mechanism response.

        This method attempts to authenticate, passing a block of SASL data for
        the security mechanism at the server side.
        """
        return MethodPayload(
            class_id=10,
            method_id=21,
            Longstr(response),
        )

    def on_tune(self,
                channel_max: Short,
                frame_max: Long,
                heartbeat: Short):
        """Propose connection tuning parameters.

        This method proposes a set of connection configuration values to the
        client. The client can accept and/or adjust these.
        """

    def tune_ok(self,
                channel_max: Short,
                frame_max: Long,
                heartbeat: Short):
        """Negotiate connection tuning parameters.

        This method sends the client's connection tuning parameters to the
        server. Certain fields are negotiated, others provide capability
        information.
        """
        return MethodPayload(
            class_id=10,
            method_id=31,
            Short(channel_max),
            Long(frame_max),
            Short(heartbeat),
        )

    def open(self,
             virtual_host: Path):
        """Open connection to virtual host.

        This method opens a connection to a virtual host, which is a collection
        of resources, and acts to separate multiple application domains within a
        server. The server may apply arbitrary limits per virtual host, such as
        the number of each type of entity that may be used, per connection
        and/or in total.
        """
        return MethodPayload(
            class_id=10,
            method_id=40,
            Path(virtual_host),
            Shortstr(None),
            Bit(None),
        )

    def on_open_ok(self,
                   reserved_1: Shortstr):
        """Signal that connection is ready.

        This method signals to the client that the connection is ready for use.
        """

    def on_close(self,
                 reply_code: ReplyCode,
                 reply_text: ReplyText,
                 class_id: ClassId,
                 method_id: MethodId):
        """Request a connection close.

        This method indicates that the sender wants to close the connection.
        This may be due to internal conditions (e.g. a forced shut-down) or due
        to an error handling a specific method, i.e. an exception. When a close
        is due to an exception, the sender provides the class and method id of
        the method which caused the exception.
        """

    def close(self,
              reply_code: ReplyCode,
              reply_text: ReplyText,
              class_id: ClassId,
              method_id: MethodId):
        """Request a connection close.

        This method indicates that the sender wants to close the connection.
        This may be due to internal conditions (e.g. a forced shut-down) or due
        to an error handling a specific method, i.e. an exception. When a close
        is due to an exception, the sender provides the class and method id of
        the method which caused the exception.
        """
        return MethodPayload(
            class_id=10,
            method_id=50,
            ReplyCode(reply_code),
            ReplyText(reply_text),
            ClassId(class_id),
            MethodId(method_id),
        )

    def on_close_ok(self):
        """Confirm a connection close.

        This method confirms a Connection.Close method and tells the recipient
        that it is safe to release resources for the connection and close the
        socket.
        """

    def close_ok(self):
        """Confirm a connection close.

        This method confirms a Connection.Close method and tells the recipient
        that it is safe to release resources for the connection and close the
        socket.
        """
        return MethodPayload(
            class_id=10,
            method_id=51,
        )

    METHOD_BY_ID = {
        10: on_start,
        11: start_ok,
        20: on_secure,
        21: secure_ok,
        30: on_tune,
        31: tune_ok,
        40: open,
        41: on_open_ok,
        50: on_close,
        50: close,
        51: on_close_ok,
        51: close_ok,
    }


class Channel:
    """Work with channels.

    The channel class provides methods for a client to establish a channel
    to a server and for both peers to operate the channel thereafter.

    Grammar:

      channel             = open-channel *use-channel close-channel
      open-channel        = C:OPEN S:OPEN-OK
      use-channel         = C:FLOW S:FLOW-OK
                          / S:FLOW C:FLOW-OK
                          / functional-class
      close-channel       = C:CLOSE S:CLOSE-OK
                          / S:CLOSE C:CLOSE-OK
    """

    class_id = 20


    def open(self):
        """Open a channel for use.

        This method opens a channel to the server.
        """
        return MethodPayload(
            class_id=20,
            method_id=10,
            Shortstr(None),
        )

    def on_open_ok(self,
                   reserved_1: Longstr):
        """Signal that the channel is ready.

        This method signals to the client that the channel is ready for use.
        """

    def flow(self,
             active: Bit):
        """Enable/disable flow from peer.

        This method asks the peer to pause or restart the flow of content data
        sent by a consumer. This is a simple flow-control mechanism that a peer
        can use to avoid overflowing its queues or otherwise finding itself
        receiving more messages than it can process. Note that this method is
        not intended for window control. It does not affect contents returned by
        Basic.Get-Ok methods.
        """
        return MethodPayload(
            class_id=20,
            method_id=20,
            Bit(active),
        )

    def on_flow(self,
                active: Bit):
        """Enable/disable flow from peer.

        This method asks the peer to pause or restart the flow of content data
        sent by a consumer. This is a simple flow-control mechanism that a peer
        can use to avoid overflowing its queues or otherwise finding itself
        receiving more messages than it can process. Note that this method is
        not intended for window control. It does not affect contents returned by
        Basic.Get-Ok methods.
        """

    def flow_ok(self,
                active: Bit):
        """Confirm a flow method.

        Confirms to the peer that a flow command was received and processed.
        """
        return MethodPayload(
            class_id=20,
            method_id=21,
            Bit(active),
        )

    def on_flow_ok(self,
                   active: Bit):
        """Confirm a flow method.

        Confirms to the peer that a flow command was received and processed.
        """

    def on_close(self,
                 reply_code: ReplyCode,
                 reply_text: ReplyText,
                 class_id: ClassId,
                 method_id: MethodId):
        """Request a channel close.

        This method indicates that the sender wants to close the channel. This
        may be due to internal conditions (e.g. a forced shut-down) or due to an
        error handling a specific method, i.e. an exception. When a close is due
        to an exception, the sender provides the class and method id of the
        method which caused the exception.
        """

    def close(self,
              reply_code: ReplyCode,
              reply_text: ReplyText,
              class_id: ClassId,
              method_id: MethodId):
        """Request a channel close.

        This method indicates that the sender wants to close the channel. This
        may be due to internal conditions (e.g. a forced shut-down) or due to an
        error handling a specific method, i.e. an exception. When a close is due
        to an exception, the sender provides the class and method id of the
        method which caused the exception.
        """
        return MethodPayload(
            class_id=20,
            method_id=40,
            ReplyCode(reply_code),
            ReplyText(reply_text),
            ClassId(class_id),
            MethodId(method_id),
        )

    def on_close_ok(self):
        """Confirm a channel close.

        This method confirms a Channel.Close method and tells the recipient that
        it is safe to release resources for the channel.
        """

    def close_ok(self):
        """Confirm a channel close.

        This method confirms a Channel.Close method and tells the recipient that
        it is safe to release resources for the channel.
        """
        return MethodPayload(
            class_id=20,
            method_id=41,
        )

    METHOD_BY_ID = {
        10: open,
        11: on_open_ok,
        20: flow,
        20: on_flow,
        21: flow_ok,
        21: on_flow_ok,
        40: on_close,
        40: close,
        41: on_close_ok,
        41: close_ok,
    }


class Exchange:
    """Work with exchanges.

    Exchanges match and distribute messages across queues. Exchanges can be
    configured in the server or declared at runtime.

    Grammar:

      exchange            = C:DECLARE  S:DECLARE-OK
                          / C:DELETE   S:DELETE-OK
                          / C:BIND     S:BIND-OK
                          / C:UNBIND   S:UNBIND-OK
    """

    class_id = 40

    def __init__(self, channel):
        self.channel = channel

    def declare(self,
                exchange: ExchangeName,
                type: Shortstr,
                passive: Bit,
                durable: Bit,
                auto_delete: Bit,
                internal: Bit,
                no_wait: NoWait,
                arguments: Table):
        """Verify exchange exists, create if needed.

        This method creates an exchange if it does not already exist, and if the
        exchange exists, verifies that it is of the correct and expected class.
        """
        return MethodPayload(
            class_id=40,
            method_id=10,
            Short(None),
            ExchangeName(exchange),
            Shortstr(type),
            Bit(passive),
            Bit(durable),
            Bit(auto_delete),
            Bit(internal),
            NoWait(no_wait),
            Table(arguments),
        )

    def on_declare_ok(self):
        """Confirm exchange declaration.

        This method confirms a Declare method and confirms the name of the
        exchange, essential for automatically-named exchanges.
        """

    def delete(self,
               exchange: ExchangeName,
               if_unused: Bit,
               no_wait: NoWait):
        """Delete an exchange.

        This method deletes an exchange. When an exchange is deleted all queue
        bindings on the exchange are cancelled.
        """
        return MethodPayload(
            class_id=40,
            method_id=20,
            Short(None),
            ExchangeName(exchange),
            Bit(if_unused),
            NoWait(no_wait),
        )

    def on_delete_ok(self):
        """Confirm deletion of an exchange.

        This method confirms the deletion of an exchange.
        """

    def bind(self,
             destination: ExchangeName,
             source: ExchangeName,
             routing_key: Shortstr,
             no_wait: NoWait,
             arguments: Table):
        """Bind exchange to an exchange.

        This method binds an exchange to an exchange.
        """
        return MethodPayload(
            class_id=40,
            method_id=30,
            Short(None),
            ExchangeName(destination),
            ExchangeName(source),
            Shortstr(routing_key),
            NoWait(no_wait),
            Table(arguments),
        )

    def on_bind_ok(self):
        """Confirm bind successful.

        This method confirms that the bind was successful.
        """

    def unbind(self,
               destination: ExchangeName,
               source: ExchangeName,
               routing_key: Shortstr,
               no_wait: NoWait,
               arguments: Table):
        """Unbind an exchange from an exchange.

        This method unbinds an exchange from an exchange.
        """
        return MethodPayload(
            class_id=40,
            method_id=40,
            Short(None),
            ExchangeName(destination),
            ExchangeName(source),
            Shortstr(routing_key),
            NoWait(no_wait),
            Table(arguments),
        )

    def on_unbind_ok(self):
        """Confirm unbind successful.

        This method confirms that the unbind was successful.
        """

    METHOD_BY_ID = {
        10: declare,
        11: on_declare_ok,
        20: delete,
        21: on_delete_ok,
        30: bind,
        31: on_bind_ok,
        40: unbind,
        51: on_unbind_ok,
    }


class Queue:
    """Work with queues.

    Queues store and forward messages. Queues can be configured in the
    server or created at runtime. Queues must be attached to at least one
    exchange in order to receive messages from publishers.

    Grammar:

      queue               = C:DECLARE  S:DECLARE-OK
                          / C:BIND     S:BIND-OK
                          / C:UNBIND   S:UNBIND-OK
                          / C:PURGE    S:PURGE-OK
                          / C:DELETE   S:DELETE-OK
    """

    class_id = 50

    def __init__(self, channel):
        self.channel = channel

    def declare(self,
                queue: QueueName,
                passive: Bit,
                durable: Bit,
                exclusive: Bit,
                auto_delete: Bit,
                no_wait: NoWait,
                arguments: Table):
        """Declare queue, create if needed.

        This method creates or checks a queue. When creating a new queue the
        client can specify various properties that control the durability of the
        queue and its contents, and the level of sharing for the queue.
        """
        return MethodPayload(
            class_id=50,
            method_id=10,
            Short(None),
            QueueName(queue),
            Bit(passive),
            Bit(durable),
            Bit(exclusive),
            Bit(auto_delete),
            NoWait(no_wait),
            Table(arguments),
        )

    def on_declare_ok(self,
                      queue: QueueName,
                      message_count: MessageCount,
                      consumer_count: Long):
        """Confirms a queue definition.

        This method confirms a Declare method and confirms the name of the
        queue, essential for automatically-named queues.
        """

    def bind(self,
             queue: QueueName,
             exchange: ExchangeName,
             routing_key: Shortstr,
             no_wait: NoWait,
             arguments: Table):
        """Bind queue to an exchange.

        This method binds a queue to an exchange. Until a queue is bound it will
        not receive any messages. In a classic messaging model, store-and-
        forward queues are bound to a direct exchange and subscription queues
        are bound to a topic exchange.
        """
        return MethodPayload(
            class_id=50,
            method_id=20,
            Short(None),
            QueueName(queue),
            ExchangeName(exchange),
            Shortstr(routing_key),
            NoWait(no_wait),
            Table(arguments),
        )

    def on_bind_ok(self):
        """Confirm bind successful.

        This method confirms that the bind was successful.
        """

    def unbind(self,
               queue: QueueName,
               exchange: ExchangeName,
               routing_key: Shortstr,
               arguments: Table):
        """Unbind a queue from an exchange.

        This method unbinds a queue from an exchange.
        """
        return MethodPayload(
            class_id=50,
            method_id=50,
            Short(None),
            QueueName(queue),
            ExchangeName(exchange),
            Shortstr(routing_key),
            Table(arguments),
        )

    def on_unbind_ok(self):
        """Confirm unbind successful.

        This method confirms that the unbind was successful.
        """

    def purge(self,
              queue: QueueName,
              no_wait: NoWait):
        """Purge a queue.

        This method removes all messages from a queue which are not awaiting
        acknowledgment.
        """
        return MethodPayload(
            class_id=50,
            method_id=30,
            Short(None),
            QueueName(queue),
            NoWait(no_wait),
        )

    def on_purge_ok(self,
                    message_count: MessageCount):
        """Confirms a queue purge.

        This method confirms the purge of a queue.
        """

    def delete(self,
               queue: QueueName,
               if_unused: Bit,
               if_empty: Bit,
               no_wait: NoWait):
        """Delete a queue.

        This method deletes a queue. When a queue is deleted any pending
        messages are sent to a dead-letter queue if this is defined in the
        server configuration, and all consumers on the queue are cancelled.
        """
        return MethodPayload(
            class_id=50,
            method_id=40,
            Short(None),
            QueueName(queue),
            Bit(if_unused),
            Bit(if_empty),
            NoWait(no_wait),
        )

    def on_delete_ok(self,
                     message_count: MessageCount):
        """Confirm deletion of a queue.

        This method confirms the deletion of a queue.
        """

    METHOD_BY_ID = {
        10: declare,
        11: on_declare_ok,
        20: bind,
        21: on_bind_ok,
        50: unbind,
        51: on_unbind_ok,
        30: purge,
        31: on_purge_ok,
        40: delete,
        41: on_delete_ok,
    }


class Basic:
    """Work with basic content.

    The Basic class provides methods that support an industry-standard
    messaging model.

    Grammar:

      basic               = C:QOS S:QOS-OK
                          / C:CONSUME S:CONSUME-OK
                          / C:CANCEL S:CANCEL-OK
                          / C:PUBLISH content
                          / S:RETURN content
                          / S:DELIVER content
                          / C:GET ( S:GET-OK content / S:GET-EMPTY )
                          / C:ACK
                          / S:ACK
                          / C:REJECT
                          / C:NACK
                          / S:NACK
                          / C:RECOVER-ASYNC
                          / C:RECOVER S:RECOVER-OK
    """

    class_id = 60

    def __init__(self, channel):
        self.channel = channel

    def qos(self,
            prefetch_size: Long,
            prefetch_count: Short,
            global_: Bit):
        """Specify quality of service.

        This method requests a specific quality of service. The QoS can be
        specified for the current channel or for all channels on the connection.
        The particular properties and semantics of a qos method always depend on
        the content class semantics. Though the qos method could in principle
        apply to both peers, it is currently meaningful only for the server.
        """
        return MethodPayload(
            class_id=60,
            method_id=10,
            Long(prefetch_size),
            Short(prefetch_count),
            Bit(global_),
        )

    def on_qos_ok(self):
        """Confirm the requested qos.

        This method tells the client that the requested QoS levels could be
        handled by the server. The requested QoS applies to all active consumers
        until a new QoS is defined.
        """

    def consume(self,
                queue: QueueName,
                consumer_tag: ConsumerTag,
                no_local: NoLocal,
                no_ack: NoAck,
                exclusive: Bit,
                no_wait: NoWait,
                arguments: Table):
        """Start a queue consumer.

        This method asks the server to start a "consumer", which is a transient
        request for messages from a specific queue. Consumers last as long as
        the channel they were declared on, or until the client cancels them.
        """
        return MethodPayload(
            class_id=60,
            method_id=20,
            Short(None),
            QueueName(queue),
            ConsumerTag(consumer_tag),
            NoLocal(no_local),
            NoAck(no_ack),
            Bit(exclusive),
            NoWait(no_wait),
            Table(arguments),
        )

    def on_consume_ok(self,
                      consumer_tag: ConsumerTag):
        """Confirm a new consumer.

        The server provides the client with a consumer tag, which is used by the
        client for methods called on the consumer at a later stage.
        """

    def cancel(self,
               consumer_tag: ConsumerTag,
               no_wait: NoWait):
        """End a queue consumer.

        This method cancels a consumer. This does not affect already delivered
        messages, but it does mean the server will not send any more messages
        for that consumer. The client may receive an arbitrary number of
        messages in between sending the cancel method and receiving the cancel-
        ok reply.  It may also be sent from the server to the client in the
        event of the consumer being unexpectedly cancelled (i.e. cancelled for
        any reason other than the server receiving the corresponding
        basic.cancel from the client). This allows clients to be notified of the
        loss of consumers due to events such as queue deletion. Note that as it
        is not a MUST for clients to accept this method from the client, it is
        advisable for the broker to be able to identify those clients that are
        capable of accepting the method, through some means of capability
        negotiation.
        """
        return MethodPayload(
            class_id=60,
            method_id=30,
            ConsumerTag(consumer_tag),
            NoWait(no_wait),
        )

    def on_cancel(self,
                  consumer_tag: ConsumerTag,
                  no_wait: NoWait):
        """End a queue consumer.

        This method cancels a consumer. This does not affect already delivered
        messages, but it does mean the server will not send any more messages
        for that consumer. The client may receive an arbitrary number of
        messages in between sending the cancel method and receiving the cancel-
        ok reply.  It may also be sent from the server to the client in the
        event of the consumer being unexpectedly cancelled (i.e. cancelled for
        any reason other than the server receiving the corresponding
        basic.cancel from the client). This allows clients to be notified of the
        loss of consumers due to events such as queue deletion. Note that as it
        is not a MUST for clients to accept this method from the client, it is
        advisable for the broker to be able to identify those clients that are
        capable of accepting the method, through some means of capability
        negotiation.
        """

    def on_cancel_ok(self,
                     consumer_tag: ConsumerTag):
        """Confirm a cancelled consumer.

        This method confirms that the cancellation was completed.
        """

    def cancel_ok(self,
                  consumer_tag: ConsumerTag):
        """Confirm a cancelled consumer.

        This method confirms that the cancellation was completed.
        """
        return MethodPayload(
            class_id=60,
            method_id=31,
            ConsumerTag(consumer_tag),
        )

    def publish(self,
                exchange: ExchangeName,
                routing_key: Shortstr,
                mandatory: Bit,
                immediate: Bit):
        """Publish a message.

        This method publishes a message to a specific exchange. The message will
        be routed to queues as defined by the exchange configuration and
        distributed to any active consumers when the transaction, if any, is
        committed.
        """
        return MethodPayload(
            class_id=60,
            method_id=40,
            Short(None),
            ExchangeName(exchange),
            Shortstr(routing_key),
            Bit(mandatory),
            Bit(immediate),
        )

    def on_return_(self,
                   reply_code: ReplyCode,
                   reply_text: ReplyText,
                   exchange: ExchangeName,
                   routing_key: Shortstr):
        """Return a failed message.

        This method returns an undeliverable message that was published with the
        "immediate" flag set, or an unroutable message published with the
        "mandatory" flag set. The reply code and text provide information about
        the reason that the message was undeliverable.
        """

    def on_deliver(self,
                   consumer_tag: ConsumerTag,
                   delivery_tag: DeliveryTag,
                   redelivered: Redelivered,
                   exchange: ExchangeName,
                   routing_key: Shortstr):
        """Notify the client of a consumer message.

        This method delivers a message to the client, via a consumer. In the
        asynchronous message delivery model, the client starts a consumer using
        the Consume method, then the server responds with Deliver methods as and
        when messages arrive for that consumer.
        """

    def get(self,
            queue: QueueName,
            no_ack: NoAck):
        """Direct access to a queue.

        This method provides a direct access to the messages in a queue using a
        synchronous dialogue that is designed for specific types of application
        where synchronous functionality is more important than performance.
        """
        return MethodPayload(
            class_id=60,
            method_id=70,
            Short(None),
            QueueName(queue),
            NoAck(no_ack),
        )

    def on_get_ok(self,
                  delivery_tag: DeliveryTag,
                  redelivered: Redelivered,
                  exchange: ExchangeName,
                  routing_key: Shortstr,
                  message_count: MessageCount):
        """Provide client with a message.

        This method delivers a message to the client following a get method. A
        message delivered by 'get-ok' must be acknowledged unless the no-ack
        option was set in the get method.
        """

    def on_get_empty(self):
        """Indicate no messages available.

        This method tells the client that the queue has no messages available
        for the client.
        """

    def ack(self,
            delivery_tag: DeliveryTag,
            multiple: Bit):
        """Acknowledge one or more messages.

        When sent by the client, this method acknowledges one or more messages
        delivered via the Deliver or Get-Ok methods.  When sent by server, this
        method acknowledges one or more messages published with the Publish
        method on a channel in confirm mode.  The acknowledgement can be for a
        single message or a set of messages up to and including a specific
        message.
        """
        return MethodPayload(
            class_id=60,
            method_id=80,
            DeliveryTag(delivery_tag),
            Bit(multiple),
        )

    def on_ack(self,
               delivery_tag: DeliveryTag,
               multiple: Bit):
        """Acknowledge one or more messages.

        When sent by the client, this method acknowledges one or more messages
        delivered via the Deliver or Get-Ok methods.  When sent by server, this
        method acknowledges one or more messages published with the Publish
        method on a channel in confirm mode.  The acknowledgement can be for a
        single message or a set of messages up to and including a specific
        message.
        """

    def reject(self,
               delivery_tag: DeliveryTag,
               requeue: Bit):
        """Reject an incoming message.

        This method allows a client to reject a message. It can be used to
        interrupt and cancel large incoming messages, or return untreatable
        messages to their original queue.
        """
        return MethodPayload(
            class_id=60,
            method_id=90,
            DeliveryTag(delivery_tag),
            Bit(requeue),
        )

    def recover_async(self,
                      requeue: Bit):
        """Redeliver unacknowledged messages.

        This method asks the server to redeliver all unacknowledged messages on
        a specified channel. Zero or more messages may be redelivered.  This
        method is deprecated in favour of the synchronous Recover/Recover-Ok.
        """
        return MethodPayload(
            class_id=60,
            method_id=100,
            Bit(requeue),
        )

    def recover(self,
                requeue: Bit):
        """Redeliver unacknowledged messages.

        This method asks the server to redeliver all unacknowledged messages on
        a specified channel. Zero or more messages may be redelivered.  This
        method replaces the asynchronous Recover.
        """
        return MethodPayload(
            class_id=60,
            method_id=110,
            Bit(requeue),
        )

    def on_recover_ok(self):
        """Confirm recovery.

        This method acknowledges a Basic.Recover method.
        """

    def nack(self,
             delivery_tag: DeliveryTag,
             multiple: Bit,
             requeue: Bit):
        """Reject one or more incoming messages.

        This method allows a client to reject one or more incoming messages. It
        can be used to interrupt and cancel large incoming messages, or return
        untreatable messages to their original queue.  This method is also used
        by the server to inform publishers on channels in confirm mode of
        unhandled messages.  If a publisher receives this method, it probably
        needs to republish the offending messages.
        """
        return MethodPayload(
            class_id=60,
            method_id=120,
            DeliveryTag(delivery_tag),
            Bit(multiple),
            Bit(requeue),
        )

    def on_nack(self,
                delivery_tag: DeliveryTag,
                multiple: Bit,
                requeue: Bit):
        """Reject one or more incoming messages.

        This method allows a client to reject one or more incoming messages. It
        can be used to interrupt and cancel large incoming messages, or return
        untreatable messages to their original queue.  This method is also used
        by the server to inform publishers on channels in confirm mode of
        unhandled messages.  If a publisher receives this method, it probably
        needs to republish the offending messages.
        """

    METHOD_BY_ID = {
        10: qos,
        11: on_qos_ok,
        20: consume,
        21: on_consume_ok,
        30: cancel,
        30: on_cancel,
        31: on_cancel_ok,
        31: cancel_ok,
        40: publish,
        50: on_return_,
        60: on_deliver,
        70: get,
        71: on_get_ok,
        72: on_get_empty,
        80: ack,
        80: on_ack,
        90: reject,
        100: recover_async,
        110: recover,
        111: on_recover_ok,
        120: nack,
        120: on_nack,
    }


class Tx:
    """Work with transactions.

    The Tx class allows publish and ack operations to be batched into atomic
    units of work.  The intention is that all publish and ack requests
    issued within a transaction will complete successfully or none of them
    will. Servers SHOULD implement atomic transactions at least where all
    publish or ack requests affect a single queue.  Transactions that cover
    multiple queues may be non-atomic, given that queues can be created and
    destroyed asynchronously, and such events do not form part of any
    transaction. Further, the behaviour of transactions with respect to the
    immediate and mandatory flags on Basic.Publish methods is not defined.

    Grammar:

      tx                  = C:SELECT S:SELECT-OK
                          / C:COMMIT S:COMMIT-OK
                          / C:ROLLBACK S:ROLLBACK-OK
    """

    class_id = 90

    def __init__(self, channel):
        self.channel = channel

    def select(self):
        """Select standard transaction mode.

        This method sets the channel to use standard transactions. The client
        must use this method at least once on a channel before using the Commit
        or Rollback methods.
        """
        return MethodPayload(
            class_id=90,
            method_id=10,
        )

    def on_select_ok(self):
        """Confirm transaction mode.

        This method confirms to the client that the channel was successfully set
        to use standard transactions.
        """

    def commit(self):
        """Commit the current transaction.

        This method commits all message publications and acknowledgments
        performed in the current transaction.  A new transaction starts
        immediately after a commit.
        """
        return MethodPayload(
            class_id=90,
            method_id=20,
        )

    def on_commit_ok(self):
        """Confirm a successful commit.

        This method confirms to the client that the commit succeeded. Note that
        if a commit fails, the server raises a channel exception.
        """

    def rollback(self):
        """Abandon the current transaction.

        This method abandons all message publications and acknowledgments
        performed in the current transaction. A new transaction starts
        immediately after a rollback. Note that unacked messages will not be
        automatically redelivered by rollback; if that is required an explicit
        recover call should be issued.
        """
        return MethodPayload(
            class_id=90,
            method_id=30,
        )

    def on_rollback_ok(self):
        """Confirm successful rollback.

        This method confirms to the client that the rollback succeeded. Note
        that if an rollback fails, the server raises a channel exception.
        """

    METHOD_BY_ID = {
        10: select,
        11: on_select_ok,
        20: commit,
        21: on_commit_ok,
        30: rollback,
        31: on_rollback_ok,
    }


class Confirm:
    """Work with confirms.

    The Confirm class allows publishers to put the channel in confirm mode
    and susequently be notified when messages have been handled by the
    broker.  The intention is that all messages published on a channel in
    confirm mode will be acknowledged at some point.  By acknowledging a
    message the broker assumes responsibility for it and indicates that it
    has done something it deems reasonable with it.  Unroutable mandatory or
    immediate messages are acknowledged right after the Basic.Return method.
    Messages are acknowledged when all queues to which the message has been
    routed have either delivered the message and received an acknowledgement
    (if required), or enqueued the message (and persisted it if required).
    Published messages are assigned ascending sequence numbers, starting at
    1 with the first Confirm.Select method. The server confirms messages by
    sending Basic.Ack methods referring to these sequence numbers.

    Grammar:

      confirm            = C:SELECT S:SELECT-OK
    """

    class_id = 85

    def __init__(self, channel):
        self.channel = channel

    def select(self,
               nowait: Bit):
        """None

        This method sets the channel to use publisher acknowledgements. The
        client can only use this method on a non-transactional channel.
        """
        return MethodPayload(
            class_id=85,
            method_id=10,
            Bit(nowait),
        )

    def on_select_ok(self):
        """None

        This method confirms to the client that the channel was successfully set
        to use publisher acknowledgements.
        """

    METHOD_BY_ID = {
        10: select,
        11: on_select_ok,
    }


CLASS_BY_ID = {
    10: Connection,
    20: Channel,
    40: Exchange,
    50: Queue,
    60: Basic,
    90: Tx,
    85: Confirm,
}