from itertools import chain

import construct as c

from . import domains as d


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

    Start = 'Connection.Start' / c.Struct(
        'version_major' / d.Octet,
        'version_minor' / d.Octet,
        'server_properties' / d.PeerProperties,
        'mechanisms' / d.LongStr,
        'locales' / d.LongStr,
    )
    """Start connection negotiation.

    This method starts the connection negotiation process by telling the
    client the protocol version that the server proposes, along with a list
    of security mechanisms which the client can use for authentication.
    """

    StartOK = 'Connection.StartOK' / c.Struct(
        'client_properties' / d.PeerProperties,
        'mechanism' / d.ShortStr,
        'response' / d.LongStr,
        'locale' / d.ShortStr,
    )
    """Select security mechanism and locale.

    This method selects a SASL security mechanism.
    """

    Secure = 'Connection.Secure' / c.Struct(
        'challenge' / d.LongStr,
    )
    """Security mechanism challenge.

    The SASL protocol works by exchanging challenges and responses until
    both peers have received sufficient information to authenticate each
    other. This method challenges the client to provide more information.
    """

    SecureOK = 'Connection.SecureOK' / c.Struct(
        'response' / d.LongStr,
    )
    """Security mechanism response.

    This method attempts to authenticate, passing a block of SASL data for
    the security mechanism at the server side.
    """

    Tune = 'Connection.Tune' / c.Struct(
        'channel_max' / d.Short,
        'frame_max' / d.Long,
        'heartbeat' / d.Short,
    )
    """Propose connection tuning parameters.

    This method proposes a set of connection configuration values to the
    client. The client can accept and/or adjust these.
    """

    TuneOK = 'Connection.TuneOK' / c.Struct(
        'channel_max' / d.Short,
        'frame_max' / d.Long,
        'heartbeat' / d.Short,
    )
    """Negotiate connection tuning parameters.

    This method sends the client's connection tuning parameters to the
    server. Certain fields are negotiated, others provide capability
    information.
    """

    Open = 'Connection.Open' / c.Struct(
        'virtual_host' / d.Path,
        # Deprecated
        'capabilities' / c.Const(d.ShortStr, ''),
        # Deprecated
        'insist' / c.Const(d.Bit, 0),
    )
    """Open connection to virtual host.

    This method opens a connection to a virtual host, which is a collection
    of resources, and acts to separate multiple application domains within a
    server. The server may apply arbitrary limits per virtual host, such as
    the number of each type of entity that may be used, per connection
    and/or in total.
    """

    OpenOK = 'Connection.OpenOK' / c.Struct(
        # Deprecated
        'known_hosts' / c.Const(d.Shortstr, ''),
    )
    """Signal that connection is ready.

    This method signals to the client that the connection is ready for use.
    """

    Close = 'Connection.Close' / c.Struct(
        'reply_code' / d.ReplyCode,
        'reply_text' / d.ReplyText,
        'class_id' / d.ClassId,
        'method_id' / d.MethodId,
    )
    """Request a connection close.

    This method indicates that the sender wants to close the connection.
    This may be due to internal conditions (e.g. a forced shut-down) or due
    to an error handling a specific method, i.e. an exception. When a close
    is due to an exception, the sender provides the class and method id of
    the method which caused the exception.
    """

    CloseOK = 'Connection.CloseOK' / c.Struct()
    """Confirm a connection close.

    This method confirms a Connection.Close method and tells the recipient
    that it is safe to release resources for the connection and close the
    socket.
    """

    IDS_TO_METHODS = {
        (10, 10): Start,
        (10, 11): StartOK,
        (10, 20): Secure,
        (10, 21): SecureOK,
        (10, 30): Tune,
        (10, 31): TuneOK,
        (10, 40): Open,
        (10, 41): OpenOK,
        (10, 50): Close,
        (10, 51): CloseOK,
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

    Open = 'Channel.Open' / c.Struct(
        # Deprecated
        'out_of_band' / c.Const(d.ShortStr, ''),
    )
    """Open a channel for use.

    This method opens a channel to the server.
    """

    OpenOK = 'Channel.OpenOK' / c.Struct(
        # Deprecated
        'channel_id' / c.Const(d.LongStr, 0),
    )
    """Signal that the channel is ready.

    This method signals to the client that the channel is ready for use.
    """

    Flow = 'Channel.Flow' / c.Struct(
        'active' / d.Bit,
    )
    """Enable/disable flow from peer.

    This method asks the peer to pause or restart the flow of content data
    sent by a consumer. This is a simple flow-control mechanism that a peer
    can use to avoid overflowing its queues or otherwise finding itself
    receiving more messages than it can process. Note that this method is
    not intended for window control. It does not affect contents returned by
    Basic.Get-Ok methods.
    """

    FlowOK = 'Channel.FlowOK' / c.Struct(
        'active' / d.Bit,
    )
    """Confirm a flow method.

    Confirms to the peer that a flow command was received and processed.
    """

    Close = 'Channel.Close' / c.Struct(
        'reply_code' / d.ReplyCode,
        'reply_text' / d.ReplyText,
        'class_id' / d.ClassId,
        'method_id' / d.MethodId,
    )
    """Request a channel close.

    This method indicates that the sender wants to close the channel. This
    may be due to internal conditions (e.g. a forced shut-down) or due to an
    error handling a specific method, i.e. an exception. When a close is due
    to an exception, the sender provides the class and method id of the
    method which caused the exception.
    """

    CloseOK = 'Channel.CloseOK' / c.Struct()
    """Confirm a channel close.

    This method confirms a Channel.Close method and tells the recipient that
    it is safe to release resources for the channel.
    """

    IDS_TO_METHODS = {
        (20, 10): Open,
        (20, 11): OpenOK,
        (20, 20): Flow,
        (20, 21): FlowOK,
        (20, 40): Close,
        (20, 41): CloseOK,
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

    Declare = 'Exchange.Declare' / c.Struct(
        # Deprecated
        'ticket' / c.Const(d.Short, 0),
        'exchange' / d.ExchangeName,
        'type' / d.Shortstr,
        'passive' / d.Bit,
        'durable' / d.Bit,
        'auto_delete' / d.Bit,
        'internal' / d.Bit,
        'no_wait' / d.NoWait,
        'arguments' / d.Table,
    )
    """Verify exchange exists, create if needed.

    This method creates an exchange if it does not already exist, and if the
    exchange exists, verifies that it is of the correct and expected class.
    """

    DeclareOK = 'Exchange.DeclareOK' / c.Struct()
    """Confirm exchange declaration.

    This method confirms a Declare method and confirms the name of the
    exchange, essential for automatically-named exchanges.
    """

    Delete = 'Exchange.Delete' / c.Struct(
        # Deprecated
        'ticket' / c.Const(d.Short, 0),
        'exchange' / d.ExchangeName,
        'if_unused' / d.Bit,
        'no_wait' / d.NoWait,
    )
    """Delete an exchange.

    This method deletes an exchange. When an exchange is deleted all queue
    bindings on the exchange are cancelled.
    """

    DeleteOK = 'Exchange.DeleteOK' / c.Struct()
    """Confirm deletion of an exchange.

    This method confirms the deletion of an exchange.
    """

    Bind = 'Exchange.Bind' / c.Struct(
        # Deprecated
        'ticket' / c.Const(d.Short, 0),
        'destination' / d.ExchangeName,
        'source' / d.ExchangeName,
        'routing_key' / d.Shortstr,
        'no_wait' / d.NoWait,
        'arguments' / d.Table,
    )
    """Bind exchange to an exchange.

    This method binds an exchange to an exchange.
    """

    BindOK = 'Exchange.BindOK' / c.Struct()
    """Confirm bind successful.

    This method confirms that the bind was successful.
    """

    Unbind = 'Exchange.Unbind' / c.Struct(
        # Deprecated
        'ticket' / c.Const(d.Short, 0),
        'destination' / d.ExchangeName,
        'source' / d.ExchangeName,
        'routing_key' / d.Shortstr,
        'no_wait' / d.NoWait,
        'arguments' / d.Table,
    )
    """Unbind an exchange from an exchange.

    This method unbinds an exchange from an exchange.
    """

    UnbindOK = 'Exchange.UnbindOK' / c.Struct()
    """Confirm unbind successful.

    This method confirms that the unbind was successful.
    """

    IDS_TO_METHODS = {
        (40, 10): Declare,
        (40, 11): DeclareOK,
        (40, 20): Delete,
        (40, 21): DeleteOK,
        (40, 30): Bind,
        (40, 31): BindOK,
        (40, 40): Unbind,
        (40, 41): UnbindOK,
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

    Declare = 'Queue.Declare' / c.Struct(
        # Deprecated
        'ticket' / c.Const(d.Short, 0),
        'queue' / d.QueueName,
        'passive' / d.Bit,
        'durable' / d.Bit,
        'exclusive' / d.Bit,
        'auto_delete' / d.Bit,
        'no_wait' / d.NoWait,
        'arguments' / d.Table,
    )
    """Declare queue, create if needed.

    This method creates or checks a queue. When creating a new queue the
    client can specify various properties that control the durability of the
    queue and its contents, and the level of sharing for the queue.
    """

    DeclareOK = 'Queue.DeclareOK' / c.Struct(
        'queue' / d.QueueName,
        'message_count' / d.MessageCount,
        'consumer_count' / d.Long,
    )
    """Confirms a queue definition.

    This method confirms a Declare method and confirms the name of the
    queue, essential for automatically-named queues.
    """

    Bind = 'Queue.Bind' / c.Struct(
        # Deprecated
        'ticket' / c.Const(d.Short, 0),
        'queue' / d.QueueName,
        'exchange' / d.ExchangeName,
        'routing_key' / d.Shortstr,
        'no_wait' / d.NoWait,
        'arguments' / d.Table,
    )
    """Bind queue to an exchange.

    This method binds a queue to an exchange. Until a queue is bound it will
    not receive any messages. In a classic messaging model, store-and-
    forward queues are bound to a direct exchange and subscription queues
    are bound to a topic exchange.
    """

    BindOK = 'Queue.BindOK' / c.Struct()
    """Confirm bind successful.

    This method confirms that the bind was successful.
    """

    Unbind = 'Queue.Unbind' / c.Struct(
        # Deprecated
        'ticket' / c.Const(d.Short, 0),
        'queue' / d.QueueName,
        'exchange' / d.ExchangeName,
        'routing_key' / d.Shortstr,
        'arguments' / d.Table,
    )
    """Unbind a queue from an exchange.

    This method unbinds a queue from an exchange.
    """

    UnbindOK = 'Queue.UnbindOK' / c.Struct()
    """Confirm unbind successful.

    This method confirms that the unbind was successful.
    """

    Purge = 'Queue.Purge' / c.Struct(
        # Deprecated
        'ticket' / c.Const(d.Short, 0),
        'queue' / d.QueueName,
        'no_wait' / d.NoWait,
    )
    """Purge a queue.

    This method removes all messages from a queue which are not awaiting
    acknowledgment.
    """

    PurgeOK = 'Queue.PurgeOK' / c.Struct(
        'message_count' / d.MessageCount,
    )
    """Confirms a queue purge.

    This method confirms the purge of a queue.
    """

    Delete = 'Queue.Delete' / c.Struct(
        # Deprecated
        'ticket' / c.Const(d.Short, 0),
        'queue' / d.QueueName,
        'if_unused' / d.Bit,
        'if_empty' / d.Bit,
        'no_wait' / d.NoWait,
    )
    """Delete a queue.

    This method deletes a queue. When a queue is deleted any pending
    messages are sent to a dead-letter queue if this is defined in the
    server configuration, and all consumers on the queue are cancelled.
    """

    DeleteOK = 'Queue.DeleteOK' / c.Struct(
        'message_count' / d.MessageCount,
    )
    """Confirm deletion of a queue.

    This method confirms the deletion of a queue.
    """

    IDS_TO_METHODS = {
        (50, 10): Declare,
        (50, 11): DeclareOK,
        (50, 20): Bind,
        (50, 21): BindOK,
        (50, 30): Purge,
        (50, 31): PurgeOK,
        (50, 40): Delete,
        (50, 41): DeleteOK,
        (50, 50): Unbind,
        (50, 51): UnbindOK,
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

    Qos = 'Basic.Qos' / c.Struct(
        'prefetch_size' / d.Long,
        'prefetch_count' / d.Short,
        'global_' / d.Bit,
    )
    """Specify quality of service.

    This method requests a specific quality of service. The QoS can be
    specified for the current channel or for all channels on the connection.
    The particular properties and semantics of a qos method always depend on
    the content class semantics. Though the qos method could in principle
    apply to both peers, it is currently meaningful only for the server.
    """

    QosOK = 'Basic.QosOK' / c.Struct()
    """Confirm the requested qos.

    This method tells the client that the requested QoS levels could be
    handled by the server. The requested QoS applies to all active consumers
    until a new QoS is defined.
    """

    Consume = 'Basic.Consume' / c.Struct(
        'ticket' / c.Const(d.Short, 0),
        'queue' / d.QueueName,
        'consumer_tag' / d.ConsumerTag,
        'no_local' / d.NoLocal,
        'no_ack' / d.NoAck,
        'exclusive' / d.Bit,
        'no_wait' / d.NoWait,
        'arguments' / d.Table,
    )
    """Start a queue consumer.

    This method asks the server to start a "consumer", which is a transient
    request for messages from a specific queue. Consumers last as long as
    the channel they were declared on, or until the client cancels them.
    """

    ConsumeOK = 'Basic.ConsumeOK' / c.Struct(
        'consumer_tag' / d.ConsumerTag,
    )
    """Confirm a new consumer.

    The server provides the client with a consumer tag, which is used by the
    client for methods called on the consumer at a later stage.
    """

    Cancel = 'Basic.Cancel' / c.Struct(
        'consumer_tag' / d.ConsumerTag,
        'no_wait' / d.NoWait,
    )
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

    CancelOK = 'Basic.CancelOK' / c.Struct(
        'consumer_tag' / d.ConsumerTag,
    )
    """Confirm a cancelled consumer.

    This method confirms that the cancellation was completed.
    """

    Publish = 'Basic.Publish' / c.Struct(
        # Deprecated
        'ticket' / c.Const(d.Short, 0),
        'exchange' / d.ExchangeName,
        'routing_key' / d.Shortstr,
        'mandatory' / d.Bit,
        'immediate' / d.Bit,
    )
    """Publish a message.

    This method publishes a message to a specific exchange. The message will
    be routed to queues as defined by the exchange configuration and
    distributed to any active consumers when the transaction, if any, is
    committed.
    """

    Return = 'Basic.Return' / c.Struct(
        'reply_code' / d.ReplyCode,
        'reply_text' / d.ReplyText,
        'exchange' / d.ExchangeName,
        'routing_key' / d.Shortstr,
    )
    """Return a failed message.

    This method returns an undeliverable message that was published with the
    "immediate" flag set, or an unroutable message published with the
    "mandatory" flag set. The reply code and text provide information about
    the reason that the message was undeliverable.
    """

    Deliver = 'Basic.Deliver' / c.Struct(
        'consumer_tag' / d.ConsumerTag,
        'delivery_tag' / d.DeliveryTag,
        'redelivered' / d.Redelivered,
        'exchange' / d.ExchangeName,
        'routing_key' / d.Shortstr,
    )
    """Notify the client of a consumer message.

    This method delivers a message to the client, via a consumer. In the
    asynchronous message delivery model, the client starts a consumer using
    the Consume method, then the server responds with Deliver methods as and
    when messages arrive for that consumer.
    """

    Get = 'Basic.Get' / c.Struct(
        # Deprecated
        'ticket' / c.Const(d.Short, 0),
        'queue' / d.QueueName,
        'no_ack' / d.NoAck,
    )
    """Direct access to a queue.

    This method provides a direct access to the messages in a queue using a
    synchronous dialogue that is designed for specific types of application
    where synchronous functionality is more important than performance.
    """

    GetOK = 'Basic.GetOK' / c.Struct(
        'delivery_tag' / d.DeliveryTag,
        'redelivered' / d.Redelivered,
        'exchange' / d.ExchangeName,
        'routing_key' / d.Shortstr,
        'message_count' / d.MessageCount,
    )
    """Provide client with a message.

    This method delivers a message to the client following a get method. A
    message delivered by 'get-ok' must be acknowledged unless the no-ack
    option was set in the get method.
    """

    GetEmpty = 'Basic.GetEmpty' / c.Struct(
        # Deprecated
        'cluster_id' / c.Const(d.ShortStr, ''),
    )
    """Indicate no messages available.

    This method tells the client that the queue has no messages available
    for the client.
    """

    Ack = 'Basic.Ack' / c.Struct(
        'delivery_tag' / d.DeliveryTag,
        'multiple' / d.Bit,
    )
    """Acknowledge one or more messages.

    When sent by the client, this method acknowledges one or more messages
    delivered via the Deliver or Get-Ok methods.  When sent by server, this
    method acknowledges one or more messages published with the Publish
    method on a channel in confirm mode.  The acknowledgement can be for a
    single message or a set of messages up to and including a specific
    message.
    """

    Reject = 'Basic.Reject' / c.Struct(
        'delivery_tag' / d.DeliveryTag,
        'requeue' / d.Bit,
    )
    """Reject an incoming message.

    This method allows a client to reject a message. It can be used to
    interrupt and cancel large incoming messages, or return untreatable
    messages to their original queue.
    """

    RecoverAsync = 'Basic.RecoverAsync' / c.Struct(
        'requeue' / d.Bit,
    )
    """Redeliver unacknowledged messages.

    This method asks the server to redeliver all unacknowledged messages on
    a specified channel. Zero or more messages may be redelivered.  This
    method is deprecated in favour of the synchronous Recover/Recover-Ok.
    """

    Recover = 'Basic.Recover' / c.Struct(
        'requeue' / d.Bit,
    )
    """Redeliver unacknowledged messages.

    This method asks the server to redeliver all unacknowledged messages on
    a specified channel. Zero or more messages may be redelivered.  This
    method replaces the asynchronous Recover.
    """

    RecoverOK = 'Basic.RecoverOK' / c.Struct()
    """Confirm recovery.

    This method acknowledges a Basic.Recover method.
    """

    Nack = 'Basic.Nack' / c.Struct(
        'delivery_tag' / d.DeliveryTag,
        'multiple' / d.Bit,
        'requeue' / d.Bit,
    )
    """Reject one or more incoming messages.

    This method allows a client to reject one or more incoming messages. It
    can be used to interrupt and cancel large incoming messages, or return
    untreatable messages to their original queue.  This method is also used
    by the server to inform publishers on channels in confirm mode of
    unhandled messages.  If a publisher receives this method, it probably
    needs to republish the offending messages.
    """

    IDS_TO_METHODS = {
        (60, 10): Qos,
        (60, 11): QosOK,
        (60, 20): Consume,
        (60, 21): ConsumeOK,
        (60, 30): Cancel,
        (60, 31): CancelOK,
        (60, 40): Publish,
        (60, 50): Return,
        (60, 60): Deliver,
        (60, 70): Get,
        (60, 71): GetOK,
        (60, 72): GetEmpty,
        (60, 80): Ack,
        (60, 90): Reject,
        (60, 100): RecoverAsync,
        (60, 110): Recover,
        (60, 111): RecoverOK,
        (60, 120): Nack,
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

    Select = 'Tx.Select' / c.Struct()
    """Select standard transaction mode.

    This method sets the channel to use standard transactions. The client
    must use this method at least once on a channel before using the Commit
    or Rollback methods.
    """

    SelectOK = 'Tx.SelectOK' / c.Struct()
    """Confirm transaction mode.

    This method confirms to the client that the channel was successfully set
    to use standard transactions.
    """

    Commit = 'Tx.Commit' / c.Struct()
    """Commit the current transaction.

    This method commits all message publications and acknowledgments
    performed in the current transaction.  A new transaction starts
    immediately after a commit.
    """

    CommitOK = 'Tx.CommitOK' / c.Struct()
    """Confirm a successful commit.

    This method confirms to the client that the commit succeeded. Note that
    if a commit fails, the server raises a channel exception.
    """

    Rollback = 'Tx.Rollback' / c.Struct()
    """Abandon the current transaction.

    This method abandons all message publications and acknowledgments
    performed in the current transaction. A new transaction starts
    immediately after a rollback. Note that unacked messages will not be
    automatically redelivered by rollback; if that is required an explicit
    recover call should be issued.
    """

    RollbackOK = 'Tx.RollbackOK' / c.Struct()
    """Confirm successful rollback.

    This method confirms to the client that the rollback succeeded. Note
    that if an rollback fails, the server raises a channel exception.
    """

    IDS_TO_METHODS = {
        (90, 10): Select,
        (90, 11): SelectOK,
        (90, 20): Commit,
        (90, 21): CommitOK,
        (90, 30): Rollback,
        (90, 31): RollbackOK,
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

    Select = 'Confirm.Select' / c.Struct(
        'nowait' / d.Bit,
    )
    """Put the channel in confirm mode.

    This method sets the channel to use publisher acknowledgements. The
    client can only use this method on a non-transactional channel.
    """

    SelectOK = 'Confirm.SelectOK' / c.Struct()
    """Signal that the channel is successfully entered confirm mode.

    This method confirms to the client that the channel was successfully set
    to use publisher acknowledgements.
    """

    IDS_TO_METHODS = {
        (85, 10): Select,
        (85, 11): SelectOK,
    }


IDS_TO_METHODS = {
    key: value for key, value in chain(
        Connection.IDS_TO_METHODS.items(),
        Channel.IDS_TO_METHODS.items(),
        Exchange.IDS_TO_METHODS.items(),
        Queue.IDS_TO_METHODS.items(),
        Basic.IDS_TO_METHODS.items(),
        Tx.IDS_TO_METHODS.items(),
        Confirm.IDS_TO_METHODS.items(),
    )
}
