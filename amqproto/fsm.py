import itertools

from fsmpy import Transition as T

# Set of finite-state machines describing various
# parts of AMQP protocol.


class Connection:
    initial_state = 'disconnected'

    states = [
        'disconnected',
        'sent_ProtocolHeaderFrame',
        'received_ConnectionStart',
        'sent_ConnectionStartOK',
        'received_ConnectionSecure',
        'sent_ConnectionSecureOK',
        'received_ConnectionTune',
        'sent_ConnectionTuneOK',
        'sent_ConnectionOpen',
        'idle',
        'received_ConnectionClose',
        'sent_ConnectionClose',
    ]
    transitions = [
        T(event='send_ProtocolHeaderFrame',
          source='disconnected',
          dest='sent_ProtocolHeaderFrame'),

        T(event='receive_ProtocolHeaderFrame',
          source='sent_ProtocolHeaderFrame',
          dest='disconnected'),

        T(event='receive_ConnectionStart',
          source='sent_ProtocolHeaderFrame',
          dest='received_ConnectionStart'),

        T(event='send_ConnectionStartOK',
          source='received_ConnectionStart',
          dest='sent_ConnectionStartOK'),

        T(event='receive_ConnectionSecure',
          source='sent_ConnectionStartOK',
          dest='received_ConnectionSecure'),

        T(event='send_ConnectionSecureOK',
          source='received_ConnectionSecure',
          dest='sent_ConnectionSecureOK'),

        T(event='receive_ConnectionTune',
          source='sent_ConnectionSecureOK',
          dest='received_ConnectionTune'),

        T(event='send_ConnectionTuneOK',
          source='received_ConnectionTune',
          dest='sent_ConnectionTuneOK'),

        T(event='receive_ConnectionTune',
          source='sent_ConnectionStartOK',
          dest='received_ConnectionTune'),

        T(event='send_ConnectionTuneOK',
          source='received_ConnectionTune',
          dest='sent_ConnectionTuneOK'),

        T(event='send_ConnectionOpen',
          source='sent_ConnectionTuneOK',
          dest='sent_ConnectionOpen'),

        T(event='receive_ConnectionOpenOK',
          source='sent_ConnectionOpen',
          dest='idle'),

        T(event='receive_ConnectionClose',
          source='.*',
          dest='received_ConnectionClose'),

        T(event='send_ConnectionCloseOK',
          source='received_ConnectionClose',
          dest='disconnected'),

        T(event='send_ConnectionClose',
          source='.*',
          dest='sent_ConnectionClose'),

        T(event='receive_ConnectionCloseOK',
          source='sent_ConnectionClose',
          dest='disconnected'),
    ]


class Exchange:
    states = [
        'sent_ExchangeDeclare',
        'sent_ExchangeBind',
        'sent_ExchangeUnbind',
    ]

    transitions = [
        T(event='send_ExchangeDeclare',
          source='channel_idle',
          dest='sent_ExchangeDeclare'),

        T(event='receive_ExchangeDeclareOK',
          source='sent_ExchangeDeclare',
          dest='channel_idle'),

        T(event='send_ExchangeDeclare_nowait',
          source='channel_idle',
          dest='channel_idle'),

        T(event='receive_ExchangeDeclare_nowait',
          source='channel_idle',
          dest='channel_idle'),

        T(event='send_ExchangeBind',
          source='channel_idle',
          dest='sent_ExchangeBind'),

        T(event='receive_ExchangeBindOK',
          source='sent_ExchangeBind',
          dest='channel_idle'),

        T(event='send_ExchangeBind_nowait',
          source='channel_idle',
          dest='channel_idle'),

        T(event='send_ExchangeUnbind',
          source='channel_idle',
          dest='sent_ExchangeUnbind'),

        T(event='receive_ExchangeUnbindOK',
          source='sent_ExchangeUnbind',
          dest='channel_idle'),

        T(event='send_ExchangeUnbind_nowait',
          source='channel_idle',
          dest='channel_idle'),

        T(event='send_ExchangeDelete',
          source='channel_idle',
          dest='sent_ExchangeDelete'),

        T(event='receive_ExchangeDeleteOK',
          source='sent_ExchangeDelete',
          dest='channel_idle'),

        T(event='send_ExchangeDelete_nowait',
          source='channel_idle',
          dest='channel_idle'),
    ]


class Queue:
    states = [
        'sent_QueueDeclare',
        'sent_QueueBind',
        'sent_QueueUnbind',
        'sent_QueuePurge',
        'sent_QueueDelete',
    ]

    transitions = [
        T(event='send_QueueDeclare',
          source='channel_idle',
          dest='sent_QueueDeclare'),

        T(event='receive_QueueDeclareOK',
          source='sent_QueueDeclare',
          dest='channel_idle'),

        T(event='send_QueueDeclare_nowait',
          source='channel_idle',
          dest='channel_idle'),

        T(event='send_QueueBind',
          source='channel_idle',
          dest='sent_QueueBind'),

        T(event='receive_QueueBindOK',
          source='sent_QueueBind',
          dest='channel_idle'),

        T(event='send_QueueBind_nowait',
          source='channel_idle',
          dest='channel_idle'),

        T(event='send_QueueUnbind',
          source='channel_idle',
          dest='sent_QueueUnbind'),

        T(event='receive_QueueUnbindOK',
          source='sent_QueueUnbind',
          dest='channel_idle'),

        T(event='send_QueuePurge',
          source='channel_idle',
          dest='sent_QueuePurge'),

        T(event='receive_QueuePurgeOK',
          source='sent_QueuePurge',
          dest='channel_idle'),

        T(event='send_QueuePurge_nowait',
          source='channel_idle',
          dest='channel_idle'),

        T(event='send_QueueDelete',
          source='channel_idle',
          dest='sent_QueueDelete'),

        T(event='receive_QueueDeleteOK',
          source='sent_QueueDelete',
          dest='channel_idle'),

        T(event='send_QueueDelete_nowait',
          source='channel_idle',
          dest='channel_idle'),
    ]


class Basic:
    states = [
        'sent_BasicQos',
        'sent_BasicConsume',
        'sent_BasicCancel',
        'sent_BasicGet',
        'sent_BasicRecover',
    ]

    transitions = [
        T(event='send_BasicQos',
          source='channel_idle',
          dest='sent_BasicQos'),

        T(event='receive_BasicQosOK',
          source='sent_BasicQos',
          dest='channel_idle'),

        T(event='send_BasicConsume',
          source='channel_idle',
          dest='sent_BasicConsume'),

        T(event='receive_BasicConsumeOK',
          source='sent_BasicConsume',
          dest='channel_idle'),

        T(event='send_BasicConsume_nowait',
          source='channel_idle',
          dest='channel_idle'),

        T(event='send_BasicCancel',
          source='channel_idle',
          dest='sent_BasicCancel'),

        T(event='receive_BasicCancelOK',
          source='sent_BasicCancel',
          dest='channel_idle'),

        T(event='send_BasicCancel_nowait',
          source='channel_idle',
          dest='channel_idle'),

        T(event='send_BasicPublish',
          source='channel_idle',
          dest='channel_idle'),

        T(event='receive_BasicReturn',
          source='channel_idle',
          dest='channel_idle'),

        T(event='receive_BasicDeliver',
          source='channel_idle',
          dest='channel_idle'),

        T(event='send_BasicGet',
          source='channel_idle',
          dest='sent_BasicGet'),

        T(event='receive_BasicGetOK',
          source='sent_BasicGet',
          dest='channel_idle'),

        T(event='receive_BasicGetEmpty',
          source='sent_BasicGet',
          dest='channel_idle'),

        T(event='send_BasicAck',
          source='channel_idle',
          dest='channel_idle'),

        T(event='receive_BasicAck',
          source='channel_idle',
          dest='channel_idle'),

        T(event='send_BasicReject',
          source='channel_idle',
          dest='channel_idle'),

        T(event='send_BasicRecoverAsync',
          source='channel_idle',
          dest='channel_idle'),

        T(event='send_BasicRecover',
          source='channel_idle',
          dest='sent_BasicRecover'),

        T(event='receive_BasicRecoverOK',
          source='sent_BasicRecover',
          dest='channel_idle'),

        T(event='send_BasicNack',
          source='channel_idle',
          dest='channel_idle'),

        T(event='receive_BasicNack',
          source='channel_idle',
          dest='channel_idle'),
    ]


class Tx:
    states = [
        'sent_TxSelect',
        'sent_TxCommit'
        'sent_TxRollback',
    ]

    transitions = [
        T(event='send_TxSelect',
          # A channel can't start a transaction if it's already
          # holding a transaction or is in confirmation mode.
          source='channel_idle',
          dest='sent_TxSelect'),

        T(event='receive_TxSelectOK',
          source='sent_TxSelect',
          dest='channel_idle_tx'),

        T(event='send_TxCommit',
          # A commit may only happen in a channel holding a transaction.
          source='channel_idle_tx',
          dest='sent_TxCommit'),

        T(event='receive_TxCommitOK',
          source='sent_TxCommit',
          dest='channel_idle'),

        T(event='send_TxRollback',
          # A rollback may only happen in a channel holding a transaction.
          source='channel_idle_tx',
          dest='sent_TxRollback'),

        T(event='receive_TxRollbackOK',
          source='sent_TxRollback',
          dest='channel_idle'),
    ]


class Confirm:
    states = [
        'sent_ConfirmSelect',
    ]

    transitions = [
        T(event='send_ConfirmSelect',
          # A channel can't be selected to work in confirmation mode
          # if it's already in confirmation mode or a transaction is active.
          source='channel_idle',
          dest='sent_ConfirmSelect'),

        T(event='receive_ConfirmSelectOK',
          source='sent_ConfirmSelect',
          dest='channel_idle_confirm'),

        T(event='send_ConfirmSelect_nowait',
          source='channel_idle',
          dest='channel_idle_confirm'),

        # If confirm mode is enabled, BasicPublish should wait for BasicAck.
        T(event='send_BasicPublish',
          source='channel_idle_confirm',
          dest='sent_BasicPublish_confirm'),

        T(event='receive_BasicAck',
          source='sent_BasicPublish_confirm',
          dest='channel_idle_confirm'),
    ]


class ChannelFraming:
    initial_state = 'channel_idle'

    states = [
        'channel_idle',
        'sent_MethodFrame',
        'sent_MethodFrame_content',
        'received_MethodFrame',
        'received_MethodFrame_content',
        'sent_ContentHeaderFrame',
        'received_ContentHeaderFrame',
    ]

    transitions = [
        # Client sends something to the server
        T(event='send_MethodFrame',
          source='channel_idle',
          dest='sent_MethodFrame'),

        # and received its response.
        T(event='receive_MethodFrame',
          source='sent_MethodFrame',
          dest='channel_idle'),

        # Client sends something asynchronously.
        T(event='send_MethodFrame_nowait',
          source='channel_idle',
          dest='channel_idle'),

        # Server sends something to the client
        T(event='receive_MethodFrame',
          source='channel_idle',
          dest='received_MethodFrame'),

        # and receives clients response.
        T(event='send_MethodFrame',
          source='received_MethodFrame',
          dest='channel_idle'),

        T(event='send_MethodFrame_content',
          source='channel_idle',
          dest='sent_MethodFrame_content'),

        T(event='receive_MethodFrame_content',
          source='channel_idle',
          dest='received_MethodFrame_content'),

        T(event='receive_MethodFrame_content',
          source='sent_MethodFrame',
          dest='received_MethodFrame_content'),

        # If ContentHeader comes after MethodFrame,
        T(event='receive_ContentHeaderFrame',
          source='received_MethodFrame_content',
          dest='received_ContentHeaderFrame'),

        # zero or more ContentBody frames can arrive.
        T(event='receive_ContentBodyFrame',
          source='received_ContentHeaderFrame',
          dest='received_ContentHeaderFrame'),

        # If a MethodFrame arrives, treat this as the end of the body.
        T(event='receive_MethodFrame',
          source='received_ContentHeaderFrame',
          dest='received_MethodFrame'),

        T(event='receive_MethodFrame_content',
          source='received_ContentHeaderFrame',
          dest='received_MethodFrame_content'),

        T(event='send_MethodFrame',
          source='received_ContentHeaderFrame',
          dest='sent_MethodFrame'),

        T(event='send_MethodFrame_content',
          source='received_ContentHeaderFrame',
          dest='sent_MethodFrame_content'),

        # Same for the client.
        # If ContentHeader comes after MethodFrame,
        T(event='send_ContentHeaderFrame',
          source='sent_MethodFrame_content',
          dest='sent_ContentHeaderFrame'),

        # zero or more ContentBody frames can be sent.
        T(event='send_ContentBodyFrame',
          source='sent_ContentHeaderFrame',
          dest='sent_ContentHeaderFrame'),

        # If a MethodFrame is sent, treat this as the end of the body.
        T(event='send_MethodFrame',
          source='sent_ContentHeaderFrame',
          dest='sent_MethodFrame'),

        T(event='send_MethodFrame_content',
          source='send_MethodFrame',
          dest='sent_MethodFrame_content'),

        T(event='send_MethodFrame_content',
          source='sent_ContentHeaderFrame',
          dest='sent_MethodFrame_content'),

        # Don't forget about asynchronous methods.
        T(event='send_MethodFrame_nowait',
          source='sent_ContentHeaderFrame',
          dest='channel_idle'),

        T(event='send_MethodFrame',
          source='sent_MethodFrame',
          dest='sent_MethodFrame'),

        T(event='send_MethodFrame_nowait',
          source='sent_MethodFrame',
          dest='channel_idle'),

        # This event should be triggered when a basic message
        # is received completely (or partially, according to the spec).
        # This can happen right after received_ContentHeaderFrame
        # if body length is 0, or after a bunch of receive_ContentBodyFrame
        # events (which all ends up in received_ContentHeaderFrame state).
        # The last case is after received_MethodFrame in case a peer decided
        # to stop the transmission.
        T(event='receive_BasicMessage',
          source='.*',
          dest='channel_idle'),
    ]


class Channel:
    initial_state = 'channel_disconnected'

    states = ([
               'channel_disconnected',
               'channel_idle',
               'channel_idle_tx',
               'channel_idle_confirm',
               'sent_ChannelOpen',
               'sent_ChannelClose',
               'received_ChannelClose',
               'sent_ChannelFlow',
               'received_ChannelFlow'] +
              Exchange.states +
              Queue.states +
              Basic.states +
              Confirm.states +
              # Don't forget to add transaction-aware states
              [state + '_tx'
               for state in itertools.chain(
                   Exchange.states,
                   Queue.states,
                   Basic.states)] +
              # Don't forget to add confirm-aware states
              [state + '_confirm'
               for state in itertools.chain(
                   Exchange.states,
                   Queue.states,
                   Basic.states)] +
              Tx.states)

    transitions = ([
        T(event='send_ChannelOpen',
          source='channel_disconnected',
          dest='sent_ChannelOpen'),

        T(event='receive_ChannelOpenOK',
          source='sent_ChannelOpen',
          dest='channel_idle'),

        T(event='send_ChannelClose',
          source='.*',
          dest='sent_ChannelClose'),

        T(event='receive_ChannelCloseOK',
          source='sent_ChannelClose',
          dest='channel_disconnected'),

        T(event='receive_ChannelClose',
          source='.*',
          dest='received_ChannelClose'),

        T(event='send_ChannelCloseOK',
          source='received_ChannelClose',
          dest='channel_disconnected'),

        T(event='send_ChannelFlow',
          source='channel_idle',
          dest='sent_ChannelFlow'),

        T(event='receive_ChannelFlowOK',
          source='sent_ChannelFlow',
          dest='channel_idle'),

        T(event='receive_ChannelFlow',
          source='channel_idle',
          dest='received_ChannelFlow'),

        T(event='send_ChannelFlowOK',
          source='received_ChannelFlow',
          dest='channel_idle'),
        ] + Exchange.transitions +
                   Queue.transitions +
                   Basic.transitions +
                   Confirm.transitions +
                   # Don't forget to add transaction-aware transitions
                   [T(event=t.event, source=t.source + '_tx',
                      dest=t.dest+'_tx')
                    for t in itertools.chain(
                        Exchange.transitions,
                        Queue.transitions,
                        Basic.transitions)] +
                   # Don't forget to add confirm-aware transitions
                   [T(event=t.event, source=t.source + '_confirm',
                      dest=t.dest+'_confirm')
                    for t in itertools.chain(
                        Exchange.transitions,
                        Queue.transitions,
                        Basic.transitions)] +
                   Tx.transitions)
