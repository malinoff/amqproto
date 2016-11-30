import io
import functools
import itertools
import collections
from concurrent.futures import Future

import amqpframe
import amqpframe.basic
import amqpframe.methods

from .fsm import Transition as T, Machine
from .abstract_channel import AbstractChannel


class FramingFSM:
    initial_state = 'channel_idle'

    states = [
        'channel_idle',
        'sent_MethodFrame',
        'received_MethodFrame',
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
          dest='received_MethodFrame'),

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

        # If ContentHeader comes after MethodFrame,
        T(event='receive_ContentHeaderFrame',
          source='received_MethodFrame',
          dest='received_ContentHeaderFrame'),

        # zero or more ContentBody frames can arrive.
        T(event='receive_ContentBodyFrame',
          source='received_ContentHeaderFrame',
          dest='received_ContentHeaderFrame'),

        # If a MethodFrame arrives, treat this as the end of the body.
        T(event='receive_MethodFrame',
          source='received_ContentHeaderFrame',
          dest='received_MethodFrame'),

        # Same for the client.
        # If ContentHeader comes after MethodFrame,
        T(event='send_ContentHeaderFrame',
          source='sent_MethodFrame',
          dest='sent_ContentHeaderFrame'),

        # zero or more ContentBody frames can be sent.
        T(event='send_ContentBodyFrame',
          source='sent_ContentHeaderFrame',
          dest='sent_ContentHeaderFrame'),

        # If a MethodFrame is sent, treat this as the end of the body.
        T(event='send_MethodFrame',
          source='sent_ContentHeaderFrame',
          dest='sent_MethodFrame'),

        # Don't forget about asynchronous methods.
        T(event='send_MethodFrame_nowait',
          source='sent_ContentHeaderFrame',
          dest='channel_idle'),

    ]


class ConfirmFSM:
    states = [
        'sent_ConfirmSelect',
    ]

    transitions = [
        T(event='send_ConfirmSelect',
          # A channel can't be selected to work in confirmation mode
          # if it's already in confirmation mode or a transaction is active.
          source='channel_idle',
          dest='sent_ConfirmSelect'),

        T(event='received_ConfirmSelectOK',
          source='sent_ConfirmSelect',
          dest='channel_active_confirm'),

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




class ChannelFSM:
    from .tx import TxFSM
    from .basic import BasicFSM
    from .queue import QueueFSM
    from .exchange import ExchangeFSM

    initial_state = 'channel_idle'

    states = (['channel_idle',
               'channel_idle_tx',
               'channel_idle_confirm'] +
              ExchangeFSM.states +
              QueueFSM.states +
              BasicFSM.states +
              ConfirmFSM.states +
              # Don't forget to add transaction-aware states
              [state + '_tx'
               for state in itertools.chain(
                   ExchangeFSM.states,
                   QueueFSM.states,
                   BasicFSM.states)] +
              # Don't forget to add confirm-aware states
              [state + '_confirm'
               for state in itertools.chain(
                   ExchangeFSM.states,
                   QueueFSM.states,
                   BasicFSM.states)] +
              TxFSM.states)

    transitions = (ExchangeFSM.transitions +
                   QueueFSM.transitions +
                   BasicFSM.transitions +
                   ConfirmFSM.transitions +
                   # Don't forget to add transaction-aware transitions
                   [T(event=t.event, source=t.source + '_tx',
                      dest=t.dest+'_tx')
                    for t in itertools.chain(
                        ExchangeFSM.transitions,
                        QueueFSM.transitions,
                        BasicFSM.transitions)] +
                   # Don't forget to add confirm-aware transitions
                   [T(event=t.event, source=t.source + '_confirm',
                      dest=t.dest+'_confirm')
                    for t in itertools.chain(
                        ExchangeFSM.transitions,
                        QueueFSM.transitions,
                        BasicFSM.transitions)] +
                   TxFSM.transitions)

    # Cleanup helpers
    del TxFSM
    del BasicFSM
    del QueueFSM
    del ExchangeFSM


class Channel(AbstractChannel):

    def __init__(self, channel_id):
        super().__init__(channel_id)

        self._fsm = Machine(transitions=ChannelFSM.transitions,
                            states=ChannelFSM.states,
                            initial_state=ChannelFSM.initial_state)
        self._framing_fsm = Machine(transitions=FramingFSM.transitions,
                                    states=FramingFSM.states,
                                    initial_state=FramingFSM.initial_state)

        # amqpframe.basic.Message is instantiated in received_BasicDeliver,
        # its delivery_info is set. Later, basic properties are set in
        # handle_frame: ContentHeaderFrame and body is updated in
        # handle_frame: ContentBodyFrame
        self._message = None
        self._message_fut = None

        # Consumer callback
        self._on_message_received = None

        self.active = True

    def _setup_method_handlers(self):
        methods = amqpframe.methods
        return {
            methods.ChannelFlow: self.receive_ChannelFlow,
            methods.ChannelFlowOK: self.receive_ChannelFlowOK,

            methods.ExchangeDeclareOK: self.receive_ExchangeDeclareOK,
            methods.ExchangeBindOK: self.receive_ExchangeBindOK,
            methods.ExchangeUnbindOK: self.receive_ExchangeUnbindOK,
            methods.ExchangeDeleteOK: self.receive_ExchangeDeleteOK,

            methods.QueueDeclareOK: self.receive_QueueDeclareOK,
            methods.QueueBindOK: self.receive_QueueBindOK,
            methods.QueueUnbindOK: self.receive_QueueUnbindOK,
            methods.QueuePurgeOK: self.receive_QueuePurgeOK,
            methods.QueueDeleteOK: self.receive_QueueDeleteOK,

            methods.BasicConsumeOK: self.receive_BasicConsumeOK,
            methods.BasicCancelOK: self.receive_BasicCancelOK,
            methods.BasicDeliver: self.receive_BasicDeliver,
            methods.BasicGetOK: self.receive_BasicGetOK,
            methods.BasicGetEmpty: self.receive_BasicGetEmpty,
            methods.BasicAck: self.receive_BasicAck,
            methods.BasicQosOK: self.receive_BasicQosOK,
            methods.BasicRecoverOK: self.receive_BasicRecoverOK,
            methods.BasicReturn: self.receive_BasicReturn,

            methods.ConfirmSelectOk: self.receive_ConfirmSelectOk,

            methods.TxSelectOK: self.receive_TxSelectOK,
            methods.TxCommitOK: self.receive_TxCommitOK,
            methods.TxRollbackOK: self.receive_TxRollbackOK,
        }

    def handle_frame(self, frame):
        if isinstance(frame, amqpframe.MethodFrame):
            self._framing_fsm.trigger('received_MethodFrame')
            if self._message is not None:
                # A peer decided to stop sending the message for some reason
                self._process_message()
            self.receive_MethodFrame(frame)

        elif isinstance(frame, amqpframe.ContentHeaderFrame):
            self._framing_fsm.trigger('received_ContentHeaderFrame')
            self._message.__dict__.update(**frame.payload.properties)
            self._message.body_size = frame.payload.body_size
            if self._message.body_size == 0:
                self._process_message()

        elif isinstance(frame, amqpframe.ContentBodyFrame):
            self._framing_fsm.trigger('received_ContentBodyFrame')
            self._message.body += frame.payload
            if len(self._message.body) == self._message.body_size:
                # Message is received completely
                self._process_message()

    def handle_MethodFrame(self, frame):
        self._fsm.trigger('receive_' + frame.payload.__class__.__name__)
        super().handle_MethodFrame(frame)

    def _process_message(self):
        message, seld._message = self._message, None
        if self._message_fut is not None:
            self._message_fut.set_result(message)
            self._message_fut = None
        else:
            self._on_message_received(message)

    def flow(self, active, timeout=None):
        fut = self.send_ChannelFlow(active)
        return fut.result(timeout)

    def enable_confirms(self, no_wait=False, timeout=None):
        fut = self.send_ConfirmSelect(no_wait)
        if fut is not None:
            return fut.result(timeout)

    def send_ChannelFlow(self, active):
        method = amqpframe.methods.ChannelFlow(active=active)
        self._send_method(method)
        return self._fut

    def receive_ChannelFlowOK(self, frame):
        self._fut.set_result(frame.payload)
        self._fut = Future()

    def receive_ChannelFlow(self, frame):
        self.active = frame.payload.active
        self.send_ChannelFlowOK(active=self.active)
        self._fut.set_result(frame.payload)
        self._fut = Future()

    def send_ChannelFlowOK(self, active):
        method = amqpframe.methods.ChannelFlowOK(active=active)
        self._send_method(method)
        return self._fut

    def send_ExchangeDeclare(self, exchange, type='direct', passive=False,
                             durable=False, auto_delete=True, internal=False,
                             no_wait=False, arguments=None):
        method = amqpframe.methods.ExchangeDeclare(
            exchange=exchange, type=type, passive=passive, durable=durable,
            auto_delete=auto_delete, internal=internal, no_wait=no_wait,
            arguments=arguments
        )
        self._send_method(method)
        return self._fut

    def receive_ExchangeDeclareOK(self, frame):
        self._fut.set_result(frame.payload)
        self._fut = Future()

    def send_ExchangeDelete(self, exchange, if_unused=False, no_wait=False):
        method = amqpframe.methods.ExchangeDelete(
            exchange=exchange, if_unused=if_unused, no_wait=no_wait
        )
        self._send_method(method)
        return self._fut

    def receive_ExchangeDeleteOK(self, frame):
        self._fut.set_result(frame.payload)
        self._fut = Future()

    def send_ExchangeBind(self, destination, source='', routing_key='',
                          no_wait=False, arguments=None):
        method = amqpframe.methods.ExchangeBind(
            destination=destination, source=source, routing_key=routing_key,
            no_wait=no_wait, arguments=arguments,
        )
        self._send_method(method)

    def receive_ExchangeBindOK(self, frame):
        pass

    def send_ExchangeUnbind(self, destination, source='', routing_key='',
                            no_wait=False, arguments=None):
        method = amqpframe.methods.ExchangeUnbind(
            destination=destination, source=source, routing_key=routing_key,
            no_wait=no_wait, arguments=arguments,
        )
        self._send_method(method)
        return self._fut

    def receive_ExchangeUnbindOK(self, frame):
        self._fut.set_result(frame.payload)
        self._fut = Future()

    def send_QueueDeclare(self, queue='', passive=False, durable=False,
                          exclusive=False, auto_delete=True,
                          no_wait=False, arguments=None):
        method = amqpframe.methods.QueueDeclare(
            queue=queue, passive=passive, durable=durable,
            exclusive=exclusive, auto_delete=auto_delete,
            no_wait=no_wait, arguments=arguments,
        )
        self._send_method(method)
        if not no_wait:
            return self._fut

    def receive_QueueDeclareOK(self, frame):
        self._fut.set_result(frame.payload)
        self._fut = Future()

    def send_QueueBind(self, queue, exchange='', routing_key='',
                   no_wait=False, arguments=None):
        method = amqpframe.methods.QueueBind(
            queue=queue, exchange=exchange, routing_key=routing_key,
            no_wait=no_wait, arguments=arguments,
        )
        self._send_method(method)
        if not no_wait:
            return self._fut

    def receive_QueueBindOK(self, frame):
        self._fut.set_result(frame.payload)
        self._fut = Future()

    def send_QueueUnbind(self, queue, exchange='', routing_key='',
                         no_wait=False, arguments=None):
        method = amqpframe.methods.QueueUnbind(
            queue=queue, exchange=exchange, routing_key=routing_key,
            no_wait=no_wait, arguments=arguments,
        )
        self._send_method(method)
        if not no_wait:
            return self._fut

    def receive_QueueUnbindOK(self, frame):
        self._fut.set_result(frame.payload)
        self._fut = Future()

    def send_QueuePurge(self, queue, no_wait=False):
        method = amqpframe.methods.QueuePurge(queue=queue, no_wait=no_wait)
        self._send_method(method)
        if not no_wait:
            return self._fut

    def receive_QueuePurgeOK(self, frame):
        self._fut.set_result(frame.payload)
        self._fut = Future()

    def send_QueueDelete(self, queue, if_unused=False, if_empty=False,
                         no_wait=False):
        method = amqpframe.methods.QueueDelete(
            queue=queue, if_unused=if_unused, if_empty=if_empty,
            no_wait=no_wait,
        )
        self._send_method(method)
        if not no_wait:
            return self._fut

    def receive_QueueDeleteOK(self, frame):
        self._fut.set_result(frame.payload)
        self._fut = Future()

    def send_BasicQos(self, prefetch_size, prefetch_count, global_):
        method = amqpframe.methods.BasicQos(
            prefetch_size=prefetch_size, prefetch_count=prefetch_count,
            global_=global_,
        )
        self._send_method(method)
        return self._fut

    def receive_BasicQosOK(self, frame):
        self._fut.set_result(frame.payload)
        self._fut = Future()

    def send_BasicConsume(self, on_message_received, queue='', consumer_tag='',
                          no_local=False, no_ack=False, exclusive=False,
                          no_wait=False, arguments=None):
        self._on_message_received = on_message_received
        method = amqpframe.methods.BasicConsume(
            queue=queue, consumer_tag=consumer_tag, no_local=no_local,
            no_ack=no_ack, exclusive=exclusive, no_wait=no_wait,
            arguments=arguments,
        )
        self._send_method(method)
        if not no_wait:
            return self._fut

    def receive_BasicConsumeOK(self, frame):
        self._fut.set_result(frame.payload)
        self._fut = Future()

    def send_BasicCancel(self, consumer_tag, no_wait=False):
        method = amqpframe.methods.BasicCancel(
            consumer_tag=consumer_tag, no_wait=no_wait
        )
        self._send_method(method)
        if not no_wait:
            return self._fut

    def receive_BasicCancelOK(self, frame):
        self._fut.set_result(frame.payload)
        self._fut = Future()

    def send_BasicPublish(self, message, exchange='', routing_key='',
                          mandatory=False, immediate=False):
        method = amqpframe.methods.BasicCancel(
            consumer_tag=consumer_tag, no_wait=no_wait
        )
        self._send_method(method)
        # TODO magic with Content(Header|Body)Frame

    def receive_BasicReturn(self, frame):
        # TODO implement this
        pass

    def receive_BasicDeliver(self, frame):
        self._message = amqpframe.basic.Message(delivery_info=frame.payload)

    # API is like this:
    # messages = []
    # chan.basic_get('foo').add_done_callback(
    #     lambda fut: fut.add_done_callback(lambda msg: messages.append(msg))
    # )
    def send_BasicGet(self, queue='', no_ack=False):
        method = amqpframe.methods.BasicGet(queue=queue, no_ack=no_ack)
        self._send_method(method)
        return self._fut

    def receive_BasicGetOK(self, frame):
        self._message = amqpframe.basic.Message(delivery_info=frame.payload)
        self._message_fut = Future()
        self._fut.set_result(self._message_fut)

    def receive_BasicGetEmpty(self, frame):
        self._message_fut = Future()
        self._fut.set_result(self._message_fut)

    def send_BasicAck(self, delivery_tag='', multiple=False):
        method = amqpframe.methods.BasicAck(
            delivery_tag=delivery_tag, multiple=multiple
        )
        self._send_method(method)

    def receive_BasicAck(self, frame):
        # TODO implement this
        pass

    def send_BasicReject(self, delivery_tag='', requeue=False):
        method = amqpframe.Methods.BasicReject(
            delivery_tag=delivery_tag, requeue=requeue,
        )
        self._send_method(method)

    def send_BasicRecoverAsync(self, requeue=False):
        method = amqpframe.methods.BasicRecoverAsync(requeue=requeue)
        self._send_method(method)

    def send_BasicRecover(self, requeue=False):
        method = amqpframe.methods.BasicRecover(requeue=requeue)
        self._send_method(method)
        return self._fut

    def receive_BasicRecoverOK(self, frame):
        self._fut.set_result(frame.payload)
        self._fut = Future()

    def send_BasicNack(self, delivery_tag='', multiple=False, requeue=False):
        method = amqpframe.methods.BasicNack(
            delivery_tag=delivery_tag, multiple=multiple, requeue=requeue
        )
        self._send_method(method)

    def send_TxSelect(self):
        method = amqpframe.methods.TxSelect()
        self._send_method(method)
        return self._fut

    def receive_TxSelectOK(self, frame):
        self._fut.set_result(frame.payload)
        self._fut = Future()

    def send_TxCommit(self):
        method = amqpframe.methods.TxCommit()
        self._send_method(method)
        return self._fut

    def receive_TxCommitOK(self, frame):
        self._fut.set_result(frame.payload)
        self._fut = Future()

    def send_TxRollback(self):
        method = amqpframe.methods.TxRollback()
        self._send_method(method)
        return self._fut

    def receive_TxRollbackOK(self, frame):
        self._fut.set_result(frame.payload)
        self._fut = Future()

    def send_ConfirmSelect(self, no_wait=False):
        method = amqpframe.methods.ConfirmSelect(nowait=no_wait)
        self._send_method(method)
        if not no_wait:
            return self._fut

    def receive_ConfirmSelectOk(self, frame):
        self._fut.set_result(frame.payload)
        self._fut = Future()


def channel_factory():
    next_channel_id = 1

    def produce_channel(*args, **kwargs):
        nonlocal next_channel_id
        channel_id = next_channel_id
        next_channel_id += 1

        return Channel(id=channel_id)

    return produce_channel
