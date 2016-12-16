import io
import logging
import functools
import itertools
import collections
from concurrent.futures import Future

import amqpframe
import amqpframe.basic
import amqpframe.methods

from fsmpy import FunctionalMachine

from . import fsm
from .abstract_channel import AbstractChannel

logger = logging.getLogger(__name__)


class Channel(AbstractChannel):

    def __init__(self, channel_id, frame_max):
        super().__init__(channel_id)

        self._fsm = FunctionalMachine(
            'channel_fsm',
            transitions=fsm.Channel.transitions,
            states=fsm.Channel.states,
            initial_state=fsm.Channel.initial_state,
        )
        self._framing_fsm = FunctionalMachine(
            'framing_fsm',
            transitions=fsm.ChannelFraming.transitions,
            states=fsm.ChannelFraming.states,
            initial_state=fsm.ChannelFraming.initial_state,
        )

        # amqpframe.basic.Message is instantiated in received_BasicDeliver,
        # its delivery_info is set. Later, basic properties are set in
        # handle_frame: ContentHeaderFrame and body is updated in
        # handle_frame: ContentBodyFrame
        self._message = None
        self._message_fut = None

        self._frame_max = frame_max

        # Consumer callback
        self._on_message_received = None

        self.active = True

    def _setup_method_handlers(self):
        methods = amqpframe.methods
        return {
            methods.ChannelOpenOK: self.receive_ChannelOpenOK,
            methods.ChannelClose: self.receive_ChannelClose,
            methods.ChannelCloseOK: self.receive_ChannelCloseOK,
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

            methods.ConfirmSelectOK: self.receive_ConfirmSelectOK,

            methods.TxSelectOK: self.receive_TxSelectOK,
            methods.TxCommitOK: self.receive_TxCommitOK,
            methods.TxRollbackOK: self.receive_TxRollbackOK,
        }

    def _send_method(self, method):
        logger.debug('Sending %s [channel_id:%s]', method.__class__.__name__, self._channel_id)
        if getattr(method, 'no_wait', False) or getattr(method, 'nowait', False):
            event = 'send_MethodFrame_nowait'
        elif getattr(method, 'content', False):
            event = 'send_MethodFrame_content'
        else:
            event = 'send_MethodFrame'
        self._framing_fsm.trigger(event)
        self._fsm.trigger('send_' + method.__class__.__name__)
        frame = amqpframe.MethodFrame(self._channel_id, method)
        frame.to_bytestream(self._buffer)

    def handle_frame(self, frame):
        if isinstance(frame, amqpframe.MethodFrame):
            method = frame.payload
            logger.debug('Receiving %s [channel_id:%s]', method.__class__.__name__, self._channel_id)
            if getattr(method, 'content', False):
                event = 'receive_MethodFrame_content'
            else:
                event = 'receive_MethodFrame'
            self._framing_fsm.trigger(event)
            if self._message is not None:
                # A peer decided to stop sending the message for some reason
                self._process_message()
            self._fsm.trigger('receive_' + frame.payload.__class__.__name__)
            self.handle_MethodFrame(frame)

        elif isinstance(frame, amqpframe.ContentHeaderFrame):
            self._framing_fsm.trigger('receive_ContentHeaderFrame')

            self._message.__dict__.update(**frame.payload.properties)
            self._message.body_size = frame.payload.body_size
            if self._message.body_size == 0:
                self._process_message()

        elif isinstance(frame, amqpframe.ContentBodyFrame):
            self._framing_fsm.trigger('receive_ContentBodyFrame')

            self._message.body += frame.payload.data
            if len(self._message.body) == self._message.body_size:
                # Message is received completely
                self._process_message()

    def send_ContentHeaderFrame(self, payload):
        self._framing_fsm.trigger('send_ContentHeaderFrame')
        frame = amqpframe.ContentHeaderFrame(self._channel_id, payload)
        frame.to_bytestream(self._buffer)

    def send_ContentBodyFrame(self, payload):
        self._framing_fsm.trigger('send_ContentBodyFrame')
        frame = amqpframe.ContentBodyFrame(self._channel_id, payload)
        frame.to_bytestream(self._buffer)

    def _process_message(self):
        message, self._message = self._message, None
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

    def send_ChannelOpen(self):
        method = amqpframe.methods.ChannelOpen()
        self._send_method(method)
        return self._fut

    def receive_ChannelOpenOK(self, frame):
        self._fut.set_result(frame.payload)
        self._fut = Future()

    def send_ChannelClose(self, reply_code, reply_text, class_id=0, method_id=0):
        method = amqpframe.methods.ChannelClose(
            reply_code=reply_code, reply_text=reply_text,
            class_id=class_id, method_id=method_id,
        )
        self._send_method(method)
        return self._fut

    def receive_ChannelCloseOK(self, frame):
        self._fut.set_result(frame.payload)
        self._fut = Future()

    def receive_ChannelClose(self, frame):
        logger.debug(frame.payload)
        self.send_ChannelCloseOK()

    def send_ChannelCloseOK(self):
        method = amqpframe.methods.ChannelCloseOK()
        self._send_method(method)
        return self._fut

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
        if not no_wait:
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
                         arguments=None):
        method = amqpframe.methods.QueueUnbind(
            queue=queue, exchange=exchange, routing_key=routing_key,
            arguments=arguments,
        )
        self._send_method(method)
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
        method = amqpframe.methods.BasicPublish(
            exchange=exchange, routing_key=routing_key,
            mandatory=mandatory, immediate=immediate,
        )
        self._send_method(method)

        header_payload = amqpframe.ContentHeaderPayload(
            class_id=method.method_type[0],
            body_size=message.body_size,
            properties=message.properties,
        )
        self.send_ContentHeaderFrame(header_payload)

        body_payload = amqpframe.ContentBodyPayload(message.body)
        self.send_ContentBodyFrame(body_payload)
        if immediate:
            return self._fut

    def receive_BasicReturn(self, frame):
        logger.debug(frame)
        self._fut.set_result(frame.payload)
        self._fut = Future()

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
        self._fut = Future()

    def receive_BasicGetEmpty(self, frame):
        self._message_fut = Future()
        self._fut.set_result(self._message_fut)
        self._fut = Future()

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

    def receive_ConfirmSelectOK(self, frame):
        pass

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


def channel_factory(channel_max):
    next_channel_id = 1

    def produce_channel(frame_max):
        nonlocal next_channel_id
        channel_id = next_channel_id
        next_channel_id += 1

        return Channel(channel_id, frame_max)

    return produce_channel
