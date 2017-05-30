import io
import logging
import collections
import collections.abc
from concurrent.futures import Future

from . import protocol

from . import fsm

logger = logging.getLogger(__name__)


class Channel:

    Future = Future

    def __init__(self, channel_id, frame_max):
        self._channel_id = channel_id
        self._frame_max = frame_max

        self._buffer = io.BytesIO()
        # Future used to synchronise Do/DoOK methods
        self._fut = self.Future()
        self._method_handlers = self._setup_method_handlers()

        self._fsm = fsm.FunctionalMachine(
            'channel_fsm',
            transitions=fsm.Channel.transitions,
            states=fsm.Channel.states,
            initial_state=fsm.Channel.initial_state,
        )
        self._framing_fsm = fsm.FunctionalMachine(
            'framing_fsm',
            transitions=fsm.ChannelFraming.transitions,
            states=fsm.ChannelFraming.states,
            initial_state=fsm.ChannelFraming.initial_state,
        )

        # protocol.Message is instantiated in _receive_BasicDeliver,
        # its delivery_info is set. Later, basic properties are set in
        # handle_frame: ContentHeaderFrame and body is updated in
        # handle_frame: ContentBodyFrame
        self._message = None
        self._message_fut = None

        # consumer_tag -> message_future mapping
        self._consumers = {}

        # Sequence number of next published message requiring confirmation.
        self._next_publish_seq_no = 0
        self._unconfirmed_set = set()
        self._ack_fut = self.Future()
        self._nack_fut = self.Future()

        self.alive = True
        self.active = True

    def _setup_method_handlers(self):
        return {
            protocol.ChannelOpenOK: self._receive_ChannelOpenOK,
            protocol.ChannelClose: self._receive_ChannelClose,
            protocol.ChannelCloseOK: self._receive_ChannelCloseOK,
            protocol.ChannelFlow: self._receive_ChannelFlow,
            protocol.ChannelFlowOK: self._receive_ChannelFlowOK,

            protocol.ExchangeDeclareOK: self._receive_ExchangeDeclareOK,
            protocol.ExchangeBindOK: self._receive_ExchangeBindOK,
            protocol.ExchangeUnbindOK: self._receive_ExchangeUnbindOK,
            protocol.ExchangeDeleteOK: self._receive_ExchangeDeleteOK,

            protocol.QueueDeclareOK: self._receive_QueueDeclareOK,
            protocol.QueueBindOK: self._receive_QueueBindOK,
            protocol.QueueUnbindOK: self._receive_QueueUnbindOK,
            protocol.QueuePurgeOK: self._receive_QueuePurgeOK,
            protocol.QueueDeleteOK: self._receive_QueueDeleteOK,

            protocol.BasicConsumeOK: self._receive_BasicConsumeOK,
            protocol.BasicCancelOK: self._receive_BasicCancelOK,
            protocol.BasicDeliver: self._receive_BasicDeliver,
            protocol.BasicGetOK: self._receive_BasicGetOK,
            protocol.BasicGetEmpty: self._receive_BasicGetEmpty,
            protocol.BasicAck: self._receive_BasicAck,
            protocol.BasicNack: self._receive_BasicNack,
            protocol.BasicQosOK: self._receive_BasicQosOK,
            protocol.BasicRecoverOK: self._receive_BasicRecoverOK,
            protocol.BasicReturn: self._receive_BasicReturn,

            protocol.ConfirmSelectOK: self._receive_ConfirmSelectOK,

            protocol.TxSelectOK: self._receive_TxSelectOK,
            protocol.TxCommitOK: self._receive_TxCommitOK,
            protocol.TxRollbackOK: self._receive_TxRollbackOK,
        }

    def data_to_send(self):
        data = self._buffer.getvalue()
        self._buffer = io.BytesIO()
        return data

    def _send_method(self, method):
        logger.debug(
            'Sending MethodFrame %s [channel_id:%s]',
            method.__class__.__name__, self._channel_id,
        )
        no_wait = (getattr(method, 'no_wait', False) or
                   getattr(method, 'nowait', False))
        if no_wait:
            event = 'send_MethodFrame_nowait'
        elif getattr(method, 'content', False):
            event = 'send_MethodFrame_content'
        else:
            event = 'send_MethodFrame'
        self._framing_fsm.trigger(event)
        self._fsm.trigger('send_{}{}'.format(
            method.__class__.__name__, '_nowait' if no_wait else ''
        ))
        frame = protocol.MethodFrame(self._channel_id, method)
        frame.to_bytestream(self._buffer)

    def handle_frame(self, frame):
        if isinstance(frame, protocol.MethodFrame):
            method = frame.payload
            if getattr(method, 'content', False):
                event = 'receive_MethodFrame_content'
            else:
                event = 'receive_MethodFrame'
            logger.debug(
                'Receiving MethodFrame %s [channel_id:%s]',
                method.__class__.__name__, self._channel_id,
            )
            self._framing_fsm.trigger(event)
            if self._message is not None:
                # A peer decided to stop sending the message for some reason
                self._process_message()
            self._fsm.trigger('receive_' + frame.payload.__class__.__name__)
            method = frame.payload
            handler = self._method_handlers[method.__class__]
            handler(frame)

        elif isinstance(frame, protocol.ContentHeaderFrame):
            logger.debug(
                'Receiving ContentHeaderFrame [channel_id:%s]',
                self._channel_id
            )
            self._framing_fsm.trigger('receive_ContentHeaderFrame')

            self._message.__dict__.update(**frame.payload.properties)
            self._message.body_size = frame.payload.body_size
            if self._message.body_size == 0:
                self._process_message()

        elif isinstance(frame, protocol.ContentBodyFrame):
            logger.debug(
                'Receiving ContentBodyFrame [channel_id:%s]',
                self._channel_id
            )
            self._framing_fsm.trigger('receive_ContentBodyFrame')

            self._message.body += frame.payload.data
            if len(self._message.body) == self._message.body_size:
                # Message is received completely
                self._process_message()

    def _send_ContentHeaderFrame(self, payload):
        self._framing_fsm.trigger('send_ContentHeaderFrame')
        frame = protocol.ContentHeaderFrame(self._channel_id, payload)
        frame.to_bytestream(self._buffer)

    def _send_ContentBodyFrame(self, payload):
        self._framing_fsm.trigger('send_ContentBodyFrame')
        frame = protocol.ContentBodyFrame(self._channel_id, payload)
        frame.to_bytestream(self._buffer)

    def _process_message(self):
        self._framing_fsm.trigger('receive_BasicMessage')

        message, self._message = self._message, None
        if self._message_fut is not None:
            # BasicGet route
            self._message_fut.set_result(message)
            self._message_fut = None
        else:
            # BasicConsume route
            consumer_tag = message.delivery_info.consumer_tag
            message_fut = self._consumers[consumer_tag]
            fut = self._consumers[consumer_tag] = self.Future()
            message_fut.set_result((message, fut))

    def open(self):
        method = protocol.ChannelOpen()
        self._send_method(method)
        return self._fut

    def _receive_ChannelOpenOK(self, frame):
        self.alive = True
        self._fut.set_result(None)
        self._fut = self.Future()

    def close(self, reply_code, reply_text, class_id=0, method_id=0):
        method = protocol.ChannelClose(
            reply_code=reply_code, reply_text=reply_text,
            class_id=class_id, method_id=method_id,
        )
        self._send_method(method)
        return self._fut

    def _receive_ChannelCloseOK(self, frame):
        self.alive = False
        self._fut.set_result(None)
        self._fut = self.Future()

    def _receive_ChannelClose(self, frame):
        method = frame.payload
        AMQPError = protocol.ERRORS_BY_CODE[method.reply_code]
        exc = AMQPError(
            method.reply_text,
            method.class_id,
            method.method_id,
        )
        self.send_ChannelCloseOK(exc)

    def send_ChannelCloseOK(self, _exc):
        self.alive = False
        method = protocol.ChannelCloseOK()
        self._send_method(method)
        raise _exc

    def flow(self, active):
        method = protocol.ChannelFlow(active=active)
        self._send_method(method)
        return self._fut

    def _receive_ChannelFlowOK(self, frame):
        self._fut.set_result(frame.payload.active)
        self._fut = self.Future()

    def _receive_ChannelFlow(self, frame):
        self.active = frame.payload.active
        self.send_ChannelFlowOK(active=self.active)
        self._fut.set_result(self.active)
        self._fut = self.Future()

    def send_ChannelFlowOK(self, active):
        method = protocol.ChannelFlowOK(active=active)
        self._send_method(method)
        return self._fut

    def exchange_declare(self, exchange, type='direct', passive=False,
                         durable=False, auto_delete=True, internal=False,
                         no_wait=False, arguments=None):
        method = protocol.ExchangeDeclare(
            exchange=exchange, type=type, passive=passive, durable=durable,
            auto_delete=auto_delete, internal=internal, no_wait=no_wait,
            arguments=arguments
        )
        self._send_method(method)
        if not no_wait:
            return self._fut

    def _receive_ExchangeDeclareOK(self, frame):
        self._fut.set_result(None)
        self._fut = self.Future()

    def exchange_delete(self, exchange, if_unused=False, no_wait=False):
        method = protocol.ExchangeDelete(
            exchange=exchange, if_unused=if_unused, no_wait=no_wait
        )
        self._send_method(method)
        if not no_wait:
            return self._fut

    def _receive_ExchangeDeleteOK(self, frame):
        self._fut.set_result(None)
        self._fut = self.Future()

    def exchange_bind(self, destination, source='', routing_key='',
                      no_wait=False, arguments=None):
        method = protocol.ExchangeBind(
            destination=destination, source=source, routing_key=routing_key,
            no_wait=no_wait, arguments=arguments,
        )
        self._send_method(method)
        if not no_wait:
            return self._fut

    def _receive_ExchangeBindOK(self, frame):
        self._fut.set_result(None)
        self._fut = self.Future()

    def exchange_unbind(self, destination, source='', routing_key='',
                        no_wait=False, arguments=None):
        method = protocol.ExchangeUnbind(
            destination=destination, source=source, routing_key=routing_key,
            no_wait=no_wait, arguments=arguments,
        )
        self._send_method(method)
        if not no_wait:
            return self._fut

    def _receive_ExchangeUnbindOK(self, frame):
        self._fut.set_result(None)
        self._fut = self.Future()

    def queue_declare(self, queue='', passive=False, durable=False,
                      exclusive=False, auto_delete=True,
                      no_wait=False, arguments=None):
        method = protocol.QueueDeclare(
            queue=queue, passive=passive, durable=durable,
            exclusive=exclusive, auto_delete=auto_delete,
            no_wait=no_wait, arguments=arguments,
        )
        self._send_method(method)
        if not no_wait:
            return self._fut

    def _receive_QueueDeclareOK(self, frame):
        method = frame.payload
        self._fut.set_result((
            method.queue, method.message_count, method.consumer_count
        ))
        self._fut = self.Future()

    def queue_bind(self, queue, exchange='', routing_key='',
                   no_wait=False, arguments=None):
        method = protocol.QueueBind(
            queue=queue, exchange=exchange, routing_key=routing_key,
            no_wait=no_wait, arguments=arguments,
        )
        self._send_method(method)
        if not no_wait:
            return self._fut

    def _receive_QueueBindOK(self, frame):
        self._fut.set_result(None)
        self._fut = self.Future()

    def queue_unbind(self, queue, exchange='', routing_key='', arguments=None):
        method = protocol.QueueUnbind(
            queue=queue, exchange=exchange, routing_key=routing_key,
            arguments=arguments,
        )
        self._send_method(method)
        return self._fut

    def _receive_QueueUnbindOK(self, frame):
        self._fut.set_result(None)
        self._fut = self.Future()

    def queue_purge(self, queue, no_wait=False):
        method = protocol.QueuePurge(queue=queue, no_wait=no_wait)
        self._send_method(method)
        if not no_wait:
            return self._fut

    def _receive_QueuePurgeOK(self, frame):
        self._fut.set_result(frame.payload.message_count)
        self._fut = self.Future()

    def queue_delete(self, queue, if_unused=False, if_empty=False,
                     no_wait=False):
        method = protocol.QueueDelete(
            queue=queue, if_unused=if_unused, if_empty=if_empty,
            no_wait=no_wait,
        )
        self._send_method(method)
        if not no_wait:
            return self._fut

    def _receive_QueueDeleteOK(self, frame):
        self._fut.set_result(frame.payload.message_count)
        self._fut = self.Future()

    def basic_qos(self, prefetch_size, prefetch_count, global_):
        method = protocol.BasicQos(
            prefetch_size=prefetch_size, prefetch_count=prefetch_count,
            global_=global_,
        )
        self._send_method(method)
        return self._fut

    def _receive_BasicQosOK(self, frame):
        self._fut.set_result(None)
        self._fut = self.Future()

    def basic_consume(self, queue='', consumer_tag='',
                      no_local=False, no_ack=False, exclusive=False,
                      no_wait=False, arguments=None):
        assert consumer_tag or not no_wait, 'consumer_tag must be specified'
        method = protocol.BasicConsume(
            queue=queue, consumer_tag=consumer_tag, no_local=no_local,
            no_ack=no_ack, exclusive=exclusive, no_wait=no_wait,
            arguments=arguments,
        )

        self._send_method(method)
        if not no_wait:
            return self._fut

        fut = self._consumers[method.consumer_tag] = self.Future()
        return fut, consumer_tag

    def _receive_BasicConsumeOK(self, frame):
        consumer_tag = frame.payload.consumer_tag
        fut = self._consumers[consumer_tag] = self.Future()
        self._fut.set_result((fut, consumer_tag))
        self._fut = self.Future()

    def basic_cancel(self, consumer_tag, no_wait=False):
        method = protocol.BasicCancel(
            consumer_tag=consumer_tag, no_wait=no_wait
        )
        self._send_method(method)
        if not no_wait:
            return self._fut
        else:
            del self._consumers[method.consumer_tag]

    def _receive_BasicCancelOK(self, frame):
        consumer_tag = frame.payload.consumer_tag
        del self._consumers[consumer_tag]
        self._fut.set_result(consumer_tag)
        self._fut = self.Future()

    def basic_publish(self, message, exchange='', routing_key='',
                      mandatory=False, immediate=False):
        if self._next_publish_seq_no > 0:
            self._unconfirmed_set.add(self._next_publish_seq_no)
            self._next_publish_seq_no += 1
        method = protocol.BasicPublish(
            exchange=exchange, routing_key=routing_key,
            mandatory=mandatory, immediate=immediate,
        )
        self._send_method(method)

        header_payload = protocol.ContentHeaderPayload(
            class_id=method.method_type[0],
            body_size=message.body_size,
            properties=message.properties,
        )
        self._send_ContentHeaderFrame(header_payload)

        max_payload_size = self._frame_max - protocol.Frame.METADATA_SIZE
        for chunk in _chunked(message.body, max_payload_size):
            body_payload = protocol.ContentBodyPayload(chunk)
            self._send_ContentBodyFrame(body_payload)

    def _receive_BasicReturn(self, frame):
        # RabbitMQ does not support BasicPublish with immediate=True
        # http://www.rabbitmq.com/blog/2012/11/19/breaking-things-with-rabbitmq-3-0/
        # If you *really* *really* need to handle this case with an other
        # broker, simply hook into the specific IO adapter
        # and write this logic by yourself.
        raise NotImplementedError

    def _receive_BasicDeliver(self, frame):
        consumer_tag = frame.payload.consumer_tag
        if consumer_tag not in self._consumers:
            class_id, method_id = frame.payload.method_type
            raise protocol.CommandInvalid(
                'server has delivered a message to consumer with tag {}'
                'but there is no such consumer'.format(consumer_tag),
                class_id, method_id
            )
        self._message = protocol.BasicMessage(delivery_info=frame.payload)

    def basic_get(self, queue, no_ack=False):
        method = protocol.BasicGet(queue=queue, no_ack=no_ack)
        self._send_method(method)
        return self._fut

    def _receive_BasicGetOK(self, frame):
        method = frame.payload
        self._message = protocol.BasicMessage(delivery_info={
            'delivery_tag': method.delivery_tag,
            'redelivered': method.redelivered,
            'exchange': method.exchange,
            'routing_key': method.routing_key,
            'message_count': method.message_count,
        })
        self._message_fut = self.Future()
        self._fut.set_result(self._message_fut)
        self._fut = self.Future()

    def _receive_BasicGetEmpty(self, frame):
        self._fut.set_result(None)
        self._fut = self.Future()

    def basic_ack(self, delivery_tag='', multiple=False):
        method = protocol.BasicAck(
            delivery_tag=delivery_tag, multiple=multiple
        )
        self._send_method(method)

    def _receive_BasicAck(self, frame):
        delivery_tag = frame.payload.delivery_tag
        multiple = frame.payload.multiple
        if multiple:
            self._unconfirmed_set.difference_update(
                {i for i in range(delivery_tag)}
            )
        else:
            self._unconfirmed_set.remove(delivery_tag)

        new_ack_fut = self.Future()
        self._ack_fut.set_result(new_ack_fut)
        self._ack_fut = new_ack_fut

    def basic_reject(self, delivery_tag='', requeue=False):
        method = protocol.BasicReject(
            delivery_tag=delivery_tag, requeue=requeue,
        )
        self._send_method(method)

    def basic_recover_async(self, requeue=False):
        method = protocol.BasicRecoverAsync(requeue=requeue)
        self._send_method(method)

    def basic_recover(self, requeue=False):
        method = protocol.BasicRecover(requeue=requeue)
        self._send_method(method)
        return self._fut

    def _receive_BasicRecoverOK(self, frame):
        self._fut.set_result(None)
        self._fut = self.Future()

    def basic_nack(self, delivery_tag='', multiple=False, requeue=False):
        method = protocol.BasicNack(
            delivery_tag=delivery_tag, multiple=multiple, requeue=requeue
        )
        self._send_method(method)

    def _receive_BasicNack(self, frame):
        delivery_tag = frame.payload.delivery_tag
        multiple = frame.payload.multiple
        if multiple:
            self._unconfirmed_set.difference_update(
                {i for i in range(delivery_tag)}
            )
        else:
            self._unconfirmed_set.remove(delivery_tag)

        new_nack_fut = self.Future()
        self._nack_fut.set_result(new_nack_fut)
        self._nack_fut = new_nack_fut

    def tx_select(self):
        method = protocol.TxSelect()
        self._send_method(method)
        return self._fut

    def _receive_TxSelectOK(self, frame):
        self._fut.set_result(None)
        self._fut = self.Future()

    def tx_commit(self):
        method = protocol.TxCommit()
        self._send_method(method)
        return self._fut

    def _receive_TxCommitOK(self, frame):
        self._fut.set_result(None)
        self._fut = self.Future()

    def tx_rollback(self):
        method = protocol.TxRollback()
        self._send_method(method)
        return self._fut

    def _receive_TxRollbackOK(self, frame):
        self._fut.set_result(None)
        self._fut = self.Future()

    def confirm_select(self, no_wait=False):
        method = protocol.ConfirmSelect(nowait=no_wait)
        self._send_method(method)
        if not no_wait:
            return self._fut
        if self._next_publish_seq_no == 0:
            self._next_publish_seq_no = 1
        return self._ack_fut, self._nack_fut

    def _receive_ConfirmSelectOK(self, frame):
        if self._next_publish_seq_no == 0:
            self._next_publish_seq_no = 1
        self._fut.set_result((self._ack_fut, self._nack_fut))
        self._fut = self.Future()


class ChannelsManager(collections.abc.Mapping):

    def __init__(self, channel_max, frame_max, channel_cls):
        self.channel_cls = channel_cls
        self.channel_max = channel_max
        self.frame_max = frame_max
        self._next_channel_id = 1
        self._channels = {}

    def __getitem__(self, channel_id):
        if channel_id is None:
            channel_id = self._next_channel_id
            if channel_id > self.channel_max:
                raise protocol.HardError(
                    "can't create a channel, channel_max ({}) "
                    "is reached".format(self.channel_max)
                )
            self._next_channel_id += 1
            channel = self.channel_cls(channel_id, self.frame_max)
            self._channels[channel_id] = channel
        else:
            channel = self._channels.get(channel_id, None)
            if channel is None:
                channel = self.channel_cls(channel_id, self.frame_max)
                self._channels[channel_id] = channel
        return channel

    def __iter__(self):  # pragma: no cover
        return iter(self._channels)

    def __len__(self):  # pragma: no cover
        return len(self._channels)


def _chunked(source, size):
    for i in range(0, len(source), size):
        yield source[i:i+size]
