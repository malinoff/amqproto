import io
import uuid
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
        # Future used to synchronise AMQP methods with OK replies
        self._fut = None
        self._method_handlers = self._setup_method_handlers()

        self._fsm = fsm.Channel()

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

        self.active = True

    def _setup_method_handlers(self):
        return {
            protocol.ChannelOpenOK: self._receive_ChannelOpenOK,
            protocol.ChannelClose: self._receive_ChannelClose,
            protocol.ChannelCloseOK: self._receive_ChannelCloseOK,
            protocol.ChannelFlow: self._receive_ChannelFlow,
            protocol.ChannelFlowOK: self._receive_method,

            protocol.ExchangeDeclareOK: self._receive_method,
            protocol.ExchangeBindOK: self._receive_method,
            protocol.ExchangeUnbindOK: self._receive_method,
            protocol.ExchangeDeleteOK: self._receive_method,

            protocol.QueueDeclareOK: self._receive_method,
            protocol.QueueBindOK: self._receive_method,
            protocol.QueueUnbindOK: self._receive_method,
            protocol.QueuePurgeOK: self._receive_method,
            protocol.QueueDeleteOK: self._receive_method,

            protocol.BasicConsumeOK: self._receive_BasicConsumeOK,
            protocol.BasicCancelOK: self._receive_method,
            protocol.BasicDeliver: self._receive_BasicDeliver,
            protocol.BasicGetOK: self._receive_BasicGetOK,
            protocol.BasicGetEmpty: self._receive_method,
            protocol.BasicAck: self._receive_BasicAck,
            protocol.BasicNack: self._receive_BasicNack,
            protocol.BasicQosOK: self._receive_method,
            protocol.BasicRecoverOK: self._receive_method,
            protocol.BasicReturn: self._receive_BasicReturn,

            protocol.ConfirmSelectOK: self._receive_ConfirmSelectOK,

            protocol.TxSelectOK: self._receive_method,
            protocol.TxCommitOK: self._receive_method,
            protocol.TxRollbackOK: self._receive_method,
        }

    def _receive_method(self, method):
        self._fut.set_result(method)

    def data_to_send(self):
        data = self._buffer.getvalue()
        self._buffer = io.BytesIO()
        return data

    def _send_method(self, method, has_reply=True, header=None, bodies=None):
        frame = protocol.MethodFrame(self._channel_id, method)
        frame.to_bytestream(self._buffer)
        if header is not None and bodies is not None:
            frame = protocol.ContentHeaderFrame(self._channel_id, header)
            frame.to_bytestream(self._buffer)
            for body in bodies:
                frame = protocol.ContentBodyFrame(self._channel_id, body)
                frame.to_bytestream(self._buffer)
        flush_future = self._flush_outbound(has_reply=has_reply)
        if has_reply:
            self._fut = self.Future()
            return self._fut
        return flush_future

    def _flush_outbound(self, has_reply):
        # To be overriden in io adapters
        pass

    def handle_frame(self, frame):
        if isinstance(frame, protocol.MethodFrame):
            method = frame.payload
            logger.debug(
                'Receiving MethodFrame %s [channel_id:%s]',
                method.__class__.__name__, self._channel_id,
            )
            if self._message is not None:
                # A peer decided to stop sending the message for some reason
                self._process_message()
            method = frame.payload
            handler = self._method_handlers[method.__class__]
            return handler(method)

        elif isinstance(frame, protocol.ContentHeaderFrame):
            logger.debug(
                'Receiving ContentHeaderFrame [channel_id:%s]',
                self._channel_id
            )

            self._message.__dict__.update(**frame.payload.properties)
            self._message.body_size = frame.payload.body_size
            if self._message.body_size == 0:
                self._process_message()

        elif isinstance(frame, protocol.ContentBodyFrame):
            logger.debug(
                'Receiving ContentBodyFrame [channel_id:%s]',
                self._channel_id
            )

            self._message.body += frame.payload.data
            if len(self._message.body) == self._message.body_size:
                # Message is received completely
                self._process_message()

    def _process_message(self):
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
            message_fut.set_result((fut, message))

    def open(self):
        self._fsm.initiate()
        method = protocol.ChannelOpen()
        return self._send_method(method)

    def _receive_ChannelOpenOK(self, method):
        self._fsm.open()
        self._fut.set_result(method)

    def close(self, reply_code, reply_text, class_id=0, method_id=0):
        self._fsm.close()
        method = protocol.ChannelClose(
            reply_code=reply_code, reply_text=reply_text,
            class_id=class_id, method_id=method_id,
        )
        return self._send_method(method)

    def _receive_ChannelCloseOK(self, method):
        self._fsm.terminate()
        self._fut.set_result(method)

    def _receive_ChannelClose(self, method):
        self._fsm.close()
        AMQPError = protocol.ERRORS_BY_CODE[method.reply_code]
        exc = AMQPError(
            method.reply_text,
            method.class_id,
            method.method_id,
        )
        self._send_ChannelCloseOK(exc)

    def _send_ChannelCloseOK(self, _exc):
        self._fsm.terminate()
        method = protocol.ChannelCloseOK()
        # XXX maybe not raise the exception here?
        self._send_method(method)
        raise _exc

    def flow(self, active):
        method = protocol.ChannelFlow(active=active)
        return self._send_method(method)

    def _receive_ChannelFlow(self, method):
        self.active = method.active
        return self._send_ChannelFlowOK(active=self.active)

    def _send_ChannelFlowOK(self, active):
        method = protocol.ChannelFlowOK(active=active)
        return self._send_method(method)

    def exchange_declare(self, exchange, type='direct', passive=False,
                         durable=False, auto_delete=True, internal=False,
                         no_wait=False, arguments=None):
        method = protocol.ExchangeDeclare(
            exchange=exchange, type=type, passive=passive, durable=durable,
            auto_delete=auto_delete, internal=internal, no_wait=no_wait,
            arguments=arguments
        )
        return self._send_method(method, has_reply=not no_wait)

    def exchange_delete(self, exchange, if_unused=False, no_wait=False):
        method = protocol.ExchangeDelete(
            exchange=exchange, if_unused=if_unused, no_wait=no_wait
        )
        return self._send_method(method, has_reply=not no_wait)

    def exchange_bind(self, destination, source='', routing_key='',
                      no_wait=False, arguments=None):
        method = protocol.ExchangeBind(
            destination=destination, source=source, routing_key=routing_key,
            no_wait=no_wait, arguments=arguments,
        )
        return self._send_method(method, has_reply=not no_wait)

    def exchange_unbind(self, destination, source='', routing_key='',
                        no_wait=False, arguments=None):
        method = protocol.ExchangeUnbind(
            destination=destination, source=source, routing_key=routing_key,
            no_wait=no_wait, arguments=arguments,
        )
        return self._send_method(method, has_reply=not no_wait)

    def queue_declare(self, queue='', passive=False, durable=False,
                      exclusive=False, auto_delete=True,
                      no_wait=False, arguments=None):
        method = protocol.QueueDeclare(
            queue=queue, passive=passive, durable=durable,
            exclusive=exclusive, auto_delete=auto_delete,
            no_wait=no_wait, arguments=arguments,
        )
        return self._send_method(method, has_reply=not no_wait)

    def queue_bind(self, queue, exchange='', routing_key='',
                   no_wait=False, arguments=None):
        method = protocol.QueueBind(
            queue=queue, exchange=exchange, routing_key=routing_key,
            no_wait=no_wait, arguments=arguments,
        )
        return self._send_method(method, has_reply=not no_wait)

    def queue_unbind(self, queue, exchange='', routing_key='', arguments=None):
        method = protocol.QueueUnbind(
            queue=queue, exchange=exchange, routing_key=routing_key,
            arguments=arguments,
        )
        return self._send_method(method)

    def queue_purge(self, queue, no_wait=False):
        method = protocol.QueuePurge(queue=queue, no_wait=no_wait)
        return self._send_method(method, has_reply=not no_wait)

    def queue_delete(self, queue, if_unused=False, if_empty=False,
                     no_wait=False):
        method = protocol.QueueDelete(
            queue=queue, if_unused=if_unused, if_empty=if_empty,
            no_wait=no_wait,
        )
        return self._send_method(method, has_reply=not no_wait)

    def basic_qos(self, prefetch_size, prefetch_count, global_):
        method = protocol.BasicQos(
            prefetch_size=prefetch_size, prefetch_count=prefetch_count,
            global_=global_,
        )
        return self._send_method(method)

    def basic_consume(self, queue='', consumer_tag='',
                      no_local=False, no_ack=False, exclusive=False,
                      no_wait=False, arguments=None):
        if not consumer_tag:
            consumer_tag = str(uuid.uuid4())
        method = protocol.BasicConsume(
            queue=queue, consumer_tag=consumer_tag, no_local=no_local,
            no_ack=no_ack, exclusive=exclusive, no_wait=no_wait,
            arguments=arguments,
        )
        if not no_wait:
            return self._send_method(method)
        fut = self._consumers[method.consumer_tag] = self.Future()
        self._send_method(method, has_reply=False)
        return fut, consumer_tag

    def _receive_BasicConsumeOK(self, method):
        consumer_tag = method.consumer_tag
        consume_future = self._consumers[consumer_tag] = self.Future()
        self._fut.set_result((consume_future, consumer_tag))

    def basic_cancel(self, consumer_tag, no_wait=False):
        method = protocol.BasicCancel(
            consumer_tag=consumer_tag, no_wait=no_wait
        )
        del self._consumers[method.consumer_tag]
        return self._send_method(method, has_reply=not no_wait)

    def basic_publish(self, message, exchange='', routing_key='',
                      mandatory=False, immediate=False):
        if self._next_publish_seq_no > 0:
            self._unconfirmed_set.add(self._next_publish_seq_no)
            self._next_publish_seq_no += 1
        method = protocol.BasicPublish(
            exchange=exchange, routing_key=routing_key,
            mandatory=mandatory, immediate=immediate,
        )

        header = protocol.ContentHeaderPayload(
            class_id=method.method_type[0],
            body_size=message.body_size,
            properties=message.properties,
        )

        max_payload_size = self._frame_max - protocol.Frame.METADATA_SIZE
        bodies = [protocol.ContentBodyPayload(chunk)
                  for chunk in _chunked(message.body, max_payload_size)]
        return self._send_method(method, has_reply=False,
                                 header=header, bodies=bodies)

    def _receive_BasicReturn(self, method):
        # RabbitMQ does not support BasicPublish with immediate=True
        # http://www.rabbitmq.com/blog/2012/11/19/breaking-things-with-rabbitmq-3-0/
        # If you *really* *really* need to handle this case with an other
        # broker, simply hook into the specific IO adapter
        # and write this logic by yourself.
        raise NotImplementedError

    def _receive_BasicDeliver(self, method):
        consumer_tag = method.consumer_tag
        if consumer_tag not in self._consumers:
            class_id, method_id = method.method_type
            raise protocol.CommandInvalid(
                'server has delivered a message to consumer with tag {}'
                'but there is no such consumer'.format(consumer_tag),
                class_id, method_id
            )
        self._message = protocol.BasicMessage(delivery_info=method)

    def basic_get(self, queue, no_ack=False):
        method = protocol.BasicGet(queue=queue, no_ack=no_ack)
        return self._send_method(method)

    def _receive_BasicGetOK(self, method):
        self._message = protocol.BasicMessage(delivery_info=method)
        self._message_fut = self.Future()
        self._fut.set_result(self._message_fut)

    def basic_ack(self, delivery_tag='', multiple=False):
        method = protocol.BasicAck(
            delivery_tag=delivery_tag, multiple=multiple
        )
        return self._send_method(method, has_reply=False)

    def _receive_BasicAck(self, method):
        delivery_tag = method.delivery_tag
        multiple = method.multiple
        if multiple:
            self._unconfirmed_set.difference_update(
                set(range(delivery_tag + 1))
            )
        else:
            self._unconfirmed_set.remove(delivery_tag)
        if not self._unconfirmed_set:
            self._ack_fut.set_result(True)
            self._ack_fut = self.Future()

    def basic_reject(self, delivery_tag='', requeue=False):
        method = protocol.BasicReject(
            delivery_tag=delivery_tag, requeue=requeue,
        )
        return self._send_method(method, has_reply=False)

    def basic_recover_async(self, requeue=False):
        method = protocol.BasicRecoverAsync(requeue=requeue)
        return self._send_method(method, has_reply=False)

    def basic_recover(self, requeue=False):
        method = protocol.BasicRecover(requeue=requeue)
        return self._send_method(method)

    def basic_nack(self, delivery_tag='', multiple=False, requeue=False):
        method = protocol.BasicNack(
            delivery_tag=delivery_tag, multiple=multiple, requeue=requeue
        )
        return self._send_method(method, has_reply=False)

    def _receive_BasicNack(self, method):
        raise NotImplementedError

    def tx_select(self):
        method = protocol.TxSelect()
        return self._send_method(method)

    def tx_commit(self):
        method = protocol.TxCommit()
        return self._send_method(method)

    def tx_rollback(self):
        method = protocol.TxRollback()
        return self._send_method(method)

    def confirm_select(self, no_wait=False):
        if self._next_publish_seq_no == 0:
            self._next_publish_seq_no = 1
        method = protocol.ConfirmSelect(nowait=no_wait)
        fut = self._send_method(method, has_reply=not no_wait)
        if not no_wait:
            return fut
        return self._ack_fut

    def _receive_ConfirmSelectOK(self, method):
        self._fut.set_result(self._ack_fut)


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
