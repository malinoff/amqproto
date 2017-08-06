import asyncio
from asyncio.streams import _DEFAULT_LIMIT

from amqproto import protocol
from amqproto.util import override_signature
from amqproto.channel import Channel as SansioChannel
from amqproto.connection import Connection as SansioConnection

__all__ = ['Connection']


class Channel(SansioChannel):

    def __init__(self, *args,
                 loop: asyncio.BaseEventLoop,
                 writer,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.loop = loop
        self.writer = writer
        # Used to synchronize AMQP methods with OK replies
        self._reply_future = None

        self._get_future = None
        self._ack_future = None

        self._consumed_messages = asyncio.Queue(loop=self.loop)
        self._returned_messages = asyncio.Queue(loop=self.loop)

    def _receive_method(self, method):
        self._reply_future.set_result(method)

    def _process_message(self):
        message, self._message = self._message, None
        if self._get_future is not None:
            # BasicGet
            self._get_future.set_result(message)
        elif isinstance(message.delivery_info, protocol.BasicReturn):
            # BasicReturn
            self._returned_messages.put_nowait(message)
        else:
            # BasicConsume
            self._consumed_messages.put_nowait(message)

    async def _flush_outbound(self, has_reply):
        self.writer.write(self.data_to_send())
        if has_reply:
            future = self._reply_future = self.loop.create_future()
        await self.writer.drain()
        if has_reply:
            return await future

    async def __aenter__(self):
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if not self.closed:
            reply_code = 0
            reply_text = ''
            if isinstance(exc, protocol.AMQPError):
                reply_code = exc.reply_code
                reply_text = exc.reply_text
            await self.close(reply_code, reply_text)

    @override_signature(SansioChannel.close)
    async def open(self):
        has_reply = super().open()
        return await self._flush_outbound(has_reply)

    def _receive_ChannelOpenOK(self, method):
        super()._receive_ChannelOpenOK(method)
        self._reply_future.set_result(method)

    @override_signature(SansioChannel.close)
    async def close(self, *args, **kwargs):
        if not self.closed:
            has_reply = super().close(*args, **kwargs)
            self._reply_future = self.loop.create_future()
            await self._flush_outbound(has_reply)
            await self._reply_future

    def _receive_ChannelCloseOK(self, method):
        super()._receive_ChannelCloseOK(method)
        self._reply_future.set_result(method)

    def _send_ChannelCloseOK(self, exc):
        super()._send_ChannelCloseOK(exc)
        self._reply_future.set_exception(exc)

    @override_signature(SansioChannel.flow)
    async def flow(self, *args, **kwargs):
        has_reply = super().flow(*args, **kwargs)
        return await self._flush_outbound(has_reply)

    @override_signature(SansioChannel.exchange_declare)
    async def exchange_declare(self, *args, **kwargs):
        has_reply = super().exchange_declare(*args, **kwargs)
        return await self._flush_outbound(has_reply)

    @override_signature(SansioChannel.exchange_delete)
    async def exchange_delete(self, *args, **kwargs):
        has_reply = super().exchange_delete(*args, **kwargs)
        return await self._flush_outbound(has_reply)

    @override_signature(SansioChannel.exchange_bind)
    async def exchange_bind(self, *args, **kwargs):
        has_reply = super().exchange_bind(*args, **kwargs)
        return await self._flush_outbound(has_reply)

    @override_signature(SansioChannel.exchange_unbind)
    async def exchange_unbind(self, *args, **kwargs):
        has_reply = super().exchange_unbind(*args, **kwargs)
        return await self._flush_outbound(has_reply)

    @override_signature(SansioChannel.queue_declare)
    async def queue_declare(self, *args, **kwargs):
        has_reply = super().queue_declare(*args, **kwargs)
        return await self._flush_outbound(has_reply)

    @override_signature(SansioChannel.queue_bind)
    async def queue_bind(self, *args, **kwargs):
        has_reply = super().queue_bind(*args, **kwargs)
        return await self._flush_outbound(has_reply)

    @override_signature(SansioChannel.queue_unbind)
    async def queue_unbind(self, *args, **kwargs):
        has_reply = super().queue_unbind(*args, **kwargs)
        return await self._flush_outbound(has_reply)

    @override_signature(SansioChannel.queue_purge)
    async def queue_purge(self, *args, **kwargs):
        has_reply = super().queue_purge(*args, **kwargs)
        return await self._flush_outbound(has_reply)

    @override_signature(SansioChannel.queue_delete)
    async def queue_delete(self, *args, **kwargs):
        has_reply = super().queue_delete(*args, **kwargs)
        return await self._flush_outbound(has_reply)

    @override_signature(SansioChannel.basic_qos)
    async def basic_qos(self, *args, **kwargs):
        has_reply = super().basic_qos(*args, **kwargs)
        return await self._flush_outbound(has_reply)

    @override_signature(SansioChannel.basic_consume)
    async def basic_consume(self, *args, **kwargs):
        has_reply = super().basic_consume(*args, **kwargs)
        return await self._flush_outbound(has_reply)

    def _receive_BasicConsumeOK(self, method):
        super()._receive_BasicConsumeOK(method)
        self._reply_future.set_result(method)

    @override_signature(SansioChannel.basic_cancel)
    async def basic_cancel(self, *args, **kwargs):
        has_reply = super().basic_cancel(*args, **kwargs)
        return await self._flush_outbound(has_reply)

    def _receive_BasicCancelOK(self, method):
        super()._receive_BasicCancelOK(method)
        self._reply_future.set_result(method)

    @override_signature(SansioChannel.basic_publish)
    async def basic_publish(self, *args, **kwargs):
        has_reply = super().basic_publish(*args, **kwargs)
        return await self._flush_outbound(has_reply)

    @override_signature(SansioChannel.basic_get)
    async def basic_get(self, *args, **kwargs):
        has_reply = super().basic_get(*args, **kwargs)
        reply = await self._flush_outbound(has_reply)
        if reply is not None:
            return await self._get_future

    def _receive_BasicGetOK(self, method):
        super()._receive_BasicGetOK(method)
        self._get_future = self.loop.create_future()
        self._reply_future.set_result(self._get_future)

    def _receive_BasicGetEmpty(self, method):
        super()._receive_BasicGetEmpty(method)
        self._reply_future.set_result(None)

    @override_signature(SansioChannel.basic_ack)
    async def basic_ack(self, *args, **kwargs):
        has_reply = super().basic_ack(*args, **kwargs)
        return await self._flush_outbound(has_reply)

    def _receive_BasicAck(self, method):
        super()._receive_BasicAck(method)
        if not self._unconfirmed_set:
            self._ack_future.set_result(True)
            self._ack_future = self.loop.create_future()

    @override_signature(SansioChannel.basic_reject)
    async def basic_reject(self, *args, **kwargs):
        has_reply = super().basic_reject(*args, **kwargs)
        return await self._flush_outbound(has_reply)

    @override_signature(SansioChannel.basic_recover_async)
    async def basic_recover_async(self, *args, **kwargs):
        has_reply = super().basic_recover_async(*args, **kwargs)
        return await self._flush_outbound(has_reply)

    @override_signature(SansioChannel.basic_recover)
    async def basic_recover(self, *args, **kwargs):
        has_reply = super().basic_recover(*args, **kwargs)
        return await self._flush_outbound(has_reply)

    @override_signature(SansioChannel.basic_nack)
    async def basic_nack(self, *args, **kwargs):
        has_reply = super().basic_nack(*args, **kwargs)
        return await self._flush_outbound(has_reply)

    @override_signature(SansioChannel.tx_select)
    async def tx_select(self, *args, **kwargs):
        has_reply = super().tx_select(*args, **kwargs)
        return await self._flush_outbound(has_reply)

    @override_signature(SansioChannel.tx_commit)
    async def tx_commit(self, *args, **kwargs):
        has_reply = super().tx_commit(*args, **kwargs)
        return await self._flush_outbound(has_reply)

    @override_signature(SansioChannel.tx_rollback)
    async def tx_rollback(self, *args, **kwargs):
        has_reply = super().tx_rollback(*args, **kwargs)
        return await self._flush_outbound(has_reply)

    @override_signature(SansioChannel.confirm_select)
    async def confirm_select(self, *args, **kwargs):
        has_reply = super().confirm_select(*args, **kwargs)
        return await self._flush_outbound(has_reply)

    async def next_consumed_message(self):
        return await self._consumed_messages.get()

    async def consumed_messages(self):
        while True:
            message = await self.next_consumed_message()
            yield message

    async def next_returned_message(self):
        return await self._returned_messages.get()

    async def returned_messages(self):
        while True:
            message = await self.next_returned_message()
            yield message

    async def wait_for_confirmations(self):
        await self._ack_future


class Connection(SansioConnection):

    def __init__(self, host='localhost', port=5672, *,
                 limit=_DEFAULT_LIMIT, ssl=None, family=0, proto=0,
                 flags=0, sock=None, local_addr=None, server_hostname=None,
                 loop: asyncio.BaseEventLoop, **kwargs):
        super().__init__(**kwargs)
        self.loop = loop
        self._connect_args = {
            'host': host,
            'port': port,
            'limit': limit,
            'ssl': ssl,
            'family': family,
            'proto': proto,
            'flags': flags,
            'sock': sock,
            'local_addr': local_addr,
            'server_hostname': server_hostname,
        }
        self.reader = self.writer = None
        # Used to synchronize AMQP methods with OK replies
        self._reply_future = None

    def Channel(self, *args, **kwargs):
        return Channel(*args, loop=self.loop, writer=self.writer, **kwargs)

    def _receive_method(self, method):
        self._reply_future.set_result(method)

    async def open(self):
        self.reader, self.writer = await asyncio.open_connection(
            **self._connect_args
        )
        self.initiate_connection()
        await self._flush_outbound()
        future = self._reply_future = self.loop.create_future()
        self._communicate_task = self.loop.create_task(self._communicate())
        await future

    def _receive_ConnectionOpenOK(self, method):
        super()._receive_ConnectionOpenOK(method)
        self._reply_future.set_result(method)

    async def _communicate(self):
        data = bytearray()
        while not self.closed:
            frame_max = self.properties['frame_max']
            chunk = await self.reader.read(10 * frame_max)
            if not chunk:
                break
            data += chunk
            frames = self.receive_frames(data)
            for frame, frame_size in frames:
                self.handle_frame(frame)
                await self._flush_outbound()
                data = data[frame_size:]

    async def __aenter__(self):
        await self.open()
        return self

    async def close(self, reply_code, reply_text, class_id=0, method_id=0):
        if not self.closed:
            future = self._reply_future = self.loop.create_future()
            super().close(reply_code, reply_text, class_id, method_id)
            await self._flush_outbound()
            await future
            self._communicate_task.cancel()
            self.writer.close()

    def _receive_ConnectionCloseOK(self, method):
        self._reply_future.set_result(method)

    async def __aexit__(self, exc_type, exc, tb):
        if not self.closed:
            reply_code = 0
            reply_text = ''
            if isinstance(exc, protocol.AMQPError):
                reply_code = exc.reply_code
                reply_text = exc.reply_text
            await self.close(reply_code, reply_text)

    async def _flush_outbound(self):
        self.writer.write(self.data_to_send())
        await self.writer.drain()
