import asyncio
import asyncio.streams

from amqproto import AMQPConnection


class AMQP(asyncio.streams.FlowControlMixin, asyncio.Protocol):

    def __init__(self, *, loop):
        super().__init__(loop=loop)
        self._sansio_connection = AMQPConnection()

        self._transport = None
        self._ready_fut = None

    def connection_made(self, transport):
        super().connection_made(transport)

        self._transport = transport
        fut = self._sansio_connection.initiate_connection()
        self._ready_fut = asyncio.wrap_future(fut, loop=self._loop)

        transport.write(self._sansio_connection.data_to_send())

    def data_received(self, data):
        super().data_received(data)

        frames = self._sansio_connection.receive_frames(data)
        for frame in frames:
            self._sansio_connection.handle_frame(frame)

        self._transport.write(self._sansio_connection.data_to_send())

    @asyncio.coroutine
    def drain(self):
        if self._transport is not None:
            if self._transport.is_closing():
                # Yield to the event loop so connection_lost() may be
                # called.  Without this, _drain_helper() would return
                # immediately, and code that calls
                #     write(...); yield from drain()
                # in a loop would never call connection_lost(), so it
                # would not see an error when the socket is closed.
                yield
        yield from self._drain_helper()


class Connection:

    def __init__(self, transport, protocol):
        self._transport = transport
        self._protocol = protocol
        self._loop = protocol._loop
        self._sansio_connection = protocol._sansio_connection

    async def open(self):
        return await self._protocol._ready_fut

    async def close(self, reply_code=0, reply_text=''):
        fut = self._sansio_connection.send_ConnectionClose(
            reply_code, reply_text
        )
        self._transport.write(self._sansio_connection.data_to_send())
        await self._protocol.drain()
        return await asyncio.wrap_future(fut, loop=self._loop)

    async def __aenter__(self):
        await self.open()

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    def get_channel(self):
        sansio_channel = self._sansio_connection.get_channel()
        return Channel(sansio_channel, self._transport, self._protocol)


class Channel:

    def __init__(self, sansio_channel, transport, protocol):
        self._transport = transport
        self._protocol = protocol
        self._sansio_channel = sansio_channel
        self._loop = protocol._loop

    async def open(self):
        fut = self._channel.send_ChannelOpen()
        self._transport.write(self._channel.data_to_send())
        await self._protocol.drain()
        return await asyncio.wrap_future(fut, loop=self._protocol._loop)

    async def close(self, reply_code=0, reply_text=''):
        fut = self._channel.send_ChannelClose(reply_code, reply_text)
        self._transport.write(self._channel.data_to_send())
        await self._protocol.drain()
        return await asyncio.wrap_future(fut, loop=self._loop)

    async def __aenter__(self):
        await self.open()

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def actually_send(self, future):
        self._transport.write(self._sansio_channel.data_to_send())
        await self._protocol.drain()
        if future is not None:
            return asyncio.wrap_future(future, loop=self._loop)

    async def flow(self, active):
        return await self.actually_send(
            self._sansio_channel.send_ChannelFlow(active)
        )

    async def exchange_declare(self, exchange, type='direct', passive=False,
                               durable=False, auto_delete=True, internal=False,
                               no_wait=False, arguments=None):
        future = await self.actually_send(
            self._sansio_channel.send_ExchangeDeclare(
                exchange, type=type, passive=passive, durable=durable,
                auto_delete=auto_delete, internal=internal,
                no_wait=no_wait, arguments=arguments,
            )
        )
        if future is not None:
            return await future

    async def exchange_delete(self, exchange, if_unused=False, no_wait=False):
        future = await self.actually_send(
            self._sansio_channel.send_ExchangeDelete(
                exchange, if_unused=if_unused, no_wait=no_wait
            )
        )
        if future is not None:
            return await future

    async def exchange_bind(self, *args, **kwargs):
        future = await self.actually_send(
            self._sansio_channel.send_ExchangeBind(*args, **kwargs)
        )
        if future is not None:
            return await future

    async def exchange_unbind(self, *args, **kwargs):
        future = await self.actually_send(
            self._sansio_channel.send_ExchangeUnbind(*args, **kwargs)
        )
        if future is not None:
            return await future

    async def queue_declare(self, *args, **kwargs):
        future = await self.actually_send(
            self._sansio_channel.send_QueueDeclare(*args, **kwargs)
        )
        if future is not None:
            return await future

    async def queue_bind(self, *args, **kwargs):
        future = await self.actually_send(
            self._sansio_channel.send_QueueBind(*args, **kwargs)
        )
        if future is not None:
            return await future

    async def queue_unbind(self, *args, **kwargs):
        future = await self.actually_send(
            self._sansio_channel.send_QueueUnbind(*args, **kwargs)
        )
        if future is not None:
            return await future

    async def queue_purge(self, *args, **kwargs):
        future = await self.actually_send(
            self._sansio_channel.send_QueuePurge(*args, **kwargs)
        )
        if future is not None:
            return await future

    async def queue_delete(self, *args, **kwargs):
        future = await self.actually_send(
            self._sansio_channel.send_QueueDelete(*args, **kwargs)
        )
        if future is not None:
            return await future

    async def basic_qos(self, *args, **kwargs):
        future = await self.actually_send(
            self._sansio_channel.send_BasicQos(*args, **kwargs)
        )
        if future is not None:
            return await future

    async def basic_consume(self, *args, **kwargs):
        future = await self.actually_send(
            self._sansio_channel.send_BasicConsume(*args, **kwargs)
        )
        return await future
