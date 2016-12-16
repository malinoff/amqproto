import asyncio
import asyncio.streams

from amqproto import AMQPConnection


class AMQP(asyncio.streams.FlowControlMixin, asyncio.Protocol):

    def __init__(self, *, loop):
        super().__init__(loop=loop)
        self.connection = AMQPConnection()

        self._transport = None
        self._ready_fut = None

    def connection_made(self, transport):
        super().connection_made(transport)

        self._transport = transport
        fut = self.connection.initiate_connection()
        self._ready_fut = asyncio.wrap_future(fut, loop=self._loop)

        transport.write(self.connection.data_to_send())

    def data_received(self, data):
        super().data_received(data)

        frames = self.connection.receive_frames(data)
        for frame in frames:
            self.connection.handle_frame(frame)

        self._transport.write(self.connection.data_to_send())

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
        self._connection = protocol.connection

    async def open(self):
        return await self._protocol._ready_fut

    async def close(self, reply_code=0, reply_text=''):
        fut = self._connection.send_ConnectionClose(reply_code, reply_text)
        self._transport.write(self._connection.data_to_send())
        await self._protocol.drain()
        return await asyncio.wrap_future(fut, loop=self._loop)

    async def __aenter__(self):
        await self.open()

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    def get_channel(self):
        channel = self._connection.get_channel()
        channel = Channel(channel, self._transport, self._protocol)
        return channel


class Channel:

    def __init__(self, channel, transport, protocol):
        self._transport = transport
        self._protocol = protocol
        self._channel = channel
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


class Exchange:

    def __init__(self, channel, name):
        self.name = name
        self._channel = channel

    async def declare(self):
        ch = self._channel._channel
        fut = ch.send_ExchangeDeclare(self.name)
        self._channel._transport.write(ch.data_to_send())
        await self._channel._protocol.drain()
        return await asyncio.wrap_future(fut, loop=self._channel._protocol._loop)

    async def publish(self, message, routing_key='',
                      mandatory=False, immediate=False):
        ch = self._channel._channel
        fut = ch.send_BasicPublish(message, self.name, routing_key=routing_key,
                                   mandatory=mandatory, immediate=immediate)
        self._channel._transport.write(ch.data_to_send())
        await self._channel._protocol.drain()
        if fut is not None:
            fut = asyncio.wrap_future(fut, loop=self._channel._protocol._loop)
            return await fut

    async def delete(self):
        ch = self._channel._channel
        fut = ch.send_ExchangeDelete(self.name)
        self._channel._transport.write(ch.data_to_send())
        await self._channel._protocol.drain()
        fut = asyncio.wrap_future(fut, loop=self._channel._protocol._loop)
        return await fut


class Queue:

    def __init__(self, channel, name):
        self._transport = channel._transport
        self.name = name
        self.channel = channel._channel
        self.protocol = channel._protocol
        self.loop = channel._protocol._loop

    async def declare(self):
        fut = self.channel.send_QueueDeclare(self.name)
        self._transport.write(self.channel.data_to_send())
        await self.protocol.drain()
        return await asyncio.wrap_future(fut, loop=self.loop)

    async def bind(self, exchange):
        if isinstance(exchange, Exchange):
            exchange = exchange.name
        fut = self.channel.send_QueueBind(self.name, exchange=exchange)
        self._transport.write(self.channel.data_to_send())
        await self.protocol.drain()
        return await asyncio.wrap_future(fut, loop=self.loop)

    async def unbind(self, exchange):
        if isinstance(exchange, Exchange):
            exchange = exchange.name
        fut = self.channel.send_QueueUnbind(self.name, exchange=exchange)
        self._transport.write(self.channel.data_to_send())
        await self.protocol.drain()
        return await asyncio.wrap_future(fut, loop=self.loop)

    async def get(self):
        fut = self.channel.send_BasicGet(self.name)
        self._transport.write(self.channel.data_to_send())
        await self.protocol.drain()
        fut = await asyncio.wrap_future(fut, loop=self.loop)
        return await asyncio.wrap_future(fut, loop=self.loop)

    async def delete(self):
        fut = self.channel.send_QueueDelete(self.name)
        self._transport.write(self.channel.data_to_send())
        await self.protocol.drain()
        return await asyncio.wrap_future(fut, loop=self.loop)
