import asyncio.streams

import amqpframe.errors
from ..channel import Channel as SansioChannel
from ..connection import Connection as SansioConnection


class AMQP(asyncio.Protocol):

    def __init__(self, connection):
        self.connection = connection
        self.ready_future = None
        self._transport = None

    def connection_made(self, transport):
        super().connection_made(transport)
        self._transport = transport
        self.ready_future = self.connection.initiate_connection()
        transport.write(self.connection.data_to_send())

    def data_received(self, data):
        super().data_received(data)

        conn = self.connection
        frames = conn.receive_frames(data)
        for frame in frames:
            conn.handle_frame(frame)

        self._transport.write(conn.data_to_send())


class Channel(SansioChannel):

    def __init__(self, *args, loop, transport, protocol, **kwargs):
        self.loop = loop
        super().__init__(*args, **kwargs)
        self.transport = transport
        self.protocol = protocol

    def Future(self):
        return self.loop.create_future()

    async def __aenter__(self):
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        reply_code = 0
        reply_text = ''
        if isinstance(exc, amqpframe.errors.AMQPError):
            reply_code = exc.reply_code
            reply_text = exc.reply_text
        await self.close(reply_code, reply_text)

    def _send_method(self, method):
        super()._send_method(method)
        self.transport.write(self.data_to_send())


class Connection(SansioConnection):

    def __init__(self, *args, loop,
                 host=None, port=None, ssl=None, local_addr=None,
                 server_hostname=None,
                 **kwargs):
        self.loop = loop
        super().__init__(*args, **kwargs)
        self._connect_args = {
            'host': host,
            'port': port,
            'ssl': ssl,
            'local_addr': local_addr,
            'server_hostname': server_hostname,
        }

        self.transport = self.protocol = None

    def Future(self):
        return self.loop.create_future()

    def Channel(self, *args, **kwargs):
        kwargs.update(
            loop=self.loop, transport=self.transport, protocol=self.protocol
        )
        return Channel(*args, **kwargs)

    async def open(self):
        transport, protocol = await self.loop.create_connection(
            lambda: AMQP(self, loop=self.loop), **self._connect_args
        )
        self.transport = transport
        self.protocol = protocol
        await protocol.ready_future

    async def __aenter__(self):
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        reply_code = 0
        reply_text = ''
        if isinstance(exc, amqpframe.errors.AMQPError):
            reply_code = exc.reply_code
            reply_text = exc.reply_text
        await self.close(reply_code, reply_text)

    def _send_method(self, method):
        super()._send_method(method)
        self.transport.write(self.data_to_send())
