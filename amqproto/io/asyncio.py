import asyncio
from asyncio.streams import _DEFAULT_LIMIT

from amqproto import protocol
from amqproto.channel import Channel as SansioChannel
from amqproto.connection import Connection as SansioConnection


class Channel(SansioChannel):

    def __init__(self, *args,
                 loop: asyncio.BaseEventLoop,
                 writer,
                 **kwargs):
        self.loop = loop
        self.writer = writer
        # Late call because we need self.Future available.
        super().__init__(*args, **kwargs)

    def Future(self):
        return self.loop.create_future()

    async def __aenter__(self):
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        reply_code = 0
        reply_text = ''
        if isinstance(exc, protocol.AMQPError):
            reply_code = exc.reply_code
            reply_text = exc.reply_text
        await self.close(reply_code, reply_text)

    def _flush_outbound(self, has_reply):
        self.writer.write(self.data_to_send())
        if not has_reply:
            return self.writer.drain()


class Connection(SansioConnection):

    def __init__(self, host='localhost', port=5672, *,
                 limit=_DEFAULT_LIMIT, ssl=None, family=0, proto=0,
                 flags=0, sock=None, local_addr=None, server_hostname=None,
                 loop: asyncio.BaseEventLoop, **kwargs):
        self.loop = loop
        # Late call because we need self.Future available.
        super().__init__(**kwargs)

        async def do_open_connection():
            self.reader, self.writer = await asyncio.open_connection(
                host=host, port=port, loop=loop, limit=limit,
                ssl=ssl, family=family, proto=proto, flags=flags,
                sock=sock, local_addr=local_addr,
                server_hostname=server_hostname,
            )
            fut = self.initiate_connection()
            self._communicate_task = loop.create_task(self._communicate())
            await fut
        self._open_connection = do_open_connection

    def Future(self):
        return self.loop.create_future()

    def Channel(self, *args, **kwargs):
        return Channel(*args, loop=self.loop, writer=self.writer, **kwargs)

    async def _communicate(self):
        read, writer = self.reader.read, self.writer
        writer.write(self.data_to_send())
        await writer.drain()

        data = bytearray()
        while self._fsm.state != 'CLOSED':
            frame_max = self.properties['frame_max']
            chunk = await read(10 * frame_max)
            if not chunk:
                break
            data += chunk
            frames = self.receive_frames(data)
            for frame, frame_size in frames:
                future = self.handle_frame(frame)
                if future is not None:
                    await future
                data = data[frame_size:]

    async def __aenter__(self):
        await self._open_connection()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        reply_code = 0
        reply_text = ''
        if isinstance(exc, protocol.AMQPError):
            reply_code = exc.reply_code
            reply_text = exc.reply_text
        await self.close(reply_code, reply_text)
        self._communicate_task.cancel()
        self.writer.close()

    def _flush_outbound(self, has_reply):
        self.writer.write(self.data_to_send())
        if not has_reply:
            return self.writer.drain()
