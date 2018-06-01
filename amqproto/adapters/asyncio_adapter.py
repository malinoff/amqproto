import logging
import asyncio
try:
    from asyncio import run
except ImportError:
    from ._asyncio_compat import run  # noqa

from async_generator import async_generator, yield_

from ..connection import Connection
from ..channel import Channel, BaseChannel
from ..replies import Reply, AsynchronousReply, ConnectionAborted
from ..methods import BasicDeliver, BasicReturn, ChannelClose, ConnectionClose


class AsyncioBaseChannel(BaseChannel):

    def __init__(self, writer, write_limit=50, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._writer = writer
        # The server can send us two things: a response to a method or
        # a connection/channel exception.
        self._response = asyncio.Queue(maxsize=1)
        self._server_exception = asyncio.Queue(maxsize=1)
        # A client exception can also happen in
        # AsyncioConnection._communicate.
        self._client_exception = asyncio.Queue(maxsize=1)

        self._write_limit = write_limit
        self._wrote_without_response = 0

    async def _prepare_for_sending(self, method):
        # Because of asynchronous nature of AMQP, error handling
        # is difficult. First, we can't know in advance
        # the exact moment in future when the broker decides to send
        # us an exception. Second, we can send methods without waiting
        # for replies from the broker - essentially moving handling
        # the error from the first errored method call into the second
        # method call!
        if self.state in {'closed', 'closing'} and not method.closing:
            # This covers the case of two methods being sent without waiting
            # for responses, e.g.:
            # await channel.method(no_wait=True)
            # [here an exception is received]
            # await channel.method(no_wait=True)  [raises the exception]

            # Raising the exception from the second call is at least
            # confusing, but I don't know a better way to inform
            # the user about the error.
            exc = await self._server_exception.get()
            raise AsynchronousReply(exc)
        logging.info('[channel_id %s] sending %s', self.channel_id, method)
        super()._prepare_for_sending(method)
        self._writer.write(self.data_to_send())
        if not method.has_response():
            # If there's no response we cant wait for, we should only
            # drain for I/O to complete. Possible error handling
            # is deferred to the next await, as described above.
            await self._writer.drain()

            # On a non-paused connection, drain won't yield to the event loop
            # making it impossible for _communicate to ever run.
            # We fix this by introducing a limit of writes without
            # awaiting for a reply (basic_publish case). Once the limit is hit,
            # we explicitly yield to the event loop via asyncio.sleep(0).
            #
            # Yes, we could sleep after every single drain but that is
            # _very_ inefficient.

            self._wrote_without_response += 1
            if self._wrote_without_response > self._write_limit:
                await asyncio.sleep(0)
                self._wrote_without_response = 0
            return
        self._wrote_without_response = 0
        # If there is a response, then we can handle possible errors
        # right here, yay!
        return await self._result_or_exception(self._response.get())

    async def _result_or_exception(self, coro=None):
        fs = [self._client_exception.get(), self._server_exception.get()]
        if coro is not None:
            fs.append(coro)
        done, pending = await asyncio.wait(
            fs, timeout=2, return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()
        response = None
        for task in done:
            response = await task
            if isinstance(response, Exception):
                raise response
        return response

    async def _receive_method(self, method):
        logging.info('[channel_id %s] receiving %s', self.channel_id, method)
        # _method_handlers are defined in subclasses
        handler = self._method_handlers.get(method.__class__)
        if handler is not None:
            fut = handler(method)
            if fut is not None:
                await fut
        if isinstance(method, (BasicDeliver, BasicReturn,
                               ChannelClose, ConnectionClose)):
            # These methods have their own special handling
            return
        if method.has_response():
            # If the received method has responses, it means the server
            # sent this method, not the client, thus the client doesn't
            # wait for the reply and we shouldn't put the method into
            # self._response
            return
        await self._response.put(method)

    async def __aenter__(self):
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        if isinstance(exc, Reply):
            await self.close(
                exc.reply_code, exc.reply_text, exc.class_id, exc.method_id,
            )
        else:
            await self.close()


class AsyncioChannel(AsyncioBaseChannel, Channel):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._delivered_messages = asyncio.Queue()
        self._method_handlers.update({
            BasicReturn: self._handle_basic_return,
            BasicDeliver: self._handle_basic_deliver,
        })

    async def open(self):
        """Open the channel."""
        await self._channel_open()

    async def close(self, reply_code=200, reply_text='OK',
                    class_id=0, method_id=0):
        """Close the channel."""
        if self.state != 'open':
            return
        await self._channel_close(
            reply_code, reply_text, class_id, method_id
        )

    async def _handle_basic_return(self, method):
        await self._delivered_messages.put(method.content)

    async def _handle_basic_deliver(self, method):
        await self._delivered_messages.put(method.content)

    async def _handle_channel_close(self, method):
        await super()._handle_channel_close(method)
        exc = Reply.from_close_method(method)
        await self._server_exception.put(exc)
        self.state = 'closed'

    @async_generator
    async def delivered_messages(self):
        """Yields delivered messages."""
        while self.state == 'open':
            message = await self._delivered_messages.get()
            await yield_(message)


class AsyncioConnection(AsyncioBaseChannel, Connection):

    def __init__(self, host='localhost', port=5672, *,
                 ssl=None, flags=0, sock=None, local_addr=None,
                 server_hostname=None, **kwargs):
        super().__init__(writer=None, **kwargs)
        self._reader = None
        self._connect_args = {
            'host': host,
            'port': port,
            'ssl': ssl,
            'flags': flags,
            'sock': sock,
            'local_addr': local_addr,
            'server_hostname': server_hostname,
        }
        self._negotiation = asyncio.Event()
        self._heartbeat_task = None
        self._communicate_task = None

    def _make_channel(self, channel_id):
        return AsyncioChannel(self._writer, self._write_limit, channel_id)

    async def _handle_connection_tune(self, method):
        self._heartbeat_task = asyncio.ensure_future(self._start_heartbeat())

        await super()._handle_connection_tune(method)
        self._negotiation.set()

    async def _send_heartbeat(self):
        super()._send_heartbeat()
        self._writer.write(self.data_to_send())
        await self._writer.drain()

    async def _start_heartbeat(self):
        if self.negotiated_settings.heartbeat == 0:
            return
        transport_closing = self._writer.transport.is_closing
        while self.state in {'opening', 'open'} and not transport_closing():
            await self._send_heartbeat()
            self._missed_heartbeats += 1
            await asyncio.sleep(self.negotiated_settings.heartbeat)

    async def open(self):
        """Open the connection."""
        self._reader, self._writer = await asyncio.open_connection(
            **self._connect_args,
        )
        self.initiate_connection()
        self._writer.write(self.data_to_send())

        self._communicate_task = asyncio.ensure_future(self._communicate())

        await self._result_or_exception(self._negotiation.wait())
        await self._connection_open()

    async def close(self, reply_code=200, reply_text='OK',
                    class_id=0, method_id=0):
        """Close the connection and all its channels."""
        if self.state != 'open':
            return
        for channel in self.channels.values():
            if channel.channel_id == 0:
                continue  # Do not try close itself.
            await channel.close(
                reply_code, reply_text, class_id, method_id,
            )
        await self._connection_close(
            reply_code, reply_text, class_id, method_id
        )
        self._writer.close()
        self._heartbeat_task.cancel()
        await self._communicate_task

    async def _handle_connection_close(self, method):
        await super()._handle_connection_close(method)
        exc = Reply.from_close_method(method)
        await self._server_exception.put(exc)
        self.state = 'closed'

    async def _communicate(self):
        try:
            transport_closing = self._writer.transport.is_closing
            while (self.state in {'opening', 'open'} and
                    not transport_closing()):
                frame_max = self.negotiated_settings.frame_max
                data = await self._reader.read(frame_max)
                if not data:
                    raise ConnectionAborted
                for channel_id, methods in self.parse_data(data).items():
                    channel = self.channels[channel_id]
                    for method in methods:
                        await channel._receive_method(method)
        except Exception as exc:
            await self._client_exception.put(exc)
