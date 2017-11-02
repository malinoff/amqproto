import asyncio

from .. import replies
from ..connection import Connection as _Connection
from ..channel import Channel as _Channel, BaseChannel as _BaseChannel


class AsyncioBaseChannel(_BaseChannel):

    def __init__(self, reader, writer, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._reader = reader
        self._writer = writer
        self._response = asyncio.Queue(maxsize=1)

    def _prepare_for_sending(self, method):
        super()._prepare_for_sending(method)
        self._writer.write(self.data_to_send())
        if method.has_response():
            # No need to drain, we can only receive a response after
            # a successful request is actually sent.
            return self._response.get()
        return self._writer.drain()

    async def _receive_method(self, method):
        # _method_handlers are defined in subclasses
        handler = self._method_handlers.get(method.__class__)
        if handler is not None:
            fut = handler(method)
            if fut is not None:
                await fut
        if not method.has_response():
            # If the received method has responses, it means the server
            # sent this method, not the client, thus the client doesn't
            # wait for the reply and we shouldn't put the method into
            # self._response
            await self._response.put(method)
        # TODO handle close

    async def __aenter__(self):
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        if exc is not None:
            if not isinstance(exc, replies.AMQPError):
                exc = replies.InternalError(repr(exc))
            await self.close(
                exc.reply_code, exc.reply_text, exc.class_id, exc.method_id,
            )
        else:
            await self.close()


class AsyncioChannel(AsyncioBaseChannel, _Channel):

    async def open(self):
        """Open the channel."""
        await self._channel_open()

    async def close(self, reply_code=200, reply_text='OK',
                    class_id=0, method_id=0):
        """Close the channel."""
        if not self.closed:
            await self._channel_close(
                reply_code, reply_text, class_id, method_id
            )


class AsyncioConnection(AsyncioBaseChannel, _Connection):

    def __init__(self, host='localhost', port=5672, *,
                 ssl=None, flags=0, sock=None, local_addr=None,
                 server_hostname=None, **kwargs):
        super().__init__(reader=None, writer=None, **kwargs)
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
        self._communicate_task = None

    def Channel(self, *args, **kwargs):
        return AsyncioChannel(self._reader, self._writer, *args, **kwargs)

    async def _handle_connection_tune(self, method):
        await super()._handle_connection_tune(method)
        self._negotiation.set()

    async def open(self):
        """Open the connection."""
        self._reader, self._writer = await asyncio.open_connection(
            **self._connect_args
        )
        self.initiate_connection()
        self._writer.write(self.data_to_send())

        loop = asyncio.get_event_loop()
        self._communicate_task = loop.create_task(self._communicate())

        await self._negotiation.wait()
        await self._connection_open()

    async def close(self, reply_code=200, reply_text='OK',
                    class_id=0, method_id=0):
        """Close the connection and all its channels."""
        if self.closed:
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
        # await self._writer.drain()

        await self._communicate_task

    async def _communicate(self):
        while not self.closed or not self._writer.transport.is_closing():
            frame_max = self.negotiated_settings.frame_max
            data = await self._reader.read(frame_max)
            if not data:
                break  # XXX how to handle this properly?
            for method in self.parse_data(data):
                channel = self.channels[method.channel_id]
                await channel._receive_method(method)


# Copy-pasted from https://github.com/python/asyncio/pull/465
def _cleanup(main_task, loop):
    pending = []
    if not main_task.done():
        pending.append(main_task)
    try:
        # `shutdown_asyncgens` was added in Python 3.6; not all
        # event loops might support it.
        shutdown_asyncgens = loop.shutdown_asyncgens
        pending.append(shutdown_asyncgens())
    except AttributeError:
        pass

    try:
        if pending:
            pending = asyncio.gather(*pending)
            loop.run_until_complete(pending)
        main_task.result()
    finally:
        asyncio.set_event_loop(None)
        loop.close()


def run(main, *, debug=False):
    """Run a coroutine.

    This function runs the passed coroutine, taking care of
    managing the asyncio event loop and finalizing asynchronous
    generators.

    This function cannot be called when another asyncio event loop
    is running.

    If debug is True, the event loop will be run in debug mode.

    This function handles KeyboardInterrupt exception in a way that
    the passed coroutine gets cancelled and KeyboardInterrupt
    is ignored. Therefore, to clean up an asyncio.CancelledError
    exception should be caught within the passed coroutine.

    This function should be used as a main entry point for
    asyncio programs, and should not be used to call asynchronous
    APIs.

    Example:

        from amqproto.io.asyncio import run

        async def main():
            await asyncio.sleep(1)
            print('hello')

        run(main())
    """
    if asyncio._get_running_loop() is not None:
        raise RuntimeError(
            "run() cannot be called from a running event loop")
    if not asyncio.iscoroutine(main):
        raise ValueError("a coroutine was expected, got {!r}".format(main))

    loop = asyncio.new_event_loop()
    main_task = loop.create_task(main)
    try:
        asyncio.set_event_loop(loop)

        if debug:
            loop.set_debug(True)

        return loop.run_until_complete(main_task)
    except KeyboardInterrupt:
        main_task.cancel()
    finally:
        _cleanup(main_task, loop)
