import asyncio

from .. import methods
from ..replies import AMQPError, InternalError
from ..connection import Connection
from ..channel import Channel, BaseChannel


class AsyncioBaseChannel(BaseChannel):

    def __init__(self, writer, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._writer = writer
        # The server can send us two things: a response to a method or
        # a connection/channel exception.
        self._response = asyncio.Queue(maxsize=1)
        self._exception = asyncio.Queue(maxsize=1)

    async def _prepare_for_sending(self, method):
        # Because of asynchronous nature of AMQP, error handling
        # is difficult. First, we can't know in advance
        # the exact moment in future when the broker decides to send
        # us an exception. Second, we can send methods without waiting
        # for replies from the broker - essentially moving handling
        # the error from the first errored method call into the second
        # method call!

        if not self._exception.empty():
            # This covers the case of two methods being sent without waiting
            # for responses, e.g.:
            # await channel.basic_publish()
            # [here an exception is received]
            # await channel.basic_publish()  [raises the exception]

            # Raising the exception from the second call is at least
            # confusing, but yet I don't know a better way to inform
            # the user about the error. Maybe wrapping the exception into a
            # some sort of DelayedError is worth exploring.
            raise (await self._exception.get())
        super()._prepare_for_sending(method)
        self._writer.write(self.data_to_send())
        if not method.has_response():
            # If there's no response we cant wait for, we should only
            # drain for I/O to complete. Possible error handling
            # is deferred to the next await, as described above.
            await self._writer.drain()
            # When the IO is not paused, `drain` simply yields to the loop,
            # making `await` above useless - it does not actually
            # switch to the next task. Let's solve this problem.
            await asyncio.sleep(0)
            return
        # If there is a response, then we can handle possible errors
        # right here, yay!
        done, pending = await asyncio.wait(
            fs=(self._response.get(), self._exception.get()),
            return_when=asyncio.FIRST_COMPLETED,
        )
        # There is a situation when both .get() calls may be done
        # and that point: a race condition, in which both client and server
        # close the channel, client - normally, server - via an exception.
        # In that case, `pending` is empty.
        if pending:
            pending.pop().cancel()
        for task in done:
            response = await task
            if isinstance(response, AMQPError):
                raise response
        return response

    async def _receive_method(self, method):
        # _method_handlers are defined in subclasses
        handler = self._method_handlers.get(method.__class__)
        if handler is not None:
            fut = handler(method)
            if fut is not None:
                await fut
        if isinstance(method, (methods.BasicDeliver,
                               methods.BasicReturn,
                               methods.ChannelClose,
                               methods.ConnectionClose)):
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
        if exc is not None:
            if not isinstance(exc, AMQPError):
                exc = InternalError(reply_text=repr(exc))
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
            methods.BasicReturn: self._handle_basic_return,
            methods.BasicDeliver: self._handle_basic_deliver,
        })

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

    async def _handle_basic_return(self, method):
        await self._delivered_messages.put(method.content)

    async def _handle_basic_deliver(self, method):
        await self._delivered_messages.put(method.content)

    async def _handle_channel_close(self, method):
        await super()._handle_channel_close(method)
        exc = AMQPError.from_close_method(method)
        await self._exception.put(exc)

    async def delivered_messages(self):
        """Yields delivered messages."""
        while not self.closed:
            message = await self._delivered_messages.get()
            yield message


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
        self._communicate_task = None

    def Channel(self, *args, **kwargs):
        return AsyncioChannel(self._writer, *args, **kwargs)

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

    async def _handle_connection_close(self, method):
        await super()._handle_conneciton_close(method)
        exc = AMQPError.from_close_method(method)
        await self._exception.put(exc)

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
