import asyncio

from .. import methods


class Channel:

    def __init__(self, channel_id, connection, reader, writer):
        self._conn = connection
        self._chan = connection.channels[channel_id]

        self._reader = reader
        self._writer = writer

        self._response = asyncio.Queue(maxsize=1)
        self._deliveries = asyncio.Queue()

        self._outbound_handlers = {
            methods.ExchangeBind: self._send_exchange_bind_unbind,
            methods.ExchangeUnbind: self._send_exchange_bind_unbind,
            methods.BasicConsume: self._send_basic_consume,
            methods.BasicPublish: self._send_basic_publish,
            methods.ConfirmSelect: self._send_confirm_select,
        }

        self._inbound_handlers = {
            methods.ChannelFlow: self._handle_flow_ok,
            methods.BasicConsumeOK: self._handle_basic_consume_ok,
            methods.BasicDeliver: self._handle_basic_deliver,
            methods.BasicAck: self._handle_basic_ack_nack,
            methods.BasicNack: self._handle_basic_ack_nack,
            methods.BasicReturn: self._handle_basic_return,
            methods.TxSelectOK: self._handle_tx_select_ok,
        }

    async def send_method(self, method):
        handler = self._outbound_handlers.get(method.__class__)
        if handler is not None:
            handler(method)
        self._conn.method_to_send(method)
        self._writer.write(self._conn.data_to_send())
        await self._writer.drain()
        if method.has_response():
            response = await self._response.get()
            assert response.__class__ in method.responses
            return response

    async def handle_method(self, method):
        assert method.channel_id == self.channel_id
        handler = self._handlers.get(method.__class__, self._response.put)
        await handler(method)

    def _send_exchange_bind_unbind(self, method):
        self._conn.capable_of(
            method.__class__, 'exchange_exchange_bindings'
        )

    def _send_basic_consume(self, method):
        if not method.has_response():
            self._chan.add_consumer(method.consumer_tag)

    def _send_basic_publish(self, method):
        self._chan.publish_needs_confirmations(method.content)

    def _send_confirm_select(self, method):
        self._conn.capable_of(
            method.__class__, 'publisher_confirms'
        )

    async def _handle_flow_ok(self, method):
        self._chan.flow = method.active
        await asyncio.sleep(0)  # Trigger the switch

    async def _handle_basic_consume_ok(self, method):
        self._chan.add_consumer(method.consumer_tag)
        await asyncio.sleep(0)  # Trigger the switch

    async def _handle_basic_return(self, method):
        await self._received_content.put(method.content)

    async def _handle_basic_deliver(self, method):
        await self._received_content.put(method.content)

    async def _handle_basic_ack_nack(self, method):
        await self._confirmations.put(method.content)

    async def _handle_tx_select_ok(self, method):
        self._chan.transaction_active = True
        await asyncio.sleep(0)  # Trigger the switch

    async def _handle_tx_commit_ok(self, method):
        self._chan.transaction_active = False
        await asyncio.sleep(0)  # Trigger the switch

    async def _handle_tx_rollback_ok(self, method):
        self._chan.transaction_active = False
        await asyncio.sleep(0)  # Trigger the switch

    async def _handle_confirm_select_ok(self, method):
        self._chan.publisher_confirms_active = True
        await asyncio.sleep(0)  # Trigger the switch


class Connection:

    def __init__(self, reader, writer, **kwargs):
        self._reader = reader
        self._writer = writer
        self._conn = 'conn'


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
