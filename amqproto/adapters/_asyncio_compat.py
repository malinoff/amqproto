import asyncio


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
