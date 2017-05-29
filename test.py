from amqproto.adapters.asyncio_simple import Connection


async def go(loop):
    async with Connection(loop=loop, port='5672') as conn:
        async with conn.get_channel() as chan:
            await chan.flow(active=True)


import asyncio
loop = asyncio.get_event_loop()
loop.run_until_complete(go(loop))
