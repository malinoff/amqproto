import asyncio
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
from amqproto.adapters.asyncio_adapter import AsyncioConnection, run
# import logging
# logging.basicConfig(level=10)

MSGS = 4000


async def go():
    async with AsyncioConnection() as conn:
        async with conn.get_channel(1) as chan:
            await chan.queue_declare('hello')
            for i in range(1, MSGS + 1):
                await chan.basic_publish(b'hello, world', routing_key='hello')

            reply = await chan.basic_consume('hello')
            consumer_tag = reply.consumer_tag
            async for message in chan.delivered_messages():
                await chan.basic_ack(message.delivery_info.delivery_tag)
                if message.delivery_info.delivery_tag == MSGS:
                    break
            await chan.basic_cancel(consumer_tag)

run(go())
