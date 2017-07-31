import asyncio
from amqproto.protocol import BasicMessage
from amqproto.io.asyncio import *

MSGS = 4000

async def go(loop):
    async with Connection(loop=loop) as conn:
        async with conn.get_channel(1) as chan:
            await chan.queue_declare('hello')
            for i in range(1, MSGS + 1):
                await chan.basic_publish(BasicMessage(b'hello, world'), routing_key='hello')

            fut, consumer_tag = await chan.basic_consume('hello')
            for i in range(1, MSGS + 1):
                fut, message = await fut
                await chan.basic_ack(message.delivery_info.delivery_tag)
            await chan.basic_cancel(consumer_tag)

loop = asyncio.get_event_loop()
loop.run_until_complete(go(loop))
loop.close()
