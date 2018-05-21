#!/usr/bin/env python3
import asyncio

from async_generator import aclosing
from amqproto.adapters.asyncio_adapter import AsyncioConnection, run


async def main():
    try:
        async with AsyncioConnection(host='localhost') as connection:
            async with connection.get_channel() as channel:
                await channel.queue_declare('hello')

                await channel.basic_consume('hello', no_ack=True)
                print(' [*] Waiting for messages. To exit press CTRL+C')
                async with aclosing(channel.delivered_messages()) as messages:
                    async for message in messages:
                        print(" [x] Received %r" % message.body)
    except asyncio.CancelledError:
        print(' [x] Bye!')

run(main())
