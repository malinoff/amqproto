#!/usr/bin/env python3
from amqproto.adapters.asyncio_adapter import AsyncioConnection, run


async def main():
    async with AsyncioConnection(host='localhost') as connection:
        async with connection.get_channel() as channel:
            await channel.queue_declare('hello')

            await channel.basic_publish(
                b'Hello, world!',
                exchange='',
                routing_key='hello',
            )
            print(" [x] Sent 'Hello World!'")

run(main())
