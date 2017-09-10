#!/usr/bin/env python3
from amqproto.protocol import BasicMessage
from amqproto.io.asyncio import Connection, run


async def main():
    async with Connection(host='localhost') as connection:
        async with connection.get_channel() as channel:
            await channel.queue_declare('hello')

            message = BasicMessage('Hello world!')
            await channel.basic_publish(
                message,
                exchange='',
                routing_key='hello',
            )
            print(" [x] Sent 'Hello World!'")

run(main())
