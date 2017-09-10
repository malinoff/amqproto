#!/usr/bin/env python3
import asyncio
from amqproto.io.asyncio import Connection, run


async def main():
    try:
        async with Connection(host='localhost') as connection:
            async with connection.get_channel() as channel:
                await channel.queue_declare('hello')

                await channel.basic_consume('hello', no_ack=True)
                print(' [*] Waiting for messages. To exit press CTRL+C')
                async for message in channel.consumed_messages():
                    print(" [x] Received %r" % message.decoded_body)
    except asyncio.CancelledError:
        print(' [x] Bye!')

run(main())
