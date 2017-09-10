#!/usr/bin/env python3
import asyncio

from amqproto.io.asyncio import Connection, run


async def main():
    try:
        async with Connection(host='localhost') as connection:
            async with connection.get_channel() as channel:
                await channel.exchange_declare('logs', type='fanout')
                result = await channel.queue_declare(exclusive=True)
                queue_name = result.queue

                await channel.queue_bind(exchange='logs', queue=queue_name)

                await channel.basic_consume(queue_name, no_ack=True)
                print(' [x] Waiting for logs. To exit press CTRL+C')

                async for message in channel.consumed_messages():
                    body = message.decoded_body
                    print(" [x] Received %r" % body)
    except asyncio.CancelledError:
        print(' [x] Bye!')


run(main())
