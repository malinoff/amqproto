#!/usr/bin/env python3
import asyncio

from async_generator import aclosing

from amqproto.adapters.asyncio_adapter import AsyncioConnection, run


async def main():
    try:
        async with AsyncioConnection(host='localhost') as connection:
            async with connection.get_channel() as channel:
                await channel.exchange_declare('logs', type='fanout')
                reply = await channel.queue_declare('', exclusive=True)
                await channel.queue_bind(reply.queue, exchange='logs')

                await channel.basic_consume(reply.queue, no_ack=True)
                print(' [x] Waiting for logs. To exit press CTRL+C')

                async with aclosing(channel.delivered_messages()) as messages:
                    async for message in messages:
                        print(" [x] Received %r" % message.body.decode('utf-8'))
    except asyncio.CancelledError:
        print(' [x] Bye!')


run(main())
