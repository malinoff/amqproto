#!/usr/bin/env python3
import sys

from amqproto.adapters.asyncio_adapter import AsyncioConnection, run


async def main():
    async with AsyncioConnection(host='localhost') as connection:
        async with connection.get_channel() as channel:
            await channel.exchange_declare('logs', type='fanout')

            body = ' '.join(sys.argv[1:]) or "info: Hello World!"
            await channel.basic_publish(body.encode('utf-8'), exchange='logs')
            print(" [x] Sent %r" % body)

run(main())
