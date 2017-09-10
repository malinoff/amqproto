#!/usr/bin/env python3
import sys

from amqproto.protocol import BasicMessage
from amqproto.io.asyncio import Connection, run


async def main():
    async with Connection(host='localhost') as connection:
        async with connection.get_channel() as channel:
            await channel.exchange_declare('logs', type='fanout')

            body = ' '.join(sys.argv[1:]) or "info: Hello World!"
            message = BasicMessage(body)
            await channel.basic_publish(message, exchange='logs')
            print(" [x] Sent %r" % body)

run(main())
