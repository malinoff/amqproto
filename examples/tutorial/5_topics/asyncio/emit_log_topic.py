#!/usr/bin/env python3
import sys

from amqproto.protocol import BasicMessage
from amqproto.io.asyncio import Connection, run


async def main():
    async with Connection(host='localhost') as connection:
        async with connection.get_channel() as channel:
            await channel.exchange_declare('topic_logs', type='topic')

            severity = sys.argv[1] if len(sys.argv) > 2 else 'anonymous.info'
            body = ' '.join(sys.argv[2:]) or "Hello World!"
            message = BasicMessage(body)

            await channel.basic_publish(
                message,
                exchange='topic_logs',
                routing_key=severity,
            )
            print(" [x] Sent %r:%r" % (severity, body))

run(main())
