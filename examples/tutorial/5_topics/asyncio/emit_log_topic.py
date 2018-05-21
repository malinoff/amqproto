#!/usr/bin/env python3
import sys

from amqproto.adapters.asyncio_adapter import AsyncioConnection, run


async def main():
    async with AsyncioConnection(host='localhost') as connection:
        async with connection.get_channel() as channel:
            await channel.exchange_declare('topic_logs', type='topic')

            severity = sys.argv[1] if len(sys.argv) > 2 else 'anonymous.info'
            body = ' '.join(sys.argv[2:]) or "Hello World!"
            await channel.basic_publish(
                body.encode('utf-8'),
                exchange='topic_logs',
                routing_key=severity,
            )
            print(" [x] Sent %r:%r" % (severity, body))

run(main())
