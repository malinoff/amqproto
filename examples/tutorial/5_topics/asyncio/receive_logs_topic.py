#!/usr/bin/env python3
import sys
import asyncio

from async_generator import aclosing

from amqproto.adapters.asyncio_adapter import AsyncioConnection, run


async def main():
    try:
        async with AsyncioConnection(host='localhost') as connection:
            async with connection.get_channel() as channel:
                await channel.exchange_declare('topic_logs', type='topic')
                result = await channel.queue_declare('', exclusive=True)
                queue_name = result.queue

                severities = sys.argv[1:]
                if not severities:
                    sys.stderr.write("Usage: %s [binding_key]...\n" % sys.argv[0])
                    sys.exit(1)

                for severity in severities:
                    await channel.queue_bind(
                        queue=queue_name,
                        exchange='topic_logs',
                        routing_key=severity,
                    )

                await channel.basic_consume(queue_name, no_ack=True)
                print(' [x] Waiting for logs. To exit press CTRL+C')

                async with aclosing(channel.delivered_messages()) as messages:
                    async for message in messages:
                        body = message.body.decode('utf-8')
                        severity = message.delivery_info.routing_key
                        print(" [x] Received %r:%r" % (severity, body))
    except asyncio.CancelledError:
        print(' [x] Bye!')


run(main())
