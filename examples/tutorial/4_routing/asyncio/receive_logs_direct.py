#!/usr/bin/env python3
import sys
import asyncio

from amqproto.io.asyncio import Connection, run


async def main():
    try:
        async with Connection(host='localhost') as connection:
            async with connection.get_channel() as channel:
                await channel.exchange_declare('direct_logs', type='direct')
                result = await channel.queue_declare(exclusive=True)
                queue_name = result.queue

                severities = sys.argv[1:]
                if not severities:
                    sys.stderr.write("Usage: %s [info] [warning] [error]\n" % sys.argv[0])
                    sys.exit(1)

                for severity in severities:
                    await channel.queue_bind(
                        exchange='direct_logs',
                        queue=queue_name,
                        routing_key=severity,
                    )

                await channel.basic_consume(queue_name, no_ack=True)
                print(' [x] Waiting for logs. To exit press CTRL+C')

                async for message in channel.consumed_messages():
                    body = message.decoded_body
                    severity = message.delivery_info.routing_key
                    print(" [x] Received %r:%r" % (severity, body))
    except asyncio.CancelledError:
        print(' [x] Bye!')

run(main())
