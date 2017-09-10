#!/usr/bin/env python3
import sys

from amqproto.io.asyncio import Connection, run
from amqproto.protocol import BasicMessage, DeliveryMode


async def main():
    async with Connection(host='localhost') as connection:
        async with connection.get_channel() as channel:
            await channel.queue_declare('task_queue', durable=True)

            body = ' '.join(sys.argv[1:]) or "Hello World!"
            message = BasicMessage(body, delivery_mode=DeliveryMode.Persistent)
            await channel.basic_publish(
                message,
                exchange='',
                routing_key='task_queue',
            )
            print(" [x] Sent %r" % body)

run(main())
