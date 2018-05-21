#!/usr/bin/env python3
import sys

from amqproto.content import BasicContent, BasicProperties
from amqproto.adapters.asyncio_adapter import AsyncioConnection, run


async def main():
    async with AsyncioConnection(host='localhost') as connection:
        async with connection.get_channel() as channel:
            await channel.queue_declare('task_queue', durable=True)

            body = ' '.join(sys.argv[1:]) or "Hello World!"
            properties = BasicProperties(delivery_mode=2)
            content = BasicContent(body.encode('utf-8'), properties=properties)
            await channel.basic_publish(
                content,
                exchange='',
                routing_key='task_queue',
            )
            print(" [x] Sent %r" % body)

run(main())
