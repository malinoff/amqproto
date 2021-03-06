#!/usr/bin/env python3
import asyncio

from async_generator import aclosing

from amqproto.adapters.asyncio_adapter import AsyncioConnection, run


async def main():
    try:
        async with AsyncioConnection(host='localhost') as connection:
            async with connection.get_channel() as channel:
                await channel.queue_declare('task_queue', durable=True)

                await channel.basic_qos(prefetch_count=1)
                await channel.basic_consume('task_queue')
                print(' [x] Waiting for messages. To exit press CTRL+C')

                async with aclosing(channel.delivered_messages()) as messages:
                    async for message in messages:
                        body = message.body.decode('utf-8')
                        to_sleep = body.count('.')
                        print(" [x] Received %r, sleeping for %d seconds" % (
                            body, to_sleep
                        ))
                        await asyncio.sleep(to_sleep)
                        print(" [x] Done")
                        await channel.basic_ack(
                            delivery_tag=message.delivery_info.delivery_tag
                        )
    except asyncio.CancelledError:
        print(' [x] Bye!')


run(main())
