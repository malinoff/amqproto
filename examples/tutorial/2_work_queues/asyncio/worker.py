#!/usr/bin/env python3
import asyncio

from amqproto.io.asyncio import Connection, run


async def main():
    try:
        async with Connection(host='localhost') as connection:
            async with connection.get_channel() as channel:
                await channel.queue_declare('task_queue', durable=True)

                await channel.basic_qos(prefetch_count=1)
                await channel.basic_consume('task_queue')
                print(' [x] Waiting for messages. To exit press CTRL+C')

                async for message in channel.consumed_messages():
                    body = message.decoded_body
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
