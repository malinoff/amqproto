#!/usr/bin/env python3
import asyncio

from amqproto.protocol import BasicMessage
from amqproto.io.asyncio import Connection, run


def fib(n):
    if n == 0:
        return 0
    elif n == 1:
        return 1
    else:
        return fib(n-1) + fib(n-2)


async def main():
    try:
        async with Connection(host='localhost') as connection:
            async with connection.get_channel() as channel:
                await channel.queue_declare('rpc_queue', durable=True)

                await channel.basic_qos(prefetch_count=1)
                await channel.basic_consume('rpc_queue')
                print(' [x] Waiting for messages. To exit press CTRL+C')

                async for message in channel.consumed_messages():
                    body = message.decoded_body
                    n = int(body)
                    print(" [x] fib(%s)" % n)
                    response = fib(n)
                    print(" [x] Done: %s" % response)

                    reply_message = BasicMessage(
                        str(response),
                        correlation_id=message.correlation_id,
                    )
                    await channel.basic_publish(
                        reply_message, exchange='', routing_key=message.reply_to
                    )
                    await channel.basic_ack(
                        delivery_tag=message.delivery_info.delivery_tag
                    )
    except asyncio.CancelledError:
        print(' [x] Bye!')

run(main())
