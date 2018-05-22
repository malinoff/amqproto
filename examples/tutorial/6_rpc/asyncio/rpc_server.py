#!/usr/bin/env python3
import asyncio

from async_generator import aclosing

from amqproto import BasicContent, BasicProperties
from amqproto.adapters.asyncio_adapter import AsyncioConnection, run


def fib(n):
    if n == 0:
        return 0
    elif n == 1:
        return 1
    else:
        return fib(n-1) + fib(n-2)


async def main():
    try:
        async with AsyncioConnection(host='localhost') as connection:
            async with connection.get_channel() as channel:
                await channel.queue_declare('rpc_queue', durable=True)

                await channel.basic_qos(prefetch_count=1)
                await channel.basic_consume('rpc_queue')
                print(' [x] Waiting for messages. To exit press CTRL+C')

                async with aclosing(channel.delivered_messages()) as messages:
                    async for message in messages:
                        body = message.body.decode('utf-8')
                        n = int(body)
                        print(" [x] fib(%s)" % n)
                        response = fib(n)
                        print(" [x] Done: %s" % response)

                        reply_message = BasicContent(
                            str(response),
                            properties=BasicProperties(
                                correlation_id=message.properties.correlation_id,
                            )
                        )
                        await channel.basic_publish(
                            reply_message.encode('utf-8'),
                            exchange='',
                            routing_key=message.properties.reply_to
                        )
                        await channel.basic_ack(
                            delivery_tag=message.delivery_info.delivery_tag
                        )
    except asyncio.CancelledError:
        print(' [x] Bye!')

run(main())
