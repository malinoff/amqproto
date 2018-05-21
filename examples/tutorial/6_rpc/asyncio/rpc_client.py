#!/usr/bin/env python3
import uuid

from async_generator import aclosing

from amqproto import BasicContent, BasicProperties
from amqproto.adapters.asyncio_adapter import AsyncioConnection, run


class FibonacciRpcClient:

    def __init__(self, channel):
        self.channel = channel

    async def __aenter__(self):
        self.reply_queue = await self.channel.queue_declare('', exclusive=True)
        await self.channel.basic_consume(self.reply_queue.queue, no_ack=True)
        return self

    async def __aexit__(self, *_):
        pass

    async def call(self, n):
        correlation_id = str(uuid.uuid4())
        message = BasicContent(
            str(n).encode('utf-8'),
            properties=BasicProperties(
                reply_to=self.reply_queue.queue,
                correlation_id=correlation_id,
            ),
        )
        await self.channel.basic_publish(
            message, exchange='', routing_key='rpc_queue',
        )
        async with aclosing(self.channel.delivered_messages()) as messages:
            async for reply in messages:
                if correlation_id == reply.correlation_id:
                    return int(reply.body.decode('utf-8'))


async def main():
    async with AsyncioConnection(host='localhost') as connection:
        async with connection.get_channel() as channel:

            fibonacci_rpc = FibonacciRpcClient(channel)
            async with fibonacci_rpc:
                print(" [x] Requesting fib(30)")
                response = await fibonacci_rpc.call(30)
                print(" [.] Got %r" % response)

run(main())
