import pytest

from amqproto import protocol
from amqproto.io.asyncio import Connection


@pytest.mark.asyncio
async def test_produce_and_get(event_loop):
    async with Connection(loop=event_loop) as conn:
        async with conn.get_channel() as chan:
            await chan.queue_declare('hello')
            message = protocol.BasicMessage(b'hello world')
            chan.basic_publish(message, exchange='', routing_key='hello')
            fut = await chan.basic_get('hello')
            received_message = await fut
            assert received_message.body == b'hello world'


@pytest.mark.asyncio
async def test_produce_and_consume(event_loop):
    async with Connection(loop=event_loop) as conn:
        async with conn.get_channel() as chan:
            await chan.queue_declare('hello')
            message = protocol.BasicMessage(b'hello world')
            chan.basic_publish(message, exchange='', routing_key='hello')
            fut, consumer_tag = await chan.basic_consume('hello')

            fut, received_message = await fut
            assert received_message.body == b'hello world'
            assert received_message.delivery_info.routing_key == b'hello'
            await chan.basic_cancel(consumer_tag)
