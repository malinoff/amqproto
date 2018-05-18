import asyncio

import pytest
import requests

from amqproto import BaseReply
from amqproto.methods import BasicDeliver, BasicReturn
from amqproto.adapters.asyncio_adapter import AsyncioConnection


@pytest.fixture
async def connection():
    async with AsyncioConnection() as conn:
        yield conn


@pytest.fixture
async def channel(connection):
    async with connection.get_channel() as chan:
        yield chan


@pytest.mark.asyncio()
async def test_can_connect():
    async with AsyncioConnection():
        pass


@pytest.mark.asyncio()
async def test_can_open_channel(connection):
    async with connection.get_channel():
        pass


@pytest.mark.asyncio()
async def test_can_declare_exchange(channel):
    exchange_name = 'amqproto_test'
    await channel.exchange_declare(exchange_name)

    def call_rabbit_for_exchange(exchange_name_):
        return requests.get(
            'http://localhost:15672/api/exchanges/%2f/' + exchange_name_,
            auth=('guest', 'guest')
        )

    assert call_rabbit_for_exchange(exchange_name).status_code == 200
    # now delete it
    await channel.exchange_delete(exchange_name)
    assert call_rabbit_for_exchange(exchange_name).status_code == 404


@pytest.mark.asyncio()
async def test_can_declare_queue(channel):
    queue_name = 'amqproto_test_q'
    await channel.queue_declare(queue_name)

    def call_rabbit_for_queue(q_name):
        return requests.get(
            'http://localhost:15672/api/queues/%2f/' + q_name,
            auth=('guest', 'guest'))

    assert call_rabbit_for_queue(queue_name).status_code == 200
    # now delete it
    await channel.queue_delete(queue_name)
    assert call_rabbit_for_queue(queue_name).status_code == 404


@pytest.mark.asyncio()
async def test_can_bind_queue_to_exchange(channel):
    exchange_name = 'amqproto_test'
    queue_name = 'amqproto_test_q'
    await channel.exchange_declare(exchange_name)
    await channel.queue_declare(queue_name)
    await channel.queue_bind(queue_name, exchange_name)

    def is_queue_bound(q_name, exchange_name):
        result = requests.get(
            'http://localhost:15672/api/bindings/%2f/e/' + exchange_name +
            '/q/' + q_name,
            auth=('guest', 'guest'))
        return result.json()

    assert is_queue_bound(queue_name, exchange_name)
    # now remove it
    await channel.queue_unbind(exchange_name, exchange_name)
    await channel.queue_delete(queue_name)
    await channel.exchange_delete(exchange_name)
    assert not is_queue_bound(queue_name, exchange_name)


@pytest.mark.asyncio()
async def test_can_publish_and_get_messages(channel):
    await channel.queue_declare('hello')
    message = b'hello world'
    await channel.basic_publish(message, exchange='', routing_key='hello')
    response = await channel.basic_get('hello')
    assert response.content.body == b'hello world'


@pytest.mark.asyncio()
async def test_can_produce_and_consume_messages(channel):
    await channel.queue_declare('hello')
    reply = await channel.basic_consume('hello')
    consumer_tag = reply.consumer_tag

    message = b'hello world'
    await channel.basic_publish(message, exchange='', routing_key='hello')

    async for message in channel.delivered_messages():
        assert message.body == b'hello world'
        assert isinstance(message.delivery_info, BasicDeliver)
        assert message.delivery_info.routing_key == 'hello'

        await channel.basic_ack(message.delivery_info.delivery_tag)
        break

    assert channel._delivered_messages.empty()

    await channel.basic_cancel(consumer_tag)


@pytest.mark.asyncio()
async def test_mandatory_flag_handles_undelivered_messages(channel):
    message = b'some message'
    exchange_name = 'amqproto_test'
    await channel.exchange_declare(exchange_name)

    await channel.basic_publish(
        message,
        exchange=exchange_name,
        routing_key='foobar',  # this queue doesnt exist
        mandatory=True,
    )
    async for message in channel.delivered_messages():
        assert message.body == message.body
        assert isinstance(message.delivery_info, BasicReturn)
        break

    assert channel._delivered_messages.empty()

    await channel.exchange_delete(exchange_name)


@pytest.mark.asyncio()
async def test_mandatory_flag_on_existing_queue(channel):
    exchange_name = 'amq.direct'
    queue_name = 'amqproto_test_q'
    message = b'some message'
    await channel.queue_declare(queue_name)
    await channel.queue_bind(queue_name, exchange_name)

    await channel.basic_publish(
        message, routing_key='amqproto_test_q', mandatory=True
    )
    # Make sure there are no dangling messages in the queue
    response = await channel.basic_get(queue_name)
    assert response.content.body == message
    assert channel._delivered_messages.empty()

    # cleanup
    await channel.queue_unbind(queue_name, exchange_name)
    await channel.queue_delete(queue_name)


@pytest.mark.asyncio()
async def test_channel_errors_are_handled_properly(channel):
    with pytest.raises(BaseReply):
        await channel.queue_unbind('amqproto_test_q', '')


@pytest.mark.asyncio()
async def test_heartbeats():
    # Should not raise any connection errors
    async with AsyncioConnection(heartbeat=1) as conn:
        async with conn.get_channel():
            await asyncio.sleep(2)
