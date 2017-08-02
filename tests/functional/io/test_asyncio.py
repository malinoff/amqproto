import asyncio

import pytest
import requests

from amqproto import protocol
from amqproto.io.asyncio import Connection


@pytest.fixture()
async def connection(event_loop):
    async with Connection(loop=event_loop) as connection:
        yield connection


@pytest.fixture()
async def channel(connection):
    async with connection.get_channel() as channel:
        yield channel


@pytest.mark.asyncio(forbid_global_loop=True)
async def test_can_connect(event_loop):
    async with Connection(loop=event_loop) as conn:
        pass


@pytest.mark.asyncio(forbid_global_loop=True)
async def test_can_open_channel(connection):
    async with connection.get_channel() as channel:
        pass


@pytest.mark.asyncio(forbid_global_loop=True)
async def test_can_declare_exchange(channel):
    exchange_name = 'amqproto_test'
    await channel.exchange_declare(exchange_name)

    def call_rabbit_for_exchange(exchange_name_):
        return requests.get(
                'http://localhost:15672/api/exchanges/%2f/' + exchange_name_,
                auth=('guest', 'guest'))

    assert call_rabbit_for_exchange(exchange_name).status_code == 200
    # now delete it
    await channel.exchange_delete(exchange_name)
    assert call_rabbit_for_exchange(exchange_name).status_code == 404


@pytest.mark.asyncio(forbid_global_loop=True)
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


@pytest.mark.asyncio(forbid_global_loop=True)
async def test_can_publish_message_on_exchange(channel):
    msg = b'This is a test message'
    message = protocol.BasicMessage(msg)
    await channel.basic_publish(message)


@pytest.mark.asyncio(forbid_global_loop=True)
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


@pytest.mark.asyncio(forbid_global_loop=True)
async def test_can_get_messages(channel):
    await channel.queue_declare('hello')
    message = protocol.BasicMessage(b'hello world')
    await channel.basic_publish(message, exchange='', routing_key='hello')
    fut = await channel.basic_get('hello')
    received_message = await fut
    assert received_message.body == b'hello world'


@pytest.mark.asyncio(forbid_global_loop=True)
async def test_produce_and_consume(channel):
    await channel.queue_declare('hello')
    message = protocol.BasicMessage(b'hello world')
    await channel.basic_publish(message, exchange='', routing_key='hello')
    fut, consumer_tag = await channel.basic_consume('hello')

    fut, received_message = await fut
    assert received_message.body == b'hello world'
    assert received_message.delivery_info.routing_key == b'hello'
    await channel.basic_cancel(consumer_tag)


@pytest.mark.asyncio(forbid_global_loop=True)
async def test_mandatory_flag_handles_undelivered_messages(channel):
    message = protocol.BasicMessage(b'some message')
    exchange_name = 'amqproto_test'
    await channel.exchange_declare(exchange_name)

    await channel.basic_publish(
        message,
        exchange=exchange_name,
        routing_key='foobar',  # this queue doesnt exist
        mandatory=True,
    )
    await asyncio.sleep(1, loop=channel.loop)
    returned_messages = list(channel.returned_messages())
    assert len(returned_messages) == 1
    assert returned_messages[0].body == message.body

    await channel.exchange_delete(exchange_name)


@pytest.mark.asyncio(forbid_global_loop=True)
async def test_mandatory_flag_on_existing_queue(channel):
    exchange_name = 'amq.direct'
    queue_name = 'amqproto_test_q'
    message = protocol.BasicMessage(b'some message')
    await channel.queue_declare(queue_name)
    await channel.queue_bind(queue_name, exchange_name)

    await channel.basic_publish(
        message, routing_key='amqproto_test_q', mandatory=True
    )
    await asyncio.sleep(1, loop=channel.loop)
    assert not list(channel.returned_messages())

    # cleanup
    await channel.queue_unbind(queue_name, exchange_name)
    await channel.queue_delete(queue_name)


@pytest.mark.asyncio(forbid_global_loop=True)
async def test_channel_errors_are_handled_properly(channel):
    with pytest.raises(protocol.AMQPError):
        await channel.queue_unbind('amqproto_test_q', '')
