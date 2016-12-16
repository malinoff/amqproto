import asyncio

import pytest
import requests

from amqproto.adapters.asyncio import AMQP, Connection
from amqproto.adapters.asyncio import Channel, Exchange, Queue
from amqpframe.basic import Message


@pytest.fixture()
def connection(event_loop):
    coro = event_loop.create_connection(
        lambda: AMQP(loop=event_loop), 'localhost', 5672
    )
    transport, protocol = event_loop.run_until_complete(coro)
    return Connection(transport, protocol)


@pytest.fixture()
async def channel(event_loop, connection, request) -> Channel:
    await connection.open()
    channel_ = connection.get_channel()
    await channel_.open()

    def close():
        event_loop.run_until_complete(channel_.close())
        event_loop.run_until_complete(connection.close())
    request.addfinalizer(close)

    return channel_


@pytest.fixture()
async def exchange(event_loop, channel, request) -> Exchange:
    exchange_ = Exchange(channel, 'amqproto_test')
    await exchange_.declare()

    def delete():
        event_loop.run_until_complete(exchange_.delete())
    request.addfinalizer(delete)

    return exchange_


@pytest.mark.asyncio(forbid_global_loop=True)
async def test_can_connect_to_amqp_server(connection):
    async with connection:
        pass


@pytest.mark.asyncio(forbid_global_loop=True)
async def test_can_open_channel(connection):
    async with connection:
        channel = connection.get_channel()
        async with channel:
            pass


@pytest.mark.asyncio(forbid_global_loop=True)
async def test_can_declare_exchange(channel):
    exhange_name = 'amqproto_test'
    exchange = Exchange(channel, exhange_name)
    await exchange.declare()

    def call_rabbit_for_exchange(exchange_name_):
        return requests.get(
                'http://localhost:15672/api/exchanges/%2f/' + exchange_name_,
                auth=('guest', 'guest'))

    assert call_rabbit_for_exchange(exhange_name).status_code == 200

    # now delete it
    await exchange.delete()

    assert call_rabbit_for_exchange(exhange_name).status_code == 404


@pytest.mark.asyncio(forbid_global_loop=True)
async def test_can_declare_queue(channel):
    queue_name = 'amqproto_test_q'
    queue = Queue(channel, queue_name)
    await queue.declare()

    def call_rabbit_for_queue(q_name):
        return requests.get(
            'http://localhost:15672/api/queues/%2f/' + q_name,
            auth=('guest', 'guest'))

    assert call_rabbit_for_queue(queue_name).status_code == 200
    # now delete it
    await queue.delete()
    assert call_rabbit_for_queue(queue_name).status_code == 404


@pytest.mark.asyncio(forbid_global_loop=True)
async def test_can_publish_message_on_exchange(exchange):
    msg = b'This is a test message'
    message = Message(msg, body_size=len(msg))
    await exchange.publish(message)


@pytest.mark.asyncio(forbid_global_loop=True)
async def test_can_bind_queue_to_exchange(exchange):
    queue_name = 'amqproto_test_q'
    queue = Queue(exchange._channel, queue_name)
    await queue.declare()
    await queue.bind(exchange)

    def is_queue_bound(q_name, exchange_name):
        result = requests.get(
            'http://localhost:15672/api/bindings/%2f/e/' + exchange_name +
            '/q/' + q_name,
            auth=('guest', 'guest'))
        json = result.json()
        return len(json) > 0

    assert is_queue_bound(queue_name, exchange.name)

    # now remove it
    await queue.unbind(exchange)
    assert not is_queue_bound(queue_name, exchange.name)


@pytest.mark.asyncio(forbid_global_loop=True)
async def test_can_recieve_message(event_loop, exchange):
    # setup
    queue_name = 'amqproto_test_q'
    queue = Queue(exchange._channel, queue_name)
    await queue.declare()
    await queue.bind(exchange)
    body = b'this is a test message'
    await exchange.publish(Message(body, body_size=len(body)))

    message = await queue.get()

    assert message.body == body

    await queue.unbind(exchange)
