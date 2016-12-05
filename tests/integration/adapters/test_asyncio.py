import asyncio

import pytest

import requests

from amqproto.adapters.asyncio import AMQP, Connection
from amqproto.adapters.asyncio import Channel, Exchange, Queue
from amqproto.message import Message


def run_async(loop_, coro):
    try:
        return loop_.run_until_complete(
            asyncio.wait_for(coro, timeout=3, loop=loop_))
    except asyncio.TimeoutError:
        raise pytest.fail('Coro: {} did not complete in 3 seconds'
                          .format(coro))


@pytest.fixture()
def connection(loop):
    coro = loop.create_connection(
        lambda: AMQP(loop=loop), 'localhost', 5672
    )
    transport, protocol = loop.run_until_complete(coro)
    return Connection(transport, protocol)


@pytest.fixture()
def channel(loop, connection, request) -> Channel:
    channel_ = None
    async def connect():
        nonlocal channel_
        await connection.open()
        channel_ = connection.get_channel()
        await channel_.open()

    def on_finish():
        async def close():
            await channel_.close()
            await connection.close()
        run_async(loop, close())

    run_async(loop, connect())
    request.addfinalizer(on_finish)
    return channel_


@pytest.fixture()
def exchange(loop, channel, request) -> Exchange:
    exchange_ = Exchange(channel, 'amqproto_test')
    async def declare():
        await exchange_.declare()

    def on_finish():
        async def delete():
            await exchange_.delete()

        run_async(loop, delete())

    run_async(loop, declare())
    request.addfinalizer(on_finish)
    return exchange_


def test_can_connect_to_amqp_server(loop, connection):
    async def test():
        await connection.open()
        await connection.close()

    run_async(loop, test())


def test_can_open_channel(loop, connection):
    async def test():
        await connection.open()
        try:
            channel = connection.get_channel()
            assert isinstance(channel, Channel)
            await channel.open()
            await channel.close()
        finally:
            await connection.close()

    run_async(loop, test())


def test_can_declare_exchange(loop, channel):
    exhange_name = 'amqproto_test'
    exchange = Exchange(channel, exhange_name)
    async def test():
        await exchange.declare()
    run_async(loop, test())

    def call_rabbit_for_exchange(exchange_name_):
        return requests.get(
                'http://localhost:15672/api/exchanges/%2f/' + exchange_name_,
                auth=('guest', 'guest'))

    assert call_rabbit_for_exchange(exhange_name).status_code == 200

    # now delete it
    async def test_delete():
        await exchange.delete()
    run_async(loop, test_delete())

    assert call_rabbit_for_exchange(exhange_name).status_code == 404


def test_can_declare_queue(loop, channel):
    queue_name = 'amqproto_test_q'
    queue = Queue(channel, queue_name)
    async def test():
        await queue.declare()

    def call_rabbit_for_queue(q_name):
        return requests.get(
            'http://localhost:15672/api/queues/%2f/' + q_name,
            auth=('guest', 'guest'))

    assert call_rabbit_for_queue(queue_name).status_code == 200
    run_async(loop, test())

    # now delete it
    async def test_delete():
        await queue.delete()
    run_async(loop, test_delete())

    assert call_rabbit_for_queue(queue_name).status_code == 404


def test_can_publish_message_on_exchange(loop, exchange):
    async def test():
        message = Message('This is a test message')
        await exchange.publish(message)

    run_async(loop, test())


def test_can_bind_queue_to_exchange(loop, exchange):
    queue_name = 'amqproto_test_q'
    queue = Queue(exchange._channel, queue_name)
    async def test():
        await queue.declare()
        await queue.bind(exchange)

    def is_queue_bound(q_name, exchange_name):
        result = requests.get(
            'http://localhost:15672/api/bindings/%2f/e/' + exchange_name +
            '/q/' + q_name,
            auth=('guest', 'guest'))
        json = result.json()
        return len(json) > 0

    run_async(loop, test())

    assert is_queue_bound(queue_name, exchange.name)

    # now remove it
    async def cleanup():
        await queue.unbind(exchange)

    run_async(loop, cleanup())

    assert not is_queue_bound(queue_name, exchange.name)


def test_can_recieve_message(loop, exchange):
    # setup
    queue_name = 'amqproto_test_q'
    queue = Queue(exchange._channel, queue_name)
    async def setup():
        await queue.declare()
        await queue.bind(exchange)

    run_async(setup())

    # Do the test
    async def test():
        return await queue.get()

    message = run_async(loop, test())
    assert message

    # cleanup
    async def cleanup():
        await queue.unbind(exchange)

    run_async(loop, cleanup())


