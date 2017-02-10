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
async def test_can_receive_message(exchange):
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
    await queue.delete()


@pytest.mark.asyncio(forbid_global_loop=True)
async def test_mandatory_flag_raises_when_not_delivered(channel):
    # setup
    exchange = Exchange(channel, 'amq.direct')

    with pytest.raises(NotImplementedError):
        # ToDo: This should raise an UnroutableMessageException
        await exchange.publish(Message(b'some message'),
                               routing_key='foobar',  # this queue doesnt exist
                               mandatory=True)


@pytest.mark.asyncio(forbid_global_loop=True)
async def test_mandatory_flag_on_existing_queue(channel):
    # setup
    queue = Queue(channel, 'amqproto_test_q')
    exchange = Exchange(channel, 'amq.direct')
    await queue.declare()
    await queue.bind(exchange)

    # should not throw an exception
    await exchange.publish(Message(b'some message'),
                           routing_key='amqproto_test_q',
                           mandatory=True)

    # cleanup
    await queue.unbind(exchange)
    await queue.delete()


@pytest.mark.asyncio(forbid_global_loop=True)
async def test_topic_exchange_flow(channel):
    exchange = Exchange(channel, 'myexchange')
    queue = Queue(channel, 'amqproto_test_q')
    await Exchange.declare(type='topic')
    await queue.declare()
    await queue.bind(exchange, routing_key='my.routing.key')

    await exchange.publish(Message(b'Some Message'),
                           routing_key='my.routing.key')

    message = asyncio.wait_for(queue.get(), 4)
    assert message

    # cleanup
    await queue.unbind(exchange)
    await queue.delete()
    await exchange.delete()


@pytest.mark.asyncio(forbid_global_loop=True)
async def test_fanout_exchange_flow(channel):
    exchange = Exchange(channel, 'myexchange')
    queue1 = Queue(channel, 'amqproto_test_q')
    queue2 = Queue(channel, 'amqproto_test_q2')
    await Exchange.declare(type='fanout')
    await queue1.declare()
    await queue2.declare()
    await queue1.bind(exchange)
    await queue2.bind(exchange)

    await exchange.publish(Message(b'Some Message'),
                           routing_key='whatever')

    message1 = asyncio.wait_for(queue1.get(), 4)
    message2 = asyncio.wait_for(queue2.get(), 4)
    # they should both get the message
    assert message1 == message2

    # cleanup
    await queue1.unbind(exchange)
    await queue2.unbind(exchange)
    await queue1.delete()
    await queue2.delete()
    await exchange.delete()
