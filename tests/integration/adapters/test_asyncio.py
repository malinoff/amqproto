import asyncio

import pytest

from amqproto.adapters.asyncio import AMQP, Connection

@pytest.fixture()
def connection(loop):
    coro = loop.create_connection(
        lambda: AMQP(loop=loop), 'localhost', 5672
    )
    transport, protocol = loop.run_until_complete(coro)
    return Connection(transport, protocol)


def test_can_connect_to_amqp_server(loop, connection):
    async def test():
        await connection.open()
        await connection.close()

    loop.run_until_complete(test())
