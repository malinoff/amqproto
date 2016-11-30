import logging
logging.basicConfig(level=logging.DEBUG)

import asyncio
from amqproto.adapters.asyncio import open_connection

async def do(reader, writer):
    data = await reader.read(1024*10)
    writer.write(data)


def main():
    loop = asyncio.get_event_loop()
    coro = open_connection('127.0.0.1', 5672, loop=loop)
    reader, writer = loop.run_until_complete(coro)
    loop.run_until_complete(do(reader, writer))


main()
