import asyncio

from amqproto import AMQPConnection

class AMQP(asyncio.StreamReaderProtocol):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.connection = AMQPConnection()
        self.transport = None

    def connection_made(self, transport):
        super().connection_made(transport)

        self.transport = transport
        self.connection.initiate_connection()
        transport.write(self.connection.data_to_send())

    def data_received(self, data):
        super().data_received(data)

        frames = self.connection.receive_frames(data)
        d = self.connection.data_to_send()
        print(d)

        self.transport.write(d)

        for frame in frames:
            self.connection.handle_frame(frame)

        d = self.connection.data_to_send()
        print(d)

        self.transport.write(d)


_DEFAULT_LIMIT = 2 ** 16


@asyncio.coroutine
def open_connection(host=None, port=None, *,
                    loop=None, limit=_DEFAULT_LIMIT, **kwds):
    """A wrapper for create_connection() returning a (reader, writer) pair.
    The reader returned is a StreamReader instance; the writer is a
    StreamWriter instance.
    The arguments are all the usual arguments to create_connection()
    except protocol_factory; most common are positional host and port,
    with various optional keyword arguments following.
    Additional optional keyword arguments are loop (to set the event loop
    instance to use) and limit (to set the buffer limit passed to the
    StreamReader).
    (If you want to customize the StreamReader and/or
    StreamReaderProtocol classes, just copy the code -- there's
    really nothing special here except some convenience.)
    """
    if loop is None:
        loop = events.get_event_loop()
    reader = asyncio.StreamReader(limit=limit, loop=loop)
    protocol = AMQP(reader, loop=loop)
    transport, _ = yield from loop.create_connection(
        lambda: protocol, host, port, **kwds)
    writer = asyncio.StreamWriter(transport, protocol, reader, loop)
    return reader, writer
