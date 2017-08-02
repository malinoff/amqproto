amqproto
========

.. warning::

    This library is in the early stage of development and is not indended
    for production use. However, any input, real-life usage examples
    and bug reports are highly appreciated!

AMQP is a messaging protocol that enables conforming client applications
to communicate with conforming messaging middleware brokers. 

This library implements client side of the AMQP protocol as a pure
state-machine which only takes in bytes and returns a list of parsed events.
This leaves users free to use any I/O approach they see fit
(asyncio_, curio_, Twisted_, etc.). Such approach is called sans-io_.

``amqproto`` comes with a built-in set of adapters for various networking
libraries:

===========  ======================= ============
I/O library  Adapter                 Requirements
===========  ======================= ============
asyncio_     ``amqproto.io.asyncio`` Python 3.5+
curio_       **Planned**
Twisted_     **Planned**
blocking_    **Planned**
===========  ======================= ============

Feel free to make a pull request adding your own I/O layer of preference.

.. _asyncio: https://docs.python.org/3/library/asyncio.html
.. _curio: https://github.com/dabeaz/curio
.. _Twisted: https://twistedmatrix.com/
.. _blocking: https://docs.python.org/3/library/socket.html
.. _sans-io: http://sans-io.readthedocs.io/

Example
===================================

Here is the most simple example of use, sending a message with ``asyncio``
adapter::

    import asyncio
    from amqproto.protocol import BasicMessage
    from amqproto.io.asyncio import Connection

    loop = asyncio.get_event_loop()
    async def main():
        message = BasicMessage('hello from amqproto')
        async with Connection(loop=loop) as connection:
            async with connection.get_channel() as channel:
                await channel.basic_publish(
                    message, exchange='example', routing_key='test rk'
                )
    loop.run_until_complete(main())
