amqproto
========

.. warning::

    This library is in the early stage of development and is not indended
    for production use. However, any input, real-life usage examples
    and bug reports are highly appreciated!

AMQP is a messaging protocol that enables conforming client applications to communicate with conforming messaging middleware brokers. 

This library implements client side of the AMQP protocol as a pure state-machine which only takes in bytes and returns a list of parsed events.
This leaves users free to use any I/O approach they see fit (asyncio_, curio_, Twisted_, etc.).
Such approach is called sans-io_.

``amqproto`` comes with a built-in set of adapters for various networking libraries:

===========  ==================================== =============
I/O library  Adapter                              Requirements
===========  ==================================== =============
asyncio_     ``amqproto.adapters.asyncio_adater`` Python 3.5.3+
curio_       **Planned**
Twisted_     **Planned**
blocking_    **Planned**
===========  ==================================== =============

Feel free to make a pull request adding your own I/O layer of preference.

.. _asyncio: https://docs.python.org/3/library/asyncio.html
.. _curio: https://github.com/dabeaz/curio
.. _Twisted: https://twistedmatrix.com/
.. _blocking: https://docs.python.org/3/library/socket.html
.. _sans-io: http://sans-io.readthedocs.io/

Installation
============

**NOTE**: ``amqproto`` is not yet available in ``pypi``. Please, clone this repository to install ``amqproto``, then execute::

    $ pip install .

Example
===================================

Here is the most simple example of use, sending a message with ``asyncio`` adapter::

    import asyncio
    from amqproto.adapters.asyncio_adapter import AsyncioConnection, run

    async def main():
        async with AsyncioConnection(loop=loop) as connection:
            async with connection.get_channel() as channel:
                await channel.basic_publish(
                    b'hello from amqproto',
                    exchange='example',
                    routing_key='test rk',
                )
    run(main())


Design goals
============

General
-------

We provide as much high level APIs as we can, while hiding implementation details as much as possible.

We provide extension points using dependency injection principle, not inheritance, or callbacks, or some sort of "signals".
Let's take a custom authentication protocol for example.
A proper way to let users to use their own authentication protocol is to inject something into the ``Connection``'s constructor::

    # Auth base class acts like the interface, you don't have to subclass it
    from amqproto.sasl import SASL

    class CustomAuth(Auth):
        mechanism = b'CUSTOMAUTH'

        def __init__(self, secret: bytes):
            self.secret = secret

        def to_bytes(self):
            return b'secret: %s\0' % self.secret

    connection = Connection(auth=CustomAuth(b'some secret'))

A not-so-proper way would be to require subclassing ``Connection`` and overriding its default authentication credentials ("username" and "password").

Peformance
----------

#. Make it work.
#. Make it right.
#. Make it fast.

We're now at the second phase.

For some time, amqproto had been using construct_ to parse and build AMQP frames.
It turned out that construct is amazingly slow, making amqproto 18 times slower than the other existing AMQP clients.
That's why there have been an attempt to rewrite construct; the rewrite is 4-5 times faster but this is still much slower than existing AMQP clients.
Because of that, construct has been rejected and ``amqproto.serialization`` was implemented.

This helped to achieve performance similar to py-amqp and pika.
But, installing and enabling ``uvloop`` makes amqproto much, much faster: 3-4 times faster than py-amqp and 7-8 times faster than pika.

.. _construct: https://construct.readthedocs.io/en/latest/
