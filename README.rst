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

===========  ======================= =============
I/O library  Adapter                 Requirements
===========  ======================= =============
asyncio_     ``amqproto.io.asyncio`` Python 3.5.3+
curio_       **Planned**
Twisted_     **Planned**
blocking_    **Planned**
===========  ======================= =============

Feel free to make a pull request adding your own I/O layer of preference.

.. _asyncio: https://docs.python.org/3/library/asyncio.html
.. _curio: https://github.com/dabeaz/curio
.. _Twisted: https://twistedmatrix.com/
.. _blocking: https://docs.python.org/3/library/socket.html
.. _sans-io: http://sans-io.readthedocs.io/

Installation
============

**NOTE**: ``amqproto`` is not yet available in ``pypi``. Please, clone this
repository to install ``amqproto``.

For the end-user of ``amqproto``, it doesn't make much sense
to install it without installing a specific I/O adapter.
For this we leverage ``setuptools`` extras feature::

    $ pip install .[asyncio]

Replace ``asyncio`` with the I/O adapter of your preference.

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


Design goals
============

General
-------

We provide as much high level APIs as we can, while hiding
implementation details as much as possible.

We provide extension points using dependency injection principle,
not inheritance, or callbacks, or some sort of "signals". Let's take
a custom authentication protocol for example. A proper way to let
users to use their own authentication protocol is to inject something
into the ``Connection``'s constructor::

    # Auth base class acts like the interface, you don't have to subclass it
    from amqproto.auth import Auth

    class CustomAuth(Auth):

        mechanism = b'CUSTOMAUTH'

        def __init__(self, secret):
            self.secret = secret

        def to_bytestream(self, stream):
            stream.write(b'secret:')
            stream.write(self.secret)
            stream.write(b'\0')

    connection = Connection(auth=CustomAuth(b'some secret'))

A not-so-proper way would be to require subclassing ``Connection``
and overriding its default authentication credentials
("username" and "password").

Peformance
----------

#. Make it work.
#. Make it right.
#. Make it fast.

We're now at the second phase.
