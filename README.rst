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
(asyncio_, curio_, Twisted_, etc.).

Unlike other existing sans-io_ libraries, amqproto comes with a built-in set
of adapters for various networking libraries:

===========  =============================
I/O library  Adapter
===========  =============================
asyncio_     ``amqproto.adapters.asyncio``
curio_       **Not supported yet**
Twisted_     **Not supported yet**
socket_      **Not supported yet**
===========  =============================

Feel free to make a pull request adding your own I/O layer of preference.

.. _asyncio: https://docs.python.org/3/library/asyncio.html
.. _curio: https://github.com/dabeaz/curio
.. _Twisted: https://twistedmatrix.com/
.. _socket: https://docs.python.org/3/library/socket.html
.. _sans-io: http://sans-io.readthedocs.io/
