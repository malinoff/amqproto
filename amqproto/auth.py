"""
amqproto.auth
~~~~~~~~~~~~~

AMQP SASL protocols.
"""

import io

from .protocol import types


class Auth:
    """Base class for SASL protocols."""
    mechanism = None

    def to_bytestream(self, stream: io.BytesIO):
        """Serialize the authentication payload to the byte stream."""
        raise NotImplementedError

    def handle_challenge(self, challenge: bytes) -> bytes:
        """This method is called when a challenge from a peer is received
        during :class:`~amqproto.protocol.methods.ConnectionSecure`.
        Returned bytes are sent as a response.
        """
        raise NotImplementedError


class PLAIN(Auth):
    """SASL PLAIN authentication. This is enabled by default in the RabbitMQ
    server and clients, and is the default for most other clients.
    """
    mechanism = b'PLAIN'

    def __init__(self, username: bytes, password: bytes):
        self.username = username
        self.password = password

    def to_bytestream(self, stream: io.BytesIO):
        stream.write(b'\x00')
        stream.write(self.username)
        stream.write(b'\x00')
        stream.write(self.password)

    def handle_challenge(self, challenge: bytes) -> bytes:
        raise RuntimeError('PLAIN SASL method does not support challenging')


class AMQPLAIN(Auth):
    """Non-standard version of PLAIN as defined by the AMQP 0-8 specification.
    This is enabled by default in the RabbitMQ server, and is the default for
    QPid's Python client.
    """
    mechanism = b'AMQPLAIN'

    def __init__(self, username: bytes, password: bytes):
        self.username = username
        self.password = password

    def to_bytestream(self, stream: io.BytesIO):
        val = types.Table((
            (b'LOGIN', self.username),
            (b'PASSWORD', self.password),
        ))
        val.to_bytestream(stream)

    def handle_challenge(self, challenge: bytes) -> bytes:
        raise RuntimeError('AMQPLAIN SASL method does not support challenging')
