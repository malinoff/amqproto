"""
amqproto.sasl
~~~~~~~~~~~~~

AMQP SASL protocols.
"""

from . import domains as d


class SASL:
    """Base class for SASL protocols."""
    mechanism = None

    def to_bytes(self):
        """Serialize the authentication payload to bytes."""
        raise NotImplementedError

    def handle_challenge(self, challenge: bytes) -> bytes:
        """This method is called when a challenge from a peer is received
        during :class:`~amqproto.protocol.methods.ConnectionSecure`.
        Returned bytes are sent as a response.
        """
        raise NotImplementedError


class PLAIN(SASL):
    """SASL PLAIN authentication. This is enabled by default in the RabbitMQ
    server and clients, and is the default for most other clients.
    """
    mechanism = 'PLAIN'

    def __init__(self, username: str, password: str):
        self.username = username.encode('utf-8')
        self.password = password.encode('utf-8')

    def to_bytes(self):
        return b'\x00%s\x00%s' % (self.username, self.password)

    def handle_challenge(self, challenge: bytes) -> bytes:
        raise RuntimeError('PLAIN SASL method does not support challenging')


class AMQPLAIN(SASL):
    """Non-standard version of PLAIN as defined by the AMQP 0-8 specification.
    This is enabled by default in the RabbitMQ server, and is the default for
    QPid's Python client.
    """
    mechanism = 'AMQPLAIN'

    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password

    def to_bytes(self):
        return d.Table.build({
            'LOGIN': self.username,
            'PASSWORD': self.password
        })

    def handle_challenge(self, challenge: bytes) -> bytes:
        raise RuntimeError('AMQPLAIN SASL method does not support challenging')
