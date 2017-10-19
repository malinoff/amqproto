"""
amqproto.connection
~~~~~~~~~~~~~~~~~~~

Sans-I/O implementations of AMQP connections.
"""
import io
import typing
import platform
import pkg_resources

from . import sasl
from .methods import Method
from .channel import Channel
from .settings import Settings
from .frames import Frame, ProtocolHeader


# The properties SHOULD contain at least these fields:
# * "product", giving the name of the client product,
# * "version", giving the name of the client version,
# * "platform", giving the name of the operating system,
# * "copyright", if appropriate, and
# * "information", giving other general information.
DEFAULT_CLIENT_PROPERTIES = {
    'product': 'amqproto',
    'version': str(pkg_resources.get_distribution('amqproto').version),
    'platform': platform.platform(),
    'copyright': '(c) Dmitry Malinovsky (aka malinoff) and contributors',
    'information': '',
}


class Connection(Channel):
    """Sans-I/O implementation of AMQP connection. Maintains per-connection
    state and creates channels.

    :param auth: SASL method to authenticate with the server.
    :param client_properties: a dictionary of arbitrary client properties.
        Unlikely needs to be ever changed.
    :param locale: preferred locale. Unlikely needs to be ever changed.
    :param channel_max: limit maximum channels. 0 means no specific limit.
        Unlikely needs to be ever changed.
    :param frame_max: limit maximum frame size. 0 means no specific limit.
        Unlikely needs to be ever changed.
    :param heartbeat: heartbeat delay in seconds. 0 means no heartbeats,
        which is not recommended.
    """

    def __init__(self,
                 auth: sasl.SASL = None,
                 client_properties: dict = None,
                 locale: str = 'en_US',
                 channel_max: int = 0,
                 frame_max: int = 0,
                 heartbeat: int = 60):
        if client_properties is None:
            client_properties = DEFAULT_CLIENT_PROPERTIES
        settings = Settings(
            client_properties=client_properties,
            locale=locale, mechanism=auth.mechanism,
            channel_max=channel_max, frame_max=frame_max,
            heartbeat=heartbeat,
        )
        super().__init__(0, settings)

        self.protocol_version = (0, 9, 1)
        self.channels = {}
        self._next_channel_id = 1

        if auth is None:
            auth = sasl.PLAIN('guest', 'guest')
        self.auth = auth

        # The buffer to accumulate received bytes.
        self._inbound_buffer = io.BytesIO()

    def parse_data(self, data: bytes) -> typing.List[Method]:
        """Parse some bytes (that may be received by an I/O transmission)
        into list of AMQP methods. This method also handles buffering,
        so it's intended to directly pass bytes received from elsewhere.
        """
        self._inbound_buffer.write(data)
        frames = Frame[:].parse_stream(self._inbound_buffer)
        if not frames:
            return []
        self._inbound_buffer = io.BytesIO(self._inbound_buffer.read())
        methods = []
        for frame in frames:
            handler = self._handle_frame
            if frame.channel_id != self.channel_id:
                # pylint: disable=protected-access
                handler = self.channels[frame.channel_id]._handle_frame
            methods.extend(handler(frame))
        return methods

    def data_to_send(self) -> bytes:
        data = super().data_to_send()
        for channel in self.channels.values():
            data += channel.data_to_send()
        return data

    def create_channel(self, channel_id: int = None) -> Channel:
        """Create a channel."""
        if channel_id is None:
            channel_id = self._next_channel_id
            self._next_channel_id += 1
        return Channel(channel_id, self.settings)

    def initiate_connection(self):
        """Initiate connection with the server."""
        ProtocolHeader.build_stream({
            'protocol_major': self.protocol_version[0],
            'protocol_minor': self.protocol_version[1],
            'protocol_revision': self.protocol_version[2],
        }, self._outbound_buffer)

    def handshake(self, server_properties, mechanisms, locales):
        """Negotiate parameters received from the server via
        Connection.Start method.
        """
        self.settings.negotiate_handshake(
            server_properties, mechanisms, locales,
        )

    def tune(self, channel_max, frame_max, heartbeat):
        """Negotiate parameters received from the server via
        Connection.Tune method.
        """
        self.settings.negotiate_tune(channel_max, frame_max, heartbeat)

    def secure_step(self, challenge):
        """Generate a response for a challenge received from the server via
        Connection.Secure method.
        """
        response = self.auth.handle_challenge(challenge)
        self.settings.secure_step(challenge, response)
