"""
amqproto.connection
~~~~~~~~~~~~~~~~~~~

Sans-I/O implementations of AMQP connections.
"""

# pylint doesn't know how to deal with attrs-based classes
# pylint: disable=attribute-defined-outside-init
# pylint: disable=no-member,assigning-non-slot

import io
import typing

import attr
import construct as c

from . import sasl
from . import replies
from .utils import chunker
from .methods import Method
from .content import Content
from .settings import Settings
from .frames import Frame, ProtocolHeader


@attr.s()
class Connection:
    """Sans-I/O implementation of AMQP connection. Maintains per-connection
    state and creates channels.

    :param auth: SASL method to authenticate with the server.
    """

    virtual_host: str = attr.ib(default='/')
    auth: sasl.SASL = attr.ib(
        default=attr.Factory(lambda: sasl.PLAIN('guest', 'guest')),
    )
    client_settings: Settings = attr.ib(
        default=attr.Factory(
            takes_self=True,
            factory=lambda self: Settings(mechanisms=self.auth.mechanism),
        )
    )
    server_settings: Settings = attr.ib(
        default=attr.Factory(lambda: Settings(type='server')),
        init=False,
    )
    negotiated_settings: Settings = attr.ib(
        default=attr.Factory(lambda: Settings(type='negotiated')),
    )

    protocol_version = attr.ib(default=(0, 9, 1), init=False)

    def __attrs_post_init__(self):
        self.channels = {}
        self.channels[0] = self
        self._next_channel_id = 1

        # The buffer to accumulate received bytes.
        self._inbound_buffer = io.BytesIO()
        # The buffer to accumulate all bytes required to be sent.
        self._outbound_buffer = io.BytesIO()

    def data_to_send(self) -> bytes:
        """Return some data to send. It's intended to pass the data
        directly to a some sort of I/O write method.
        """
        data = self._outbound_buffer.getvalue()
        if data:
            # Avoid unnecessary reallocations if there is nothing to send
            self._outbound_buffer = io.BytesIO()
        return data

    def method_to_send(self, channel_id: int, method: Method):
        """Prepare the method for sending (including the content,
        if there is one).

        Note that it's the caller responsibility to check if flow
        is active on the channel, and to delay the I/O if it's
        not active.
        """
        Frame.build_stream({
            'frame_type': 'method',
            'channel_id': channel_id,
            'payload': attr.asdict(method),
        }, self._outbound_buffer)
        if method.content is None:
            return
        Frame.build_stream({
            'frame_type': 'content_header',
            'channel_id': channel_id,
            'payload': {
                'class_id': method.content.class_id,
                'body_size': method.content.body_size,
                'properties': attr.asdict(method.content.properties),
            }
        }, self._outbound_buffer)
        # 8 is the frame metadata size:
        # frame_type is UnsignedByte (1),
        # channel_id is UnsignedShort (1 + 2)
        # payload length is UnsignedLong (3 + 4)
        # frame_end is UnsignedByte (7 + 1)
        max_frame_size = self.negotiated_settings.frame_max - 8
        for chunk in chunker(method.content.body, max_frame_size):
            Frame.build_stream({
                'frame_type': 'content_body',
                'channel_id': channel_id,
                'payload': chunk,
            }, self._outbound_buffer)

    def parse_data(self, data: bytes) -> typing.List[Method]:
        """Parse some bytes (that may be received by an I/O transmission)
        into list of AMQP frames. This method also handles buffering,
        so it's intended to directly pass bytes received from elsewhere.
        """
        self._inbound_buffer.write(data)
        frames = c.Select(
            ProtocolHeader[1],
            Frame[:],
        ).parse_stream(self._inbound_buffer)
        if not frames:
            return []
        if frames[0].frame_type == 'protocol_header':
            frame = frames[0]
            raise replies.ConnectionAborted(
                'AMQP version mismatch, we are {}, server is {}'.format(
                    '.'.join(self.protocol_version),
                    '.'.join((
                        frame.payload.protocol_major,
                        frame.payload.protocol_minor,
                        frame.payload.protocol_revision,
                    )),
                )
            )
        self._inbound_buffer = io.BytesIO(self._inbound_buffer.read())
        methods = []
        for frame in frames:
            extra = []
            if frame.frame_type == 'method':
                extra = self.method_received(frame)
            elif frame.frame_type == 'content_header':
                extra = self.content_header_received(frame)
            elif frame.frame_type == 'content_body':
                extra = self.content_body_received(frame)
            elif frame.frame_type == 'heartbeat':
                pass
            methods.extend(extra)
        return methods

    def method_received(self, frame):
        """Handle the received method."""
        channel_id = frame.channel_id
        method = frame.payload
        method.channel_id = channel_id
        if channel_id == 0:
            return [method]
        channel = self.channels[channel_id]
        return channel.method_received(method)

    def content_header_received(self, frame):
        """Handle the received content header."""
        channel_id = frame.channel_id
        payload = frame.payload
        assert channel_id != 0
        channel = self.channels[channel_id]
        return channel.content_header_received(payload)

    def content_body_received(self, frame):
        """Handle the received content body."""
        channel_id = frame.channel_id
        payload = frame.payload
        assert channel_id != 0
        channel = self.channels[channel_id]
        return channel.content_body_received(payload)

    def heartbeat_received(self, frame):  # pylint: disable=no-self-use
        """Handle the received heartbeat."""
        assert frame.channel_id == 0

    def capable_of(self, method: Method, capability: str):
        """Check if the server is capable of the method.
        Can be used to prevent client implementations from sending
        unsupported methods.
        """
        capabilities = self.settings.server['properties']['capabilities']
        if not capabilities.get(capability, False):
            raise replies.NotImplemented(
                'the server does not support {}'.format(capability),
                method.class_id, method.method_id,
            )

    def get_channel(self, channel_id: int = None) -> 'Channel':
        """Get a channel by channel_id, or create one."""
        if channel_id is None:
            channel_id = self._next_channel_id
            self._next_channel_id += 1
        else:
            channel = self.channels.get(channel_id)
            if channel is not None:
                return channel
        channel = self.channels[channel_id] = Channel(channel_id)
        return channel

    def initiate_connection(self):
        """Initiate connection with the server."""
        # pylint: disable=unsubscriptable-object
        ProtocolHeader.build_stream({
            'protocol_major': self.protocol_version[0],
            'protocol_minor': self.protocol_version[1],
            'protocol_revision': self.protocol_version[2],
        }, self._outbound_buffer)

    def handshake(self, server_properties, mechanisms, locales):
        """Negotiate preferred SASL mechanism and locale."""
        client = self.client_settings
        server = self.server_settings
        server.properties = server_properties
        server.mechanisms = mechanisms
        server.locales = locales

        if client.mechanisms not in server.mechanisms:
            raise replies.ConnectionAborted(
                'unable to agree on auth mechanism'
            )
        if client.locales not in server.locales:
            raise replies.ConnectionAborted(
                'unable to agree on locale'
            )

        self.negotiated_settings.mechanisms = client.mechanisms
        self.negotiated_settings.locales = client.locales

    def tune(self, channel_max, frame_max, heartbeat):
        """Negotiate preferred channel_max, frame_max and heartbeat values."""
        self.server_settings.channel_max = channel_max
        self.server_settings.frame_max = frame_max
        self.server_settings.heartbeat = heartbeat

        for name in ('channel_max', 'frame_max', 'heartbeat'):
            client_value = getattr(self.client_settings, name)
            server_value = getattr(self.server_settings, name)
            negotiate = max if client_value == 0 or server_value == 0 else min
            negotiated_value = negotiate(client_value, server_value)
            setattr(self.negotiated_settings, name, negotiated_value)


@attr.s()
class Channel:
    """Sans-I/O implementation of AMQP channels.
    Maintains per-channel state.

    :param channel_id: channel id, unique within a single connection.
    """

    channel_id: int = attr.ib()
    flow: bool = attr.ib(default=True, init=False)
    transaction_active: bool = attr.ib(default=False, init=False)
    publisher_confirms_active: bool = attr.ib(default=False, init=False)

    def __attrs_post_init__(self):
        # Reference to the last received method waiting for its content.
        self._content_waiter = None
        # Set of all active consumer tags.
        self._consumers = set()
        # Mapping (delivery tag -> content) of unconfirmed content.
        self._unconfirmed_content = {}
        self._next_delivery_tag = 1

    def add_consumer(self, consumer_tag: str):
        """Add the consumer tag to the list of active consumers."""
        self._consumers.add(consumer_tag)

    def remove_consumer(self, consumer_tag: str):
        """Remove the consumer tag from the list of active consumers."""
        self._consumers.remove(consumer_tag)

    def publish_needs_confirmation(self, content):
        """Point that a publish needs confirmations."""
        if self.publisher_confirms_active:
            self._unconfirmed_content[self._next_delivery_tag] = content
            self._next_delivery_tag += 1

    def unconfirmed_content(self):
        """Returns a list of unconfirmed contents."""
        content = list(self._unconfirmed_content.values())
        self._unconfirmed_content = {}
        return content

    def confirm_publication(self, delivery_tag: int, multiple: bool = False):
        """Confirm a publication using its delivery tag.
        If ``multiple`` is ``True``, then ``delivery_tag`` is
        treated as "up to".
        """
        if multiple:
            self._unconfirmed_content = {
                tag: content
                for tag, content in self._unconfirmed_content.items()
                if delivery_tag not in range(1, delivery_tag + 1)
            }
        else:
            self._unconfirmed_content.pop(delivery_tag)

    def method_received(self, method):
        """Handle the received method."""
        # We assume that the server won't send us methods we don't explicitly
        # declare as supported via client properties, so there's no
        # explicit check here.
        to_return = []
        if self._content_waiter is not None:
            # The server decided to stop sending the content
            waiter, self._content_waiter = self._content_waiter, None
            to_return.append(waiter)
        if not method.followed_by_content():
            to_return.append(method)
        else:
            self._content_waiter = method
        return to_return

    def content_header_received(self, payload):
        """Handle the received content header."""
        content = Content(**payload, delivery_info=self._content_waiter)
        self._content_waiter.content = content
        if content.complete():
            # An empty body
            waiter, self._content_waiter = self._content_waiter, None
            return [waiter]
        return []

    def content_body_received(self, payload):
        """Handle the received content body."""
        waiter = self._content_waiter
        waiter.content.body += payload
        if waiter.content.complete():
            waiter, self._content_waiter = self._content_waiter, None
            return [waiter]
        return []
