"""
amqproto.connection
~~~~~~~~~~~~~~~~~~~

Sans-I/O implementations of AMQP connections.
"""

# Pylint can't handle attrs magic.
# pylint: disable=attribute-defined-outside-init,no-member,assigning-non-slot

import typing
from collections import defaultdict

import attr

from . import sasl
from . import replies
from . import methods
from .settings import Settings
from .serialization import Reader
from .channel import BaseChannel, Channel


@attr.s()
class Connection(BaseChannel):
    """Sans-I/O implementation of AMQP connection. Maintains per-connection
    state and creates channels.

    :param auth: SASL method to authenticate with the server.
    """

    virtual_host = attr.ib(default='/')

    auth = attr.ib(
        default=attr.Factory(lambda: sasl.PLAIN('guest', 'guest')),
    )

    properties = attr.ib(default=None)
    locale = attr.ib(default='en_US')
    channel_max = attr.ib(default=0)
    frame_max = attr.ib(default=0)
    heartbeat = attr.ib(default=60)

    client_settings = attr.ib(
        default=attr.Factory(
            takes_self=True,
            factory=lambda self: Settings(
                properties=self.properties,
                mechanisms=self.auth.mechanism,
                locales=self.locale,
                channel_max=self.channel_max,
                frame_max=self.frame_max,
                heartbeat=self.heartbeat,
            ),
        ),
        init=False
    )

    def __attrs_post_init__(self):
        super().__attrs_post_init__()
        # Handle the simplest case of auth=(username, password).
        if isinstance(self.auth, (list, tuple)):
            # pylint: disable=not-an-iterable
            self.auth = sasl.PLAIN(*self.auth)
        self.channels = {0: self}
        self._next_channel_id = 1

        self._inbound_buffer = Reader()

        self._missed_heartbeats = 0

        self._method_handlers = {
            methods.ConnectionStart: self._handle_connection_start,
            methods.ConnectionSecure: self._handle_connection_secure,
            methods.ConnectionTune: self._handle_connection_tune,
            methods.ConnectionOpenOK: self._handle_connection_open_ok,
            methods.ConnectionCloseOK: self._handle_connection_close_ok,
            methods.ConnectionClose: self._handle_connection_close,
        }

    def parse_data(self, data: bytes) -> typing.List[methods.Method]:
        """
        Parse some bytes (that may be received by an I/O transmission)
        into list of AMQP frames. This method also handles buffering,
        so it's intended to directly pass bytes received from elsewhere.
        """
        self._missed_heartbeats = 0

        self._inbound_buffer.feed(data)
        if data[0] == b'A':
            server_version = self._inbound_buffer.read_protocol_header()
            raise replies.ConnectionAborted(
                'AMQP version mismatch, we are {}, server is {}'.format(
                    '.'.join(self.protocol_version),
                    '.'.join(server_version),
                )
            )

        frames = self._inbound_buffer.read_frames()

        received_methods = defaultdict(list)
        for channel_id, frame_type, payload in frames:
            # pylint: disable=protected-access
            channel = self.channels[channel_id]
            if frame_type == 'method':
                received_methods[channel_id].extend(
                    channel._handle_method(payload)
                )
            elif frame_type == 'content_header':
                received_methods[channel_id].extend(
                    channel._handle_content_header(payload)
                )
            elif frame_type == 'content_body':
                received_methods[channel_id].extend(
                    channel._handle_content_body(payload)
                )
            elif frame_type == 'heartbeat':
                pass
        return received_methods

    def _make_channel(self, channel_id):
        return Channel(channel_id)

    def get_channel(self, channel_id: int = None) -> Channel:
        """Get a channel by channel_id, or create one."""
        if channel_id is None:
            channel_id = self._next_channel_id
            self._next_channel_id += 1
        else:
            channel = self.channels.get(channel_id)
            if channel is not None:
                return channel
        channel = self.channels[channel_id] = self._make_channel(channel_id)
        channel.server_settings = self.server_settings
        channel.negotiated_settings = self.negotiated_settings
        return channel

    def initiate_connection(self):
        """Initiate connection with the server."""
        # pylint: disable=unsubscriptable-object
        self.state = 'opening'
        self._outbound_buffer.write_protocol_header(*self.protocol_version)

    def _handle_connection_start(self, method):
        self.server_settings.properties = method.server_properties
        self.server_settings.mechanisms = method.mechanisms
        self.server_settings.locales = method.locales

        if self.client_settings.mechanisms not in method.mechanisms:
            raise replies.ConnectionAborted(
                'unable to agree on auth mechanism'
            )
        if self.client_settings.locales not in method.locales:
            raise replies.ConnectionAborted(
                'unable to agree on locale'
            )

        self.negotiated_settings.mechanisms = self.client_settings.mechanisms
        self.negotiated_settings.locales = self.client_settings.locales

        method = methods.ConnectionStartOK(
            self.client_settings.properties,
            self.negotiated_settings.mechanisms,
            self.auth.to_bytes(),
            self.negotiated_settings.locales,
        )
        return self._prepare_for_sending(method)

    def _handle_connection_secure(self, method):
        response = self.auth.handle_challenge(method.challenge)
        method = methods.ConnectionSecureOK(response)
        return self._prepare_for_sending(method)

    def _handle_connection_tune(self, method):
        self.server_settings.channel_max = method.channel_max
        self.server_settings.frame_max = method.frame_max
        self.server_settings.heartbeat = method.heartbeat

        for name in ('channel_max', 'frame_max', 'heartbeat'):
            client_value = getattr(self.client_settings, name)
            server_value = getattr(self.server_settings, name)
            negotiate = max if client_value == 0 or server_value == 0 else min
            negotiated_value = negotiate(client_value, server_value)
            setattr(self.negotiated_settings, name, negotiated_value)

        method = methods.ConnectionTuneOK(
            self.negotiated_settings.channel_max,
            self.negotiated_settings.frame_max,
            self.negotiated_settings.heartbeat,
        )
        return self._prepare_for_sending(method)

    def _connection_open(self):
        method = methods.ConnectionOpen(self.virtual_host)
        return self._prepare_for_sending(method)

    def _handle_connection_open_ok(self, method):
        # pylint: disable=unused-argument
        self.state = 'open'

    def _connection_close(self, reply_code, reply_text, class_id, method_id):
        method = methods.ConnectionClose(
            reply_code, reply_text, class_id, method_id,
        )
        self.state = 'closing'
        return self._prepare_for_sending(method)

    def _handle_connection_close_ok(self, method):
        # pylint: disable=unused-argument
        self.state = 'closed'

    def _handle_connection_close(self, method):
        method = methods.ConnectionCloseOK()
        self.state = 'closing'
        return self._prepare_for_sending(method)

    def _send_heartbeat(self):
        if self._missed_heartbeats >= 2:
            timeout = self.negotiated_settings.heartbeat
            raise replies.ConnectionForced(
                f'missed heartbeats from server, timeout: {timeout}s'
            )
        self._outbound_buffer.write_frame_heartbeat(self.channel_id)
