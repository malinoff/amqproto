"""
amqproto.connection
~~~~~~~~~~~~~~~~~~~

Sans-I/O implementations of AMQP connections.
"""

# Pylint can't handle attrs magic.
# pylint: disable=attribute-defined-outside-init,no-member,assigning-non-slot

import io
import typing

import attr
import construct as c

from . import sasl
from . import replies
from . import methods
from .settings import Settings
from .channel import BaseChannel, Channel
from .frames import Frame, ProtocolHeader


@attr.s()
class Connection(BaseChannel):
    """Sans-I/O implementation of AMQP connection. Maintains per-connection
    state and creates channels.

    :param auth: SASL method to authenticate with the server.
    """

    virtual_host: str = attr.ib(default='/')

    auth: sasl.SASL = attr.ib(
        default=attr.Factory(lambda: sasl.PLAIN('guest', 'guest')),
    )

    properties: dict = attr.ib(default=None, repr=False, cmp=False, hash=False)
    locale: str = attr.ib(default='en_US', repr=False, cmp=False, hash=False)
    channel_max: int = attr.ib(default=0, repr=False, cmp=False, hash=False)
    frame_max: int = attr.ib(default=0, repr=False, cmp=False, hash=False)
    heartbeat: int = attr.ib(default=60, repr=False, cmp=False, hash=False)

    client_settings: Settings = attr.ib(
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

    Channel = Channel

    def __attrs_post_init__(self):
        super().__attrs_post_init__()
        # Handle the simplest case of auth=(username, password).
        if isinstance(self.auth, (list, tuple)):
            # pylint: disable=not-an-iterable
            self.auth = sasl.PLAIN(*self.auth)
        self.channels = {0: self}
        self._next_channel_id = 1

        # The buffer to accumulate received bytes.
        self._inbound_buffer = io.BytesIO()

        self._missed_heartbeats = 0

        self._method_handlers = {
            methods.ConnectionStart: self._handle_connection_start,
            methods.ConnectionSecure: self._handle_connection_secure,
            methods.ConnectionTune: self._handle_connection_tune,
            methods.ConnectionOpenOK: self._handle_connection_open_ok,
            methods.ConnectionCloseOK: self._handle_connection_close_ok,
            methods.ConnectionClose: self._handle_connection_close,
        }

    def _safe_parse_frames(self, data):
        self._inbound_buffer.write(data)
        self._inbound_buffer.seek(0)
        frames = c.Select(
            ProtocolHeader[1],
            Frame[:],
        ).parse_stream(self._inbound_buffer)
        new_buffer = io.BytesIO()
        new_buffer.write(self._inbound_buffer.read())
        self._inbound_buffer = new_buffer
        return frames

    def parse_data(self, data: bytes) -> typing.List[methods.Method]:
        """Parse some bytes (that may be received by an I/O transmission)
        into list of AMQP frames. This method also handles buffering,
        so it's intended to directly pass bytes received from elsewhere.
        """
        frames = self._safe_parse_frames(data)
        if not frames:
            return []
        self._missed_heartbeats = 0
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
        received_methods = []
        for frame in frames:
            # pylint: disable=protected-access
            channel = self.channels[frame.channel_id]
            if frame.frame_type == 'method':
                received_methods.extend(
                    channel._handle_method(frame.payload)
                )
            elif frame.frame_type == 'content_header':
                received_methods.extend(
                    channel._handle_content_header(frame.payload)
                )
            elif frame.frame_type == 'content_body':
                received_methods.extend(
                    channel._handle_content_body(frame.payload)
                )
            elif frame.frame_type == 'heartbeat':
                pass
        return received_methods

    def get_channel(self, channel_id: int = None) -> Channel:
        """Get a channel by channel_id, or create one."""
        if channel_id is None:
            channel_id = self._next_channel_id
            self._next_channel_id += 1
        else:
            channel = self.channels.get(channel_id)
            if channel is not None:
                return channel
        channel = self.channels[channel_id] = self.Channel(channel_id)
        channel.server_settings = self.server_settings
        channel.negotiated_settings = self.negotiated_settings
        return channel

    def initiate_connection(self):
        """Initiate connection with the server."""
        # pylint: disable=unsubscriptable-object
        self.state = 'opening'
        ProtocolHeader.build_stream({
            'payload': {
                'protocol_major': self.protocol_version[0],
                'protocol_minor': self.protocol_version[1],
                'protocol_revision': self.protocol_version[2],
            }
        }, self._outbound_buffer)

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
        Frame.build_stream({
            'frame_type': 'heartbeat',
            'channel_id': self.channel_id,
            'payload': None,
        }, self._outbound_buffer)
