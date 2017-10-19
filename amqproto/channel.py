"""
amqproto.channel
~~~~~~~~~~~~~~~~

Sans-I/O implementations of AMQP channels.
"""
import io

import attr

from . import replies
from .frames import Frame
from .utils import chunker
from .methods import Method
from .content import Content
from .settings import Settings


class Channel:  # pylint: disable=too-many-instance-attributes
    """Sans-I/O implementation of AMQP channels.
    Maintains per-channel state.

    :param channel_id: channel id, unique within a single connection.
    :param settings: structure holding information about various
        client-side, server-side and negotiated settings.
    """

    def __init__(self, channel_id: int, settings: Settings):
        self.channel_id = channel_id
        self.settings = settings

        # The buffer to accumulate all bytes required to be sent.
        self._outbound_buffer = io.BytesIO()

        # Set of all active consumer tags.
        self._consumers = set()
        # Reference to the last received method waiting for its content.
        self._content_waiter = None
        #: Controls if content flow is enabled on this channel.
        self.flow = True
        #: Controls if this channel holds a transaction.
        self.transaction_active = False
        #: Controls if this channel is in publisher confirms mode.
        self.publisher_confirms_active = False
        self._next_delivery_tag = 1
        # Set of unconfirmed delivery tags.
        self._unconfirmed_set = set()

    def add_consumer(self, consumer_tag: str):
        """Add the consumer tag to the list of active consumers."""
        self._consumers.add(consumer_tag)

    def remove_consumer(self, consumer_tag: str):
        """Remove the consumer tag from the list of active consumers."""
        self._consumers.remove(consumer_tag)

    def publish_needs_confirmation(self):
        """Point that a publish needs confirmations."""
        if self.publisher_confirms_active:
            self._unconfirmed_set.add(self._next_delivery_tag)
            self._next_delivery_tag += 1

    def unconfirmed_delivery_tags(self):
        """Returns a set of unconfirmed delivery tags."""
        return self._unconfirmed_set

    def confirm_delivery_tag(self, delivery_tag: int,
                             multiple: bool = False):
        """Confirm a publication using its delivery tag.
        If ``multiple`` is ``True``, then ``delivery_tag`` is
        treated as "up to".
        """
        if multiple:
            self._unconfirmed_set.difference_update(
                set(range(delivery_tag + 1))
            )
        else:
            self._unconfirmed_set.remove(delivery_tag)

    def capable_of(self, class_id: int, method_id: int,
                   capability: str):
        """Check if the server is capable of the method.
        Can be used to prevent client implementations from sending
        unsupported methods.
        """
        capabilities = self.settings.server['properties']['capabilities']
        if not capabilities.get(capability, False):
            raise replies.NotImplemented(
                'the server does not support {}'.format(capability),
                class_id, method_id,
            )

    def data_to_send(self) -> bytes:
        """Return some data to send. It's intended to pass the data
        directly to a some sort of I/O write method.
        """
        data = self._outbound_buffer.getvalue()
        if data:
            self._outbound_buffer = io.BytesIO()
        return data

    def method_to_send(self, method: Method):
        """Prepare the method for sending (including the content,
        if there is one).

        Note that it's the caller responsibility to check if flow
        is active on this channel, and to delay the I/O if it's
        not active.
        """
        Frame.build_stream({
            'frame_type': 'method',
            'channel_id': self.channel_id,
            'payload': attr.asdict(method),
        }, self._outbound_buffer)
        if method.content is None:
            return
        Frame.build_stream({
            'frame_type': 'content_header',
            'channel_id': self.channel_id,
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
        max_frame_size = self.settings.tune['frame_max'] - 8
        for chunk in chunker(method.content.body, max_frame_size):
            Frame.build_stream({
                'frame_type': 'content_body',
                'channel_id': self.channel_id,
                'payload': chunk,
            }, self._outbound_buffer)

    def _handle_frame(self, frame):
        if frame.frame_type == 'method':
            return self._handle_method(frame)
        elif frame.frame_type == 'content_header':
            return self._handle_content_header(frame)
        elif frame.frame_type == 'content_body':
            return self._handle_content_body(frame)
        return []

    def _handle_method(self, frame):
        method = frame.payload
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

    def _handle_content_header(self, frame):
        content = Content(**frame.payload, delivery_info=self._content_waiter)
        self._content_waiter.content = content
        if content.complete():
            # An empty body
            waiter, self._content_waiter = self._content_waiter, None
            return [waiter]
        return []

    def _handle_content_body(self, frame):
        waiter = self._content_waiter
        waiter.content.body += frame.payload
        if waiter.content.complete():
            waiter, self._content_waiter = self._content_waiter, None
            return [waiter]
        return []
