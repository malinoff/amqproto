"""
amqproto.replies
~~~~~~~~~~~~~~~~

AMQP replies (connection or channel exceptions).
"""

# pylint: disable=redefined-builtin

import attr

from . import methods


class BaseReply(Exception):
    """Base class for all replies, synchronous and asynchronous."""


@attr.s(str=True)
class Reply(BaseReply):
    """Generic synchronous reply."""

    reply_text = attr.ib()
    class_id = attr.ib(default=0)
    method_id = attr.ib(default=0)

    BY_ID = {}

    @classmethod
    def from_close_method(cls, method):
        """Instantiate the appropriate AMQP exception from
        ConnectionClose or ChannelClose methods.
        """
        assert isinstance(
            method, (methods.ConnectionClose, methods.ChannelClose)
        )
        cls = cls.BY_ID[method.reply_code]
        return cls(
            reply_text=method.reply_text,
            class_id=method.reply_class_id,
            method_id=method.reply_method_id,
        )

    def __init_subclass__(cls, soft, reply_code, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.soft = attr.ib(default=soft)
        cls.reply_code = attr.ib(default=reply_code)
        cls.BY_ID[reply_code] = cls


@attr.s(str=True)
class ConnectionAborted(Reply, soft=False, reply_code=0):
    """The server closed the connection abruptly."""


@attr.s(str=True)
class ContentTooLarge(Reply, soft=True, reply_code=311):
    """The client attempted to transfer content larger than the server could
    accept at the present time. The client may retry at a later time.
    """


@attr.s(str=True)
class NoConsumers(Reply, soft=True, reply_code=313):
    """When the exchange cannot deliver to a consumer when the immediate flag
    is set. As a result of pending data on the queue or the absence of any
    consumers of the queue.
    """


@attr.s(str=True)
class ConnectionForced(Reply, soft=False, reply_code=320):
    """An operator intervened to close the connection for some reason. The
    client may retry at some later date.
    """


@attr.s(str=True)
class InvalidPath(Reply, soft=False, reply_code=402):
    """The client tried to work with an unknown virtual host.
    """


@attr.s(str=True)
class AccessRefused(Reply, soft=True, reply_code=403):
    """The client attempted to work with a server entity to which it has no
    access due to security settings.
    """


@attr.s(str=True)
class NotFound(Reply, soft=True, reply_code=404):
    """The client attempted to work with a server entity that does not exist.
    """


@attr.s(str=True)
class ResourceLocked(Reply, soft=True, reply_code=405):
    """The client attempted to work with a server entity to which it has no
    access because another client is working with it.
    """


@attr.s(str=True)
class PreconditionFailed(Reply, soft=True, reply_code=406):
    """The client requested a method that was not allowed because some
    precondition failed.
    """


@attr.s(str=True)
class FrameError(Reply, soft=False, reply_code=501):
    """The sender sent a malformed frame that the recipient could not decode.
    This strongly implies a programming error in the sending peer.
    """


@attr.s(str=True)
class SyntaxError(Reply, soft=False, reply_code=502):
    """The sender sent a frame that contained illegal values for one or more
    fields. This strongly implies a programming error in the sending peer.
    """


@attr.s(str=True)
class CommandInvalid(Reply, soft=False, reply_code=503):
    """The client sent an invalid sequence of frames, attempting to perform an
    operation that was considered invalid by the server. This usually implies a
    programming error in the client.
    """


@attr.s(str=True)
class ChannelError(Reply, soft=False, reply_code=504):
    """The client attempted to work with a channel that had not been correctly
    opened. This most likely indicates a fault in the client layer.
    """


@attr.s(str=True)
class UnexpectedFrame(Reply, soft=False, reply_code=505):
    """The peer sent a frame that was not expected, usually in the context of a
    content header and body.  This strongly indicates a fault in the peer's
    content processing.
    """


@attr.s(str=True)
class ResourceError(Reply, soft=False, reply_code=506):
    """The server could not complete the method because it lacked sufficient
    resources. This may be due to the client creating too many of some type of
    entity.
    """


@attr.s(str=True)
class NotAllowed(Reply, soft=False, reply_code=530):
    """The client tried to work with some entity in a manner that is prohibited
    by the server, due to security settings or by some other criteria.
    """


@attr.s(str=True)
class NotImplemented(Reply, soft=False, reply_code=540):
    """The client tried to use functionality that is not implemented in the
    server.
    """


@attr.s(str=True)
class InternalError(Reply, soft=False, reply_code=541):
    """The server could not complete the method because of an internal error.
    The server may require intervention by an operator in order to resume
    normal operations.
    """


@attr.s(str=True)
class AsynchronousReply(BaseReply):
    """A tiny wrapper to indicate that the reply have been received
    asynchronously and the corresponding traceback is likely useless.

    ``cause`` should be carefully inspected in order to find an actual
    source of the error.
    """

    cause = attr.ib()
