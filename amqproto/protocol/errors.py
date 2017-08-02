"""
amqproto.protocol.errors
~~~~~~~~~~~~~~~~~~~~~~~~

AMQP errors classes.

This file was generated 2017-08-02 from
/codegen/amqp0-9-1.extended.xml.

"""
# Some exceptions may shadow builtins.
# pylint: disable=redefined-builtin

from itertools import chain


class AMQPError(Exception):
    """Base class for all AMQP errors."""

    reply_code = None
    soft = None

    def __init__(self, reply_text, class_id=0, method_id=0):
        super().__init__()
        if isinstance(reply_text, str):
            reply_text = reply_text.encode('utf-8')
        self.reply_text = reply_text
        self.class_id = class_id
        self.method_id = method_id

    def __str__(self):
        return self.reply_text.decode('utf-8')

    def __repr__(self):
        return '<AMQPError: {}, {}, {}>'.format(
            self.reply_code, self.class_id, self.method_id
        )


class SoftError(AMQPError):
    """Soft errors are recoverable which means if such error happens,
    only the channel where the error happened closes, other channels
    can continue to operate.
    """

    soft = True


class HardError(AMQPError):
    """Hard errors are not recoverable which means if such error happens,
    the whole connection must be closed as soon as possible.
    """

    soft = False


class ContentTooLarge(SoftError):
    """The client attempted to transfer content larger than the server could
    accept at the present time. The client may retry at a later time.
    """

    reply_code = 311


class NoConsumers(SoftError):
    """When the exchange cannot deliver to a consumer when the immediate flag
    is set. As a result of pending data on the queue or the absence of any
    consumers of the queue.
    """

    reply_code = 313


class ConnectionForced(HardError):
    """An operator intervened to close the connection for some reason. The
    client may retry at some later date.
    """

    reply_code = 320


class InvalidPath(HardError):
    """The client tried to work with an unknown virtual host.
    """

    reply_code = 402


class AccessRefused(SoftError):
    """The client attempted to work with a server entity to which it has no
    access due to security settings.
    """

    reply_code = 403


class NotFound(SoftError):
    """The client attempted to work with a server entity that does not exist.
    """

    reply_code = 404


class ResourceLocked(SoftError):
    """The client attempted to work with a server entity to which it has no
    access because another client is working with it.
    """

    reply_code = 405


class PreconditionFailed(SoftError):
    """The client requested a method that was not allowed because some
    precondition failed.
    """

    reply_code = 406


class FrameError(HardError):
    """The sender sent a malformed frame that the recipient could not decode.
    This strongly implies a programming error in the sending peer.
    """

    reply_code = 501


class SyntaxError(HardError):
    """The sender sent a frame that contained illegal values for one or more
    fields. This strongly implies a programming error in the sending peer.
    """

    reply_code = 502


class CommandInvalid(HardError):
    """The client sent an invalid sequence of frames, attempting to perform an
    operation that was considered invalid by the server. This usually implies a
    programming error in the client.
    """

    reply_code = 503


class ChannelError(HardError):
    """The client attempted to work with a channel that had not been correctly
    opened. This most likely indicates a fault in the client layer.
    """

    reply_code = 504


class UnexpectedFrame(HardError):
    """The peer sent a frame that was not expected, usually in the context of
    a content header and body. This strongly indicates a fault in the peer's
    content processing.
    """

    reply_code = 505


class ResourceError(HardError):
    """The server could not complete the method because it lacked sufficient
    resources. This may be due to the client creating too many of some type of
    entity.
    """

    reply_code = 506


class NotAllowed(HardError):
    """The client tried to work with some entity in a manner that is
    prohibited by the server, due to security settings or by some other
    criteria.
    """

    reply_code = 530


class NotImplemented(HardError):
    """The client tried to use functionality that is not implemented in the
    server.
    """

    reply_code = 540


class InternalError(HardError):
    """The server could not complete the method because of an internal error.
    The server may require intervention by an operator in order to resume
    normal operations.
    """

    reply_code = 541


ERRORS_BY_CODE = {
    cls.reply_code: cls
    for cls in chain(SoftError.__subclasses__(), HardError.__subclasses__())
}

__all__ = list(locals())