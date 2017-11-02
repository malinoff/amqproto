"""
amqproto.methods
~~~~~~~~~~~~~~~~

AMQP methods.
"""
# flake8: noqa=E701
# pylint: disable=missing-docstring

import attr
import construct as c

from . import domains as d
from .utils import make_struct


@attr.s()
class Method:
    """Describes an AMQP method."""
    struct = None

    BY_ID = {}

    def __init_subclass__(cls, class_id, method_id, response_to=None,
                          followed_by_content=False, **kwargs):
        super().__init_subclass__(**kwargs)

        cls.followed_by_content = followed_by_content
        if followed_by_content:
            cls.content = attr.ib()

        cls.struct = make_struct(cls, exclude_attrs={'content'})

        cls.class_id = attr.ib(default=class_id, init=False)
        cls.method_id = attr.ib(default=method_id, init=False)
        cls.channel_id = attr.ib(default=None, init=False)

        cls.responses = []
        cls.response_to = response_to
        if response_to is not None:
            response_to.responses.append(cls)

        cls.BY_ID[(class_id, method_id)] = cls

    def has_response(self):
        """Tells if this method has response that needs to be awaited."""
        return self.responses and not getattr(self, 'no_wait', False)


@attr.s()
class ConnectionStart(Method, class_id=10, method_id=10):
    version_major: d.Octet = attr.ib(default=0, init=False)
    version_minor: d.Octet = attr.ib(default=9, init=False)
    server_properties: d.PeerProperties = attr.ib()
    mechanisms: d.LongStr = attr.ib()
    locales: d.LongStr = attr.ib()


@attr.s()
class ConnectionStartOK(Method, class_id=10, method_id=11,
                        response_to=ConnectionStart):
    client_properties: d.PeerProperties = attr.ib()
    mechanism: d.ShortStr = attr.ib()
    response: c.PascalString(d.UnsignedLong) = attr.ib()
    locale: d.ShortStr = attr.ib()


@attr.s()
class ConnectionSecure(Method, class_id=10, method_id=20):
    challenge: d.LongStr = attr.ib()


@attr.s()
class ConnectionSecureOK(Method, class_id=10, method_id=21,
                         response_to=ConnectionSecure):
    response: d.LongStr = attr.ib()


@attr.s()
class ConnectionTune(Method, class_id=10, method_id=30):
    channel_max: d.Short = attr.ib()
    frame_max: d.Long = attr.ib()
    heartbeat: d.Short = attr.ib()


@attr.s()
class ConnectionTuneOK(Method, class_id=10, method_id=31,
                       response_to=ConnectionTune):
    channel_max: d.Short = attr.ib()
    frame_max: d.Long = attr.ib()
    heartbeat: d.Short = attr.ib()


@attr.s()
class ConnectionOpen(Method, class_id=10, method_id=40):
    virtual_host: d.Path = attr.ib()
    # Deprecated
    capabilities: d.ShortStr = attr.ib(default='', init=False, repr=False)
    # Deprecated
    insist: d.Bit = attr.ib(default=0, init=False, repr=False)


@attr.s()
class ConnectionOpenOK(Method, class_id=10, method_id=41,
                       response_to=ConnectionOpen):
    # Deprecated
    known_hosts: d.Shortstr = attr.ib(default='', init=False, repr=False)


@attr.s()
class ConnectionClose(Method, class_id=10, method_id=50):
    reply_code: d.ReplyCode = attr.ib()
    reply_text: d.ReplyText = attr.ib()
    reply_class_id: d.ClassId = attr.ib()
    reply_method_id: d.MethodId = attr.ib()


@attr.s()
class ConnectionCloseOK(Method, class_id=10, method_id=51,
                        response_to=ConnectionClose):
    pass


@attr.s()
class ChannelOpen(Method, class_id=20, method_id=10):
    # Deprecated
    out_of_band: d.ShortStr = attr.ib(default='', init=False, repr=False)


@attr.s()
class ChannelOpenOK(Method, class_id=20, method_id=11,
                    response_to=ChannelOpen):
    # Deprecated
    channel_id: d.LongStr = attr.ib(default='', init=False, repr=False)


@attr.s()
class ChannelFlow(Method, class_id=20, method_id=20):
    active: d.Bit = attr.ib()


@attr.s()
class ChannelFlowOK(Method, class_id=20, method_id=21,
                    response_to=ChannelFlow):
    active: d.Bit = attr.ib()


@attr.s()
class ChannelClose(Method, class_id=20, method_id=40):
    reply_code: d.ReplyCode = attr.ib()
    reply_text: d.ReplyText = attr.ib()
    reply_class_id: d.ClassId = attr.ib()
    reply_method_id: d.MethodId = attr.ib()


@attr.s()
class ChannelCloseOK(Method, class_id=20, method_id=41,
                     response_to=ChannelClose):
    pass


@attr.s()
class ExchangeDeclare(Method, class_id=40, method_id=10):
    # Deprecated
    ticket: d.Short = attr.ib(default=0, init=False, repr=False)
    exchange: d.ExchangeName = attr.ib()
    type: d.Shortstr = attr.ib()
    passive: d.Bit = attr.ib()
    durable: d.Bit = attr.ib()
    auto_delete: d.Bit = attr.ib()
    internal: d.Bit = attr.ib()
    no_wait: d.NoWait = attr.ib()
    arguments: d.Table = attr.ib()


@attr.s()
class ExchangeDeclareOK(Method, class_id=40, method_id=11,
                        response_to=ExchangeDeclare):
    pass


@attr.s()
class ExchangeDelete(Method, class_id=40, method_id=20):
    # Deprecated
    ticket: d.Short = attr.ib(default=0, init=False, repr=False)
    exchange: d.ExchangeName = attr.ib()
    if_unused: d.Bit = attr.ib()
    no_wait: d.NoWait = attr.ib()


@attr.s()
class ExchangeDeleteOK(Method, class_id=40, method_id=21,
                       response_to=ExchangeDelete):
    pass


@attr.s()
class ExchangeBind(Method, class_id=40, method_id=30):
    # Deprecated
    ticket: d.Short = attr.ib(default=0, init=False, repr=False)
    destination: d.ExchangeName = attr.ib()
    source: d.ExchangeName = attr.ib()
    routing_key: d.Shortstr = attr.ib()
    no_wait: d.NoWait = attr.ib()
    arguments: d.Table = attr.ib()


@attr.s()
class ExchangeBindOK(Method, class_id=40, method_id=31,
                     response_to=ExchangeBind):
    pass


@attr.s()
class ExchangeUnbind(Method, class_id=40, method_id=40):
    # Deprecated
    ticket: d.Short = attr.ib(default=0, init=False, repr=False)
    destination: d.ExchangeName = attr.ib()
    source: d.ExchangeName = attr.ib()
    routing_key: d.Shortstr = attr.ib()
    no_wait: d.NoWait = attr.ib()
    arguments: d.Table = attr.ib()


@attr.s()
class ExchangeUnbindOK(Method, class_id=40, method_id=41,
                       response_to=ExchangeUnbind):
    pass


@attr.s()
class QueueDeclare(Method, class_id=50, method_id=10):
    # Deprecated
    ticket: d.Short = attr.ib(default=0, init=False, repr=False)
    queue: d.QueueName = attr.ib()
    passive: d.Bit = attr.ib()
    durable: d.Bit = attr.ib()
    exclusive: d.Bit = attr.ib()
    auto_delete: d.Bit = attr.ib()
    no_wait: d.NoWait = attr.ib()
    arguments: d.Table = attr.ib()


@attr.s()
class QueueDeclareOK(Method, class_id=50, method_id=11,
                     response_to=QueueDeclare):
    queue: d.QueueName = attr.ib()
    message_count: d.MessageCount = attr.ib()
    consumer_count: d.Long = attr.ib()


@attr.s()
class QueueBind(Method, class_id=50, method_id=20):
    # Deprecated
    ticket: d.Short = attr.ib(default=0, init=False, repr=False)
    queue: d.QueueName = attr.ib()
    exchange: d.ExchangeName = attr.ib()
    routing_key: d.Shortstr = attr.ib()
    no_wait: d.NoWait = attr.ib()
    arguments: d.Table = attr.ib()


@attr.s()
class QueueBindOK(Method, class_id=50, method_id=21, response_to=QueueBind):
    pass


@attr.s()
class QueueUnbind(Method, class_id=50, method_id=50):
    # Deprecated
    ticket: d.Short = attr.ib(default=0, init=False, repr=False)
    queue: d.QueueName = attr.ib()
    exchange: d.ExchangeName = attr.ib()
    routing_key: d.Shortstr = attr.ib()
    arguments: d.Table = attr.ib()


@attr.s()
class QueueUnbindOK(Method, class_id=50, method_id=51,
                    response_to=QueueUnbind):
    pass


@attr.s()
class QueuePurge(Method, class_id=50, method_id=30):
    # Deprecated
    ticket: d.Short = attr.ib(default=0, init=False, repr=False)
    queue: d.QueueName = attr.ib()
    no_wait: d.NoWait = attr.ib()


@attr.s()
class QueuePurgeOK(Method, class_id=50, method_id=31,
                   response_to=QueuePurge):
    message_count: d.MessageCount = attr.ib()


@attr.s()
class QueueDelete(Method, class_id=50, method_id=40):
    # Deprecated
    ticket: d.Short = attr.ib(default=0, init=False, repr=False)
    queue: d.QueueName = attr.ib()
    if_unused: d.Bit = attr.ib()
    if_empty: d.Bit = attr.ib()
    no_wait: d.NoWait = attr.ib()


@attr.s()
class QueueDeleteOK(Method, class_id=50, method_id=41,
                    response_to=QueueDelete):
    message_count: d.MessageCount = attr.ib()


@attr.s()
class BasicQos(Method, class_id=60, method_id=10):
    prefetch_size: d.Long = attr.ib()
    prefetch_count: d.Short = attr.ib()
    global_: d.Bit = attr.ib()


@attr.s()
class BasicQosOK(Method, class_id=60, method_id=11, response_to=BasicQos):
    pass


@attr.s()
class BasicConsume(Method, class_id=60, method_id=20):
    # Deprecated
    ticket: d.Short = attr.ib(default=0, init=False, repr=False)
    queue: d.QueueName = attr.ib()
    consumer_tag: d.ConsumerTag = attr.ib()
    no_local: d.NoLocal = attr.ib()
    no_ack: d.NoAck = attr.ib()
    exclusive: d.Bit = attr.ib()
    no_wait: d.NoWait = attr.ib()
    arguments: d.Table = attr.ib()


@attr.s()
class BasicConsumeOK(Method, class_id=60, method_id=21,
                     response_to=BasicConsume):
    consumer_tag: d.ConsumerTag = attr.ib()


@attr.s()
class BasicCancel(Method, class_id=60, method_id=30):
    consumer_tag: d.ConsumerTag = attr.ib()
    no_wait: d.NoWait = attr.ib()


@attr.s()
class BasicCancelOK(Method, class_id=60, method_id=31,
                    response_to=BasicCancel):
    consumer_tag: d.ConsumerTag = attr.ib()


@attr.s()
class BasicPublish(Method, class_id=60, method_id=40,
                   followed_by_content=True):
    # Deprecated
    ticket: d.Short = attr.ib(default=0, init=False, repr=False)
    exchange: d.ExchangeName = attr.ib()
    routing_key: d.Shortstr = attr.ib()
    mandatory: d.Bit = attr.ib()
    immediate: d.Bit = attr.ib()


@attr.s()
class BasicReturn(Method, class_id=60, method_id=50,
                  followed_by_content=True):
    reply_code: d.ReplyCode = attr.ib()
    reply_text: d.ReplyText = attr.ib()
    exchange: d.ExchangeName = attr.ib()
    routing_key: d.Shortstr = attr.ib()


@attr.s()
class BasicDeliver(Method, class_id=60, method_id=60,
                   followed_by_content=True):
    consumer_tag: d.ConsumerTag = attr.ib()
    delivery_tag: d.DeliveryTag = attr.ib()
    redelivered: d.Redelivered = attr.ib()
    exchange: d.ExchangeName = attr.ib()
    routing_key: d.Shortstr = attr.ib()


@attr.s()
class BasicGet(Method, class_id=60, method_id=70):
    # Deprecated
    ticket: d.Short = attr.ib(default=0, init=False, repr=False)
    queue: d.QueueName = attr.ib()
    no_ack: d.NoAck = attr.ib(default=False)


@attr.s()
class BasicGetOK(Method, class_id=60, method_id=71,
                 response_to=BasicGet, followed_by_content=True):
    delivery_tag: d.DeliveryTag = attr.ib()
    redelivered: d.Redelivered = attr.ib()
    exchange: d.ExchangeName = attr.ib()
    routing_key: d.Shortstr = attr.ib()
    message_count: d.MessageCount = attr.ib()


@attr.s()
class BasicGetEmpty(Method, class_id=60, method_id=72, response_to=BasicGet):
    # Deprecated
    cluster_id: d.ShortStr = attr.ib(default='', init=False, repr=False)


@attr.s()
class BasicAck(Method, class_id=60, method_id=80):
    delivery_tag: d.DeliveryTag = attr.ib()
    multiple: d.Bit = attr.ib(default=False)


@attr.s()
class BasicReject(Method, class_id=60, method_id=90):
    delivery_tag: d.DeliveryTag = attr.ib()
    requeue: d.Bit = attr.ib()


@attr.s()
class BasicRecoverAsync(Method, class_id=60, method_id=100):
    requeue: d.Bit = attr.ib()


@attr.s()
class BasicRecover(Method, class_id=60, method_id=110):
    requeue: d.Bit = attr.ib()


@attr.s()
class BasicRecoverOK(Method, class_id=60, method_id=111,
                     response_to=BasicRecover):
    pass


@attr.s()
class BasicNack(Method, class_id=60, method_id=120):
    delivery_tag: d.DeliveryTag = attr.ib()
    multiple: d.Bit = attr.ib()
    requeue: d.Bit = attr.ib()


@attr.s()
class TxSelect(Method, class_id=90, method_id=10):
    pass


@attr.s()
class TxSelectOK(Method, class_id=90, method_id=11, response_to=TxSelect):
    pass


@attr.s()
class TxCommit(Method, class_id=90, method_id=20):
    pass


@attr.s()
class TxCommitOK(Method, class_id=90, method_id=21, response_to=TxCommit):
    pass


@attr.s()
class TxRollback(Method, class_id=90, method_id=30):
    pass


@attr.s()
class TxRollbackOK(Method, class_id=90, method_id=31, response_to=TxRollback):
    pass


@attr.s()
class ConfirmSelect(Method, class_id=85, method_id=10):
    no_wait: d.Bit = attr.ib(default=False)


@attr.s()
class ConfirmSelectOK(Method, class_id=85, method_id=11,
                      response_to=ConfirmSelect):
    pass
