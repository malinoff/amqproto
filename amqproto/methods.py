"""
amqproto.methods
~~~~~~~~~~~~~~~~

AMQP methods.
"""

import attr


@attr.s(slots=True)
class Method:
    """Describes an AMQP method."""

    BY_ID = {}

    @classmethod
    def register(cls, spec, class_id, method_id,
                 response_to=None, closing=False):
        def decorator(method):
            method.spec = spec
            method.closing = closing
            fields = [field.name for field in attr.fields(method)]
            method.followed_by_content = 'content' in fields
            method._field_names = fields

            method.responses = set()
            method.response_to = response_to
            if response_to is not None:
                response_to.responses.add(method)

            method.class_id = class_id
            method.method_id = method_id

            cls.BY_ID[(class_id, method_id)] = method
            return method
        return decorator

    def has_response(self):
        """Tells if this method has response that needs to be awaited."""
        return self.responses and not getattr(self, 'no_wait', False)

    @classmethod
    def load(cls, stream):
        if cls is Method:
            class_id, method_id = load('HH', stream)
            return cls.BY_ID[(class_id, method_id)].load(stream)
        return cls(*load(cls.spec, stream))

    def dump(self):
        fields = [getattr(self, field) for field in self._field_names
                  if field != 'content']
        return dump('HH' + self.spec, self.class_id, self.method_id, *fields)


@Method.register(spec='BBTSS', class_id=10, method_id=10)
@attr.s(slots=True)
class ConnectionStart(Method):
    version_major = attr.ib(default=0)
    version_minor = attr.ib(default=9)
    server_properties = attr.ib(default=attr.Factory(dict))
    mechanisms = attr.ib(default=attr.Factory(lambda: ['PLAIN']))
    locales = attr.ib(default=attr.Factory(lambda: ['EN_US']))


@Method.register(spec='TsSs', class_id=10, method_id=11,
                 response_to=ConnectionStart)
@attr.s(slots=True)
class ConnectionStartOK(Method):
    client_properties = attr.ib()
    mechanism = attr.ib()
    response = attr.ib(repr=False)  # do not leak passwords via __repr__
    locale = attr.ib()


@Method.register(spec='S', class_id=10, method_id=20)
@attr.s(slots=True)
class ConnectionSecure(Method):
    challenge = attr.ib()


@Method.register(spec='S', class_id=10, method_id=21,
                 response_to=ConnectionSecure)
@attr.s(slots=True)
class ConnectionSecureOK(Method):
    response = attr.ib()


@Method.register(spec='HLH', class_id=10, method_id=30)
@attr.s(slots=True)
class ConnectionTune(Method):
    channel_max = attr.ib()
    frame_max = attr.ib()
    heartbeat = attr.ib()


@Method.register(spec='HLH', class_id=10, method_id=31,
                 response_to=ConnectionTune)
@attr.s(slots=True)
class ConnectionTuneOK(Method):
    channel_max = attr.ib()
    frame_max = attr.ib()
    heartbeat = attr.ib()


@Method.register(spec='ss?', class_id=10, method_id=40)
@attr.s(slots=True)
class ConnectionOpen(Method):
    virtual_host = attr.ib()
    # Deprecated
    capabilities = attr.ib(default='', repr=False)
    # Deprecated
    insist = attr.ib(default=False, repr=False)


@Method.register(spec='s', class_id=10, method_id=41,
                 response_to=ConnectionOpen)
@attr.s(slots=True)
class ConnectionOpenOK(Method):
    # Deprecated
    known_hosts = attr.ib(default='', repr=False)


@Method.register(spec='HsHH', class_id=10, method_id=50, closing=True)
@attr.s(slots=True)
class ConnectionClose(Method):
    reply_code = attr.ib()
    reply_text = attr.ib()
    reply_class_id = attr.ib()
    reply_method_id = attr.ib()


@Method.register(spec='', class_id=10, method_id=51,
                 response_to=ConnectionClose, closing=True)
@attr.s(slots=True)
class ConnectionCloseOK(Method):
    pass


@Method.register(spec='s', class_id=20, method_id=10)
@attr.s(slots=True)
class ChannelOpen(Method):
    # Deprecated
    out_of_band = attr.ib(default='', repr=False)


@Method.register(spec='L', class_id=20, method_id=11,
                 response_to=ChannelOpen)
@attr.s(slots=True)
class ChannelOpenOK(Method):
    # Deprecated
    channel_id = attr.ib(default='', repr=False)


@Method.register(spec='?', class_id=20, method_id=20)
@attr.s(slots=True)
class ChannelFlow(Method):
    active = attr.ib()


@Method.register(spec='?', class_id=20, method_id=21,
                 response_to=ChannelFlow)
@attr.s(slots=True)
class ChannelFlowOK(Method):
    active = attr.ib()


@Method.register(spec='HsHH', class_id=20, method_id=40, closing=True)
@attr.s(slots=True)
class ChannelClose(Method):
    reply_code = attr.ib()
    reply_text = attr.ib()
    reply_class_id = attr.ib()
    reply_method_id = attr.ib()


@Method.register(spec='', class_id=20, method_id=41,
                 response_to=ChannelClose, closing=True)
@attr.s(slots=True)
class ChannelCloseOK(Method):
    pass


@Method.register(spec='Hss?????T', class_id=40, method_id=10)
@attr.s(slots=True)
class ExchangeDeclare(Method):
    # Deprecated
    ticket = attr.ib(repr=False)
    exchange = attr.ib()
    type = attr.ib()
    passive = attr.ib()
    durable = attr.ib()
    auto_delete = attr.ib()
    internal = attr.ib()
    no_wait = attr.ib()
    arguments = attr.ib()


@Method.register(spec='', class_id=40, method_id=11,
                 response_to=ExchangeDeclare)
@attr.s(slots=True)
class ExchangeDeclareOK(Method):
    pass


@Method.register(spec='Hs??', class_id=40, method_id=20)
@attr.s(slots=True)
class ExchangeDelete(Method):
    # Deprecated
    ticket = attr.ib(repr=False)
    exchange = attr.ib()
    if_unused = attr.ib()  # noqa
    no_wait = attr.ib()


@Method.register(spec='', class_id=40, method_id=21,
                 response_to=ExchangeDelete)
@attr.s(slots=True)
class ExchangeDeleteOK(Method):
    pass


@Method.register(spec='Hsss?T', class_id=40, method_id=30)
@attr.s(slots=True)
class ExchangeBind(Method):
    # Deprecated
    ticket = attr.ib(repr=False)
    destination = attr.ib()
    source = attr.ib()
    routing_key = attr.ib()
    no_wait = attr.ib()
    arguments = attr.ib()


@Method.register(spec='', class_id=40, method_id=31,
                 response_to=ExchangeBind)
@attr.s(slots=True)
class ExchangeBindOK(Method):
    pass


@Method.register(spec='Hsss?T', class_id=40, method_id=40)
@attr.s(slots=True)
class ExchangeUnbind(Method):
    # Deprecated
    ticket = attr.ib(repr=False)
    destination = attr.ib()
    source = attr.ib()
    routing_key = attr.ib()
    no_wait = attr.ib()
    arguments = attr.ib()


@Method.register(spec='', class_id=40, method_id=41,
                 response_to=ExchangeUnbind)
@attr.s(slots=True)
class ExchangeUnbindOK(Method):
    pass


@Method.register(spec='Hs?????T', class_id=50, method_id=10)
@attr.s(slots=True)
class QueueDeclare(Method):
    # Deprecated
    ticket = attr.ib(repr=False)
    queue = attr.ib()
    passive = attr.ib()
    durable = attr.ib()
    exclusive = attr.ib()
    auto_delete = attr.ib()
    no_wait = attr.ib()
    arguments = attr.ib()


@Method.register(spec='sLL', class_id=50, method_id=11,
                 response_to=QueueDeclare)
@attr.s(slots=True)
class QueueDeclareOK(Method):
    queue = attr.ib()
    message_count = attr.ib()
    consumer_count = attr.ib()


@Method.register(spec='Hsss?T', class_id=50, method_id=20)
@attr.s(slots=True)
class QueueBind(Method):
    # Deprecated
    ticket = attr.ib(repr=False)
    queue = attr.ib()
    exchange = attr.ib()
    routing_key = attr.ib()
    no_wait = attr.ib()
    arguments = attr.ib()


@Method.register(spec='', class_id=50, method_id=21,
                 response_to=QueueBind)
@attr.s(slots=True)
class QueueBindOK(Method):
    pass


@Method.register(spec='HsssT', class_id=50, method_id=50)
@attr.s(slots=True)
class QueueUnbind(Method):
    # Deprecated
    ticket = attr.ib(repr=False)
    queue = attr.ib()
    exchange = attr.ib()
    routing_key = attr.ib()
    arguments = attr.ib()


@Method.register(spec='', class_id=50, method_id=51,
                 response_to=QueueUnbind)
@attr.s(slots=True)
class QueueUnbindOK(Method):
    pass


@Method.register(spec='Hs?', class_id=50, method_id=30)
@attr.s(slots=True)
class QueuePurge(Method):
    # Deprecated
    ticket = attr.ib(repr=False)
    queue = attr.ib()
    no_wait = attr.ib()


@Method.register(spec='L', class_id=50, method_id=31,
                 response_to=QueuePurge)
@attr.s(slots=True)
class QueuePurgeOK(Method):
    message_count = attr.ib()


@Method.register(spec='Hs???', class_id=50, method_id=40)
@attr.s(slots=True)
class QueueDelete(Method):
    # Deprecated
    ticket = attr.ib(repr=False)
    queue = attr.ib()
    if_unused = attr.ib()  # noqa
    if_empty = attr.ib()  # noqa
    no_wait = attr.ib()


@Method.register(spec='L', class_id=50, method_id=41,
                 response_to=QueueDelete)
@attr.s(slots=True)
class QueueDeleteOK(Method):
    message_count = attr.ib()


@Method.register(spec='LH?', class_id=60, method_id=10)
@attr.s(slots=True)
class BasicQos(Method):
    prefetch_size = attr.ib()
    prefetch_count = attr.ib()
    global_ = attr.ib()


@Method.register(spec='', class_id=60, method_id=11,
                 response_to=BasicQos)
@attr.s(slots=True)
class BasicQosOK(Method):
    pass


@Method.register(spec='Hss????T', class_id=60, method_id=20)
@attr.s(slots=True)
class BasicConsume(Method):
    # Deprecated
    ticket = attr.ib(repr=False)
    queue = attr.ib()
    consumer_tag = attr.ib()
    no_local = attr.ib()
    no_ack = attr.ib()
    exclusive = attr.ib()
    no_wait = attr.ib()
    arguments = attr.ib()


@Method.register(spec='s', class_id=60, method_id=21,
                 response_to=BasicConsume)
@attr.s(slots=True)
class BasicConsumeOK(Method):
    consumer_tag = attr.ib()


@Method.register(spec='s?', class_id=60, method_id=30)
@attr.s(slots=True)
class BasicCancel(Method):
    consumer_tag = attr.ib()
    no_wait = attr.ib()


@Method.register(spec='s', class_id=60, method_id=31,
                 response_to=BasicCancel)
@attr.s(slots=True)
class BasicCancelOK(Method):
    consumer_tag = attr.ib()


@Method.register(spec='Hss??', class_id=60, method_id=40)
@attr.s(slots=True)
class BasicPublish(Method):
    # Deprecated
    ticket = attr.ib(repr=False)
    exchange = attr.ib()
    routing_key = attr.ib()
    mandatory = attr.ib()
    immediate = attr.ib()

    content = attr.ib(default=None)


@Method.register(spec='Hsss', class_id=60, method_id=50)
@attr.s(slots=True)
class BasicReturn(Method):
    reply_code = attr.ib()
    reply_text = attr.ib()
    exchange = attr.ib()
    routing_key = attr.ib()

    content = attr.ib(default=None)


@Method.register(spec='sQ?ss', class_id=60, method_id=60)
@attr.s(slots=True)
class BasicDeliver(Method):
    consumer_tag = attr.ib()
    delivery_tag = attr.ib()
    redelivered = attr.ib()
    exchange = attr.ib()
    routing_key = attr.ib()

    content = attr.ib(default=None)


@Method.register(spec='Hs?', class_id=60, method_id=70)
@attr.s(slots=True)
class BasicGet(Method):
    # Deprecated
    ticket = attr.ib(repr=False)
    queue = attr.ib()
    no_ack = attr.ib()


@Method.register(spec='Q?ssL', class_id=60, method_id=71, response_to=BasicGet)
@attr.s(slots=True)
class BasicGetOK(Method):
    delivery_tag = attr.ib()
    redelivered = attr.ib()
    exchange = attr.ib()
    routing_key = attr.ib()
    message_count = attr.ib()

    content = attr.ib(default=None)


@Method.register(spec='s', class_id=60, method_id=72,
                 response_to=BasicGet)
@attr.s(slots=True)
class BasicGetEmpty(Method):
    # Deprecated
    cluster_id = attr.ib(default='', repr=False)


@Method.register(spec='Q?', class_id=60, method_id=80)
@attr.s(slots=True)
class BasicAck(Method):
    delivery_tag = attr.ib()
    multiple = attr.ib()


@Method.register(spec='Q?', class_id=60, method_id=90)
@attr.s(slots=True)
class BasicReject(Method):
    delivery_tag = attr.ib()
    requeue = attr.ib()


@Method.register(spec='?', class_id=60, method_id=100)
@attr.s(slots=True)
class BasicRecoverAsync(Method):
    requeue = attr.ib()


@Method.register(spec='?', class_id=60, method_id=110)
@attr.s(slots=True)
class BasicRecover(Method):
    requeue = attr.ib()


@Method.register(spec='', class_id=60, method_id=111,
                 response_to=BasicRecover)
@attr.s(slots=True)
class BasicRecoverOK(Method):
    pass


@Method.register(spec='Q??', class_id=60, method_id=120)
@attr.s(slots=True)
class BasicNack(Method):
    delivery_tag = attr.ib()
    multiple = attr.ib()
    requeue = attr.ib()


@Method.register(spec='', class_id=90, method_id=10)
@attr.s(slots=True)
class TxSelect(Method):
    pass


@Method.register(spec='', class_id=90, method_id=11,
                 response_to=TxSelect)
@attr.s(slots=True)
class TxSelectOK(Method):
    pass


@Method.register(spec='', class_id=90, method_id=20)
@attr.s(slots=True)
class TxCommit(Method):
    pass


@Method.register(spec='', class_id=90, method_id=21,
                 response_to=TxCommit)
@attr.s(slots=True)
class TxCommitOK(Method):
    pass


@Method.register(spec='', class_id=90, method_id=30)
@attr.s(slots=True)
class TxRollback(Method):
    pass


@Method.register(spec='', class_id=90, method_id=31,
                 response_to=TxRollback)
@attr.s(slots=True)
class TxRollbackOK(Method):
    pass


@Method.register(spec='?', class_id=85, method_id=10)
@attr.s(slots=True)
class ConfirmSelect(Method):
    no_wait = attr.ib(default=False)


@Method.register(spec='', class_id=85, method_id=11,
                      response_to=ConfirmSelect)
@attr.s(slots=True)
class ConfirmSelectOK(Method):
    pass


from .serialization import load, dump
