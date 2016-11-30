from .fsm import Transition as T


class ExchangeFSM:
    states = [
        'sent_ExchangeDeclare',
        'sent_ExchangeBind',
        'sent_ExchangeUnbind',
    ]

    transitions = [
        T(event='send_ExchangeDeclare',
          source='channel_idle',
          dest='sent_ExchangeDeclare'),

        T(event='receive_ExchangeDeclareOk',
          source='sent_ExchangeDeclare',
          dest='channel_idle'),

        T(event='receive_ExchangeDeclare_nowait',
          source='channel_idle',
          dest='channel_idle'),

        T(event='send_ExchangeBind',
          source='channel_idle',
          dest='sent_ExchangeBind'),

        T(event='received_ExchangeBindOk',
          source='sent_ExchangeBind',
          dest='channel_idle'),

        T(event='send_ExchangeBind_nowait',
          source='channel_idle',
          dest='channel_idle'),

        T(event='send_ExchangeUnbind',
          source='channel_idle',
          dest='sent_ExchangeUnbind'),

        T(event='receive_ExchangeUnbindOk',
          source='sent_ExchangeUnbind',
          dest='channel_idle'),

        T(event='send_ExchangeUnbind_nowait',
          source='channel_idle',
          dest='channel_idle'),

        T(event='send_ExchangeDelete',
          source='channel_idle',
          dest='sent_ExchangeDelete'),

        T(event='received_ExchangeDeleteOk',
          source='sent_ExchangeDelete',
          dest='channel_idle'),

        T(event='send_ExchangeDelete_nowait',
          source='channel_idle',
          dest='channel_idle'),
    ]


class Exchange:

    def __init__(self, channel, name, type='direct', passive=False,
                 durable=False, auto_delete=True, internal=False):
        self._channel = channel
        self.name = name
        self.type = type
        self.passive = passive
        self.durable = durable
        self.auto_delete = auto_delete
        self.internal = internal

    def declare(self, no_wait=False, arguments=None, timeout=None):
        fut = self._channel.send_ExchangeDeclare(
            self.name, self.type, self.passive, self.durable, self.auto_delete,
            self.internal, no_wait, arguments,
        )
        if fut is not None:
            return fut.result(timeout)

    def delete(self, if_unused=False, no_wait=False, timeout=None):
        fut = self._channel.send_ExchangeDelete(self.name, if_unused, no_wait)
        if fut is not None:
            return fut.result(timeout)

    def bind(self, source='', routing_key='', no_wait=False, arguments=None,
             timeout=None):
        fut = self._channel.send_ExchangeBind(
            self.name, source, routing_key, no_wait, arguments,
        )
        if fut is not None:
            return fut.result(timeout)

    def unbind(self, source='', routing_key='', no_wait=False, arguments=None,
               timeout=None):
        fut = self._channel.send_ExchangeUnbind(
            self.name, source, routing_key, no_wait, arguments,
        )
        if fut is not None:
            return fut.result(timeout)
