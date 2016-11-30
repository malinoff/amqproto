from .fsm import Transition as T


class QueueFSM:
    states = [
        'sent_QueueDeclare',
        'sent_QueueBind',
        'sent_QueueUnbind',
        'sent_QueuePurge',
        'sent_QueueDelete',
    ]

    transitions = [
        T(event='send_QueueDeclare',
          source='channel_idle',
          dest='sent_QueueDeclare'),

        T(event='receive_QueueDeclareOk',
          source='sent_QueueDeclare',
          dest='channel_idle'),

        T(event='send_QueueDeclare_nowait',
          source='channel_idle',
          dest='channel_idle'),

        T(event='send_QueueBind',
          source='channel_idle',
          dest='sent_QueueBind'),

        T(event='receive_QueueBindOk',
          source='sent_QueueBind',
          dest='channel_idle'),

        T(event='send_QueueBind_nowait',
          source='channel_idle',
          dest='channel_idle'),

        T(event='send_QueueUnbind',
          source='channel_idle',
          dest='sent_QueueUnbind'),

        T(event='receive_QueueUnbindOk',
          source='sent_QueueUnbind',
          dest='channel_idle'),

        T(event='send_QueuePurge',
          source='channel_idle',
          dest='sent_QueuePurge'),

        T(event='receive_QueuePurgeOk',
          source='sent_QueuePurged',
          dest='channel_idle'),

        T(event='send_QueuePurge_nowait',
          source='channel_idle',
          dest='channel_idle'),

        T(event='send_QueueDelete',
          source='channel_idle',
          dest='sent_QueueDelete'),

        T(event='received_QueueDeleteOk',
          source='sent_QueueDelete',
          dest='channel_idle'),

        T(event='send_QueueDelete_nowait',
          source='channel_idle',
          dest='channel_idle'),
    ]


class Queue:

    def __init__(self, channel, name='', passive=False,
                 durable=False, exclusive=False, auto_delete=True):
        self._channel = channel

        self.name = name
        self.passive = passive
        self.durable = durable
        self.exclusive = exclusive
        self.auto_delete = auto_delete

    def declare(self, no_wait=False, arguments=None, timeout=None):
        fut = self._channel.send_QueueDeclare(
            self.name, self.passive, self.durable,
            self.exclusive, self.auto_delete, no_wait, arguments,
        )
        if fut is not None:
            return fut.result(timeout)

    def bind(self, exchange='', routing_key='', no_wait=False, arguments=None,
             timeout=None):
        fut = self._channel.send_QueueBind(
            self.name, exchange, routing_key, no_wait, arguments,
        )
        if fut is not None:
            return fut.result(timeout)

    def unbind(self, exchange='', routing_key='', no_wait=False,
               arguments=None, timeout=None):
        fut = self._channel.send_QueueUnbind(
            self.name, exchange, routing_key, no_wait, arguments,
        )
        if fut is not None:
            return fut.result(timeout)

    def purge(self, no_wait=False, timeout=None):
        fut = self._channel.send_QueuePurge(self.name, no_wait)
        if fut is not None:
            return fut.result(timeout)

    def delete(self, if_unused=False, if_empty=False, no_wait=False,
               timeout=None):
        fut = self._channel.send_QueueDelete(
            self.name, if_unused, if_empty, no_wait
        )
        if fut is not None:
            return fut.result(timeout)
