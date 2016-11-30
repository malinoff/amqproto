from .fsm import Transition as T


class BasicFSM:
    states = [
        'sent_BasicQos',
        'sent_BasicConsume',
        'sent_BasicCancel',
        'sent_BasicGet',
        'sent_BasicRecover',
    ]

    transitions = [
        T(event='send_BasicQos',
          source='channel_active',
          dest='sent_BasicQos'),

        T(event='receive_BasicQosOk',
          source='sent_BasicQos',
          dest='channel_active'),

        T(event='send_BasicConsume',
          source='channel_active',
          dest='sent_BasicConsume'),

        T(event='receive_BasicConsumeOk',
          source='sent_BasicConsume',
          dest='channel_active'),

        T(event='send_BasicConsume_nowait',
          source='channel_active',
          dest='channel_active'),

        T(event='send_BasicCancel',
          source='channel_active',
          dest='sent_BasicCancel'),

        T(event='receive_BasicCancelOk',
          source='sent_BasicCancel',
          dest='channel_active'),

        T(event='send_BasicCancel_nowait',
          source='channel_active',
          dest='channel_active'),

        T(event='sent_BasicPublish',
          source='channel_active',
          dest='channel_active'),

        T(event='received_BasicReturn',
          source='channel_active',
          dest='channel_active'),

        T(event='received_BasicDeliver',
          source='channel_active',
          dest='channel_active'),

        T(event='send_BasicGet',
          source='channel_active',
          dest='sent_BasicGet'),

        T(event='received_BasicGetOk',
          source='sent_BasicGet',
          dest='channel_active'),

        T(event='received_BasicGetEmpty',
          source='sent_BasicGet',
          dest='channel_active'),

        T(event='send_BasicAck',
          source='channel_active',
          dest='channel_active'),

        T(event='receive_BasicAck',
          source='channel_active',
          dest='channel_active'),

        T(event='send_BasicReject',
          source='channel_active',
          dest='channel_active'),

        T(event='send_BasicRecoverAsync',
          source='channel_active',
          dest='channel_active'),

        T(event='send_BasicRecover',
          source='channel_active',
          dest='sent_BasicRecover'),

        T(event='receive_BasicRecoverOk',
          source='sent_BasicRecover',
          dest='channel_active'),

        T(event='send_BasicNack',
          source='channel_active',
          dest='channel_active'),

        T(event='receive_BasicNack',
          source='channel_active',
          dest='channel_active'),
    ]


class Basic:

    def __init__(self, channel):
        self._channel = channel

    def qos(self, prefetch_size, prefetch_count, global_, timeout=None):
        fut = self._channel.send_BasicQos(
            prefetch_size, prefetch_count, global_,
        )
        return fut.result(timeout)

    def consume(self, on_message_received, queue='', consumer_tag='',
                no_local=False, no_ack=False, exclusive=False, no_wait=False,
                arguments=None, timeout=None):
        fut = self._channel.send_BasicConsume(
            on_message_received, queue, consumer_tag, no_local, no_ack,
            exclusive, no_wait, arguments,
        )
        if fut is not None:
            return fut.result(timeout)

    def cancel(self, consumer_tag, no_wait, timeout=None):
        fut = self._channel.send_BasicCances(consumer_tag, no_wait)
        if fut is not None:
            return fut.result(timeout)

    def publish(self, message, exchange='', routing_key='', mandatory=False,
                immediate=False):
        self._channel.send_BasicPublish(
            message, exchange, routing_key, mandatory, immediate,
        )

    def get(self, queue='', no_ack=False, timeout=None):
        fut = self._channel.send_BasicGet(queue, no_ack)
        message_fut = fut.result(timeout)
        return message_fut.result(timeout)

    def ack(self, delivery_tag='', multiple=False):
        self._channel.send_BasicAck(delivery_tag, multiple)

    def reject(self, delivery_tag='', requeue=False):
        self._channel.send_BasicReject(delivery_tag, requeue)

    def recover(self, requeue=False, no_wait=False, timeout=None):
        if no_wait:
            sender = self._channel.send_BasicRecoverAsync
        else:
            sender = self._channel.send_BasicRecover
        fut = sender(requeue)
        if fut is not None:
            return fut.result(timeout)

    def nack(self, delivery_tag='', multiple=False, requeue=False):
        self._channel.send_BasicNack(delivery_tag, multiple, requeue)
