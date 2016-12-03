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
