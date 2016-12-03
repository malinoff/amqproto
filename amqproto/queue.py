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
