import io

import pytest
import hypothesis as h
import hypothesis.strategies as hs

import amqpframe
import amqpframe.methods

from .strategies import methods


@h.given(hs.data())
@pytest.mark.parametrize('method_cls,ok_method_cls', [
    (amqpframe.methods.ExchangeDeclare, amqpframe.methods.ExchangeDeclareOK),
    (amqpframe.methods.ExchangeDelete, amqpframe.methods.ExchangeDeleteOK),
    (amqpframe.methods.ExchangeBind, amqpframe.methods.ExchangeBindOK),
    (amqpframe.methods.ExchangeUnbind, amqpframe.methods.ExchangeUnbindOK),
])
def test_exchange_related_methods(method_cls, ok_method_cls, subtest, data):
    @subtest
    def do_test(ready_channel):
        method = data.draw(methods(method_cls))
        ok_method = data.draw(methods(ok_method_cls))

        sender = getattr(ready_channel, 'send_' + method.__class__.__name__)
        args = {name: getattr(method, name)
                for name, _ in method.field_info
                if not name.startswith('reserved')}

        fut = sender(**args)
        if method.no_wait:
            assert fut is None

            method_bytes = io.BytesIO()
            method.to_bytestream(method_bytes)
            assert method_bytes.getvalue() in ready_channel.data_to_send()
        else:
            frame = amqpframe.MethodFrame(ready_channel._channel_id, ok_method)

            ready_channel.handle_frame(frame)

            assert fut.done() and not fut.cancelled()
