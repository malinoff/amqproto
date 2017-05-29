import io

import pytest
import hypothesis as h
import hypothesis.strategies as hs

from amqproto import protocol

from .strategies import methods


@h.given(hs.data())
@pytest.mark.parametrize('method_name,method_cls,ok_method_cls', [
    ('exchange_declare', protocol.ExchangeDeclare, protocol.ExchangeDeclareOK),
    ('exchange_delete', protocol.ExchangeDelete, protocol.ExchangeDeleteOK),
    ('exchange_bind', protocol.ExchangeBind, protocol.ExchangeBindOK),
    ('exchange_unbind', protocol.ExchangeUnbind, protocol.ExchangeUnbindOK),
])
def test_exchange_related_methods(method_name, method_cls, ok_method_cls, subtest, data):
    @subtest
    def do_test(ready_channel):
        method = data.draw(methods(method_cls))
        ok_method = data.draw(methods(ok_method_cls))

        sender = getattr(ready_channel, method_name)
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
            frame = protocol.MethodFrame(ready_channel._channel_id, ok_method)

            ready_channel.handle_frame(frame)

            assert fut.done() and not fut.cancelled()
