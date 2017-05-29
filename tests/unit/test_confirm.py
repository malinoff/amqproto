import io
import concurrent.futures

import pytest

from amqproto import protocol


@pytest.mark.parametrize('no_wait', [False, True])
def test_ConfirmSelect(no_wait, ready_channel):
    fut = ready_channel.confirm_select(no_wait=no_wait)

    method_bytes = io.BytesIO()
    method = protocol.ConfirmSelect(nowait=no_wait)
    method.to_bytestream(method_bytes)
    assert method_bytes.getvalue() in ready_channel.data_to_send()

    if not no_wait:
        method = protocol.ConfirmSelectOK()
        frame = protocol.MethodFrame(ready_channel._channel_id, method)
        ready_channel.handle_frame(frame)
        assert fut.done() and not fut.cancelled()
        ack_fut, nack_fut = fut.result()
    else:
        ack_fut, nack_fut = fut
    assert isinstance(ack_fut, concurrent.futures.Future)
    assert isinstance(nack_fut, concurrent.futures.Future)
    assert ready_channel._next_publish_seq_no == 1


def test_publish_in_confirm_mode(confirm_channel):
    ack_fut = confirm_channel._ack_fut
    nack_fut = confirm_channel._nack_fut

    # First, test BasicAck handling
    body = b'hello, world'
    message = protocol.basic.Message(body)
    confirm_channel.basic_publish(message)

    assert not ack_fut.done()
    assert not nack_fut.done()

    method = protocol.BasicAck(delivery_tag=1, multiple=False)
    frame = protocol.MethodFrame(confirm_channel._channel_id, method)
    confirm_channel.handle_frame(frame)

    assert ack_fut.done() and not ack_fut.cancelled()

    # Second, test BasicNack handling
    confirm_channel.basic_publish(message)
    confirm_channel.basic_publish(message)

    method = protocol.BasicNack(
        delivery_tag=4, multiple=True, requeue=False
    )
    frame = protocol.MethodFrame(confirm_channel._channel_id, method)
    confirm_channel.handle_frame(frame)

    assert nack_fut.done() and not nack_fut.cancelled()
