import io
import concurrent.futures

import pytest

import amqpframe
import amqpframe.basic
import amqpframe.methods


@pytest.mark.parametrize('no_wait', [False, True])
def test_ConfirmSelect(no_wait, ready_channel):
    fut = ready_channel.send_ConfirmSelect(no_wait=no_wait)

    method_bytes = io.BytesIO()
    method = amqpframe.methods.ConfirmSelect(nowait=no_wait)
    method.to_bytestream(method_bytes)
    assert method_bytes.getvalue() in ready_channel.data_to_send()

    if not no_wait:
        method = amqpframe.methods.ConfirmSelectOK()
        frame = amqpframe.MethodFrame(ready_channel._channel_id, method)
        ready_channel.handle_frame(frame)
        assert fut.done() and not fut.cancelled()
        ack_fut, nack_fut = fut.result()
    else:
        ack_fut, nack_fut = fut
    assert isinstance(ack_fut, concurrent.futures.Future)
    assert isinstance(nack_fut, concurrent.futures.Future)
    assert ready_channel._next_publish_seq_no == 1


def test_publish_in_confirm_mode(confirm_channel):
    fut = confirm_channel._confirm_fut

    # First, test BasicAck handling
    body = b'hello, world'
    message = amqpframe.basic.Message(body)
    confirm_channel.send_BasicPublish(message)

    assert not fut.ready()

    method = amqpframe.methods.BasicAck(delivery_tag=1, multiple=False)
    frame = amqpframe.MethodFrame(confirm_channel._channel_id, method)
    confirm_channel.handle_frame(frame)

    assert fut.ready() and not fut.cancelled()
    ack_fut, nack_fut = fut.result()

    # Second, test BasicNack handling
    confirm_channel.send_BasicPublish(message)
    confirm_channel.send_BasicPublish(message)

    method = amqpframe.methods.BasicNack(
        delivery_tag=4, multiple=True, requeue=False
    )
    frame = amqpframe.MethodFrame(confirm_channel._channel_id, method)
    confirm_channel.handle_frame(frame)

    assert confirm_fut.ready() and not confirm_fut.cancelled()
    confirm_fut = confirm_fut.result()
