import io

import pytest

import amqpframe
import amqpframe.methods


@pytest.mark.parametrize('no_wait', [False, True])
def test_ConfirmSelect(no_wait, ready_channel):
    fut = ready_channel.send_ConfirmSelect(no_wait=no_wait)

    method_bytes = io.BytesIO()
    method = amqpframe.methods.ConfirmSelect(nowait=no_wait)
    method.to_bytestream(method_bytes)
    assert method_bytes.getvalue() in ready_channel.data_to_send()

    if no_wait:
        assert fut is None
    else:
        method = amqpframe.methods.ConfirmSelectOK()
        frame = amqpframe.MethodFrame(ready_channel._channel_id, method)
        ready_channel.handle_frame(frame)
        assert fut.done() and not fut.cancelled()
