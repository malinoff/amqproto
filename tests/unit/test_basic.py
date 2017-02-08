import io

import amqpframe
import amqpframe.methods

from .strategies import draw_method_example


def test_BasicQos(ready_channel):
    method, args = draw_method_example(amqpframe.methods.BasicQos)
    frame = amqpframe.MethodFrame(ready_channel._channel_id, method)

    fut = ready_channel.send_BasicQos(**args)
    method_bytes = io.BytesIO()
    method.to_bytestream(method_bytes)
    assert method_bytes.getvalue() in ready_channel.data_to_send()

    method = amqpframe.methods.BasicQosOK()
    frame = amqpframe.MethodFrame(ready_channel._channel_id, method)
    ready_channel.handle_frame(frame)

    assert fut.done() and not fut.cancelled()
