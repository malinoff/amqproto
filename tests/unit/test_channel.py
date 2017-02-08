import io

import pytest

import amqpframe
import amqpframe.methods

import amqproto

from .strategies import draw_method_example


def test_correct_channel_opening(ready_connection):
    channel = ready_connection.get_channel()
    assert channel._channel_id == 1

    channel.send_ChannelOpen()

    payload = amqpframe.methods.ChannelOpenOK()
    frame = amqpframe.MethodFrame(channel._channel_id, payload)

    ready_connection.handle_frame(frame)
    assert channel.alive

    channel2 = ready_connection.get_channel()
    assert channel2._channel_id == 2


def test_incorrect_channel_opening(ready_connection):
    for i in range(ready_connection.properties['channel_max']):
        channel = ready_connection.get_channel()
        assert channel._channel_id == i + 1

    with pytest.raises(amqproto.exceptions.UnrecoverableError):
        ready_connection.get_channel()


def test_correct_ChannelClose_sending(ready_channel):
    method, args = draw_method_example(amqpframe.methods.ChannelClose)

    fut = ready_channel.send_ChannelClose(**args)
    method_bytes = io.BytesIO()
    method.to_bytestream(method_bytes)
    assert method_bytes.getvalue() in ready_channel.data_to_send()

    method = amqpframe.methods.ChannelCloseOK()
    frame = amqpframe.MethodFrame(ready_channel._channel_id, method)
    ready_channel.handle_frame(frame)

    assert fut.done() and not fut.cancelled()
    assert not ready_channel.alive


def test_correct_ChannelClose_handling(ready_channel):
    method, args = draw_method_example(amqpframe.methods.ChannelClose)
    frame = amqpframe.MethodFrame(ready_channel._channel_id, method)

    with pytest.raises(amqproto.exceptions.ChannelClosed) as excinfo:
        ready_channel.handle_frame(frame)

    method_bytes = io.BytesIO()
    method = amqpframe.methods.ChannelCloseOK()
    method.to_bytestream(method_bytes)
    assert method_bytes.getvalue() in ready_channel.data_to_send()

    assert not ready_channel.alive

    exc = excinfo.value
    assert exc.reply_code == args['reply_code']
    assert exc.reply_text == args['reply_text']
    assert exc.class_id == args['class_id']
    assert exc.method_id == args['method_id']


def test_ChannelFlow_sending(ready_channel):
    fut = ready_channel.send_ChannelFlow(active=False)

    method = amqpframe.methods.ChannelFlowOK(active=False)
    frame = amqpframe.MethodFrame(ready_channel._channel_id, method)

    ready_channel.handle_frame(frame)

    assert fut.done() and not fut.cancelled()
    assert not fut.result()
    assert ready_channel.active


def test_ChannelFlow_receiving(ready_channel):
    method = amqpframe.methods.ChannelFlow(active=False)
    frame = amqpframe.MethodFrame(ready_channel._channel_id, method)

    ready_channel.handle_frame(frame)

    method_bytes = io.BytesIO()
    method = amqpframe.methods.ChannelFlowOK(active=False)
    method.to_bytestream(method_bytes)

    assert method_bytes.getvalue() in ready_channel.data_to_send()
    assert not ready_channel.active
