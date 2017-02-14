import io
import concurrent.futures

import pytest

import amqpframe
import amqpframe.basic
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


def test_BasicPublish(ready_channel):
    # We build a relatively large message - to exceed frame_max a bit
    body = b'hello, world' * 100
    message = amqpframe.basic.Message(body)
    ready_channel.send_BasicPublish(message)

    data_to_send = ready_channel.data_to_send()

    stream = io.BytesIO()
    method = amqpframe.methods.BasicPublish(
        exchange='', routing_key='', mandatory=False, immediate=False,
    )
    frame = amqpframe.MethodFrame(ready_channel._channel_id, method)
    frame.to_bytestream(stream)

    payload = amqpframe.ContentHeaderPayload(
        class_id=method.method_type[0],
        body_size=message.body_size,
        properties=message.properties,
    )
    frame = amqpframe.ContentHeaderFrame(ready_channel._channel_id, payload)
    frame.to_bytestream(stream)

    max_payload_size = ready_channel._frame_max - amqpframe.Frame.METADATA_SIZE

    chunk = body[:max_payload_size]
    payload = amqpframe.ContentBodyPayload(chunk)
    frame = amqpframe.ContentBodyFrame(ready_channel._channel_id, payload)
    frame.to_bytestream(stream)

    chunk = body[max_payload_size:]
    payload = amqpframe.ContentBodyPayload(chunk)
    frame = amqpframe.ContentBodyFrame(ready_channel._channel_id, payload)
    frame.to_bytestream(stream)

    assert stream.getvalue() in data_to_send


def test_BasicGet_OK(ready_channel):
    fut = ready_channel.send_BasicGet('myqueue')

    delivery_info = {
        'delivery_tag': 125,
        'redelivered': False,
        'exchange': b'myexchange',
        'routing_key': b'myroutingkey',
        'message_count': 10,
    }
    method = amqpframe.methods.BasicGetOK(**delivery_info)
    frame = amqpframe.MethodFrame(ready_channel._channel_id, method)
    ready_channel.handle_frame(frame)
    assert fut.done() and not fut.cancelled()

    message_fut = fut.result()

    body = b'hello, world' * 100
    message = amqpframe.basic.Message(body)

    payload = amqpframe.ContentHeaderPayload(
        class_id=method.method_type[0],
        body_size=message.body_size,
        properties=message.properties,
    )
    frame = amqpframe.ContentHeaderFrame(ready_channel._channel_id, payload)
    ready_channel.handle_frame(frame)
    assert not message_fut.done()

    max_payload_size = ready_channel._frame_max - amqpframe.Frame.METADATA_SIZE

    chunk = body[:max_payload_size]
    payload = amqpframe.ContentBodyPayload(chunk)
    frame = amqpframe.ContentBodyFrame(ready_channel._channel_id, payload)
    ready_channel.handle_frame(frame)
    assert not message_fut.done()

    chunk = body[max_payload_size:]
    payload = amqpframe.ContentBodyPayload(chunk)
    frame = amqpframe.ContentBodyFrame(ready_channel._channel_id, payload)
    ready_channel.handle_frame(frame)
    assert message_fut.done() and not message_fut.cancelled()

    received_message = message_fut.result()

    assert received_message.body == message.body
    assert received_message.properties == message.properties

    for key in ('delivery_tag', 'redelivered', 'exchange', 'routing_key',
                'message_count'):
        assert received_message.delivery_info[key] == delivery_info[key]


def test_BasicGet_Empty(ready_channel):
    fut = ready_channel.send_BasicGet('myqueue')

    method = amqpframe.methods.BasicGetEmpty()
    frame = amqpframe.MethodFrame(ready_channel._channel_id, method)
    ready_channel.handle_frame(frame)

    assert fut.done() and not fut.cancelled()
    assert fut.result() is None


def test_BasicConsume(ready_channel):
    fut = ready_channel.send_BasicConsume()

    method = amqpframe.methods.BasicConsumeOK(consumer_tag=b'foo')
    frame = amqpframe.MethodFrame(ready_channel._channel_id, method)
    ready_channel.handle_frame(frame)

    assert fut.done() and not fut.cancelled()
    assert isinstance(fut.result(), concurrent.futures.Future)
    fut = fut.result()

    delivery_info = {
        'consumer_tag': b'foo',
        'delivery_tag': 125,
        'redelivered': False,
        'exchange': b'myexchange',
        'routing_key': b'myroutingkey',
    }
    method = amqpframe.methods.BasicDeliver(**delivery_info)
    frame = amqpframe.MethodFrame(ready_channel._channel_id, method)
    ready_channel.handle_frame(frame)

    body = b'hello, world'
    message = amqpframe.basic.Message(body)

    payload = amqpframe.ContentHeaderPayload(
        class_id=method.method_type[0],
        body_size=message.body_size,
        properties=message.properties,
    )
    frame = amqpframe.ContentHeaderFrame(ready_channel._channel_id, payload)
    ready_channel.handle_frame(frame)

    payload = amqpframe.ContentBodyPayload(body)
    frame = amqpframe.ContentBodyFrame(ready_channel._channel_id, payload)
    ready_channel.handle_frame(frame)

    msg, new_message_fut = fut.result()
    assert msg.body == body
    assert isinstance(new_message_fut, concurrent.futures.Future)

    fut = ready_channel.send_BasicCancel(b'foo')

    method = amqpframe.methods.BasicCancelOK(consumer_tag=b'foo')
    frame = amqpframe.MethodFrame(ready_channel._channel_id, method)
    ready_channel.handle_frame(frame)

    assert fut.done() and not fut.cancelled()

    method = amqpframe.methods.BasicDeliver(**delivery_info)
    frame = amqpframe.MethodFrame(ready_channel._channel_id, method)
    with pytest.raises(amqpframe.errors.CommandInvalid):
        ready_channel.handle_frame(frame)
