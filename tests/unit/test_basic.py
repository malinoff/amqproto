import io
import concurrent.futures

import pytest

from amqproto import protocol

from .strategies import draw_method_example


def test_BasicQos(ready_channel):
    method, args = draw_method_example(protocol.BasicQos)
    frame = protocol.MethodFrame(ready_channel._channel_id, method)

    fut = ready_channel.basic_qos(**args)
    method_bytes = io.BytesIO()
    method.to_bytestream(method_bytes)
    assert method_bytes.getvalue() in ready_channel.data_to_send()

    method = protocol.BasicQosOK()
    frame = protocol.MethodFrame(ready_channel._channel_id, method)
    ready_channel.handle_frame(frame)

    assert fut.done() and not fut.cancelled()


def test_BasicPublish(ready_channel):
    # We build a relatively large message - to exceed frame_max a bit
    body = b'hello, world' * 100
    message = protocol.BasicMessage(body)
    ready_channel.basic_publish(message)

    data_to_send = ready_channel.data_to_send()

    stream = io.BytesIO()
    method = protocol.BasicPublish(
        exchange='', routing_key='', mandatory=False, immediate=False,
    )
    frame = protocol.MethodFrame(ready_channel._channel_id, method)
    frame.to_bytestream(stream)

    payload = protocol.ContentHeaderPayload(
        class_id=method.method_type[0],
        body_size=message.body_size,
        properties=message.properties,
    )
    frame = protocol.ContentHeaderFrame(ready_channel._channel_id, payload)
    frame.to_bytestream(stream)

    max_payload_size = ready_channel._frame_max - protocol.Frame.METADATA_SIZE

    chunk = body[:max_payload_size]
    payload = protocol.ContentBodyPayload(chunk)
    frame = protocol.ContentBodyFrame(ready_channel._channel_id, payload)
    frame.to_bytestream(stream)

    chunk = body[max_payload_size:]
    payload = protocol.ContentBodyPayload(chunk)
    frame = protocol.ContentBodyFrame(ready_channel._channel_id, payload)
    frame.to_bytestream(stream)

    assert stream.getvalue() in data_to_send


def test_BasicGet_OK(ready_channel):
    fut = ready_channel.basic_get('myqueue')

    delivery_info = {
        'delivery_tag': 125,
        'redelivered': False,
        'exchange': b'myexchange',
        'routing_key': b'myroutingkey',
        'message_count': 10,
    }
    method = protocol.BasicGetOK(**delivery_info)
    frame = protocol.MethodFrame(ready_channel._channel_id, method)
    ready_channel.handle_frame(frame)
    assert fut.done() and not fut.cancelled()

    message_fut = fut.result()

    body = b'hello, world' * 100
    message = protocol.BasicMessage(body)

    payload = protocol.ContentHeaderPayload(
        class_id=method.method_type[0],
        body_size=message.body_size,
        properties=message.properties,
    )
    frame = protocol.ContentHeaderFrame(ready_channel._channel_id, payload)
    ready_channel.handle_frame(frame)
    assert not message_fut.done()

    max_payload_size = ready_channel._frame_max - protocol.Frame.METADATA_SIZE

    chunk = body[:max_payload_size]
    payload = protocol.ContentBodyPayload(chunk)
    frame = protocol.ContentBodyFrame(ready_channel._channel_id, payload)
    ready_channel.handle_frame(frame)
    assert not message_fut.done()

    chunk = body[max_payload_size:]
    payload = protocol.ContentBodyPayload(chunk)
    frame = protocol.ContentBodyFrame(ready_channel._channel_id, payload)
    ready_channel.handle_frame(frame)
    assert message_fut.done() and not message_fut.cancelled()

    received_message = message_fut.result()

    assert received_message.body == message.body
    assert received_message.properties == message.properties

    for key in ('delivery_tag', 'redelivered', 'exchange', 'routing_key',
                'message_count'):
        assert getattr(received_message.delivery_info, key) == delivery_info[key]


def test_BasicGet_Empty(ready_channel):
    fut = ready_channel.basic_get('myqueue')

    method = protocol.BasicGetEmpty()
    frame = protocol.MethodFrame(ready_channel._channel_id, method)
    ready_channel.handle_frame(frame)

    assert fut.done() and not fut.cancelled()
    assert isinstance(fut.result(), protocol.BasicGetEmpty)


@pytest.mark.parametrize('no_wait', [False, True])
def test_BasicConsume(no_wait, ready_channel):
    consumer_tag = b'foo'
    fut = ready_channel.basic_consume(consumer_tag=consumer_tag, no_wait=no_wait)

    if no_wait:
        fut, remote_consumer_tag = fut
    else:
        method = protocol.BasicConsumeOK(consumer_tag=consumer_tag)
        frame = protocol.MethodFrame(ready_channel._channel_id, method)
        ready_channel.handle_frame(frame)

        assert fut.done() and not fut.cancelled()
        fut, remote_consumer_tag = fut.result()
        assert consumer_tag == remote_consumer_tag
        assert isinstance(fut, concurrent.futures.Future)

    delivery_info = {
        'consumer_tag': b'foo',
        'delivery_tag': 125,
        'redelivered': False,
        'exchange': b'myexchange',
        'routing_key': b'myroutingkey',
    }
    method = protocol.BasicDeliver(**delivery_info)
    frame = protocol.MethodFrame(ready_channel._channel_id, method)
    ready_channel.handle_frame(frame)

    body = b'hello, world'
    message = protocol.BasicMessage(body)

    payload = protocol.ContentHeaderPayload(
        class_id=method.method_type[0],
        body_size=message.body_size,
        properties=message.properties,
    )
    frame = protocol.ContentHeaderFrame(ready_channel._channel_id, payload)
    ready_channel.handle_frame(frame)

    payload = protocol.ContentBodyPayload(body)
    frame = protocol.ContentBodyFrame(ready_channel._channel_id, payload)
    ready_channel.handle_frame(frame)

    new_message_fut, msg = fut.result()
    assert msg.body == body
    assert isinstance(new_message_fut, concurrent.futures.Future)

    # Test a couple of edge cases: 0 body size message
    method = protocol.BasicDeliver(**delivery_info)
    frame = protocol.MethodFrame(ready_channel._channel_id, method)
    ready_channel.handle_frame(frame)

    body = b''
    message = protocol.BasicMessage(body)

    payload = protocol.ContentHeaderPayload(
        class_id=method.method_type[0],
        body_size=message.body_size,
        properties=message.properties,
    )
    frame = protocol.ContentHeaderFrame(ready_channel._channel_id, payload)
    ready_channel.handle_frame(frame)

    new_message_fut, msg = new_message_fut.result()
    assert msg.body == body
    assert isinstance(new_message_fut, concurrent.futures.Future)

    # and partial sending
    method = protocol.BasicDeliver(**delivery_info)
    frame = protocol.MethodFrame(ready_channel._channel_id, method)
    ready_channel.handle_frame(frame)

    body = b'hello, world'
    message = protocol.BasicMessage(body)

    payload = protocol.ContentHeaderPayload(
        class_id=method.method_type[0],
        body_size=message.body_size,
        properties=message.properties,
    )
    frame = protocol.ContentHeaderFrame(ready_channel._channel_id, payload)
    ready_channel.handle_frame(frame)

    payload = protocol.ContentBodyPayload(b'hello')
    frame = protocol.ContentBodyFrame(ready_channel._channel_id, payload)
    ready_channel.handle_frame(frame)

    method = protocol.ChannelFlow(active=True)
    frame = protocol.MethodFrame(ready_channel._channel_id, method)
    ready_channel.handle_frame(frame)

    new_message_fut, msg = new_message_fut.result()
    assert msg.body == b'hello'
    assert isinstance(new_message_fut, concurrent.futures.Future)

    # Test that we can cancel the consumer
    fut = ready_channel.basic_cancel(b'foo', no_wait=no_wait)

    if not no_wait:
        method = protocol.BasicCancelOK(consumer_tag=b'foo')
        frame = protocol.MethodFrame(ready_channel._channel_id, method)
        ready_channel.handle_frame(frame)

        assert fut.done() and not fut.cancelled()

    method = protocol.BasicDeliver(**delivery_info)
    frame = protocol.MethodFrame(ready_channel._channel_id, method)
    with pytest.raises(protocol.CommandInvalid):
        ready_channel.handle_frame(frame)
