import io
import pytest

from amqproto import protocol

from amqproto.connection import Connection


@pytest.fixture
def ready_connection():
    conn = Connection(heartbeat=1)
    conn.initiate_connection()

    conn._heartbeater.update_received_time()

    payload = protocol.ConnectionStart(
        version_major=0,
        version_minor=9,
        server_properties={'foo': 'bar'},
        mechanisms=b'PLAIN AMQPLAIN',
        locales=b'en_US ru_RU',
    )
    frame = protocol.MethodFrame(0, payload)
    conn.handle_frame(frame)

    conn._heartbeater.update_received_time()

    channel_max = 2
    frame_max = 1000
    heartbeat = 1
    payload = protocol.ConnectionTune(
        channel_max=channel_max,
        frame_max=frame_max,
        heartbeat=heartbeat,
    )
    frame = protocol.MethodFrame(0, payload)
    conn.handle_frame(frame)

    conn._heartbeater.update_received_time()

    payload = protocol.ConnectionOpenOK()
    frame = protocol.MethodFrame(0, payload)
    conn.handle_frame(frame)

    conn.data_to_send()
    return conn


@pytest.fixture
def ready_channel(ready_connection):
    channel = ready_connection.get_channel()
    channel.open()

    payload = protocol.ChannelOpenOK()
    frame = protocol.MethodFrame(channel._channel_id, payload)

    ready_connection.handle_frame(frame)
    return channel


@pytest.fixture
def tx_channel(ready_channel):
    fut = ready_channel.tx_select()

    method_bytes = io.BytesIO()
    method = protocol.TxSelect()
    method.to_bytestream(method_bytes)
    ready_channel.data_to_send()

    method = protocol.TxSelectOK()
    frame = protocol.MethodFrame(ready_channel._channel_id, method)
    ready_channel.handle_frame(frame)
    return ready_channel


@pytest.fixture
def confirm_channel(ready_channel):
    fut = ready_channel.confirm_select()

    method_bytes = io.BytesIO()
    method = protocol.ConfirmSelect(nowait=False)
    method.to_bytestream(method_bytes)
    ready_channel.data_to_send()

    method = protocol.ConfirmSelectOK()
    frame = protocol.MethodFrame(ready_channel._channel_id, method)
    ready_channel.handle_frame(frame)
    return ready_channel


# Taken from https://github.com/untitaker/pytest-subtesthack
# Reasoning: https://github.com/pytest-dev/pytest/issues/916
import pytest
from _pytest.python import Function


@pytest.fixture
def subtest(request):
    parent_test = request.node

    def inner(func):
        item = Function(
            name=request.function.__name__ + '[]',
            parent=parent_test.parent,
            callobj=func,
        )
        nextitem = parent_test  # prevents pytest from tearing down module fixtures

        item.ihook.pytest_runtest_setup(item=item)
        item.ihook.pytest_runtest_call(item=item)
        item.ihook.pytest_runtest_teardown(item=item, nextitem=nextitem)
    return inner
