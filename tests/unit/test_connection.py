import io

import pytest

import amqpframe
import amqpframe.methods

import amqproto.errors
from amqproto.auth import Auth, PLAIN
from amqproto.connection import Connection


def test_correct_connection_initiation():
    conn = Connection(
        protocol_major=12,
        protocol_minor=34,
        protocol_revision=56,
    )
    assert conn._channel_id == 0
    conn.initiate_connection()

    frame_bytes = io.BytesIO()
    payload = amqpframe.ProtocolHeaderPayload(12, 34, 56)
    frame = amqpframe.ProtocolHeaderFrame(conn._channel_id, payload)
    frame.to_bytestream(frame_bytes)

    assert frame_bytes.getvalue() in conn.data_to_send()


def test_incorrect_connection_initiation():
    conn = Connection()
    conn.initiate_connection()

    payload = amqpframe.ProtocolHeaderPayload(1, 0, 0)
    frame = amqpframe.ProtocolHeaderFrame(conn._channel_id, payload)

    with pytest.raises(amqproto.errors.HardError):
        conn.handle_frame(frame)


def test_correct_ConnectionStart_handling():
    conn = Connection()
    conn.initiate_connection()

    payload = amqpframe.methods.ConnectionStart(
        version_major=0,
        version_minor=9,
        server_properties={'foo': 'bar'},
        mechanisms=b'PLAIN AMQPLAIN',
        locales=b'en_US ru_RU',
    )
    frame = amqpframe.MethodFrame(conn._channel_id, payload)

    conn.handle_frame(frame)

    # Make sure all properties are handled
    server_properties = conn._handshake_properties['server']
    assert server_properties['properties']
    assert server_properties['locales'] == [b'en_US', b'ru_RU']
    assert server_properties['mechanisms'] == [b'PLAIN', b'AMQPLAIN']

    chosen_properties = conn._handshake_properties['chosen']
    assert chosen_properties['locale'] == b'en_US'
    assert chosen_properties['mechanism'] == b'PLAIN'

    # Make sure ConnectionStartOK is sent
    stream = io.BytesIO()
    PLAIN(b'guest', b'guest').to_bytestream(stream)
    response = stream.getvalue()

    stream = io.BytesIO()
    client_properties = conn._handshake_properties['client']['properties']
    mechanism = conn._handshake_properties['chosen']['mechanism']
    locale = conn._handshake_properties['chosen']['locale']

    method = amqpframe.methods.ConnectionStartOK(
        client_properties=client_properties,
        mechanism=mechanism,
        response=response,
        locale=locale
    )
    method.to_bytestream(stream)
    method_bytes = stream.getvalue()

    assert method_bytes in conn.data_to_send()


@pytest.mark.parametrize('arguments', [
    {  # Should fail because version_major is wrong
        'version_major': 1,
        'version_minor': 0,
        'server_properties': {},
        'mechanisms': b'',
        'locales': b'',
    },
    {  # Should fail because version_minor is wrong
        'version_major': 0,
        'version_minor': 0,
        'server_properties': {},
        'mechanisms': b'',
        'locales': b'',
    },
    {  # Should fail because mechanisms is empty
        'version_major': 0,
        'version_minor': 9,
        'server_properties': {},
        'mechanisms': b'',
        'locales': b'',
    },
    {  # Should fail because en_US is not in locales
        'version_major': 0,
        'version_minor': 9,
        'server_properties': {},
        'mechanisms': b'PLAIN',
        'locales': b'',
    },
    {  # Should fail because there's no PLAIN mechanism
        'version_major': 0,
        'version_minor': 9,
        'server_properties': {},
        'mechanisms': b'AMQPLAIN',
        'locales': b'en_US',
    },
    {  # Should fail because there's no ru_RU locale
        'version_major': 0,
        'version_minor': 9,
        'server_properties': {},
        'mechanisms': b'PLAIN',
        'locales': b'en_US',
    },
])
def test_incorrect_ConnectionStart_handling(arguments):
    conn = Connection(locale=b'ru_RU')
    conn.initiate_connection()

    payload = amqpframe.methods.ConnectionStart(**arguments)
    frame = amqpframe.MethodFrame(conn._channel_id, payload)

    with pytest.raises(amqproto.errors.HardError):
        conn.handle_frame(frame)


@pytest.mark.parametrize('properties', [
    {
        'client': {
            'channel_max': 0,
            'frame_max': 0,
            'heartbeat': 0,
        },
        'server': {
            'channel_max': 10,
            'frame_max': 12000,
            'heartbeat': 60,
        },
        'negotiated': {
            'channel_max': 10,
            'frame_max': 12000,
            'heartbeat': 0,
        }
    },
    {
        'client': {
            'channel_max': 0,
            'frame_max': 0,
            'heartbeat': 60,
        },
        'server': {
            'channel_max': 0,
            'frame_max': 0,
            'heartbeat': 0,
        },
        'negotiated': {
            # UnsignedShort.MAX
            'channel_max': (1 << 16) - 1,
            # UnsignedLong.MAX
            'frame_max': (1 << 32) - 1,
            'heartbeat': 0,
        }
    },
    {
        'client': {
            'channel_max': 10,
            'frame_max': 12000,
            'heartbeat': 60,
        },
        'server': {
            'channel_max': 20,
            'frame_max': 14000,
            'heartbeat': 120,
        },
        'negotiated': {
            'channel_max': 10,
            'frame_max': 12000,
            'heartbeat': 120,
        }
    }
])
def test_ConnectionTune_handling(properties):
    conn = Connection(**properties['client'])
    conn.initiate_connection()

    payload = amqpframe.methods.ConnectionStart(
        version_major=0,
        version_minor=9,
        server_properties={'foo': 'bar'},
        mechanisms=b'PLAIN AMQPLAIN',
        locales=b'en_US ru_RU',
    )
    frame = amqpframe.MethodFrame(conn._channel_id, payload)

    conn.handle_frame(frame)

    payload = amqpframe.methods.ConnectionTune(**properties['server'])
    frame = amqpframe.MethodFrame(conn._channel_id, payload)

    conn.handle_frame(frame)

    negotiated = properties['negotiated']
    for prop in ('channel_max', 'frame_max', 'heartbeat'):
        assert conn.properties[prop] == negotiated[prop]

    data_to_send = conn.data_to_send()

    # Make sure ConnectionTuneOK is sent
    stream = io.BytesIO()
    method = amqpframe.methods.ConnectionTuneOK(**conn.properties)
    method.to_bytestream(stream)
    method_bytes = stream.getvalue()

    assert method_bytes in data_to_send

    # Make sure ConnectionOpen is also sent
    stream = io.BytesIO()
    method = amqpframe.methods.ConnectionOpen(virtual_host='/')
    method.to_bytestream(stream)
    method_bytes = stream.getvalue()

    assert method_bytes in data_to_send


def test_ConnectionOpenOK_handling():
    conn = Connection()
    fut = conn.initiate_connection()

    payload = amqpframe.methods.ConnectionStart(
        version_major=0,
        version_minor=9,
        server_properties={'foo': 'bar'},
        mechanisms=b'PLAIN AMQPLAIN',
        locales=b'en_US ru_RU',
    )
    frame = amqpframe.MethodFrame(conn._channel_id, payload)

    conn.handle_frame(frame)

    channel_max = 10
    frame_max = 131072
    heartbeat = 100
    payload = amqpframe.methods.ConnectionTune(
        channel_max=channel_max,
        frame_max=frame_max,
        heartbeat=heartbeat,
    )
    frame = amqpframe.MethodFrame(conn._channel_id, payload)

    conn.handle_frame(frame)

    payload = amqpframe.methods.ConnectionOpenOK()
    frame = amqpframe.MethodFrame(conn._channel_id, payload)

    conn.handle_frame(frame)

    assert fut.done() and not fut.cancelled()
    assert conn.alive


def test_correct_ConnectionClose_sending(ready_connection):
    fut = ready_connection.send_ConnectionClose(15, b'foo', 0, 0)
    method_bytes = io.BytesIO()
    method = amqpframe.methods.ConnectionClose(
        reply_code=15, reply_text=b'foo', class_id=0, method_id=0
    )
    method.to_bytestream(method_bytes)
    assert method_bytes.getvalue() in ready_connection.data_to_send()

    method = amqpframe.methods.ConnectionCloseOK()
    frame = amqpframe.MethodFrame(ready_connection._channel_id, method)
    ready_connection.handle_frame(frame)

    assert fut.done() and not fut.cancelled()
    assert not ready_connection.alive


def test_correct_ConnectionClose_handling(ready_connection):
    method = amqpframe.methods.ConnectionClose(
        reply_code=501, reply_text=b'foo', class_id=100, method_id=200
    )
    frame = amqpframe.MethodFrame(ready_connection._channel_id, method)

    with pytest.raises(amqproto.errors.AMQPError) as excinfo:
        ready_connection.handle_frame(frame)

    method_bytes = io.BytesIO()
    method = amqpframe.methods.ConnectionCloseOK()
    method.to_bytestream(method_bytes)
    assert method_bytes.getvalue() in ready_connection.data_to_send()

    assert not ready_connection.alive

    exc = excinfo.value
    assert exc.reply_code == 501
    assert exc.reply_text == b'foo'
    assert exc.class_id == 100
    assert exc.method_id == 200

    assert isinstance(exc, amqproto.errors.ERRORS_BY_CODE[501])


def test_can_receive_valid_frame(ready_connection):
    stream = io.BytesIO()

    expected_frames = []
    payload = amqpframe.methods.ChannelOpenOK()
    frame = amqpframe.MethodFrame(1, payload)
    frame.to_bytestream(stream)
    expected_frames.append(frame)

    payload = amqpframe.methods.ChannelOpenOK()
    frame = amqpframe.MethodFrame(1, payload)
    frame.to_bytestream(stream)
    expected_frames.append(frame)

    data = stream.getvalue()

    parsed_frames = list(ready_connection.receive_frames(data))
    assert parsed_frames == expected_frames


def test_receive_frame_too_big(ready_connection):
    stream = io.BytesIO()
    payload = amqpframe.ContentBodyPayload(b'a' * 10000)
    frame = amqpframe.ContentBodyFrame(1, payload)
    frame.to_bytestream(stream)
    data = stream.getvalue()

    with pytest.raises(amqproto.errors.FrameError):
        list(ready_connection.receive_frames(data))


class CustomAuth(Auth):
    mechanism = b'CUSTOMAUTH'

    def __init__(self, username, password):
        self.username = username
        self.password = password

    def to_bytestream(self, stream):
        stream.write(self.mechanism)
        stream.write(b'\x01')
        stream.write(self.username)
        stream.write(b'\x01')
        stream.write(self.password)

    def handle_challenge(self, challenge):
        number = int(challenge)
        return str(number * 2).encode('utf-8')


def test_ConnectionSecure_handling():
    conn = Connection(auth=CustomAuth(b'foo', b'bar'))
    conn.initiate_connection()

    payload = amqpframe.methods.ConnectionStart(
        version_major=0,
        version_minor=9,
        server_properties={'foo': 'bar'},
        mechanisms=b'PLAIN CUSTOMAUTH',
        locales=b'en_US ru_RU',
    )
    frame = amqpframe.MethodFrame(conn._channel_id, payload)

    conn.handle_frame(frame)

    payload = amqpframe.methods.ConnectionSecure(
        challenge=b'123'
    )
    frame = amqpframe.MethodFrame(conn._channel_id, payload)

    conn.handle_frame(frame)

    method_bytes = io.BytesIO()
    method = amqpframe.methods.ConnectionSecureOK(response=b'246')
    method.to_bytestream(method_bytes)

    assert method_bytes.getvalue() in conn.data_to_send()
    assert conn._handshake_properties['secure']['challenge'] == b'123'
    assert conn._handshake_properties['secure']['response'] == b'246'
