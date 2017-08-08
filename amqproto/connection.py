import io
import time
import logging
import platform
import pkg_resources
from concurrent.futures import Future


from . import fsm
from . import channel
from . import protocol
from . import auth as _auth_methods

logger = logging.getLogger(__name__)


# The properties SHOULD contain at least these fields:
# * "product", giving the name of the client product,
# * "version", giving the name of the client version,
# * "platform", giving the name of the operating system,
# * "copyright", if appropriate, and
# * "information", giving other general information.
DEFAULT_CLIENT_PROPERTIES = {
    'product': 'amqproto',
    'version': str(pkg_resources.get_distribution('amqproto').version),
    'platform': platform.platform(),
    'copyright': '(c) Dmitry Malinovsky (aka malinoff) and contributors',
    'information': '',
}


class Connection:

    Channel = channel.Channel

    def __init__(self,
                 virtual_host='/',
                 protocol_major=0,
                 protocol_minor=9,
                 protocol_revision=1,
                 client_properties=None,
                 locale=b'en_US',
                 auth=None,
                 # Maximum channels, 0 means no specific limit.
                 channel_max=0,
                 # Maximum frame length, 0 means no specific limit.
                 frame_max=0,
                 # Heartbeat delay in seconds, 0 means no heartbeats.
                 heartbeat=60):
        self._fsm = fsm.Connection()

        self._channel_id = 0
        self._buffer = io.BytesIO()
        self._method_handlers = self._setup_method_handlers()

        self._channels_manager = None

        self._virtual_host = virtual_host
        self._protocol_major = protocol_major
        self._protocol_minor = protocol_minor
        self._protocol_revision = protocol_revision

        if client_properties is None:
            client_properties = DEFAULT_CLIENT_PROPERTIES
        if auth is None:
            auth = _auth_methods.PLAIN(b'guest', b'guest')

        self._auth = auth

        self._handshake_properties = {
            'client': {
                'properties': client_properties,
                'locale': locale,
                'mechanism': auth.mechanism,
            },
            'server': {
                'properties': {},
                'locales': None,
                'mechanisms': None,
            },
            'chosen': {'locale': None, 'mechanism': None},
            'secure': {'challenge': None, 'response': None},
        }

        self._tune_properties = {
            'client': {
                'channel_max': channel_max,
                'frame_max': frame_max,
                'heartbeat': heartbeat,
            },
            'server': {
                'channel_max': None,
                'frame_max': None,
                'heartbeat': None,
            },
        }

        self.properties = {
            # Irrelevant until receive_ConnectionTune is called
            'channel_max': None,
            # Before max frame size is negotiated, FRAME_MIN_SIZE
            # is max frame size (yeah).
            'frame_max': protocol.FRAME_MIN_SIZE,
            'heartbeat': None,
        }

        self._heartbeater = Heartbeater(self)

    def _setup_method_handlers(self):
        return {
            protocol.ConnectionStart: self._receive_ConnectionStart,
            protocol.ConnectionSecure: self._receive_ConnectionSecure,
            protocol.ConnectionTune: self._receive_ConnectionTune,
            protocol.ConnectionOpenOK: self._receive_ConnectionOpenOK,
            protocol.ConnectionClose: self._receive_ConnectionClose,
            protocol.ConnectionCloseOK: self._receive_ConnectionCloseOK,
        }

    def data_to_send(self):
        data = self._buffer.getvalue()
        if self._channels_manager is not None:
            for channel in self._channels_manager.values():
                data += channel.data_to_send()
        self._buffer = io.BytesIO()
        return data

    def receive_frames(self, data):
        if data:
            # Any received octet counts as Heartbeat frame
            self._heartbeater.update_received_time()
        stream = io.BytesIO(data)
        build_frame = protocol.Frame.from_bytestream
        data_length = len(data)
        while stream.tell() != data_length:
            current_position = stream.tell()
            try:
                frame = build_frame(stream)
            except ValueError:
                # In case of a partial frame received
                stream.seek(current_position)
                break
            # Check if frame size fits into the negotiated value.
            if frame.size > self.properties['frame_max']:
                if isinstance(frame, protocol.MethodFrame):
                    class_id, method_id = frame.payload.method_type
                else:
                    class_id, method_id = 0, 0
                reply_text = 'received frame is too large ({} bytes)'.format(
                    frame.size
                )
                raise protocol.FrameError(reply_text, class_id, method_id)
            yield frame

    def handle_frame(self, frame):
        if isinstance(frame, protocol.MethodFrame):
            method = frame.payload
            logger.debug(
                'Receiving MethodFrame %s [channel_id:%s]',
                method.__class__.__name__, self._channel_id
            )
            handler = self._method_handlers[method.__class__]
            return handler(method)
        elif isinstance(frame, protocol.ProtocolHeaderFrame):
            logger.debug(
                'Receiving ProtocolHeaderFrame [channel_id:%s]',
                self._channel_id
            )
            self._handle_ProtocolHeaderFrame(frame)
        elif isinstance(frame, protocol.HeartbeatFrame):  # pragma: no cover
            logger.debug(
                'Receiving HeartbeatFrame [channel_id:%s]',
                self._channel_id
            )

    def check_heartbeats(self):
        return self._heartbeater.check()

    def send_heartbeat(self):
        return self._heartbeater.send()

    def initiate_connection(self):
        self._fsm.initiate()
        return self._send_ProtocolHeaderFrame()

    def get_channel(self, channel_id=None):
        return self._channels_manager[channel_id]

    def _send_method(self, method):
        frame = protocol.MethodFrame(self._channel_id, method)
        self._send_frame(frame)

    def _send_frame(self, frame):
        frame.to_bytestream(self._buffer)

    def _send_HeartbeatFrame(self):
        payload = protocol.HeartbeatPayload()
        frame = protocol.HeartbeatFrame(self._channel_id, payload)
        self._send_frame(frame)

    # Handshake
    def _send_ProtocolHeaderFrame(self):
        payload = protocol.ProtocolHeaderPayload(
            self._protocol_major,
            self._protocol_minor,
            self._protocol_revision,
        )
        frame = protocol.ProtocolHeaderFrame(
            self._channel_id, payload=payload
        )
        self._send_frame(frame)

    def _handle_ProtocolHeaderFrame(self,
                                    frame: protocol.ProtocolHeaderFrame):
        """If the server cannot support the protocol specified
        in the protocol header, it MUST respond with a valid protocol header
        and then close the socket connection.
        """
        reply_text = 'unsupported protocol: {}.{}.{}'.format(
            frame.payload.protocol_major,
            frame.payload.protocol_minor,
            frame.payload.protocol_revision,
        )
        raise protocol.HardError(reply_text)

    def _receive_ConnectionStart(self, method):
        self._fsm.start()
        if method.version_major != self._protocol_major:
            reply_text = 'major version mismatch (expected {}, got {})'.format(
                self._protocol_major, method.version_major
            )
            raise protocol.HardError(reply_text)
        if method.version_minor != self._protocol_minor:
            reply_text = 'minor version mismatch (expected {}, got {})'.format(
                self._protocol_minor, method.version_minor
            )
            raise protocol.HardError(reply_text)
        if not method.mechanisms:
            raise protocol.HardError('server did not sent any auth mechanisms')
        if b'en_US' not in method.locales:
            reply_text = "server did not sent b'en_US' locale, got {}".format(
                method.locales
            )
            raise protocol.HardError(reply_text)

        self._handshake_properties['server'] = {
            'properties': method.server_properties,
            'mechanisms': method.mechanisms.split(b' '),
            'locales': method.locales.split(b' '),
        }
        # Choose mechanism and locale as soon as possible
        chosen = self._choose_handshake_properties(
            self._handshake_properties['client'],
            self._handshake_properties['server'],
        )
        self._handshake_properties['chosen'] = chosen

        return self._send_ConnectionStartOK()

    def _choose_handshake_properties(self, client, server):
        if client['mechanism'] not in server['mechanisms']:
            raise protocol.HardError('unable to agree on auth mechanism')
        if client['locale'] not in server['locales']:
            raise protocol.HardError('unable to agree on locale')
        return {'mechanism': client['mechanism'], 'locale': client['locale']}

    def _send_ConnectionStartOK(self):
        stream = io.BytesIO()
        self._auth.to_bytestream(stream)
        response = stream.getvalue()

        client_properties = self._handshake_properties['client']['properties']
        mechanism = self._handshake_properties['chosen']['mechanism']
        locale = self._handshake_properties['chosen']['locale']

        method = protocol.ConnectionStartOK(
            client_properties=client_properties,
            mechanism=mechanism,
            response=response,
            locale=locale
        )
        self._send_method(method)

    def _receive_ConnectionSecure(self, method):
        self._fsm.secure()
        # Fail-fast
        response = self._auth.handle_challenge(method.challenge)
        self._handshake_properties['secure']['challenge'] = method.challenge
        self._handshake_properties['secure']['response'] = response

        return self._send_ConnectionSecureOK()

    def _send_ConnectionSecureOK(self):
        method = protocol.ConnectionSecureOK(
            response=self._handshake_properties['secure']['response']
        )
        self._send_method(method)

    def _receive_ConnectionTune(self, method):
        self._fsm.tune()
        client_props = self._tune_properties['client']
        server_props = self._tune_properties['server']

        for index, item in enumerate(('channel_max', 'frame_max')):
            client_value = client_props[item]
            server_props[item] = server_value = getattr(method, item)

            if client_value == 0 or server_value == 0:
                negotiated = max(client_value, server_value)
            else:
                negotiated = min(client_value, server_value)
            if negotiated == 0:
                type_ = method.field_info[index][1]
                negotiated = type_.MAX
            self.properties[item] = negotiated

        client_value = client_props['heartbeat']
        server_props['heartbeat'] = server_value = method.heartbeat
        if client_value == 0 or server_value == 0:
            self.properties['heartbeat'] = 0
        else:
            self.properties['heartbeat'] = max(client_value, server_value)

        self._channels_manager = channel.ChannelsManager(
            self.properties['channel_max'], self.properties['frame_max'],
            self.Channel,
        )
        return self._send_ConnectionTuneOK()

    def _send_ConnectionTuneOK(self):
        method = protocol.ConnectionTuneOK(**self.properties)
        frame = protocol.MethodFrame(self._channel_id, method)
        frame.to_bytestream(self._buffer)
        self._send_ConnectionOpen()

    def _send_ConnectionOpen(self):
        method = protocol.ConnectionOpen(virtual_host=self._virtual_host)
        frame = protocol.MethodFrame(self._channel_id, method)
        frame.to_bytestream(self._buffer)

    def _receive_ConnectionOpenOK(self, method):
        self._fsm.open()

    @property
    def closed(self):
        return self._fsm.state == 'CLOSED'

    def close(self, reply_code, reply_text, class_id=0, method_id=0):
        method = protocol.ConnectionClose(
            reply_code=reply_code, reply_text=reply_text,
            class_id=class_id, method_id=method_id
        )
        self._send_method(method)

    def _receive_ConnectionCloseOK(self, method):
        pass

    def _receive_ConnectionClose(self, method):
        AMQPError = protocol.ERRORS_BY_CODE[method.reply_code]
        exc = AMQPError(
            method.reply_text,
            method.class_id,
            method.method_id,
        )
        return self._send_ConnectionCloseOK(exc)

    def _send_ConnectionCloseOK(self, _exc):
        method = protocol.ConnectionCloseOK()
        self._send_method(method)
        raise _exc


class Heartbeater:

    def __init__(self, connection):
        self.connection = connection

        self._previous_heartbeat_received = 0
        self._previous_heartbeat_sent = 0

    @property
    def interval(self):
        return self.connection.properties['heartbeat']

    def check(self):
        elapsed = time.monotonic() - self._previous_heartbeat_received
        if elapsed >= 2 * self.interval:
            raise protocol.HardError('missed two or more heartbeats')
        return True

    def update_received_time(self):
        self._previous_heartbeat_received = time.monotonic()

    def update_sent_time(self):
        self._previous_heartbeat_sent = time.monotonic()

    def send(self):
        elapsed = time.monotonic() - self._previous_heartbeat_sent
        # / 2 is there so that we don't end up waiting for nearly
        # 2 * self.interval before sending a heartbeat in the boundary case
        # where the last message was sent just after a heartbeat.
        if elapsed >= self.interval / 2:
            self.connection._send_HeartbeatFrame()
        self.update_sent_time()
