import io
import logging
import platform
import itertools
import pkg_resources
from concurrent.futures import Future

import amqpframe
import amqpframe.methods
from amqpframe.frames import FRAME_MIN_SIZE

from fsmpy import FunctionalMachine

from . import fsm
from . import channel
from . import auth as _auth_methods
from .abstract_channel import AbstractChannel

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


class Connection(AbstractChannel):

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

        super().__init__(channel_id=0)

        self._fsm = FunctionalMachine(
            'connection_fsm',
            transitions=fsm.Connection.transitions,
            states=fsm.Connection.states,
            initial_state=fsm.Connection.initial_state
        )

        self._channel_factory = None
        self._channels = {}

        self._virtual_host = virtual_host
        # Will be ajusted in receive_vaild_protocol_header
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
            'secure': {'response': None},
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
            'heartbeat': None,
            # Before max frame size is negotiated, FRAME_MIN_SIZE
            # is max frame size (yeah).
            'frame_max': FRAME_MIN_SIZE,
        }

    def _setup_method_handlers(self):
        methods = amqpframe.methods
        return {
            methods.ConnectionStart: self.receive_ConnectionStart,
            methods.ConnectionSecure: self.receive_ConnectionSecure,
            methods.ConnectionTune: self.receive_ConnectionTune,
            methods.ConnectionOpenOK: self.receive_ConnectionOpenOK,
            methods.ConnectionClose : self.receive_ConnectionClose,
            methods.ConnectionCloseOK: self.receive_ConnectionCloseOK,
        }

    def _send_method(self, method):
        self._fsm.trigger('send_' + method.__class__.__name__)
        frame = amqpframe.MethodFrame(self._channel_id, method)
        frame.to_bytestream(self._buffer)

    def handle_frame(self, frame):
        channel_id = frame.channel_id
        if channel_id != 0:
            channel = self._channels[channel_id]
            channel.handle_frame(frame)
            self._buffer.write(channel.data_to_send())
            return

        if isinstance(frame, amqpframe.MethodFrame):
            self.handle_MethodFrame(frame)
        elif isinstance(frame, amqpframe.ProtocolHeaderFrame):
            self.receive_ProtocolHeaderFrame(frame)
        elif isinstance(frame, amqpframe.HeartbeatFrame):
            pass

    def initiate_connection(self):
        self.send_ProtocolHeaderFrame()
        return self._fut

    def get_channel(self):
        channel = self._channel_factory(self.properties['frame_max'])
        self._channels[channel._channel_id] = channel
        return channel

    def handle_MethodFrame(self, frame):
        self._fsm.trigger('receive_' + frame.payload.__class__.__name__)
        super().handle_MethodFrame(frame)

    # Handshake
    def send_ProtocolHeaderFrame(self):
        logger.debug('Sending ProtocolHeaderFrame')
        self._fsm.trigger('send_ProtocolHeaderFrame')
        payload = amqpframe.ProtocolHeaderPayload(
            self._protocol_major,
            self._protocol_minor,
            self._protocol_revision,
        )
        frame = amqpframe.ProtocolHeaderFrame(
            self._channel_id, payload=payload
        )
        frame.to_bytestream(self._buffer)

    def handle_ProtocolHeaderFrame(self,
                                   frame: amqpframe.ProtocolHeaderFrame):
        """If the server cannot support the protocol specified
        in the protocol header, it MUST respond with a valid protocol header
        and then close the socket connection.
        """
        logger.debug('Receiving ProtocolHeaderFrame')
        self._fsm.trigger('receive_ProtocolHeaderFrame')
        self._protocol_major = frame.payload.protocol_major
        self._protocol_minor = frame.payload.protocol_minor
        self._protocol_revision = frame.payload.protocol_revision

    def receive_ConnectionStart(self, frame):
        logger.debug('Receiving ConnectionStart')

        method = frame.payload
        assert method.version_major == self._protocol_major
        assert method.version_minor == self._protocol_minor
        assert method.mechanisms
        assert b'en_US' in method.locales

        self._handshake_properties['server'] = {
            'properties': method.server_properties,
            'mechanisms': method.mechanisms,
            'locales': method.locales,
        }
        # Fail-fast approach: choose mechanism and locale as soon as possible
        chosen = self._choose_handshake_properties(
            self._handshake_properties['client'],
            self._handshake_properties['server'],
        )
        self._handshake_properties['chosen'] = chosen

        self.send_ConnectionStartOK()

    def _choose_handshake_properties(self, client, server):
        if client['mechanism'] not in server['mechanisms']:
            raise RuntimeError('Unable to agree on auth mechanism')
        if client['locale'] not in server['locales']:
            raise RuntimeError('Unable to agree on locale')
        return {'mechanism': client['mechanism'], 'locale': client['locale']}

    def send_ConnectionStartOK(self):
        logger.debug('Sending ConnectionStartOK')
        stream = io.BytesIO()
        self._auth.to_bytestream(stream)
        response = stream.getvalue()

        client_properties = self._handshake_properties['client']['properties']
        mechanism = self._handshake_properties['chosen']['mechanism']
        locale = self._handshake_properties['chosen']['locale']

        method = amqpframe.methods.ConnectionStartOK(
            client_properties=client_properties,
            mechanism=mechanism,
            response=response,
            locale=locale
        )
        self._send_method(method)

    def receive_ConnectionSecure(self, frame):
        logger.debug('Receiving ConnectionSecure')
        method = frame.payload
        # Fail-fast
        response = self._auth.handle_challenge(method.challenge)
        if response is None:
            response = b''
        self._handshake_properties['secure']['response'] = response

        self.send_ConnectionSecureOK()

    def send_ConnectionSecureOK(self):
        logger.debug('Sending ConnectionSecureOK')
        method = amqpframe.methods.ConnectionSecureOK(
            response=self._handshake_properties['secure']['response']
        )
        self._handshake_properties['secure']['response'] = None
        self._send_method(method)

    def receive_ConnectionTune(self, frame):
        logger.debug('Receiving ConnectionTune')
        method = frame.payload
        for item in ('channel_max', 'frame_max', 'heartbeat'):
            client_value = self._tune_properties['client'][item]
            server_value = getattr(method, item)
            self._tune_properties['server'][item] = server_value

            if client_value == 0 or server_value == 0:
                self.properties[item] = max(client_value, server_value)
            else:
                self.properties[item] = min(client_value, server_value)

        self._channel_factory = channel.channel_factory(
            channel_max=self.properties['channel_max']
        )
        self.send_ConnectionTuneOK()

    def send_ConnectionTuneOK(self):
        logger.debug('Sending ConnectionTuneOK')
        method = amqpframe.methods.ConnectionTuneOK(**self.properties)
        self._send_method(method)

        self.send_ConnectionOpen()

    def send_ConnectionOpen(self):
        logger.debug('Sending ConnectionOpen')
        method = amqpframe.methods.ConnectionOpen(
            virtual_host=self._virtual_host
        )
        self._send_method(method)

    def receive_ConnectionOpenOK(self, frame):
        logger.debug('Receiving ConnectionOpenOK')
        self._fut.set_result(frame.payload)
        self._fut = Future()

    def send_ConnectionClose(self,
                             reply_code, reply_text,
                             class_id=0, method_id=0):
        method = amqpframe.methods.ConnectionClose(
            reply_code=reply_code, reply_text=reply_text,
            class_id=class_id, method_id=method_id
        )
        self._send_method(method)
        return self._fut

    def receive_ConnectionCloseOK(self, frame):
        self._fut.set_result(frame.payload)
        self._fut = Future()

    def receive_ConnectionClose(self, frame):
        method = frame.payload
        logger.error({'event': 'ConnectionClose',
                      'reply_code': method.reply_code,
                      'reply_text': method.reply_text,
                      'class_id': method.class_id,
                      'method_id': method.method_id})
        self.send_ConnectionCloseOK()
        return self._fut

    def send_ConnectionCloseOK(self):
        method = amqpframe.methods.ConnectionCloseOK()
        self._send_method(method)
