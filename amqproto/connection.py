import io
import logging
import platform
import itertools
import pkg_resources
from concurrent.futures import Future

import amqpframe
import amqpframe.methods

from . import channel
from . import auth as _auth_methods
from .fsm import Transition as T, Machine
from .abstract_channel import AbstractChannel


logger = logging.getLogger(__name__)


class ConnectionFSM:
    initial_state = 'connection_inactive'

    states = [
        'connection_inactive',

        # Handshake states
        'sent_ProtocolHeaderFrame',
        'received_ConnectionStart',
        'sent_ConnectionStartOK',
        'received_ConnectionSecure',
        'sent_ConnectionSecureOK',
        'received_ConnectionTune',
        'sent_ConnectionTuneOK',
        'sent_ConnectionOpen',

        # Working state
        'connection_active',

        # Channels states
        'sent_ChannelOpen',
        'sent_ChannelClose',
        'received_ChannelClose',

        # Disconnect states
        'received_ConnectionClose',
        'sent_ConnectionClose',
    ]

    transitions = [
        T(event='send_ProtocolHeaderFrame',
          source='connection_inactive',
          dest='sent_ProtocolHeaderFrame'),

        T(event='received_ProtocolHeaderFrame',
          source='sent_ProtocolHeaderFrame',
          dest='connection_inactive'),

        T(event='receive_ConnectionStart',
          source='sent_ProtocolHeaderFrame',
          dest='received_ConnectionStart'),

        T(event='send_ConnectionStartOK',
          source='received_ConnectionStart',
          dest='sent_ConnectionStartOK'),

        T(event='receive_ConnectionSecure',
          source='sent_ConnectionStartOK',
          dest='received_ConnectionSecure'),

        T(event='send_ConnectionSecureOK',
          source='received_ConnectionSecure',
          dest='sent_ConnectionSecureOK'),

        T(event='receive_ConnectionTune',
          source='sent_ConnectionSecureOK',
          dest='received_ConnectionTune'),

        T(event='send_ConnectionTuneOK',
          source='received_ConnectionTune',
          dest='sent_ConnectionTuneOK'),

        T(event='send_ConnectionOpen',
          source='sent_ConnectionTuneOK',
          dest='sent_ConnectionOpen'),

        T(event='receive_ConnectionOpenOK',
          source='sent_ConnectionOpen',
          dest='connection_active'),

        T(event='send_ChannelOpen',
          source='connection_active',
          dest='sent_ChannelOpen'),

        T(event='receive_ChannelOpenOK',
          source='sent_ChannelOpen',
          dest='connection_active'),

        T(event='receive_ChannelOpenOK',
          source='sent_ChannelOpen',
          dest='connection_active'),

        T(event='send_ChannelClose',
          source='connection_active',
          dest='sent_ChannelClose'),

        T(event='receive_ChannelCloseOK',
          source='sent_ChannelClose',
          dest='connection_active'),

        T(event='receive_ChannelClose',
          source='connection_active',
          dest='received_ChannelClose'),

        T(event='send_ChannelCloseOK',
          source='received_ChannelClose',
          dest='connection_active'),

        T(event='receive_ConnectionClose',
          source='connection_active',
          dest='received_ConnectionClose'),

        T(event='send_ConnectionCloseOK',
          source='received_ConnectionClose',
          dest='connection_inactive'),

        T(event='send_ConnectionClose',
          source='connection_active',
          dest='sent_ConnectionClose'),

        T(event='receive_ConnectionCloseOK',
          source='sent_ConnectionClose',
          dest='connection_inactive'),
    ]


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

        self._fsm = Machine(transitions=ConnectionFSM.transitions,
                            states=ConnectionFSM.states,
                            initial_state=ConnectionFSM.initial_state)

        self._channel_factory = channel.channel_factory()
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

        self._connection_properties = {
            'channel_max': channel_max,
            'frame_max': frame_max,
            'heartbeat': heartbeat,
        }

    def _setup_method_handlers(self):
        methods = amqpframe.methods
        return {
            methods.ConnectionStart: self.receive_ConnectionStart,
            methods.ConnectionSecure: self.receive_ConnectionSecure,
            methods.ConnectionTune: self.receive_ConnectionTune,
            methods.ConnectionOpenOK: self.receive_ConnectionOpenOK,
            methods.ChannelOpenOK: self.receive_ChannelOpenOK,
            methods.ConnectionClose : self.receive_ConnectionClose,
            methods.ConnectionCloseOK: self.receive_ConnectionCloseOK,
        }

    def data_to_send(self):
        own_data = super().data_to_send()
        channels_data = (ch.data_to_send() for ch in self._channels.values())
        return b''.join(itertools.chain([own_data], channels_data))

    def initiate_connection(self):
        self.send_ProtocolHeaderFrame()

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
        logger.debug(frame.payload)

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
        logger.debug(method)
        self._send_method(method)

    def receive_ConnectionSecure(self, frame):
        logger.debug('Receiving ConnectionSecure')
        method = frame.method
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
        method = frame.method
        for item in ('channel_max', 'frame_max', 'heartbeat'):
            client_value = self._connection_properties[item]
            server_value = getattr(method, item)
            if client_value > server_value:
                self._connection_properties[item] = server_value

        logger.debug(self._connection_properties)

        self.send_ConnectionTuneOK()

    def send_ConnectionTuneOK(self):
        logger.debug('Sending ConnectionTuneOK')
        method = amqpframe.methods.ConnectionTuneOK(
            **self._connection_properties
        )
        self._send_method(method)

    def send_ConnectionOpen(self):
        method = amqpframe.methods.ConnectionOpen(self._virtual_host)
        self._send_method(method)
        return self._fut

    def receive_ConnectionOpenOK(self, frame):
        self._fut.set_result(frame.payload)
        self._fut = Future()

    def send_ConnectionClose(self,
                             reply_code, reply_text,
                             class_id=0, method_id=0):
        method = amqpframe.methods.ConnectionClose(
            reply_code, reply_text, class_id, method_id
        )
        self._send_method(method)
        return self._fut

    def receive_ConnectionCloseOK(self, frame):
        self._fut.set_result(frame.payload)
        self._fut = Future()

    def receive_ConnectionClose(self, frame):
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

    def send_ChannelOpen(self):
        method = amqpframe.methods.ChannelOpen()
        self._send_method(method)
        return self._fut

    def receive_ChannelOpenOK(self, frame):
        self._fut.set_result(frame.payload)
        self._fut = Future()
