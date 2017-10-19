"""
amqproto.settings
~~~~~~~~~~~~~~~~~

AMQP connection settings.
"""

from .replies import HardError

FRAME_MIN_SIZE = 4096


class Settings:
    """Connection settings: client-side, server-side and
    negotiated parameters.
    """

    def __init__(self, client_properties, locale, mechanism,
                 channel_max, frame_max, heartbeat):
        self.client = {
            'properties': client_properties,
            'locale': locale,
            'mechanism': mechanism,
            'channel_max': channel_max,
            'frame_max': frame_max,
            'heartbeat': heartbeat,
        }
        self.server = {
            'properties': {},
            'locales': [],
            'mechanisms': [],
            'channel_max': None,
            'frame_max': None,
            'heartbeat': None,
        }
        self.negotiated = {
            'locale': None,
            'mechanism': None,
            'channel_max': None,
            'frame_max': FRAME_MIN_SIZE,
            'heartbeat': None,
        }
        self.secure = []

    def negotiate_handshake(self, server_properties, mechanisms, locales):
        """Negotiate preferred SASL mechanism and locale."""
        self.server.update({
            'properties': server_properties,
            'mechanisms': mechanisms,
            'locales': locales,
        })
        if self.client['mechanism'] not in mechanisms:
            raise HardError('unable to agree on auth mechanism')
        if self.client['locale'] not in locales:
            raise HardError('unable to agree on locale')
        self.negotiated.update({
            'mechanism': self.client['mechanism'],
            'locale': self.client['locale']
        })

    def secure_step(self, challenge, response):
        """Remember about a single SASL challenge -> response cycle.
        Can help debugging easier.
        """
        self.secure.append({
            'challenge': challenge,
            'response': response,
        })

    def negotiate_tune(self, channel_max, frame_max, heartbeat):
        """Negotiate preferred channel_max, frame_max and heartbeat values."""
        self.server.update({
            'channel_max': channel_max,
            'frame_max': frame_max,
            'heartbeat': heartbeat,
        })
        for name in ('channel_max', 'frame_max', 'heartbeat'):
            client_value, server_value = self.client[name], self.server[name]
            negotiate = max if client_value == 0 or server_value == 0 else min
            self.negotiated[name] = negotiate(client_value, server_value)
