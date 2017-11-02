"""
amqproto.settings
~~~~~~~~~~~~~~~~~

AMQP connection settings.
"""

import platform
import pkg_resources

import attr

FRAME_MIN_SIZE = 4096

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


@attr.s()
class Settings:  # pylint: disable=too-few-public-methods
    """Connection settings: client-side, server-side and
    negotiated parameters.

    :param properties: a dictionary of arbitrary properties.
        Unlikely needs to be ever changed.
    :param locale: preferred locale. Unlikely needs to be ever changed.
    :param channel_max: limit maximum channels. 0 means no specific limit.
        Unlikely needs to be ever changed.
    :param frame_max: limit maximum frame size. 0 means no specific limit.
        Unlikely needs to be ever changed.
    :param heartbeat: heartbeat delay in seconds. 0 means no heartbeats,
        which is not recommended.
    """

    # Either 'client', or 'server', or 'negotiated'
    type: bool = attr.ib(default='client')

    properties: dict = attr.ib(default=None)
    locales: str = attr.ib(default='en_US')
    mechanisms: str = attr.ib(default='')
    channel_max: int = attr.ib(default=0)
    frame_max: int = attr.ib(default=0)
    heartbeat: int = attr.ib(default=60)

    def __attrs_post_init__(self):
        if self.type == 'client':
            if self.properties is None:
                self.properties = DEFAULT_CLIENT_PROPERTIES
            # pylint: disable=no-member
            if len(self.locales.split(' ')) > 1:
                raise RuntimeError('more than one locale is not supported')
            if len(self.mechanisms.split(' ')) > 1:
                raise RuntimeError('more than one mechanism is not supported')

        if self.type == 'negotiated' and self.frame_max == 0:
            self.frame_max = FRAME_MIN_SIZE
