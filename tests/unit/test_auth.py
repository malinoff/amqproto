import io

import pytest

from amqproto.auth import PLAIN, AMQPLAIN


@pytest.mark.parametrize('impl,args,expected', [
    [PLAIN, (b'foo', b'bar'), b'\x00foo\x00bar'],
    [AMQPLAIN, (b'foo', b'bar'), b'\x00\x00\x00\x1f\x05LOGINS\x00\x00\x00\x03foo\x08PASSWORDS\x00\x00\x00\x03bar'],
])
def test_auth_serialization(impl, args, expected):
    auth = impl(*args)
    stream = io.BytesIO()
    auth.to_bytestream(stream)

    assert stream.getvalue() == expected
