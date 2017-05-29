import io
import operator

import amqproto.protocol.methods as am

import pytest
import hypothesis as h
import hypothesis.strategies as hs

from .. import strategies


@hs.composite
def methods(draw, method_cls, generate_arguments):
    kwargs = {}
    for name, amqptype in method_cls.field_info:
        if name.startswith('reserved'):
            continue
        if name == 'global':
            name = 'global_'

        if generate_arguments:
            kwargs[name] = draw(strategies.type_to_strategy[amqptype])
        else:
            kwargs[name] = None
    return method_cls(**kwargs)


@h.given(hs.data())
@pytest.mark.parametrize('method_cls', am.Method.__subclasses__())
def test_methods_can_be_instantiated_with_Nones(method_cls, data):
    method = data.draw(methods(method_cls, generate_arguments=False))
    assert method


def test_methods_provide_attribute_access():
    method = am.ExchangeDeclare(
        exchange='exchange', type='direct', passive=False, durable=False,
        auto_delete=True, internal=False, no_wait=False, arguments=None,
    )
    for name, value in method.values.items():
        attr_value = getattr(method, name)
        assert attr_value is value

    # Make sure accessing missing attributes raises AttributeError
    with pytest.raises(AttributeError):
        method.missing


@h.given(hs.data())
@h.settings(perform_health_check=False)
@pytest.mark.parametrize('method_cls', am.Method.__subclasses__())
def test_methods_can_be_instantiated_with_arbitrary_arguments(method_cls, data):
    method = data.draw(methods(method_cls, generate_arguments=True))
    assert method


@h.given(hs.data())
@h.settings(perform_health_check=False)
@pytest.mark.parametrize('method_cls', am.Method.__subclasses__())
def test_methods_can_be_packed_unpacked_with_arbitrary_arguments(method_cls, data):
    method = data.draw(methods(method_cls, generate_arguments=True))

    stream = io.BytesIO()
    method.to_bytestream(stream)
    raw = stream.getvalue()

    stream.seek(0)
    unpacked = am.Method.from_bytestream(stream)

    assert method == unpacked
