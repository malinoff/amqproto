import hypothesis.strategies as hs

import amqpframe.methods as am
from amqpframe import errors
from amqpframe import test_strategies

ERRORS_CLASSES = errors.SoftError.__subclasses__() + errors.HardError.__subclasses__()


@hs.composite
def methods(draw, method_cls):
    close_method = method_cls in (am.ConnectionClose, am.ChannelClose)
    kwargs = {}
    for name, amqptype in method_cls.field_info:
        if name.startswith('reserved'):
            continue
        if name == 'global':
            name = 'global_'

        if close_method and name == 'reply_code':
            kwargs[name] = draw(hs.sampled_from(cls.reply_code for cls in ERRORS_CLASSES))
        else:
            kwargs[name] = draw(test_strategies.type_to_strategy[amqptype])
    return method_cls(**kwargs)


def draw_method_example(method_cls):
    method = methods(method_cls).example()
    args = {}
    for name, _ in method.field_info:
        if name.startswith('reserved'):
            continue
        if name == 'global':
            name = 'global_'

        args[name] = getattr(method, name)
    return method, args
