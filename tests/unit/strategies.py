import hypothesis.strategies as hs

from amqpframe import test_strategies


@hs.composite
def methods(draw, method_cls):
    kwargs = {}
    for name, amqptype in method_cls.field_info:
        if name.startswith('reserved'):
            continue
        if name == 'global':
            name = 'global_'

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
