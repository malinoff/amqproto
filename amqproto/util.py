import inspect


class override_signature:

    def __init__(self, from_):
        self._from = inspect.signature(from_)

    def __call__(self, method):
        method.__signature__ = self._from
        return method


class override_docstring:

    def __init__(self, from_):
        self._from = from_.__doc__

    def __call__(self, method):
        method.__doc__ = self._from
        return method
