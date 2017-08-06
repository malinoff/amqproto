import inspect


class override_signature:

    def __init__(self, from_):
        self._from = inspect.signature(from_)

    def __call__(self, method):
        method.__signature__ = self._from
        return method
