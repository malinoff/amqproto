import io
from concurrent.futures import Future

from amqpframe import Frame, MethodFrame

__all__ = ['AbstractChannel']


class AbstractChannel:

    def __init__(self, channel_id):
        self._channel_id = channel_id
        self._buffer = io.BytesIO()
        # Future used to synchronise Do/DoOK methods
        self._fut = Future()
        self._method_handlers = self._setup_method_handlers()

    def _setup_method_handlers(self):
        raise NotImplementedError

    def data_to_send(self):
        data = self._buffer.getvalue()
        self._buffer = io.BytesIO()
        return data

    def receive_frames(self, data):
        stream = io.BytesIO(data)
        build_frame = Frame.from_bytestream
        while stream.tell() != len(data):
            yield build_frame(stream)

    def handle_MethodFrame(self, frame):
        method = frame.payload
        handler = self._method_handlers[method.__class__]
        handler(frame)

    def _send_method(self, method):
        frame = MethodFrame(self._channel_id, method)
        frame.to_bytestream(self._buffer)
