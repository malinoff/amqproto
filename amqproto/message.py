

class Message:
    def __init__(self, body, properties=None):
        self._body = body
        if not properties:
            properties = {}
        self.properties = properties

    @property
    def body(self):
        if isinstance(self._body, (bytes, bytearray)):
            return self._body
        return self._body.encode()

    @property
    def body_size(self):
        return len(self.body)
