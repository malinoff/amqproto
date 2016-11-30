import amqpframe.types


class PLAIN:
    mechanism = b'PLAIN'

    def __init__(self, username: bytes, password: bytes):
        self.username = username
        self.password = password

    def to_bytestream(self, stream):
        stream.write(b'\x00')
        stream.write(self.username)
        stream.write(b'\x00')
        stream.write(self.password)

    def handle_challenge(self, challenge):
        pass


class AMQPLAIN(PLAIN):
    mechanism = b'AMQPLAIN'

    def to_bytestream(self, stream):
        val = amqpframe.types.Table(
            {b'LOGIN': self.username, b'PASSWORD': self.password}
        )
        val.to_bytestream(stream)


# TODO
# class EXTERNAL
