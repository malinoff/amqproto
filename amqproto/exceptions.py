class UnrecoverableError(Exception):
    pass


class ConnectionClosed(UnrecoverableError):

    def __init__(self, reply_code, reply_text, class_id, method_id):
        super().__init__()
        self.reply_code = reply_code
        self.reply_text = reply_text
        self.class_id = class_id
        self.method_id = method_id


class UnsupportedProtocol(UnrecoverableError):

    def __init__(self, major, minor, revision):
        super().__init__()
        self.protocol_major = major
        self.protocol_minor = minor
        self.protocol_revision = revision


class RecoverableError(Exception):
    pass


class ChannelClosed(RecoverableError):

    def __init__(self, reply_code, reply_text, class_id, method_id):
        super().__init__()
        self.reply_code = reply_code
        self.reply_text = reply_text
        self.class_id = class_id
        self.method_id = method_id


class BasicGetEmpty(RecoverableError):
    pass
