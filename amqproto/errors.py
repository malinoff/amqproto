from itertools import chain

from amqpframe.errors import *

ERRORS_BY_CODE = {
    cls.reply_code: cls
    for cls in chain(SoftError.__subclasses__(), HardError.__subclasses__())
}
