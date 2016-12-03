import contextlib


@contextlib.contextmanager
def Transaction(channel, timeout=None):
    fut = channel.send_TxSelect()
    fut.result(timeout)
    try:
        yield
    except Exception:
        fut = channel.send_TxRollback()
        fut.result()
    else:
        fut = channel.send_TxCommit()
        fut.result()
