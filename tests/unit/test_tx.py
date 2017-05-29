import io

import pytest

from amqproto import protocol


def test_TxSelect_sending(ready_channel):
    fut = ready_channel.tx_select()

    method_bytes = io.BytesIO()
    method = protocol.TxSelect()
    method.to_bytestream(method_bytes)

    assert method_bytes.getvalue() in ready_channel.data_to_send()

    method = protocol.TxSelectOK()
    frame = protocol.MethodFrame(ready_channel._channel_id, method)
    ready_channel.handle_frame(frame)
    assert fut.done() and not fut.cancelled()


@pytest.mark.parametrize('method_name,method_cls,ok_method_cls', [
    ('tx_commit', protocol.TxCommit, protocol.TxCommitOK),
    ('tx_rollback', protocol.TxRollback, protocol.TxRollbackOK),
])
def test_commit_rollback(method_name, method_cls, ok_method_cls, tx_channel):
    method = method_cls()
    sender = getattr(tx_channel, method_name)
    fut = sender()

    method_bytes = io.BytesIO()
    method.to_bytestream(method_bytes)
    assert method_bytes.getvalue() in tx_channel.data_to_send()

    ok_method = ok_method_cls()
    frame = protocol.MethodFrame(tx_channel._channel_id, ok_method)
    tx_channel.handle_frame(frame)
    assert fut.done() and not fut.cancelled()
