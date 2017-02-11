import io

import pytest

import amqpframe
import amqpframe.methods


def test_TxSelect_sending(ready_channel):
    fut = ready_channel.send_TxSelect()

    method_bytes = io.BytesIO()
    method = amqpframe.methods.TxSelect()
    method.to_bytestream(method_bytes)

    assert method_bytes.getvalue() in ready_channel.data_to_send()

    method = amqpframe.methods.TxSelectOK()
    frame = amqpframe.MethodFrame(ready_channel._channel_id, method)
    ready_channel.handle_frame(frame)
    assert fut.done() and not fut.cancelled()


@pytest.mark.parametrize('method_cls,ok_method_cls', [
    (amqpframe.methods.TxCommit, amqpframe.methods.TxCommitOK),
    (amqpframe.methods.TxRollback, amqpframe.methods.TxRollbackOK),
])
def test_commit_rollback(method_cls, ok_method_cls, tx_channel):
    method = method_cls()
    sender = getattr(tx_channel, 'send_' + method_cls.__name__)
    fut = sender()

    method_bytes = io.BytesIO()
    method.to_bytestream(method_bytes)
    assert method_bytes.getvalue() in tx_channel.data_to_send()

    ok_method = ok_method_cls()
    frame = amqpframe.MethodFrame(tx_channel._channel_id, ok_method)
    tx_channel.handle_frame(frame)
    assert fut.done() and not fut.cancelled()
