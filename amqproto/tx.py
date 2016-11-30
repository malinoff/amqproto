import contextlib

from .fsm import Transition as T


class TxFSM:
    states = [
        'sent_TxSelect',
        'sent_TxCommit'
        'sent_TxRollback',
    ]

    transitions = [
        T(event='send_TxSelect',
          # A channel can't start a transaction if it's already
          # holding a transaction or is in confirmation mode.
          source='channel_idle',
          dest='sent_TxSelect'),

        T(event='receive_TxSelectOK',
          source='sent_TxSelect',
          dest='channel_idle_tx'),

        T(event='send_TxCommit',
          # A commit may only happen in a channel holding a transaction.
          source='channel_idle_tx',
          dest='sent_TxCommit'),

        T(event='received_TxCommitOK',
          source='sent_TxCommit',
          dest='channel_idle'),

        T(event='send_TxRollback',
          # A rollback may only happen in a channel holding a transaction.
          source='channel_idle_tx',
          dest='sent_TxRollback'),

        T(event='received_TxRollbackOK',
          source='sent_TxRollback',
          dest='channel_idle'),
    ]


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
