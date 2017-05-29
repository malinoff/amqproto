import time

import pytest

from amqproto import protocol


def test_missed_heartbeats(ready_connection):
    assert ready_connection.check_heartbeats()

    # Surely skip at least 2 heartbeats
    time.sleep(2.5 * ready_connection.properties['heartbeat'])

    with pytest.raises(protocol.HardError):
        ready_connection.check_heartbeats()


def test_sending_heartbeats(ready_connection):
    heartbeat = ready_connection.properties['heartbeat']
    for i in range(3):
        ready_connection.send_heartbeat()
        time.sleep(i * heartbeat)

    prev_sent = ready_connection._heartbeater._previous_heartbeat_sent
    assert (time.monotonic() - prev_sent) > heartbeat
