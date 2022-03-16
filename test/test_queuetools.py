import sys
import time

import pytest
from kombu import Connection
from kombu.simple import SimpleQueue

from kombuworker import queuetools as qt


QUEUENAME = "testqueue"
QUEUEURL = "amqp://localhost:5672"


def count_msgs(queue_url: str, queue_name: str) -> int:
    """Also empties the queue."""

    num_fetched = 0
    with Connection(queue_url) as conn:
        queue = conn.SimpleQueue(queue_name)

        while True:
            try:
                msg = queue.get_nowait()
                msg.ack()
                num_fetched += 1
            except SimpleQueue.Empty:
                break

        return num_fetched


def test_insert():
    try:
        nummsg_orig = qt.num_msgs(QUEUEURL, QUEUENAME)
    except RuntimeError:
        # queue doesn't exist yet
        nummsg_orig = 0
    print(f"orig length: {nummsg_orig}", file=sys.stderr)

    payloads = ["task"] * 10
    start_time = time.time()
    qt.insert_msgs(QUEUEURL, QUEUENAME, payloads)
    end_time = time.time()
    print(f"insertion call complete in {end_time-start_time:.3f}s", file=sys.stderr)
    time.sleep(0.1)

    assert count_msgs(QUEUEURL, QUEUENAME) == nummsg_orig + len(payloads)


def test_fetch():
    """Trying to make fetch happen."""
    qt.purge_queue(QUEUEURL, QUEUENAME)

    payloads = ["task"] * 11
    qt.insert_msgs(QUEUEURL, QUEUENAME, payloads)

    num_fetched = 0
    for msg in qt.fetch_msgs(
        QUEUEURL,
        QUEUENAME,
        init_waiting_period=0.1,
        max_waiting_period=1,
        verbose=False,
    ):
        qt.ack_msg(msg)
        num_fetched += 1

    assert num_fetched == len(payloads)
