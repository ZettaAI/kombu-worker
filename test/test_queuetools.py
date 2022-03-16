import sys
import time

import pytest

from kombuworker import queuetools as qt


QUEUENAME = "testqueue"
QUEUEURL = "amqp://localhost:5672"


def test_insert():
    try:
        nummsg_orig = qt.num_msgs(QUEUEURL, QUEUENAME)
    except RuntimeError:
        # queue doesn't exist yet
        nummsg_orig = 0

    payloads = ["task"] * 10
    qt.insert_msgs(QUEUEURL, QUEUENAME, payloads)

    time.sleep(10)  # not sure why this takes so long...

    nummsg = qt.num_msgs(QUEUEURL, QUEUENAME)
    assert nummsg == nummsg_orig + len(payloads)


def test_fetch():
    """Trying to make fetch happen."""
    qt.purge_queue(QUEUEURL, QUEUENAME)

    payloads = ["task"] * 11
    qt.insert_msgs(QUEUEURL, QUEUENAME, payloads)

    time.sleep(10)  # not sure why this takes so long...

    num_fetched = 0
    for msg in qt.fetch_msgs(QUEUEURL, QUEUENAME, max_waiting_period=10):
        qt.ack_msg(msg)
        num_fetched += 1

    assert num_fetched == len(payloads)
