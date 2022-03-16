import sys
import time

import pytest
import requests
from kombu import Connection
from kombu.simple import SimpleQueue

from kombuworker import queuetools as qt


QUEUENAME = "testqueue"


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


def clear_queue(queue_url: str, queue_name: str) -> None:
    try:
        qt.purge_queue(queue_url, QUEUENAME)
    except requests.exceptions.ConnectionError:
        # queue container not up yet?
        time.sleep(5)


def test_insert(rabbitMQurl):
    clear_queue(rabbitMQurl, QUEUENAME)

    payloads = ["task"] * 10
    start_time = time.time()
    qt.insert_msgs(rabbitMQurl, QUEUENAME, payloads)
    end_time = time.time()
    print(f"insertion call complete in {end_time-start_time:.3f}s", file=sys.stderr)
    time.sleep(0.1)

    assert count_msgs(rabbitMQurl, QUEUENAME) == len(payloads)


def test_fetch(rabbitMQurl):
    """Trying to make fetch happen."""
    clear_queue(rabbitMQurl, QUEUENAME)

    payloads = ["task"] * 11
    qt.insert_msgs(rabbitMQurl, QUEUENAME, payloads)

    num_fetched = 0
    for msg in qt.fetch_msgs(
        rabbitMQurl,
        QUEUENAME,
        init_waiting_period=0.1,
        max_waiting_period=1,
        verbose=False,
    ):
        qt.ack_msg(msg)
        num_fetched += 1

    assert num_fetched == len(payloads)
