"""Tests for kombuworker/queuetools.py"""
import sys
import time

from kombuworker import queuetools as qt
import utils


QUEUENAME = "testqueue"


def test_insert(rabbitMQurl):
    utils.clear_queue(rabbitMQurl, QUEUENAME)

    payloads = ["task"] * 10
    start_time = time.time()
    qt.insert_msgs(rabbitMQurl, QUEUENAME, payloads)
    end_time = time.time()
    print(f"insertion call complete in {end_time-start_time:.3f}s", file=sys.stderr)
    time.sleep(0.1)

    assert utils.count_msgs(rabbitMQurl, QUEUENAME) == len(payloads)


def test_fetch(rabbitMQurl):
    """Trying to make fetch happen."""
    utils.clear_queue(rabbitMQurl, QUEUENAME)

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
