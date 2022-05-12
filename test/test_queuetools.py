"""Tests for kombuworker/queuetools.py"""
import os
import sys
import time
import signal
import threading

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
        verbose=True,
    ):
        qt.ack_msg(msg)
        num_fetched += 1

    assert num_fetched == len(payloads)


def stub_test_indefinite(rabbitMQurl):
    """Killing an indefinite fetching generator.

    Starts a fetch_msgs generator that should never give us back control,
    but also starts a thread to kill it.

    This currently doesn't work bc pytest enforces its own signal handling.
    """
    it = None

    def sigint_handler(signum, frame):
        print("SIGINT received. Stopping fetch_msgs")
        it.close()

    prev_handler = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, sigint_handler)

    def wait_to_kill():
        time.sleep(1)
        os.kill(os.getpid(), signal.SIGINT)

    th = threading.Thread(target=wait_to_kill)
    th.daemon = True
    th.start()

    it = qt.fetch_msgs(
        rabbitMQurl,
        QUEUENAME,
        init_waiting_period=0.1,
        max_waiting_period=1,
        max_num_retries=None,
        verbose=True,
    )

    signal.signal(signal.SIGINT, prev_handler)
