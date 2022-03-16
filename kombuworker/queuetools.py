"""Kombu message queue interface."""
from __future__ import annotations

import queue
import socket
import threading
from enum import Enum
from time import sleep
from typing import Generator, Iterable, Optional
from urllib.parse import urlparse

import kombu
import requests
import tenacity
from kombu import Connection
from kombu.simple import SimpleQueue


# Defining retry behavior for submit_task (a few lines down)
retry = tenacity.retry(
    reraise=True,
    stop=tenacity.stop_after_attempt(10),
    wait=tenacity.wait_random_exponential(multiplier=0.5, max=60.0),
)

# Queues for inter-thread communication
__REC_THREADQ = queue.Queue()  # stores received messages
__ACK_THREADQ = queue.Queue()  # whether to 'ack' the received messages
# arbitrary flag for thread communication above
ACK = None


def insert_msgs(
    queue_url: str, queue_name: str, payloads: Iterable, connect_timeout: int = 60
) -> None:
    """Inserts multiple messages into a queue."""
    with Connection(queue_url, connect_timeout=connect_timeout) as conn:
        queue = conn.SimpleQueue(queue_name)
        for payload in payloads:
            submit_msg(queue, payload)


@retry
def submit_msg(queue: SimpleQueue, payload: str) -> None:
    queue.put(payload)


def fetch_msgs(
    queue_url: str,
    queue_name: str,
    init_waiting_period: int = 1,
    max_waiting_period: int = 60,
    max_num_retries: int = 5,
    verbose: bool = False,
    rec_threadq: queue.Queue = __REC_THREADQ,
    ack_threadq: queue.Queue = __ACK_THREADQ,
) -> Generator[kombu.Message, None, None]:
    """Generator for continuously pulling messages from a queue.

    This is the primary interface for pulling tasks.
    """
    th = threading.Thread(
        target=_fetch_thread,
        args=(queue_url, queue_name, rec_threadq, ack_threadq),
        kwargs=dict(verbose=verbose),
    )
    th.daemon = True
    th.start()

    waiting_period = init_waiting_period
    num_tries = 0

    while True:
        try:
            msg = rec_threadq.get_nowait()

            print(f"message received: {msg}")
            waiting_period = init_waiting_period
            num_tries = 0

            yield msg

        except queue.Empty:
            try:
                num_in_queue = num_msgs(queue_url)
                if num_in_queue == 0:
                    break
                else:
                    print(
                        f"{num_in_queue} tasks remain in the queue,"
                        f" sleeping for {waiting_period}s"
                    )

            except Exception:
                num_tries += 1
                if num_tries > max_num_retries:
                    break

            sleep(waiting_period)
            waiting_period = min(waiting_period * 2, max_waiting_period)


class ThreadState(Enum):
    """A simple state switch for fetch_thread."""

    FETCH = 1
    WAIT = 2


def _fetch_thread(
    queue_url: str,
    queue_name: str,
    rec_threadq: queue.Queue,
    ack_threadq: queue.Queue,
    connect_timeout: int = 60,
    heartbeat: int = 600,
    verbose: bool = False,
) -> None:
    """Thread for fetching raw tasks and maintaining a heartbeat."""
    with Connection(
        queue_url, connect_timeout=connect_timeout, heartbeat=heartbeat
    ) as conn:
        queue = conn.SimpleQueue(queue_name)
        state = ThreadState.FETCH
        heartbeat_cycle = 0

        while True:
            if state == ThreadState.FETCH:
                try:
                    fetch_msg(queue, rec_threadq, verbose)
                    state = ThreadState.WAIT

                except SimpleQueue.Empty:
                    conn.heartbeat_check()
                    sleep(10)

            elif state == ThreadState.WAIT:
                # delete task from queue if desired
                if not ack_threadq.empty():
                    ackmsg = ack_threadq.get()
                    ackmsg.ack()

                    state = ThreadState.FETCH
                    heartbeat_cycle = 0

                else:
                    # these count seconds since we sleep for 1s below
                    heartbeat_cycle += 1
                    if heartbeat_cycle % 60 == 0:
                        try:
                            # if there is an event on the connection
                            # this counts as an implicit heartbeat?
                            conn.drain_events(timeout=10)
                        except socket.timeout:
                            conn.heartbeat_check()
                    else:
                        sleep(1)


def fetch_msg(
    queue: SimpleQueue, rec_threadq: Optional[queue.Queue] = None, verbose: bool = False
):
    """Moves a message payload from the remote queue to the local thread queue.

    This may be useful for pulling messages in an ad-hoc fashion. Requires a
    previously-created SimpleQueue.

    Args:
        queue: The message queue
        rec_threadq: A thread-communication queue to add the message to.
            It's probably best to ignore the returned message if this option
            is used.
        verbose: Whether to report when it receives messages for logging.

    Raises:
        kombu.simple.SimpleQueue.Empty: if the remote queue is empty.
    """
    msg = queue.get_nowait()

    if verbose:
        print_msg_received(msg)

    if rec_threadq is not None:
        rec_threadq.put(msg)

    return msg


def num_msgs(
    queue_url: str, queue_name: str, username: str = "guest", password: str = "guest"
) -> int:
    """Determines how many messages are left in a queue."""
    rq_host = urlparse(queue_url).netloc.split(":")[0]

    ret = requests.get(
        f"http://{rq_host}:15672/api/queues/%2f/{queue_name}", auth=(username, password)
    )

    if not ret.ok:
        raise RuntimeError(
            f"Cannot fetch information about {queue_name}"
            " from rabbitmq management interface"
        )

    return int(ret.json()["messages"])


def print_msg_received(message) -> None:
    """Prints a simple 'message received' statement with the payload."""
    print(f"Fetched a message from the queue: {message.payload}")


def ack_msg(msg: kombu.Message, ack_threadq: queue.Queue = __ACK_THREADQ) -> None:
    """Adds a message to the ack queue to be ack'ed by the fetch thread."""
    ack_threadq.put(msg)


def purge_queue(queue_url: str, queue_name: str) -> None:
    """Removes all messages from a given queue."""
    with Connection(queue_url) as conn:
        queue = conn.SimpleQueue(queue_name)
        queue.clear()
