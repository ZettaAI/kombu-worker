"""Kombu message queue interface."""
from __future__ import annotations

import time
import queue
import socket
import threading
from enum import Enum
from time import sleep
from typing import Generator, Iterable, Optional

import kombu
import tenacity
from kombu import Connection
from kombu.simple import SimpleQueue

from .log import logger


# Defining retry behavior for submit_task (a few lines down)
retry = tenacity.retry(
    reraise=True,
    stop=tenacity.stop_after_attempt(10),
    wait=tenacity.wait_random_exponential(multiplier=0.5, max=60.0),
)

# Queues for inter-thread communication
# these should only hold one message at most unless someone's messing with them
__REC_THREADQ: queue.Queue = queue.Queue()  # stores received messages
__ACK_THREADQ: queue.Queue = queue.Queue()  # whether to 'ack' the received messages
__DIE_THREADQ: queue.Queue = queue.Queue()  # whether to 'ack' the received messages


def insert_msgs(
    queue_url: str,
    queue_name: str,
    payloads: Iterable,
    connect_timeout: int = 60,
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
    die_threadq: queue.Queue = __DIE_THREADQ,
) -> Generator[kombu.Message, None, None]:
    """Generator for continuously pulling messages from a queue.

    This is the primary interface for pulling tasks.
    """
    th = threading.Thread(
        target=_fetch_thread,
        args=(queue_url, queue_name, rec_threadq, ack_threadq, die_threadq),
        kwargs=dict(verbose=verbose, sleep_interval=init_waiting_period),
    )
    th.daemon = True
    th.start()

    waiting_period = init_waiting_period
    num_tries = 0

    conn = Connection(queue_url)
    queue = conn.SimpleQueue(queue_name)

    while True:
        try:
            msg = rec_threadq.get_nowait()

            if verbose:
                logger.info(f"message received: {msg}")
            waiting_period = init_waiting_period
            num_tries = 0

            yield msg

        except queue.Empty:
            try:
                num_in_queue = queue.qsize()
                if num_in_queue == 0:
                    if verbose:
                        logger.info("queue empty")
                    raise Exception("queue empty")  # increment num_tries
                else:
                    if verbose:
                        logger.info(
                            f"{num_in_queue} messages remain in the queue,"
                            f" sleeping for {waiting_period}s"
                        )

            except Exception:
                num_tries += 1
                if max_num_retries is not None and num_tries > max_num_retries:
                    break

            sleep(waiting_period)
            waiting_period = min(waiting_period * 2, max_waiting_period)

        except GeneratorExit:  # fetch_msgs.close()
            break

    conn.close()
    die_threadq.put("DIE")
    while th.is_alive():
        th.join()


class ThreadState(Enum):
    """A simple state switch for fetch_thread."""

    FETCH = 1
    WAIT = 2


def _fetch_thread(
    queue_url: str,
    queue_name: str,
    rec_threadq: queue.Queue,
    ack_threadq: queue.Queue,
    die_threadq: queue.Queue,
    connect_timeout: int = 120,
    heartbeat_interval: int = 60,
    verbose: bool = False,
    sleep_interval: int = 1,
) -> None:
    """Thread for fetching raw tasks and maintaining a heartbeat."""
    with Connection(
        queue_url, connect_timeout=connect_timeout, heartbeat=10 * heartbeat_interval
    ) as conn:
        queue = conn.SimpleQueue(queue_name)
        state = ThreadState.FETCH
        heartbeat_time = time.time()

        while True:

            if state == ThreadState.FETCH:
                try:
                    fetch_msg(queue, rec_threadq, verbose)
                    state = ThreadState.WAIT

                except SimpleQueue.Empty:
                    conn.heartbeat_check()
                    sleep(sleep_interval)

            elif state == ThreadState.WAIT:
                # delete task from queue if desired
                if not ack_threadq.empty():
                    msg = ack_threadq.get()
                    msg.ack()

                    state = ThreadState.FETCH
                    heartbeat_time = time.time()

                else:
                    if time.time() - heartbeat_time > heartbeat_interval:
                        try:
                            # if there is an event on the connection
                            # this counts as an implicit heartbeat?
                            conn.drain_events(timeout=10)
                        except socket.timeout:
                            conn.heartbeat_check()
                            heartbeat_time = time.time()
                    else:
                        sleep(sleep_interval)

            if not die_threadq.empty():
                # clean up if there's a dangling message
                if not ack_threadq.empty():
                    msg = ack_threadq.get()
                    msg.ack()

                die_threadq.get()
                return


def fetch_msg(
    queue: SimpleQueue,
    rec_threadq: Optional[queue.Queue] = None,
    verbose: bool = False,
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


def print_msg_received(message: kombu.Message) -> None:
    """Prints a simple 'message received' statement with the payload."""
    logger.info(f"Fetched a message from the queue: {message.payload}")


def ack_msg(msg: kombu.Message, ack_threadq: queue.Queue = __ACK_THREADQ) -> None:
    """Adds a message to the ack queue to be ack'ed by the fetch thread."""
    ack_threadq.put(msg)


def purge_queue(queue_url: str, queue_name: str) -> None:
    """Removes all messages from a given queue."""
    with Connection(queue_url) as conn:
        queue = conn.SimpleQueue(queue_name)
        queue.clear()
