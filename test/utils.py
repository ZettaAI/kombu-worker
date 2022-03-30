"""A couple utilities that account for the docker container's startup."""
import time

import requests
from kombu import Connection
from kombu.simple import SimpleQueue

from kombuworker import queuetools as qt


def count_msgs(queue_url: str, queue_name: str) -> int:
    """Counts the number of remaining messages in a queue.

    Also empties the queue.
    """
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
    """Purges a queue and retries if the container isn't up yet."""
    try:
        qt.purge_queue(queue_url, queue_name)
    except requests.exceptions.ConnectionError:
        # queue container not up yet?
        time.sleep(5)
