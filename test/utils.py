"""A couple utilities that account for the docker container's startup."""
import time

import requests
from kombu import Connection

from kombuworker import queuetools as qt


def count_msgs(queue_url: str, queue_name: str) -> int:
    """Counts the number of remaining (visible) messages in a queue.

    Also empties the queue.
    """
    with Connection(queue_url) as conn:
        queue = conn.SimpleQueue(queue_name)

        size = queue.qsize()
        queue.clear()

        return size


def clear_queue(queue_url: str, queue_name: str) -> None:
    """Purges a queue and retries if the container isn't up yet."""
    try:
        qt.purge_queue(queue_url, queue_name)
    except requests.exceptions.ConnectionError:
        # queue container not up yet?
        time.sleep(5)
