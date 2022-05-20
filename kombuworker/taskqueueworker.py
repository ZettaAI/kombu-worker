"""Task queue functionality using python-task-queue's conventions."""
from __future__ import annotations

import sys
import time
import json
import signal
from typing import Union, Iterable, Generator, Optional

from taskqueue.lib import jsonify
from taskqueue.queueables import totask, FunctionTask, RegisteredTask

from . import queuetools as qt

from .log import logger


def insert_tasks(queue_url: str, queue_name: str, tasks: Iterable):
    """Inserts tasks into a queue."""
    payloads = [jsonify(totask(task).payload()) for task in tasks]

    qt.insert_msgs(queue_url, queue_name, payloads)


def fetch_tasks(
    queue_url: str,
    queue_name: str,
    init_waiting_period: Optional[int] = 1,
    max_waiting_period: Optional[int] = 60,
    max_num_retries: Optional[int] = 5,
    verbose: Optional[bool] = False,
) -> Generator[Union[FunctionTask, RegisteredTask], None, None]:
    """Fetches tasks from the queue."""
    it = qt.fetch_msgs(
        queue_url,
        queue_name,
        init_waiting_period=init_waiting_period,
        max_waiting_period=max_waiting_period,
        max_num_retries=max_num_retries,
        verbose=verbose,
    )

    for message in it:
        try:
            yield totask(json.loads(message.payload)), message

        except GeneratorExit:
            it.close()
            return


def poll(
    queue_url: str,
    queue_name: str,
    init_waiting_period: Optional[int] = 1,
    max_waiting_period: Optional[int] = 60,
    max_num_retries: Optional[int] = 5,
    verbose: Optional[bool] = False,
) -> None:
    """Fetches tasks and executes them."""
    global KEEP_LOOPING
    KEEP_LOOPING = True

    def sigint_handler(signum, frame):
        global KEEP_LOOPING
        if KEEP_LOOPING:
            logger.info(
                "Interrupted."
                " Exiting after this task completes."
                " Interrupt again to exit now.",
            )
            KEEP_LOOPING = False
        else:
            sys.exit()

    prev_sigint_handler = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, sigint_handler)

    it = fetch_tasks(
        queue_url,
        queue_name,
        init_waiting_period=init_waiting_period,
        max_waiting_period=max_waiting_period,
        max_num_retries=max_num_retries,
        verbose=verbose,
    )

    while KEEP_LOOPING:
        try:
            task, msg = next(it)

            start_time = time.time()
            task.execute()
            elapsed = time.time() - start_time

            qt.ack_msg(msg)
            logger.info(f"Task successfully executed in {elapsed:.2f}s")

        except StopIteration:
            break

    signal.signal(signal.SIGINT, prev_sigint_handler)
    it.close()
