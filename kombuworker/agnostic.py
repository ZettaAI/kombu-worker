"""Task interface where user packages define how to parse tasks."""
import sys
import json
import time
import signal

from types import SimpleNamespace
from typing import Optional, Callable, List

from . import queuetools as qt

from kombuworker.log import logger as kwlogger


def parse_queue(
    queue_url: str, tool_name: str, queue_name: Optional[str] = None
) -> SimpleNamespace:
    """Parses queue information into a tool-specific sub-queue."""
    q = SimpleNamespace()

    if queue_url.startswith("https://"):  # SQS in disguise?
        queue_url = queue_url.replace("https://", "sqs://")

    q.url = queue_url
    q.name = tool_name if queue_name is None else f"{queue_name}::{tool_name}"

    return q


def insert_task(
    queue_url: str,
    tool_name: str,
    *args: list,
    queue_name: str = None,
    **kwargs: dict,
) -> None:
    """Submits a single task to the desired queue."""
    insert_tasks(queue_url, tool_name, [args], [kwargs], queue_name=queue_name)


def insert_tasks(
    queue_url: str,
    tool_name: str,
    task_args: List[list],
    task_kwargs: List[dict],
    queue_name: str = None,
) -> None:
    """Submits a set of tasks to the desired queue.

    Lengths of task_args and task_kwargs must match.
    """
    assert len(task_args) == len(task_kwargs), "mismatched task_args & task_kwargs"

    q = parse_queue(queue_url, tool_name, queue_name)

    packed = [
        json.dumps(dict(args=args, kwargs=kwargs))
        for (args, kwargs) in zip(task_args, task_kwargs)
    ]

    qt.insert_msgs(q.url, q.name, packed)


def poll(
    queue_url: str,
    tool_name: str,
    task_parser: Callable,
    queue_name: Optional[str] = None,
    init_waiting_period: Optional[int] = 1,
    max_waiting_period: Optional[int] = 60,
    max_num_retries: Optional[int] = 5,
    verbose: Optional[bool] = False,
) -> None:
    """Fetches tasks and executes them.

    Fetches messages from the queue. Parses them using the (tool-defined) parser
    to create tasks, and executes those tasks.
    """
    q = parse_queue(queue_url, tool_name, queue_name)

    global KEEP_LOOPING
    KEEP_LOOPING = True

    def siginthandler(signum, frame):
        global KEEP_LOOPING
        if KEEP_LOOPING:
            kwlogger.info(
                "Interrupted w/ SIGINT."
                " Exiting after this task completes."
                " Interrupt again to exit now.",
            )
            KEEP_LOOPING = False
        else:
            sys.exit()

    def sigtermhandler(signum, frame):

        kwlogger.info("Interrupted w/ SIGTERM. Exiting now.")
        sys.exit()

    prev_siginthandler = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, siginthandler)

    prev_sigtermhandler = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, sigtermhandler)

    it = qt.fetch_msgs(
        q.url,
        q.name,
        init_waiting_period=init_waiting_period,
        max_waiting_period=max_waiting_period,
        max_num_retries=max_num_retries,
        verbose=verbose,
    )

    while KEEP_LOOPING:
        try:
            msg = next(it)
            parsed = json.loads(msg.payload)
            args, kwargs = parsed["args"], parsed["kwargs"]

            task = task_parser(*args, **kwargs)

            start_time = time.time()
            task()
            elapsed = time.time() - start_time

            qt.ack_msg(msg)
            kwlogger.info(f"Task successfully executed in {elapsed:.2f}s")

        except StopIteration:
            break

    # Cleaning up in case fetch_msgs stops naturally
    signal.signal(signal.SIGINT, prev_siginthandler)
    signal.signal(signal.SIGTERM, prev_sigtermhandler)

    it.close()
