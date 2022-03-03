"""Kombu worker interface.

Will split this into intelligible modules later.
"""
import queue
import socket
import requests
import threading
from enum import Enum
from time import sleep
from typing import Iterable
from urllib.parse import urlparse

import tenacity
from kombu import Connection
from kombu.simple import SimpleQueue


# Defining retry behavior
retry = tenacity.retry(
            reraise=True, stop=tenacity.stop_after_attempt(10),
            wait=tenacity.wait_random_exponential(multiplier=0.5, max=60.0))

# acknowledged flag
ACK = None
MSG_QUEUE = queue.Queue()
CMD_QUEUE = queue.Queue()


@retry
def submit_task(queue: SimpleQueue, payload: str) -> None:
    queue.put(payload)


def insert_tasks(queue_url: str,
                 queue_name: str,
                 tasks: Iterable,
                 connect_timeout: int = 60
                 ) -> None:
    """Inserts tasks into a queue."""
    with Connection(queue_name, connect_timeout=connect_timeout) as conn:
        queue = conn.SimpleQueue(queue_name)
        for task in tasks:
            submit_task(queue, task)


def remaining_tasks(queue_url: str,
                    queue_name: str,
                    username: str = "guest",
                    password: str = "guest") -> int:
    """Determines how many tasks are left in a queue."""
    rq_host = urlparse(queue_name).netloc.split(":")[0]

    ret = requests.get(
            f"http://{rq_host}:15672/apu/queues/%2f/{queue_name}",
            auth=(username, password))

    if not ret.ok:
        raise RuntimeError(f"Cannot fetch information about {queue_name}"
                           " from rabbitmq management interface")

    queue_status = ret.json()

    return int(queue_status["messages"])


class ThreadState(Enum):
    """Simple state switch for fetch_thread."""
    FETCH = 1
    WAIT = 2


def fetch_thread(queue_url: str,
                 queue_name: str,
                 msg_queue: queue.Queue,
                 cmd_queue: queue.Queue,
                 connect_timeout: int = 60,
                 heartbeat: int = 600,
                 ) -> None:
    """Thread for fetching raw tasks and maintaining heartbeat."""
    with Connection(
            queue_url,
            connect_timeout=connect_timeout,
            heartbeat=heartbeat) as conn:
        queue = conn.SimpleQueue(queue_name)
        msg = ""
        state = ThreadState.FETCH
        heartbeat_cycle = 0

        while True:
            if state == ThreadState.FETCH:
                try:
                    msg = fetch_message(queue, conn, msg_queue)
                    state = ThreadState.WAIT

                except SimpleQueue.Empty:
                    conn.heartbeat_check()
                    sleep(10)
                    continue

            elif state == ThreadState.WAIT:
                # delete task from queue if desired
                if not cmd_queue.empty():
                    cmd = cmd_queue.get()
                    if cmd == ACK:
                        msg.ack()
                        state = ThreadState.FETCH
                        heartbeat_cycle = 0

                else:
                    # not sure how the heartbeat mechanics work yet
                    heartbeat_cycle += 1
                    if heartbeat_cycle % 60 == 0:
                        try:
                            # not sure what this is doing
                            conn.drain_events(timeout=10)
                        except socket.timeout:
                            conn.heartbeat_check()
                    else:
                        sleep(1)


def fetch_message(queue: SimpleQueue,
                  conn: Connection,
                  msg_queue: queue.Queue):
    pass


class Task:
    pass


def fetch_tasks(queue_url: str,
                queue_name: str,
                visibility_timeout: int,
                retry_times: int
                ) -> Task:
    """Generator for pulling tasks from a queue.

    This is the primary interface for pulling tasks.
    """
    th = threading.Thread(target=fetch_thread,
                          args=(queue_url, MSG_QUEUE, CMD_QUEUE))
    th.daemon = True
    th.start()

    waiting_period = 1
    num_tries = 0

    while True:
        try:
            msg = MSG_QUEUE.get_nowait()
        except queue.Empty:
            try:
                num_tasks = remaining_tasks(queue_url)
                if num_tasks == 0:
                    break
                else:
                    print(f"{num_tasks} tasks remain in the queue,"
                          f" sleeping for {waiting_period}s")

            except Exception:
                num_tries += 1
                if num_tries > retry_times:
                    break

            sleep(waiting_period)
            waiting_period = min(waiting_period * 2, 60)
            continue

        print(f"message received: {msg}")
        waiting_period = 1
        num_tries = 0

        yield Task(msg)


def delete_tasks(tasks):
    """Deletes a set of tasks from the queue."""
    for task in tasks:
        task.queue.put(ACK)
        print(f"deleted task {task.handle} in queue")
