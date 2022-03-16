"""An interactive script for testing the proper SIGINT handling."""
import time
from functools import partial

from taskqueue import queueable

from kombuworker import taskqueueworker as tqw
import utils


@queueable
def dummy_task():
    print("sleeping")
    time.sleep(1)


def main():
    queue_url = "amqp://localhost:5672"
    queue_name = "sigint"

    utils.clear_queue(queue_url, queue_name)
    tasks = [partial(dummy_task) for _ in range(100)]
    tqw.insert_tasks(queue_url, queue_name, tasks)

    tqw.poll(queue_url, queue_name)

    utils.clear_queue(queue_url, queue_name)


if __name__ == "__main__":
    main()
