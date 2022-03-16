"""Tests for kombuworker/taskqueueworker.py"""
import os
from functools import partial
from taskqueue import queueable

from kombuworker import taskqueueworker as tqw
from kombuworker import queuetools as qt
import utils


QUEUENAME = "taskqueueworker"
DUMMYDIR = "test/dummy_files"


@queueable
def dummy_task(i):
    return i


@queueable
def dummy_side_effect(i):
    os.makedirs(DUMMYDIR, exist_ok=True)
    with open(os.path.join(DUMMYDIR, str(i)), "w+") as f:
        f.write(str(i))


def test_insert_tasks(rabbitMQurl):
    utils.clear_queue(rabbitMQurl, QUEUENAME)

    tasks = [partial(dummy_task, i) for i in range(20)]
    tqw.insert_tasks(rabbitMQurl, QUEUENAME, tasks)

    assert utils.count_msgs(rabbitMQurl, QUEUENAME) == len(tasks)


def test_fetch_tasks(rabbitMQurl):
    utils.clear_queue(rabbitMQurl, QUEUENAME)

    ids = set(range(10))
    tasks = [partial(dummy_task, i) for i in ids]
    tqw.insert_tasks(rabbitMQurl, QUEUENAME, tasks)

    results = set()
    for task, msg in tqw.fetch_tasks(
        rabbitMQurl,
        QUEUENAME,
        init_waiting_period=0.1,
        max_waiting_period=1,
        verbose=False,
    ):
        results.add(task())
        qt.ack_msg(msg)

    assert results == ids


def test_poll_side_effects(rabbitMQurl):
    utils.clear_queue(rabbitMQurl, QUEUENAME)

    ids = set(range(10))
    tasks = [partial(dummy_side_effect, i) for i in ids]
    tqw.insert_tasks(rabbitMQurl, QUEUENAME, tasks)

    tqw.poll(
        rabbitMQurl,
        QUEUENAME,
        init_waiting_period=0.1,
        max_waiting_period=1,
        verbose=False,
    )

    for i in ids:
        filename = os.path.join(DUMMYDIR, str(i))
        assert os.path.exists(filename), f"{filename} doesn't exist"
        os.remove(filename)

    os.rmdir(DUMMYDIR)
