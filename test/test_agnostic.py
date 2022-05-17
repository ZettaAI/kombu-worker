"""Tests for kombuworker/taskqueueworker.py"""
import os

from kombuworker import agnostic as ag
import utils


DUMMYDIR = "test/dummy_files"


def dummy_side_effect(i):
    os.makedirs(DUMMYDIR, exist_ok=True)
    with open(os.path.join(DUMMYDIR, str(i)), "w+") as f:
        f.write(str(i))


def test_parse_queue():
    assert ag.parse_queue("sqs://fake_queue", "test").url.startswith("sqs")
    assert ag.parse_queue("https://fake_queue", "test").url.startswith("sqs")
    assert ag.parse_queue("amqp://fake_queue", "test").url.startswith("amqp")
    assert ag.parse_queue("sqs://fake_queue", "test", "name").name == "name::test"
    assert ag.parse_queue("sqs://fake_queue", "test").name == "test"


def test_insert_tasks(rabbitMQurl):
    tool_name = "pytest"
    queue_name = "agnostic"
    q = ag.parse_queue(rabbitMQurl, tool_name, queue_name)

    utils.clear_queue(q.url, q.name)

    ag.insert_task(rabbitMQurl, tool_name, "a", b=1, queue_name=queue_name)
    assert utils.count_msgs(q.url, q.name) == 1

    task_args = [["a1"], ["a2"]]
    task_kwargs = [{"b1": 1}, {"b2": 2}]

    ag.insert_tasks(
        rabbitMQurl, tool_name, task_args, task_kwargs, queue_name=queue_name
    )

    assert utils.count_msgs(q.url, q.name) == len(task_args)


def test_poll_side_effects(rabbitMQurl):
    tool_name = "pytest"
    q = ag.parse_queue(rabbitMQurl, tool_name)

    utils.clear_queue(q.url, q.name)

    ids = range(10)
    task_args = [[i] for i in ids]
    task_kwargs = [{} for i in ids]
    ag.insert_tasks(rabbitMQurl, tool_name, task_args, task_kwargs)

    def task_parser(i: int):
        """Returns a function that runs the dummy_side_effect above."""

        def fn():
            dummy_side_effect(i)

        return fn

    ag.poll(
        rabbitMQurl,
        tool_name,
        task_parser,
        init_waiting_period=0.1,
        max_waiting_period=1,
        verbose=True,
    )

    for i in ids:
        filename = os.path.join(DUMMYDIR, str(i))
        assert os.path.exists(filename), f"{filename} doesn't exist"
        os.remove(filename)

    os.rmdir(DUMMYDIR)
