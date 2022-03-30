# kombu-worker

[![](https://img.shields.io/badge/python-3.7+-blue.svg)](https://www.python.org/downloads) [![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

An interface to AMQP-based message queues (e.g., some RabbitMQ systems) using [kombu](https://github.com/celery/kombu). This repo mostly combines code from [seuron](https://github.com/ZettaAI/seuron) and [python-task-queue](https://github.com/seung-lab/python-task-queue). It is intended to help consolidate connectomics processing steps into seuron to create a more streamlined system.

## Installation

```bash
pip install git+https://github.com/ZettaAI/kombu-worker
```

## Usage

There are two intended interfaces to use these tools. 

#### taskqueueworker
The `taskqueueworker` interface abstracts most details of working with the queue, and replicates [python-task-queue](https://github.com/seung-lab/python-task-queue) functions.

```python
from kombuworker import taskqueueworker as tqw
from taskqueue import queueable

@queueable
def queueable_task(*args, **kwargs):
    pass

tasks = [partial(queueable_task, *args, **kwargs)]

# Generation/Manager
tqw.insert_tasks(queueurl, queuename, tasks)

# Worker node
tqw.poll(queueurl, queuename)
```

Note that, different from other systems like SQS, AMQP-based queues are specified by two fields: a URL to the queue host (or "exchange"), and a name to identify a specific queue within that exchange. Also note that this polling function has a few parameters for handling queues that are currently empty. Set `max_num_retries` to a high value or `np.inf` if you'd like the workers to persist indefinitely.

#### queuetools
A user can also work more directly with the raw messages within the AMQP queue using this interface. The `taskqueueworker` functions wrap around these functions, and serve as easy guides for how to handle the `queuetools` functions. For example, see `taskqueueworker.fetch_tasks` for a nice way to use the `queuetools.fetch_messages` generator.
