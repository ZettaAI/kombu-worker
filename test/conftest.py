"""Setup and teardown. Starts a RabbitMQ docker container.

Starting the container works, but it often refuses connections for some reason.
Maybe I'm not waiting long enough?
"""
import time
import pytest
import signal
import subprocess


@pytest.fixture(scope="session")
def rabbitMQurl():
    """Starts a RabbitMQ docker container and tears it down."""
    p = subprocess.Popen(
        [
            "docker",
            "run",
            "--rm",
            "--name",
            "rabbitmq",
            "-p",
            "5672:5672",
            "-p",
            "15672:15672",
            "rabbitmq:3.8-management",
        ]
    )
    time.sleep(5)  # give the queue some time to start

    yield "amqp://localhost:5672"

    kill_subprocess(p)


@pytest.fixture(scope="session")
def SQSurl():
    """Starts an ElasticMQ docker container and tears it down."""
    p = subprocess.Popen(
        [
            "docker",
            "run",
            "--rm",
            "--name",
            "elasticmq",
            "-p",
            "9324:9324",
            "-p",
            "9325:9325",
            "softwaremill/elasticmq-native",
        ]
    )
    time.sleep(0.5)

    yield "sqs://localhost:9324"

    kill_subprocess(p)


def kill_subprocess(p: subprocess.Popen) -> None:
    # Being a bit obsessive here
    retries = 10
    while retries > 0:
        try:
            p.send_signal(signal.SIGINT)
            p.wait(1)
            return
        except subprocess.TimeoutExpired:
            retries -= 1

    if p.returncode is None:
        p.terminate()
        p.wait(5)

    if p.returncode is None:
        p.kill()
        p.wait(5)

    return
