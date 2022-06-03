import sys
import setuptools
from setuptools import setup, find_packages


__version__ = "0.0.0"


setup(
    name="kombu-worker",
    version=__version__,
    description=("Some basic utilities to manage AMQP queues"),
    author="Nicholas Turner",
    author_email="nturner@zetta.ai",
    url="https://github.com/ZettaAI/kombu-worker",
    packages=setuptools.find_packages(),
    install_requires=["kombu", "tenacity", "requests"],
    extras_require={
        "test": [
            "pytest",  # potentiall would be good to pin the verision
            "task-queue",
        ]
    },
)
