from __future__ import absolute_import

import os
import time


def env_kafka_version():
    """Return the Kafka version set in the OS environment as a tuple.

    Example: '0.8.1.1' --> (0, 8, 1, 1)
    """
    if 'KAFKA_VERSION' not in os.environ:
        return ()
    return tuple(map(int, os.environ['KAFKA_VERSION'].split('.')))


def assert_message_count(messages, num_messages):
    # Make sure we got them all
    assert len(messages) == num_messages
    # Make sure there are no duplicates
    assert len(set(messages)) == num_messages


class Timer(object):
    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.interval = self.end - self.start
