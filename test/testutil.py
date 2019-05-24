from __future__ import absolute_import

import functools
import operator
import os
import time

import pytest

from test.fixtures import version_str_to_tuple, version as kafka_version


def kafka_versions(*versions):
    """
    Describe the Kafka versions this test is relevant to.

    The versions are passed in as strings, for example:
        '0.11.0'
        '>=0.10.1.0'
        '>0.9', '<1.0'  # since this accepts multiple versions args

    The current KAFKA_VERSION will be evaluated against this version. If the
    result is False, then the test is skipped. Similarly, if KAFKA_VERSION is
    not set the test is skipped.

    Note: For simplicity, this decorator accepts Kafka versions as strings even
    though the similarly functioning `api_version` only accepts tuples. Trying
    to convert it to tuples quickly gets ugly due to mixing operator strings
    alongside version tuples. While doable when one version is passed in, it
    isn't pretty when multiple versions are passed in.
    """

    def construct_lambda(s):
        if s[0].isdigit():
            op_str = '='
            v_str = s
        elif s[1].isdigit():
            op_str = s[0]  # ! < > =
            v_str = s[1:]
        elif s[2].isdigit():
            op_str = s[0:2]  # >= <=
            v_str = s[2:]
        else:
            raise ValueError('Unrecognized kafka version / operator: %s' % (s,))

        op_map = {
            '=': operator.eq,
            '!': operator.ne,
            '>': operator.gt,
            '<': operator.lt,
            '>=': operator.ge,
            '<=': operator.le
        }
        op = op_map[op_str]
        version = version_str_to_tuple(v_str)
        return lambda a: op(a, version)

    validators = map(construct_lambda, versions)

    def real_kafka_versions(func):
        @functools.wraps(func)
        def wrapper(func, *args, **kwargs):
            version = kafka_version()

            if not version:
                pytest.skip("no kafka version set in KAFKA_VERSION env var")

            for f in validators:
                if not f(version):
                    pytest.skip("unsupported kafka version")

            return func(*args, **kwargs)
        return wrapper

    return real_kafka_versions


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
