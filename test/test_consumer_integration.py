import logging
import time

from mock import patch
import pytest
from kafka.vendor import six
from kafka.vendor.six.moves import range

import kafka.codec
from kafka.errors import (
     KafkaTimeoutError, UnsupportedCodecError, UnsupportedVersionError
)
from kafka.structs import TopicPartition, OffsetAndTimestamp

from test.fixtures import random_string
from test.testutil import env_kafka_version, assert_message_count, Timer


@pytest.mark.skipif(not env_kafka_version(), reason="No KAFKA_VERSION set")
def test_kafka_consumer(kafka_producer, topic, kafka_consumer_factory, send_messages):
    """Test KafkaConsumer"""
    kafka_consumer = kafka_consumer_factory(auto_offset_reset='earliest')

    # TODO replace this with a `send_messages()` pytest fixture
    # as we will likely need this elsewhere
    send_messages(range(0, 100))

    for i in range(0, 100):
        kafka_producer.send(topic, partition=0, value=str(i).encode())
    for i in range(100, 200):
        kafka_producer.send(topic, partition=1, value=str(i).encode())
    kafka_producer.flush()

    cnt = 0
    messages = {0: set(), 1: set()}
    for message in kafka_consumer:
        logging.debug("Consumed message %s", repr(message))
        cnt += 1
        messages[message.partition].add(message.offset)
        if cnt >= 200:
            break

    assert len(messages[0]) == 100
    assert len(messages[1]) == 100
    kafka_consumer.close()


@pytest.mark.skipif(not env_kafka_version(), reason="No KAFKA_VERSION set")
def test_kafka_consumer_unsupported_encoding(
        topic, kafka_producer_factory, kafka_consumer_factory):
    # Send a compressed message
    producer = kafka_producer_factory(compression_type="gzip")
    fut = producer.send(topic, b"simple message" * 200)
    fut.get(timeout=5)
    producer.close()

    # Consume, but with the related compression codec not available
    with patch.object(kafka.codec, "has_gzip") as mocked:
        mocked.return_value = False
        consumer = kafka_consumer_factory(auto_offset_reset='earliest')
        error_msg = "Libraries for gzip compression codec not found"
        with pytest.raises(UnsupportedCodecError, match=error_msg):
            consumer.poll(timeout_ms=2000)


def test_kafka_consumer__blocking():
    TIMEOUT_MS = 500
    consumer = self.kafka_consumer(auto_offset_reset='earliest',
                                    enable_auto_commit=False,
                                    consumer_timeout_ms=TIMEOUT_MS)

    # Manual assignment avoids overhead of consumer group mgmt
    consumer.unsubscribe()
    consumer.assign([TopicPartition(self.topic, 0)])

    # Ask for 5 messages, nothing in queue, block 500ms
    with Timer() as t:
        with pytest.raises(StopIteration):
            msg = next(consumer)
    assert t.interval >= (TIMEOUT_MS / 1000.0)

    self.send_messages(0, range(0, 10))

    # Ask for 5 messages, 10 in queue. Get 5 back, no blocking
    messages = set()
    with Timer() as t:
        for i in range(5):
            msg = next(consumer)
            messages.add((msg.partition, msg.offset))
    assert len(messages) == 5
    assert t.interval < (TIMEOUT_MS / 1000.0)

    # Ask for 10 messages, get 5 back, block 500ms
    messages = set()
    with Timer() as t:
        with pytest.raises(StopIteration):
            for i in range(10):
                msg = next(consumer)
                messages.add((msg.partition, msg.offset))
    assert len(messages) == 5
    assert t.interval >= (TIMEOUT_MS / 1000.0)
    consumer.close()


@pytest.mark.skipif(env_kafka_version() < (0, 8, 1), reason="Requires KAFKA_VERSION >= 0.8.1")
def test_kafka_consumer__offset_commit_resume():
    GROUP_ID = random_string(10)

    self.send_messages(0, range(0, 100))
    self.send_messages(1, range(100, 200))

    # Start a consumer
    consumer1 = self.kafka_consumer(
        group_id=GROUP_ID,
        enable_auto_commit=True,
        auto_commit_interval_ms=100,
        auto_offset_reset='earliest',
    )

    # Grab the first 180 messages
    output_msgs1 = []
    for _ in range(180):
        m = next(consumer1)
        output_msgs1.append(m)
    assert_message_count(output_msgs1, 180)
    consumer1.close()

    # The total offset across both partitions should be at 180
    consumer2 = self.kafka_consumer(
        group_id=GROUP_ID,
        enable_auto_commit=True,
        auto_commit_interval_ms=100,
        auto_offset_reset='earliest',
    )

    # 181-200
    output_msgs2 = []
    for _ in range(20):
        m = next(consumer2)
        output_msgs2.append(m)
    assert_message_count(output_msgs2, 20)
    assert len(set(output_msgs1) | set(output_msgs2)) == 200
    consumer2.close()


@pytest.mark.skipif(env_kafka_version() < (0, 10, 1), reason="Requires KAFKA_VERSION >= 0.10.1")
def test_kafka_consumer_max_bytes_simple():
    self.send_messages(0, range(100, 200))
    self.send_messages(1, range(200, 300))

    # Start a consumer
    consumer = self.kafka_consumer(
        auto_offset_reset='earliest', fetch_max_bytes=300)
    seen_partitions = set([])
    for i in range(10):
        poll_res = consumer.poll(timeout_ms=100)
        for partition, msgs in six.iteritems(poll_res):
            for msg in msgs:
                seen_partitions.add(partition)

    # Check that we fetched at least 1 message from both partitions
    assert seen_partitions == set(TopicPartition(self.topic, 0), TopicPartition(self.topic, 1))
    consumer.close()

@pytest.mark.skipif(env_kafka_version() < (0, 10, 1), reason="Requires KAFKA_VERSION >= 0.10.1")
def test_kafka_consumer_max_bytes_one_msg():
    # We send to only 1 partition so we don't have parallel requests to 2
    # nodes for data.
    self.send_messages(0, range(100, 200))

    # Start a consumer. FetchResponse_v3 should always include at least 1
    # full msg, so by setting fetch_max_bytes=1 we should get 1 msg at a time
    # But 0.11.0.0 returns 1 MessageSet at a time when the messages are
    # stored in the new v2 format by the broker.
    #
    # DP Note: This is a strange test. The consumer shouldn't care
    # how many messages are included in a FetchResponse, as long as it is
    # non-zero. I would not mind if we deleted this test. It caused
    # a minor headache when testing 0.11.0.0.
    group = 'test-kafka-consumer-max-bytes-one-msg-' + random_string(5)
    consumer = self.kafka_consumer(
        group_id=group,
        auto_offset_reset='earliest',
        consumer_timeout_ms=5000,
        fetch_max_bytes=1)

    fetched_msgs = [next(consumer) for i in range(10)]
    assert len(fetched_msgs) == 10
    consumer.close()


@pytest.mark.skipif(env_kafka_version() < (0, 10, 1), reason="Requires KAFKA_VERSION >= 0.10.1")
def test_kafka_consumer_offsets_for_time():
    late_time = int(time.time()) * 1000
    middle_time = late_time - 1000
    early_time = late_time - 2000
    tp = TopicPartition(self.topic, 0)

    timeout = 10
    kafka_producer = self.kafka_producer()
    early_msg = kafka_producer.send(
        self.topic, partition=0, value=b"first",
        timestamp_ms=early_time).get(timeout)
    late_msg = kafka_producer.send(
        self.topic, partition=0, value=b"last",
        timestamp_ms=late_time).get(timeout)

    consumer = self.kafka_consumer()
    offsets = consumer.offsets_for_times({tp: early_time})
    assert len(offsets) == 1
    assert offsets[tp].offset == early_msg.offset
    assert offsets[tp].timestamp == early_time

    offsets = consumer.offsets_for_times({tp: middle_time})
    assert offsets[tp].offset == late_msg.offset
    assert offsets[tp].timestamp == late_time

    offsets = consumer.offsets_for_times({tp: late_time})
    assert offsets[tp].offset == late_msg.offset
    assert offsets[tp].timestamp == late_time

    offsets = consumer.offsets_for_times({})
    assert offsets == {}

    # Out of bound timestamps check

    offsets = consumer.offsets_for_times({tp: 0})
    assert offsets[tp].offset == early_msg.offset
    assert offsets[tp].timestamp == early_time

    offsets = consumer.offsets_for_times({tp: 9999999999999})
    assert offsets[tp] is None

    # Beginning/End offsets

    offsets = consumer.beginning_offsets([tp])
    assert offsets == {tp: early_msg.offset}
    offsets = consumer.end_offsets([tp])
    assert offsets == {tp: late_msg.offset + 1}
    consumer.close()


@pytest.mark.skipif(env_kafka_version() < (0, 10, 1), reason="Requires KAFKA_VERSION >= 0.10.1")
def test_kafka_consumer_offsets_search_many_partitions():
    tp0 = TopicPartition(self.topic, 0)
    tp1 = TopicPartition(self.topic, 1)

    kafka_producer = self.kafka_producer()
    send_time = int(time.time() * 1000)
    timeout = 10
    p0msg = kafka_producer.send(
        self.topic, partition=0, value=b"XXX",
        timestamp_ms=send_time).get(timeout)
    p1msg = kafka_producer.send(
        self.topic, partition=1, value=b"XXX",
        timestamp_ms=send_time).get(timeout)

    consumer = self.kafka_consumer()
    offsets = consumer.offsets_for_times({
        tp0: send_time,
        tp1: send_time
    })

    assert offsets == {
        tp0: OffsetAndTimestamp(p0msg.offset, send_time),
        tp1: OffsetAndTimestamp(p1msg.offset, send_time)
    }

    offsets = consumer.beginning_offsets([tp0, tp1])
    assert offsets == {
        tp0: p0msg.offset,
        tp1: p1msg.offset
    }

    offsets = consumer.end_offsets([tp0, tp1])
    assert offsets == {
        tp0: p0msg.offset + 1,
        tp1: p1msg.offset + 1
    }
    consumer.close()


@pytest.mark.skipif(env_kafka_version() < (0, 10, 1), reason="Requires KAFKA_VERSION >= 0.10.1")
def test_kafka_consumer_offsets_for_time_old():
    consumer = self.kafka_consumer()
    tp = TopicPartition(self.topic, 0)

    with pytest.raises(UnsupportedVersionError):
        consumer.offsets_for_times({tp: int(time.time())})


@pytest.mark.skipif(env_kafka_version() < (0, 10, 1), reason="Requires KAFKA_VERSION >= 0.10.1")
def test_kafka_consumer_offsets_for_times_errors():
    consumer = self.kafka_consumer(fetch_max_wait_ms=200,
                                    request_timeout_ms=500)
    tp = TopicPartition(self.topic, 0)
    bad_tp = TopicPartition(self.topic, 100)

    with pytest.raises(ValueError):
        consumer.offsets_for_times({tp: -1})

    with pytest.raises(KafkaTimeoutError):
        consumer.offsets_for_times({bad_tp: 0})
