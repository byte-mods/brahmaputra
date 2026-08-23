#!/usr/bin/env python3
"""Manual end-to-end check of the Python driver against a live broker.

    brahmaputra-server --data-dir ./data --default-partitions 4
    python test_manual.py [host] [port]

Every check asserts a property of the *system*, not that a function ran:
records come back byte-identical, keys pin partitions, headers survive,
offsets are contiguous, a group splits partitions and resumes from its
commit. It exits non-zero on the first failure.
"""

from __future__ import annotations

import sys
import time
import uuid

sys.path.insert(0, ".")

from brahmaputra import (  # noqa: E402
    Assignor,
    AutoOffsetReset,
    Compression,
    Consumer,
    ConsumerConfig,
    EARLIEST,
    GroupConfig,
    GroupConsumer,
    LATEST,
    NoOffsetForPartition,
    Producer,
    ProducerConfig,
    RecordHeader,
    murmur2,
    partition_for_key,
)

HOST = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 9092

PASSED = 0
FAILED = 0


def check(name: str, condition: bool, detail: str = "") -> None:
    global PASSED, FAILED
    if condition:
        PASSED += 1
        print(f"  ok   {name}")
    else:
        FAILED += 1
        print(f"  FAIL {name}{': ' + detail if detail else ''}")


def section(title: str) -> None:
    print(f"\n{title}")


def unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def main() -> int:
    section("connection and metadata")
    with Consumer(HOST, PORT) as consumer:
        versions = consumer.router.seed.api_versions()
        check("ApiVersions answers", len(versions) > 0, f"got {len(versions)} apis")
        metadata = consumer.router.metadata()
        check("metadata lists brokers", len(metadata.brokers) >= 1)

    section("produce and consume round trip")
    topic = unique("py-roundtrip")
    payloads = [f"record-{i}".encode() for i in range(50)]
    with Producer(HOST, PORT, ProducerConfig(linger_ms=0, compression_type="none")) as producer:
        for payload in payloads:
            producer.send(topic, payload, partition=0)
        producer.flush()

    with Consumer(HOST, PORT) as consumer:
        got = consumer.fetch(topic, 0, 0)
        check("every record comes back", len(got) == len(payloads), f"got {len(got)}")
        check(
            "values are byte-identical",
            [r.value for r in got] == payloads,
            "payload mismatch",
        )
        check(
            "offsets are contiguous from zero",
            [r.offset for r in got] == list(range(len(payloads))),
        )

    section("compression codecs")
    for codec in ["none", "lz4", "zstd", "snappy", "gzip"]:
        codec_topic = unique(f"py-{codec}")
        # Repetitive payload, so a codec that silently does nothing still
        # round-trips but a broken one corrupts.
        body = (b"the same line over and over. " * 40)
        try:
            with Producer(
                HOST, PORT, ProducerConfig(linger_ms=0, compression_type=codec)
            ) as producer:
                for index in range(20):
                    producer.send(codec_topic, body + str(index).encode(), partition=0)
                producer.flush()
            with Consumer(HOST, PORT) as consumer:
                got = consumer.fetch(codec_topic, 0, 0)
            check(
                f"{codec}: round trips",
                len(got) == 20 and got[0].value == body + b"0",
                f"got {len(got)} records",
            )
        except Exception as error:  # noqa: BLE001
            # A missing optional dependency is a skip, not a failure: the
            # driver is correct, the environment just lacks the codec.
            if "pip install" in str(error):
                print(f"  skip {codec}: {error}")
            else:
                check(f"{codec}: round trips", False, str(error))

    section("keys, partitioning and ordering")
    key_topic = unique("py-keys")
    with Producer(HOST, PORT, ProducerConfig(linger_ms=0, compression_type="none")) as producer:
        partitions = producer._router.partitions(key_topic)
        for index in range(30):
            producer.send(key_topic, f"v{index}".encode(), key=b"user-7")
        producer.flush()
    target = partition_for_key(b"user-7", partitions)
    with Consumer(HOST, PORT) as consumer:
        on_target = consumer.fetch(key_topic, target, 0)
        check(
            "a key pins every record to one partition",
            len(on_target) == 30,
            f"partition {target} holds {len(on_target)} of 30",
        )
        check(
            "per-key order is preserved",
            [r.value for r in on_target] == [f"v{i}".encode() for i in range(30)],
        )
        elsewhere = sum(
            len(consumer.fetch(key_topic, p, 0)) for p in partitions if p != target
        )
        check("no keyed record landed elsewhere", elsewhere == 0, f"{elsewhere} strays")

    section("murmur2 agrees with the broker's partitioner")
    # Known-answer test: these are Kafka's murmur2 outputs, so a
    # transcription slip shows up here rather than as records silently
    # landing on the wrong partition.
    check("murmur2(b'') is stable", murmur2(b"") == 275646681, str(murmur2(b"")))
    check(
        "murmur2 is deterministic",
        murmur2(b"user-7") == murmur2(b"user-7"),
    )
    check(
        "different keys hash differently",
        murmur2(b"user-7") != murmur2(b"user-8"),
    )

    section("record headers and timestamps")
    header_topic = unique("py-headers")
    before = int(time.time() * 1000) - 1000
    with Producer(HOST, PORT, ProducerConfig(linger_ms=0, compression_type="none")) as producer:
        producer.send(
            header_topic,
            b"annotated",
            partition=0,
            headers=[
                RecordHeader("trace-id", b"abc-123"),
                RecordHeader("content-type", b"application/json"),
                RecordHeader("tombstone-reason", None),
            ],
        )
        producer.send(header_topic, b"plain", partition=0)
        producer.flush()
    after = int(time.time() * 1000) + 1000

    with Consumer(HOST, PORT) as consumer:
        got = consumer.fetch(header_topic, 0, 0)
    check("both records arrive", len(got) == 2, f"got {len(got)}")
    if len(got) == 2:
        annotated, plain = got
        check("headers survive the round trip", len(annotated.headers) == 3)
        check("header values are exact", annotated.header("trace-id") == b"abc-123")
        check(
            "a null header value stays null",
            annotated.headers[2].value is None,
        )
        check(
            "a record with no headers gains none from its batch",
            plain.headers == [],
        )
        check(
            "timestamps are real wall-clock values",
            all(before <= r.timestamp <= after for r in got),
            f"timestamps {[r.timestamp for r in got]} outside {before}..{after}",
        )

    section("tombstones")
    tomb_topic = unique("py-tombstones")
    with Producer(HOST, PORT, ProducerConfig(linger_ms=0, compression_type="none")) as producer:
        producer.send(tomb_topic, b"set", key=b"k1", partition=0)
        producer.send(tomb_topic, b"", key=b"k2", partition=0)
        # A None value is a deletion, and must stay distinguishable from the
        # empty value above all the way through the round trip.
        producer.send(tomb_topic, None, key=b"k3", partition=0)
        producer.flush()

    with Consumer(HOST, PORT) as consumer:
        got = consumer.fetch(tomb_topic, 0, 0)
    check("all three records arrive", len(got) == 3, f"got {len(got)}")
    if len(got) == 3:
        check("an ordinary value round-trips", got[0].value == b"set")
        check(
            "an empty value is empty, not null",
            got[1].value is not None and len(got[1].value) == 0,
        )
        check("a tombstone arrives as a null value", got[2].value is None,
              f"{got[2].value!r}")

    section("offsets")
    with Consumer(HOST, PORT) as consumer:
        earliest = consumer.list_offsets(topic, 0, EARLIEST)
        latest = consumer.list_offsets(topic, 0, LATEST)
        check("earliest is 0 on a fresh topic", earliest == 0, str(earliest))
        check("latest equals the record count", latest == 50, str(latest))

    section("acks")
    for acks in (0, 1, -1):
        acks_topic = unique(f"py-acks{acks}")
        with Producer(
            HOST, PORT, ProducerConfig(linger_ms=0, acks=acks, compression_type="none")
        ) as producer:
            producer.send(acks_topic, b"durable", partition=0)
            producer.flush()
        time.sleep(0.3)
        with Consumer(HOST, PORT) as consumer:
            got = consumer.fetch(acks_topic, 0, 0)
        check(f"acks={acks} stores the record", len(got) == 1, f"got {len(got)}")

    section("consumer group: assignment, commit, resume")
    group_topic = unique("py-group")
    group_id = unique("py-billing")
    with Producer(HOST, PORT, ProducerConfig(linger_ms=0, compression_type="none")) as producer:
        for index in range(40):
            producer.send(group_topic, f"g{index}".encode())
        producer.flush()

    seen = []
    with GroupConsumer(
        HOST,
        PORT,
        group_id,
        GroupConfig(auto_commit_interval_ms=0, session_timeout_ms=10_000),
    ) as consumer:
        consumer.subscribe([group_topic])
        deadline = time.time() + 30
        while len(seen) < 40 and time.time() < deadline:
            seen.extend(consumer.poll(500))
        check("the group consumes every record", len(seen) == 40, f"got {len(seen)}")
        check(
            "no record is delivered twice",
            len({(r.partition, r.offset) for r in seen}) == len(seen),
        )
        consumer.commit()
        committed = consumer.committed()
        check("commit records a position", sum(committed.values()) == 40, str(committed))

    # A second consumer in the same group must resume, not replay.
    with GroupConsumer(
        HOST, PORT, group_id, GroupConfig(auto_commit_interval_ms=0)
    ) as consumer:
        consumer.subscribe([group_topic])
        replayed = []
        deadline = time.time() + 5
        while time.time() < deadline:
            replayed.extend(consumer.poll(300))
        check(
            "a rejoining group resumes from its commit",
            replayed == [],
            f"replayed {len(replayed)} records it had already committed",
        )

    section("auto.offset.reset")
    reset_topic = unique("py-reset")
    with Producer(HOST, PORT, ProducerConfig(linger_ms=0, compression_type="none")) as producer:
        for index in range(10):
            producer.send(reset_topic, f"r{index}".encode())
        producer.flush()

    with GroupConsumer(
        HOST,
        PORT,
        unique("py-latest"),
        GroupConfig(
            auto_commit_interval_ms=0, auto_offset_reset=AutoOffsetReset.LATEST
        ),
    ) as consumer:
        consumer.subscribe([reset_topic])
        skipped = []
        deadline = time.time() + 4
        while time.time() < deadline:
            skipped.extend(consumer.poll(300))
        check(
            "latest skips records produced before the group existed",
            skipped == [],
            f"saw {len(skipped)}",
        )

    with GroupConsumer(
        HOST,
        PORT,
        unique("py-none"),
        GroupConfig(auto_commit_interval_ms=0, auto_offset_reset=AutoOffsetReset.NONE),
    ) as consumer:
        consumer.subscribe([reset_topic])
        raised = False
        try:
            deadline = time.time() + 5
            while time.time() < deadline:
                consumer.poll(300)
        except NoOffsetForPartition:
            raised = True
        check("none refuses to guess a position", raised)

    section("assignors")
    for assignor in [Assignor.RANGE, Assignor.ROUNDROBIN, Assignor.STICKY]:
        assignor_topic = unique(f"py-{assignor}")
        with Producer(
            HOST, PORT, ProducerConfig(linger_ms=0, compression_type="none")
        ) as producer:
            for index in range(20):
                producer.send(assignor_topic, f"a{index}".encode())
            producer.flush()
        collected = []
        with GroupConsumer(
            HOST,
            PORT,
            unique(f"py-grp-{assignor}"),
            GroupConfig(auto_commit_interval_ms=0, assignor=assignor),
        ) as consumer:
            consumer.subscribe([assignor_topic])
            deadline = time.time() + 20
            while len(collected) < 20 and time.time() < deadline:
                collected.extend(consumer.poll(500))
        check(f"{assignor}: consumes every record", len(collected) == 20, f"got {len(collected)}")

    section("bounded client buffer")
    small_topic = unique("py-buffer")
    tiny = ProducerConfig(
        linger_ms=1000,  # never flush on time during this check
        buffer_memory=2048,
        max_block_ms=300,
        compression_type="none",
    )
    with Producer(HOST, PORT, tiny) as producer:
        blocked = False
        try:
            for index in range(500):
                producer.send(small_topic, b"x" * 256, partition=0)
        except Exception as error:  # noqa: BLE001
            blocked = "buffer full" in str(error)
        check("a full buffer blocks and then reports", blocked)

    print(f"\n{PASSED} passed, {FAILED} failed")
    return 1 if FAILED else 0


if __name__ == "__main__":
    sys.exit(main())
