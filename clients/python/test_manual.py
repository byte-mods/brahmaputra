#!/usr/bin/env python3
"""Manual end-to-end check of the Python driver against a live broker.

    brahmaputra-server --data-dir ./data --default-partitions 4
    python3 test_manual.py [host] [port]

Every check asserts a property of the *system*, not that a function ran:
records come back byte-identical, keys pin partitions, headers survive,
offsets are contiguous, a group splits partitions and resumes from its
commit. It exits non-zero if any check fails.

The sections and checks mirror clients/go/cmd/manualtest/main.go one for one.
"""

from __future__ import annotations

import os
import socket
import struct
import sys
import threading
import time
import uuid

# Import the package next to this file, whatever the current directory is.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from brahmaputra import (  # noqa: E402
    ApiKey,
    Assignor,
    Compression,
    ConsumerConfig,
    ErrorCode,
    ProtocolError,
    Record,
    AutoOffsetReset,
    Connection,
    Consumer,
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
from brahmaputra.group import _sticky_assign  # noqa: E402
from brahmaputra.protocol import (  # noqa: E402
    body_reader,
    body_writer,
    decode_record_batch,
    encode_record_batch,
    register_codec,
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
        versions, broker_version = consumer.router.seed.api_versions()
        check("ApiVersions answers", len(versions) > 0, f"got {len(versions)} apis")
        check("broker reports a version", broker_version != "", repr(broker_version))
        metadata = consumer.router.metadata(refresh=True)
        check(
            "metadata lists brokers",
            len(metadata.brokers) >= 1,
            f"{len(metadata.brokers)} brokers",
        )

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
            "values byte-identical and offsets contiguous",
            [r.value for r in got] == payloads
            and [r.offset for r in got] == list(range(len(payloads))),
        )

    section("compression codecs")
    # Only none and gzip ship in the driver; lz4/zstd/snappy are opt-in via
    # register_codec (or their optional package) so applications that do
    # not want those dependencies do not carry them.
    for codec in ["none", "gzip"]:
        codec_topic = unique(f"py-{codec}")
        # Repetitive payload, so a codec that silently does nothing still
        # round-trips but a broken one corrupts.
        body = b"the same line over and over. " * 40
        with Producer(
            HOST, PORT, ProducerConfig(linger_ms=0, compression_type=codec)
        ) as producer:
            for index in range(20):
                producer.send(codec_topic, body + str(index % 10).encode(), partition=0)
            producer.flush()
        with Consumer(HOST, PORT) as consumer:
            got = consumer.fetch(codec_topic, 0, 0)
        check(
            f"{codec}: round trips",
            len(got) == 20
            and all(r.value == body + str(i % 10).encode() for i, r in enumerate(got)),
            f"got {len(got)} records",
        )

    section("keys, partitioning and ordering")
    key_topic = unique("py-keys")
    with Producer(HOST, PORT, ProducerConfig(linger_ms=0, compression_type="none")) as producer:
        partitions = producer.router.partitions(key_topic)
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
        check(
            "headers survive the round trip",
            len(annotated.headers) == 3,
            f"{len(annotated.headers)} headers",
        )
        check("header values are exact", annotated.header("trace-id") == b"abc-123")
        check(
            "a null header value stays null",
            len(annotated.headers) == 3 and annotated.headers[2].value is None,
        )
        check(
            "a record with no headers gains none from its batch",
            plain.headers == [],
            f"{len(plain.headers)} headers",
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
        time.sleep(0.4)
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
        deadline = time.time() + 5
        while time.time() < deadline and not raised:
            try:
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
        linger_ms=10_000,  # never flush on time during this check
        buffer_memory=2048,
        max_block_ms=300,
        compression_type="none",
    )
    with Producer(HOST, PORT, tiny) as producer:
        blocked = False
        started = time.monotonic()
        waited = 0.0
        for _ in range(500):
            try:
                producer.send(small_topic, b"x" * 256, partition=0)
            except Exception as error:  # noqa: BLE001
                blocked = "buffer full" in str(error)
                waited = time.monotonic() - started
                break
        check("a full buffer blocks and then reports", blocked and waited >= 0.25,
              f"blocked={blocked} after {waited:.3f}s")

    section("wire edge cases")
    edge_topic = unique("py-edge")
    large = bytes((i * 7) & 0xFF for i in range(1 << 20))
    unicode_key = "ключ-✓-🔑".encode()
    unicode_value = "значение — 数据 — 🚀".encode()
    with Producer(HOST, PORT, ProducerConfig(linger_ms=0)) as producer:
        producer.send(edge_topic, large, partition=0)
        producer.send(
            edge_topic,
            unicode_value,
            key=unicode_key,
            partition=0,
            headers=[RecordHeader("ünïcødé-🏷", "✓".encode())],
        )
        # An empty key and an empty header value are values, not nulls.
        producer.send(
            edge_topic,
            b"empty-key",
            key=b"",
            partition=0,
            headers=[RecordHeader("empty", b""), RecordHeader("null", None)],
        )
        producer.send(edge_topic, b"null-key", key=None, partition=0)
    got = []
    with Consumer(HOST, PORT) as consumer:
        offset = 0
        while len(got) < 4:
            batch = consumer.fetch(edge_topic, 0, offset, 500)
            if not batch:
                break
            got.extend(batch)
            offset = batch[-1].offset + 1
    check("edge records all arrive", len(got) == 4, f"got {len(got)}")
    if len(got) == 4:
        check(
            "a 1 MiB value round-trips byte-identical",
            got[0].value == large,
            f"{len(got[0].value or b'')} bytes",
        )
        check(
            "unicode key, value and header key round-trip",
            got[1].key == unicode_key
            and got[1].value == unicode_value
            and len(got[1].headers) == 1
            and got[1].headers[0].key == "ünïcødé-🏷",
        )
        check(
            "an empty key stays empty, not null",
            got[2].key is not None and len(got[2].key) == 0,
            repr(got[2].key),
        )
        check(
            "an empty header value stays empty, not null",
            len(got[2].headers) == 2
            and got[2].headers[0].value is not None
            and len(got[2].headers[0].value) == 0
            and got[2].headers[1].value is None,
            repr(got[2].headers),
        )
        check("a null key stays null", got[3].key is None, repr(got[3].key))

    section("ordering under linger flushes")
    order_topic = unique("py-order")
    total = 5000
    with Producer(HOST, PORT, ProducerConfig(linger_ms=1, batch_size=256)) as producer:
        for index in range(total):
            producer.send(order_topic, str(index).encode(), partition=0)
    values = []
    with Consumer(HOST, PORT) as consumer:
        offset = 0
        while len(values) < total:
            batch = consumer.fetch(order_topic, 0, offset, 500)
            if not batch:
                break
            values.extend(int(r.value) for r in batch)
            offset = batch[-1].offset + 1
    inversions = sum(1 for a, b in zip(values, values[1:]) if b < a)
    check("every record of a partition arrives", len(values) == total, f"got {len(values)}")
    check("a partition's records keep send order", inversions == 0, f"{inversions} inversions")

    section("background flush failures are reported")
    producer = Producer(HOST, PORT, ProducerConfig(linger_ms=20))
    send_error = flush_error = None
    try:
        # Partition 999 does not exist, so the linger ticker's flush fails.
        producer.send(unique("py-bgfail"), b"lost", partition=999)
    except Exception as error:  # noqa: BLE001
        send_error = error
    time.sleep(0.3)
    try:
        producer.flush()
    except Exception as error:  # noqa: BLE001
        flush_error = error
    check(
        "a failed linger flush surfaces on the next flush",
        send_error is None and flush_error is not None,
        f"send={send_error!r} flush={flush_error!r}",
    )
    closer = threading.Thread(target=lambda: _quietly(producer.close), daemon=True)
    closer.start()
    closer.join(5.0)
    check("close returns after a failed flush", not closer.is_alive(), "hung")

    section("connection failures")
    # A broker that accepts and never answers must cost an error, not a
    # thread blocked forever.
    silent = _SilentServer()
    connection = Connection("127.0.0.1", silent.port, "py-test", 1.0)
    connection.set_request_timeout(0.3)
    started = time.monotonic()
    request_error = None
    try:
        connection.api_versions()
    except Exception as error:  # noqa: BLE001
        request_error = error
    check(
        "a request to an unresponsive broker times out",
        request_error is not None and time.monotonic() - started < 3.0,
        repr(request_error),
    )
    check("a timed-out connection is not reused", connection.broken)
    connection.close()
    silent.close()

    # A connection the broker drops is redialled, not kept forever.
    proxy = _Proxy(HOST, PORT)
    drop_topic = unique("py-drop")
    producer = Producer("127.0.0.1", proxy.port, ProducerConfig(linger_ms=0))
    producer.send(drop_topic, b"before", partition=0)
    proxy.drop_all()
    recovered: object = "not attempted"
    for _ in range(3):
        try:
            producer.send(drop_topic, b"after", partition=0)
            recovered = None
            break
        except Exception as error:  # noqa: BLE001
            recovered = error
    check("a producer recovers after its connection drops", recovered is None, repr(recovered))
    _quietly(producer.close)
    consumer = Consumer("127.0.0.1", proxy.port)
    consumer.fetch(drop_topic, 0, 0, 100)
    proxy.drop_all()
    fetch_error: object = "not attempted"
    fetched = []
    for _ in range(3):
        try:
            fetched = consumer.fetch(drop_topic, 0, 0, 100)
            fetch_error = None
            break
        except Exception as error:  # noqa: BLE001
            fetch_error = error
    check(
        "a consumer recovers after its connection drops",
        fetch_error is None and len(fetched) >= 1,
        repr(fetch_error),
    )
    consumer.close()
    proxy.close()

    section("consumer group: max.poll.interval and rejoin")
    slow_topic = unique("py-slow")
    producer = Producer(HOST, PORT, ProducerConfig(linger_ms=0))
    for index in range(10):
        producer.send(slow_topic, f"s{index}".encode())
    consumer = GroupConsumer(
        HOST,
        PORT,
        unique("py-slow-grp"),
        GroupConfig(auto_commit_interval_ms=0, max_poll_interval_ms=1500),
    )
    consumer.subscribe([slow_topic])
    first = []
    deadline = time.time() + 15
    while len(first) < 10 and time.time() < deadline:
        try:
            first.extend(consumer.poll(300))
        except Exception:  # noqa: BLE001
            break
    consumer.commit()
    # Stall past max.poll.interval.ms: the member leaves the group.
    time.sleep(2.5)
    for index in range(10, 20):
        producer.send(slow_topic, f"s{index}".encode())
    producer.close()
    second = []
    poll_error = None
    deadline = time.time() + 15
    while len(second) < 10 and time.time() < deadline:
        try:
            second.extend(consumer.poll(300))
        except Exception as error:  # noqa: BLE001
            poll_error = error
            break
    check(
        "a member that stalled rejoins on its next poll",
        len(first) == 10 and len(second) == 10 and poll_error is None,
        f"first={len(first)} second={len(second)} err={poll_error!r}",
    )
    consumer.close()

    section("consumer group: time inside poll does not count against max.poll.interval")
    join_topic = unique("py-inpoll")
    producer = Producer(HOST, PORT, ProducerConfig(linger_ms=0))
    producer.router.partitions(join_topic)
    # Far shorter than the first poll below, which spends ~1s joining (the
    # broker's initial rebalance delay) and then waits for data.
    consumer = GroupConsumer(
        HOST,
        PORT,
        unique("py-inpoll-grp"),
        GroupConfig(auto_commit_interval_ms=0, max_poll_interval_ms=600),
    )
    consumer.subscribe([join_topic])

    def produce_later() -> None:
        time.sleep(2.0)
        for index in range(10):
            _quietly(lambda: producer.send(join_topic, f"j{index}".encode()))

    feeder = threading.Thread(target=produce_later, daemon=True)
    feeder.start()
    poll_error = commit_error = None
    got = []
    # One long poll: it joins, then waits for the records above.
    try:
        got = consumer.poll(4000)
    except Exception as error:  # noqa: BLE001
        poll_error = error
    # Committed straight away, before another poll could quietly rejoin:
    # this fails if the member left the group mid-poll.
    try:
        consumer.commit()
    except Exception as error:  # noqa: BLE001
        commit_error = error
    check(
        "a member is still in its group after a long poll",
        poll_error is None and len(got) > 0 and commit_error is None,
        f"got={len(got)} poll={poll_error!r} commit={commit_error!r}",
    )
    consumer.close()
    feeder.join()
    producer.close()

    feature_checks()

    print(f"\n{PASSED} passed, {FAILED} failed")
    return 1 if FAILED else 0


def _quick(**overrides) -> Producer:
    return Producer(HOST, PORT, ProducerConfig(linger_ms=0, **overrides))


def _fetch_all(topic: str, partition: int) -> list:
    out = []
    with Consumer(HOST, PORT) as consumer:
        offset = 0
        while True:
            batch = consumer.fetch(topic, partition, offset, 100)
            if not batch:
                return out
            out.extend(batch)
            offset = batch[-1].offset + 1


def _failure(fn):
    try:
        fn()
        return None
    except Exception as error:  # noqa: BLE001
        return error


def feature_checks() -> None:
    """The client feature checklist, item by item: every setting is shown
    to change behaviour, not merely to be accepted."""
    section("producer settings")
    topic = unique("py-linger")
    producer = Producer(HOST, PORT, ProducerConfig(linger_ms=50))
    producer.send(topic, b"lingered", partition=0)
    time.sleep(0.6)
    got = _fetch_all(topic, 0)
    check("linger.ms sends a batch without an explicit flush", len(got) == 1,
          f"got {len(got)} before any flush")
    producer.close()

    topic = unique("py-batchsize")
    producer = Producer(HOST, PORT, ProducerConfig(linger_ms=60_000, batch_size=200))
    for _ in range(10):
        producer.send(topic, b"b" * 50, partition=0)
    got = _fetch_all(topic, 0)
    check("batch.size sends a full batch before linger expires", len(got) >= 3,
          f"got {len(got)} of 10 with linger 60s")
    producer.close()

    topic = unique("py-closeflush")
    producer = Producer(HOST, PORT, ProducerConfig(linger_ms=60_000))
    for i in range(5):
        producer.send(topic, f"c{i}".encode(), partition=0)
    producer.close()
    got = _fetch_all(topic, 0)
    check("close flushes buffered records", len(got) == 5, f"got {len(got)}")

    topic = unique("py-sync")
    producer = _quick()
    first = producer.send_and_wait(topic, b"s0", partition=1)
    second = producer.send_and_wait(topic, b"s1", partition=1)
    keyed = producer.send_and_wait(topic, b"s2", key=b"k")
    producer.close()
    got = _fetch_all(topic, 1)
    check("send-and-wait returns the record's offset",
          first == 0 and second == 1 and keyed >= 0 and len(got) >= 2 and got[1].value == b"s1",
          f"offsets {first} {second} {keyed}")

    topic = unique("py-roundrobin")
    producer = _quick()
    partitions = producer.router.partitions(topic)
    for i in range(2 * len(partitions)):
        producer.send(topic, f"rr{i}".encode())
    producer.close()
    counts = [len(_fetch_all(topic, p)) for p in partitions]
    check("null keys are spread round-robin", len(partitions) > 1 and all(c == 2 for c in counts),
          str(counts))

    topic = unique("py-timestamp")
    producer = _quick()
    producer.send(topic, b"t1", partition=0, timestamp_ms=1_600_000_001_000)
    producer.send(topic, b"t2", partition=0, timestamp_ms=1_600_000_002_000)
    producer.send_and_wait(topic, b"t3", partition=0, timestamp_ms=1_600_000_003_000)
    producer.close()
    got = _fetch_all(topic, 0)
    check("an explicit record timestamp is kept",
          len(got) == 3 and got[0].timestamp == 1_600_000_001_000
          and got[2].timestamp == 1_600_000_003_000,
          str([r.timestamp for r in got]))
    with Consumer(HOST, PORT) as consumer:
        by_time = consumer.list_offsets(topic, 0, 1_600_000_001_500)
        past_end = consumer.list_offsets(topic, 0, 1_700_000_000_000)
    check("list offsets by timestamp finds the first record at or after it",
          by_time == 1 and past_end == 3, f"by_time={by_time} past_end={past_end}")

    producer = _quick(acks="all", request_timeout_ms=1500)
    error = _failure(lambda: producer.send_and_wait(unique("py-acksall"), b"durable"))
    check("acks=all with request.timeout.ms is acknowledged", error is None, repr(error))
    producer.close()

    calls = {"compress": 0, "decompress": 0}

    def counted(name, fn):
        def wrapper(payload: bytes) -> bytes:
            calls[name] += 1
            return fn(payload)
        return wrapper

    register_codec(Compression.LZ4, counted("compress", _lz4_encode),
                   counted("decompress", _lz4_decode))
    topic = unique("py-lz4")
    producer = _quick(compression_type="lz4")
    body = b"registered codec " * 30
    for i in range(5):
        producer.send(topic, body, key=str(i).encode(), partition=0)
    producer.close()
    got = _fetch_all(topic, 0)
    check("a registered lz4 codec round-trips",
          len(got) == 5 and all(r.value == body for r in got)
          and calls["compress"] > 0 and calls["decompress"] > 0,
          f"got {len(got)}, calls={calls}")

    section("retries (fault-injecting proxy)")
    proxy = _FaultProxy(HOST, PORT)
    topic = unique("py-retry")

    def via_proxy(**overrides) -> Producer:
        producer = Producer("127.0.0.1", proxy.port, ProducerConfig(linger_ms=0, **overrides))
        producer.router.partitions(topic)
        return producer

    producer = via_proxy(retries=5, retry_backoff_ms=50)
    proxy.inject(ErrorCode.NOT_LEADER_OR_FOLLOWER, 2)
    result = {}
    error = _failure(lambda: result.update(offset=producer.send_and_wait(
        topic, b"eventually", partition=0)))
    check("a retriable produce error is retried until it succeeds",
          error is None and result.get("offset") == 0 and proxy.produces == 3,
          f"err={error!r} result={result} attempts={proxy.produces}")
    producer.close()

    producer = via_proxy(retries=2, retry_backoff_ms=200)
    proxy.inject(ErrorCode.NOT_LEADER_OR_FOLLOWER, -1)
    started = time.monotonic()
    error = _failure(lambda: producer.send_and_wait(topic, b"never", partition=0))
    elapsed = time.monotonic() - started
    check("retry.backoff.ms spaces the retries",
          error is not None and proxy.produces == 3 and elapsed >= 0.4,
          f"attempts={proxy.produces} elapsed={elapsed:.2f}s")
    _failure(producer.close)

    producer = via_proxy(retries=5, retry_backoff_ms=1000)
    proxy.inject(ErrorCode.INVALID_REQUEST, -1)
    started = time.monotonic()
    error = _failure(lambda: producer.send_and_wait(topic, b"rejected", partition=0))
    check("a non-retriable produce error is not retried",
          error is not None and proxy.produces == 1 and time.monotonic() - started < 1.0,
          f"attempts={proxy.produces}")
    _failure(producer.close)

    producer = via_proxy(retries=1_000_000, retry_backoff_ms=50, delivery_timeout_ms=600)
    proxy.inject(ErrorCode.NOT_LEADER_OR_FOLLOWER, -1)
    started = time.monotonic()
    error = _failure(lambda: producer.send_and_wait(topic, b"late", partition=0))
    elapsed = time.monotonic() - started
    check("delivery.timeout.ms bounds the retries",
          error is not None and elapsed < 3.0 and proxy.produces > 2,
          f"attempts={proxy.produces} elapsed={elapsed:.2f}s")
    _failure(producer.close)
    proxy.close()

    section("consumer settings")
    topic = unique("py-fetch")
    producer = _quick()
    for i in range(10):
        producer.send(topic, bytes([97 + i]) * 1000, partition=0)
    producer.close()
    with Consumer(HOST, PORT) as consumer:
        records, high_watermark = consumer.fetch_verbose(topic, 0, 0, 100)
        check("fetch reports the high watermark", high_watermark == 10 and len(records) == 10,
              f"hw={high_watermark}")
        metadata = consumer.router.metadata([topic], refresh=True)
        partitions = metadata.partitions_of(topic)
        check("metadata lists every partition with a leader",
              len(partitions) == 4
              and all((metadata.leader_of(topic, p) if metadata.leader_of(topic, p) is not None else -1) >= 0
                      for p in partitions),
              str(partitions))
    with Consumer(HOST, PORT, ConsumerConfig(fetch_max_bytes=2500)) as capped:
        got = capped.fetch(topic, 0, 0, 100)
    check("fetch.max.bytes caps a response", 1 <= len(got) < 10, f"got {len(got)} of 10")
    with Consumer(HOST, PORT, ConsumerConfig(fetch_min_bytes=1 << 20,
                                             fetch_max_wait_ms=400)) as patient:
        started = time.monotonic()
        got = patient.fetch(topic, 0, 0, 400)
        elapsed = time.monotonic() - started
    check("fetch.min.bytes waits up to fetch.max.wait.ms for more data",
          len(got) == 10 and 0.3 <= elapsed < 3.0, f"got {len(got)} after {elapsed:.2f}s")

    # A length prefix larger than what follows, or negative, is an error —
    # never a read past the end or a huge allocation.
    batch = encode_record_batch([Record(value=b"x")], max_timestamp=0)
    oversized = batch[:8] + struct.pack(">i", 0x7FFFFFFF) + batch[12:]
    negative = batch[:8] + struct.pack(">i", -16) + batch[12:]
    errors = [
        _failure(lambda: decode_record_batch(oversized, 0)),
        _failure(lambda: decode_record_batch(negative, 0)),
        _failure(lambda: body_reader(bytes([0x7E, 0x31]))),
    ]
    check("a truncated or oversized length is an error, not a crash",
          all(isinstance(e, ProtocolError) for e in errors), repr(errors))

    section("consumer group settings")

    def produce_n(topic: str, n: int) -> None:
        with _quick() as producer:
            for i in range(n):
                producer.send(topic, f"m{i}".encode())

    def group(group_id: str, **overrides) -> GroupConsumer:
        overrides.setdefault("auto_commit_interval_ms", 0)
        return GroupConsumer(HOST, PORT, group_id, GroupConfig(**overrides))

    def poll_until(consumer: GroupConsumer, want: int, limit_s: float):
        got, largest = [], 0
        deadline = time.monotonic() + limit_s
        while len(got) < want and time.monotonic() < deadline:
            try:
                records = consumer.poll(300)
            except Exception:  # noqa: BLE001
                break
            largest = max(largest, len(records))
            got.extend(records)
        return got, largest

    def sum_committed(consumer: GroupConsumer) -> int:
        try:
            return sum(consumer.committed().values())
        except Exception:  # noqa: BLE001
            return -1

    def partitions_of(consumer: GroupConsumer, topic: str) -> list:
        return sorted(p for t, p in consumer.assignment if t == topic)

    topic = unique("py-maxpoll")
    produce_n(topic, 20)
    consumer = group(unique("py-maxpoll-grp"), max_poll_records=5)
    consumer.subscribe([topic])
    got, largest = poll_until(consumer, 20, 20)
    check("max.poll.records caps one poll", len(got) == 20 and largest <= 5,
          f"got {len(got)}, largest poll {largest}")
    consumer.close()

    topic = unique("py-autocommit")
    produce_n(topic, 12)
    consumer = group(unique("py-autocommit-grp"), auto_commit_interval_ms=200)
    consumer.subscribe([topic])
    poll_until(consumer, 12, 20)
    time.sleep(0.3)
    consumer.poll(300)
    total = sum_committed(consumer)
    check("auto-commit commits delivered positions", total == 12, str(total))
    consumer.close()

    topic = unique("py-heartbeat")
    produce_n(topic, 4)
    consumer = group(unique("py-heartbeat-grp"), session_timeout_ms=1500,
                     heartbeat_interval_ms=300)
    consumer.subscribe([topic])
    poll_until(consumer, 4, 20)
    generation = consumer.generation
    time.sleep(4.0)  # well past session.timeout.ms, no polls
    error = _failure(consumer.commit)
    check("heartbeats keep an idle member in its group",
          error is None and consumer.generation == generation, repr(error))
    consumer.close()

    first_topic, second_topic = unique("py-multi-a"), unique("py-multi-b")
    produce_n(first_topic, 6)
    produce_n(second_topic, 7)
    consumer = group(unique("py-multi-grp"))
    consumer.subscribe([first_topic, second_topic])
    got, _ = poll_until(consumer, 13, 20)
    a = sum(1 for r in got if r.topic == first_topic)
    b = sum(1 for r in got if r.topic == second_topic)
    check("a member subscribed to two topics consumes both", a == 6 and b == 7, f"{a}/{b}")
    consumer.close()

    topic = unique("py-static")
    produce_n(topic, 4)
    group_id = unique("py-static-grp")
    original = group(group_id, group_instance_id="instance-1")
    original.subscribe([topic])
    poll_until(original, 4, 20)
    member_id = original.member_id
    # The same instance comes back (a restart) before the old session has
    # expired: it must reclaim the slot, not join as a stranger.
    returning = group(group_id, group_instance_id="instance-1")
    returning.subscribe([topic])
    _failure(lambda: returning.poll(2000))
    check("a returning static member reclaims its member id",
          member_id != "" and returning.member_id == member_id,
          f"{member_id!r} then {returning.member_id!r}")
    returning.close()
    original.close()

    topic = unique("py-leave")
    produce_n(topic, 8)
    group_id = unique("py-leave-grp")
    settings = dict(session_timeout_ms=30_000, rebalance_timeout_ms=10_000)
    leaving = group(group_id, **settings)
    leaving.subscribe([topic])
    poll_until(leaving, 8, 20)
    leaving.close()
    successor = group(group_id, **settings)
    successor.subscribe([topic])
    started = time.monotonic()
    while len(partitions_of(successor, topic)) < 4 and time.monotonic() - started < 15:
        _failure(lambda: successor.poll(200))
    elapsed = time.monotonic() - started
    check("close leaves the group so the next member is assigned at once",
          len(partitions_of(successor, topic)) == 4 and elapsed < 6,
          f"assigned after {elapsed:.2f}s")
    successor.close()

    topic = unique("py-fence")
    produce_n(topic, 8)
    group_id = unique("py-fence-grp")
    first = group(group_id, rebalance_timeout_ms=2000)
    first.subscribe([topic])
    poll_until(first, 8, 20)
    old_generation = first.generation
    # A second member joins while the first stops polling: the group moves
    # on without it, so its generation is superseded.
    second = group(group_id, rebalance_timeout_ms=2000)
    second.subscribe([topic])
    deadline = time.monotonic() + 15
    while second.generation <= old_generation and time.monotonic() < deadline:
        _failure(lambda: second.poll(200))
    error = _failure(first.commit)
    check("a commit from a superseded generation is fenced", error is not None,
          f"old={old_generation} new={second.generation}")

    # Both members polling settle on a split of the partitions.
    until = time.monotonic() + 8

    def keep_polling(member: GroupConsumer) -> None:
        while time.monotonic() < until:
            _failure(lambda: member.poll(200))

    workers = [threading.Thread(target=keep_polling, args=(m,)) for m in (first, second)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join()
    a, b = partitions_of(first, topic), partitions_of(second, topic)
    check("two members share the partitions without overlap",
          sorted(a + b) == [0, 1, 2, 3] and a and b, f"{a} / {b}")
    second.close()
    first.close()

    # Partitions compare as integers: 2 sorts before 10, and the member over
    # quota keeps its lowest-numbered partitions.
    held = [("t", p) for p in (10, 2, 11, 3, 0, 1, 9)]
    assignment = _sticky_assign([("a", ["t"]), ("b", ["t"])], {"t": list(range(12))},
                                {"a": held})
    check("sticky compares partitions as integers",
          assignment["a"] == [("t", p) for p in (0, 1, 2, 3, 9, 10)]
          and len(assignment["b"]) == 6, str(assignment))


def _lz4_encode(src: bytes) -> bytes:
    """A size-prefixed LZ4 block holding one literal-only sequence: valid
    LZ4, just uncompressed."""
    n = len(src)
    if n < 15:
        head = bytes([n << 4])
    else:
        rest = n - 15
        head = bytes([0xF0]) + b"\xff" * (rest // 255) + bytes([rest % 255])
    return struct.pack("<I", n) + head + src


def _lz4_decode(src: bytes) -> bytes:
    """Decode any size-prefixed LZ4 block."""
    (size,) = struct.unpack_from("<I", src, 0)
    out = bytearray()
    pos = 4

    def length(base: int) -> int:
        nonlocal pos
        n = base
        if base == 15:
            while True:
                b = src[pos]
                pos += 1
                n += b
                if b != 255:
                    break
        return n

    while pos < len(src):
        token = src[pos]
        pos += 1
        literals = length(token >> 4)
        out += src[pos : pos + literals]
        pos += literals
        if pos >= len(src):
            break
        (offset,) = struct.unpack_from("<H", src, pos)
        pos += 2
        match = length(token & 15) + 4
        if offset == 0 or offset > len(out):
            raise ValueError("lz4: bad offset")
        for _ in range(match):
            out.append(out[-offset])
    if len(out) != size:
        raise ValueError("lz4: size mismatch")
    return bytes(out)


class _FaultProxy:
    """Forwards frames to the broker one request at a time, but can answer
    Produce requests itself with an injected error code — the only way to
    make a healthy single broker return a retriable error on demand."""

    def __init__(self, host: str, port: int) -> None:
        self._target = (host, port)
        self._server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server.bind(("127.0.0.1", 0))
        self._server.listen(16)
        self.port = self._server.getsockname()[1]
        self._lock = threading.Lock()
        self._code = 0
        self._failures = 0
        self._produces = 0
        self._sockets: list = []
        threading.Thread(target=self._accept, daemon=True).start()

    @property
    def produces(self) -> int:
        with self._lock:
            return self._produces

    def inject(self, code: int, failures: int) -> None:
        with self._lock:
            self._code, self._failures, self._produces = code, failures, 0

    def close(self) -> None:
        _quietly(self._server.close)
        for sock in self._sockets:
            _quietly(sock.close)

    def _accept(self) -> None:
        while True:
            try:
                client, _ = self._server.accept()
            except OSError:
                return
            threading.Thread(target=self._serve, args=(client,), daemon=True).start()

    @staticmethod
    def _read_frame(sock: socket.socket) -> bytes:
        def exact(n: int) -> bytes:
            data = b""
            while len(data) < n:
                chunk = sock.recv(n - len(data))
                if not chunk:
                    raise OSError("closed")
                data += chunk
            return data

        head = exact(4)
        return head + exact(struct.unpack(">I", head)[0])

    def _serve(self, client: socket.socket) -> None:
        try:
            upstream = socket.create_connection(self._target)
        except OSError:
            client.close()
            return
        self._sockets += [client, upstream]
        try:
            while True:
                frame = self._read_frame(client)
                (api_key,) = struct.unpack_from(">h", frame, 4)
                if api_key == ApiKey.PRODUCE:
                    with self._lock:
                        self._produces += 1
                        fail = self._failures != 0
                        if self._failures > 0:
                            self._failures -= 1
                        code = self._code
                    if fail:
                        (client_len,) = struct.unpack_from(">H", frame, 12)
                        header = frame[4 : 14 + client_len]
                        writer = body_writer()
                        writer.string("")
                        writer.i32(0)
                        writer.i32(code)
                        writer.i64(-1)
                        writer.i64(-1)
                        body = header + writer.bytes()
                        client.sendall(struct.pack(">I", len(body)) + body)
                        continue
                upstream.sendall(frame)
                client.sendall(self._read_frame(upstream))
        except OSError:
            pass
        finally:
            _quietly(client.close)
            _quietly(upstream.close)


def _quietly(fn) -> None:
    try:
        fn()
    except Exception:  # noqa: BLE001
        pass


class _SilentServer:
    """Accepts connections and reads forever, never answering."""

    def __init__(self) -> None:
        self._listener = socket.socket()
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen()
        self.port = self._listener.getsockname()[1]
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self) -> None:
        while True:
            try:
                conn, _ = self._listener.accept()
            except OSError:
                return
            threading.Thread(target=_drain, args=(conn,), daemon=True).start()

    def close(self) -> None:
        self._listener.close()


def _drain(conn: socket.socket) -> None:
    try:
        while conn.recv(65536):
            pass
    except OSError:
        pass


class _Proxy:
    """Forwards TCP to the broker and can sever every live connection,
    which is how a broker restart or an idle timeout looks to a client."""

    def __init__(self, host: str, port: int) -> None:
        self._target = (host, port)
        self._listener = socket.socket()
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen()
        self.port = self._listener.getsockname()[1]
        self._lock = threading.Lock()
        self._live = []
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self) -> None:
        while True:
            try:
                client, _ = self._listener.accept()
            except OSError:
                return
            try:
                upstream = socket.create_connection(self._target)
            except OSError:
                client.close()
                continue
            with self._lock:
                self._live += [client, upstream]
            threading.Thread(target=_pipe, args=(client, upstream), daemon=True).start()
            threading.Thread(target=_pipe, args=(upstream, client), daemon=True).start()

    def drop_all(self) -> None:
        with self._lock:
            for conn in self._live:
                try:
                    conn.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                conn.close()
            self._live = []
        time.sleep(0.05)

    def close(self) -> None:
        self._listener.close()
        self.drop_all()


def _pipe(source: socket.socket, sink: socket.socket) -> None:
    try:
        while True:
            data = source.recv(65536)
            if not data:
                break
            sink.sendall(data)
    except OSError:
        pass
    try:
        sink.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass


if __name__ == "__main__":
    sys.exit(main())
