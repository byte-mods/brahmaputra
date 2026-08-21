# Brahmaputra client for Python

Requires Python 3.8+. Pure standard library unless you want lz4, zstd or
snappy compression.

> **Not yet executed.** No Python runtime was available on the machine
> this driver was written on, so it has not been imported, compiled or
> run. It follows the same design as the Go and Node drivers, which are
> verified 34/34 against a live broker — but that is not the same as being
> verified itself. Run `python3 test_manual.py` before trusting it.

## Produce

```python
from brahmaputra import Producer, ProducerConfig, RecordHeader

with Producer("127.0.0.1", 9092, ProducerConfig(
    acks=1,
    linger_ms=5,
    compression_type="gzip",
)) as producer:
    # Keyed: murmur2(key) % partitions, so records sharing a key keep order.
    producer.send(
        "orders",
        b'{"id":1}',
        key=b"user-7",
        headers=[RecordHeader("trace-id", b"abc-123")],
    )
    producer.flush()

    # Or wait for one record's offset. A full round trip — correct, and slow.
    offset = producer.send_and_wait("orders", b'{"id":2}')
```

## Consume one partition

```python
from brahmaputra import Consumer, EARLIEST, LATEST

with Consumer("127.0.0.1", 9092) as consumer:
    for record in consumer.fetch("orders", 0, 0):
        print(record.offset, record.key, record.value, record.timestamp)
    end = consumer.list_offsets("orders", 0, LATEST)
```

## Consume as a group

```python
from brahmaputra import Assignor, AutoOffsetReset, GroupConfig, GroupConsumer

with GroupConsumer("127.0.0.1", 9092, "billing", GroupConfig(
    assignor=Assignor.STICKY,
    auto_offset_reset=AutoOffsetReset.EARLIEST,
    auto_commit_interval_ms=0,      # commit explicitly
    group_instance_id="worker-3",   # static membership
)) as consumer:
    consumer.subscribe(["orders"])
    while True:
        for record in consumer.poll(500):
            handle(record.value)
        # At-least-once: commit after processing, never before.
        consumer.commit()
```

Closing commits and then leaves the group, so its partitions move
immediately rather than after a session timeout.

## Compression

`none` and `gzip` need nothing. The others import their library only when
actually used, and the error names what to install rather than failing at
import time:

```bash
pip install lz4             # lz4
pip install zstandard       # zstd
pip install python-snappy   # snappy
```

If you use lz4, note the broker expects a little-endian `uint32` of the
uncompressed length followed by a raw LZ4 **block** — this driver already
does that, but it is why the `lz4.frame` API cannot be substituted.
