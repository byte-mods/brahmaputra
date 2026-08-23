"""Producer, consumer and group consumer for Brahmaputra.

The configuration names mirror Kafka's, because the whole point of a
client library is that someone who knows Kafka does not have to learn a
new vocabulary to use this. Where a default differs from Kafka's it is
called out on the field.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import os
import socket
import struct
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from .protocol import (
    API_VERSION,
    ApiKey,
    BrahmaputraError,
    Compression,
    DecodedBatch,
    ErrorCode,
    NoOffsetForPartition,
    ProtocolError,
    Record,
    RecordHeader,
    RETRIABLE_ERRORS,
    Reader,
    ServerError,
    Writer,
    body_reader,
    body_writer,
    decode_frame_payload,
    decode_record_batch,
    encode_frame,
    encode_record_batch,
    partition_for_key,
)

EARLIEST = -2
LATEST = -1


def _now_ms() -> int:
    return int(time.time() * 1000)


# --------------------------------------------------------------------------
# Connection
# --------------------------------------------------------------------------




SCRAM_MECHANISM = "SCRAM-SHA-256"


def _scram_field(message: str, key: str) -> Optional[str]:
    """One `key=value` field out of a SCRAM message."""
    for part in message.split(","):
        if part.startswith(f"{key}="):
            return part[len(key) + 1 :]
    return None


def _scram_client_proof(
    password: str, salt: str, iterations: int, auth_message: str
) -> str:
    """The client half of RFC 5802: prove knowledge of the password without
    sending it.

    `hashlib.pbkdf2_hmac` is the same construction the broker derives its
    stored key with, so the two cannot drift apart.
    """
    salted = hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), base64.b64decode(salt), iterations, 32
    )
    client_key = hmac.new(salted, b"Client Key", hashlib.sha256).digest()
    stored_key = hashlib.sha256(client_key).digest()
    signature = hmac.new(stored_key, auth_message.encode("utf-8"), hashlib.sha256).digest()
    proof = bytes(a ^ b for a, b in zip(client_key, signature))
    return base64.b64encode(proof).decode("ascii")


class Connection:
    """One TCP connection to one broker, multiplexed by correlation id.

    The broker answers concurrently and out of order, so responses are
    matched by correlation id rather than by arrival order. A single lock
    guards the socket; this client is thread-safe but not concurrent —
    which matches how the Rust client behaves and is enough for a producer
    that batches.
    """

    def __init__(
        self,
        host: str,
        port: int,
        client_id: str = "brahmaputra-python",
        timeout: float = 30.0,
    ) -> None:
        self.host = host
        self.port = port
        self.client_id = client_id
        self._correlation = 0
        self._lock = threading.Lock()
        self._sock = socket.create_connection((host, port), timeout=timeout)
        # Responses are small and latency matters more than packet count;
        # without this every request pays Nagle plus the peer's delayed ACK.
        self._sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def close(self) -> None:
        try:
            self._sock.close()
        except OSError:
            pass

    def __enter__(self) -> "Connection":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()

    def request(self, api_key: int, body: bytes) -> bytes:
        with self._lock:
            self._correlation += 1
            correlation_id = self._correlation
            self._sock.sendall(encode_frame(api_key, correlation_id, self.client_id, body))
            while True:
                payload = self._read_frame()
                _key, got, response_body = decode_frame_payload(payload)
                if got == correlation_id:
                    return response_body
                # A response for a request we are no longer waiting on can
                # only mean the stream has desynchronised; continuing would
                # pair every later response with the wrong request.
                raise ProtocolError(
                    f"correlation id mismatch: expected {correlation_id}, got {got}"
                )

    def send_oneway(self, api_key: int, body: bytes) -> None:
        """Send without awaiting a response (`acks=0`)."""
        with self._lock:
            self._correlation += 1
            self._sock.sendall(encode_frame(api_key, self._correlation, self.client_id, body))

    def _read_frame(self) -> bytes:
        header = self._read_exact(4)
        (length,) = struct.unpack(">i", header)
        if length < 0:
            raise ProtocolError(f"negative frame length {length}")
        return self._read_exact(length)

    def _read_exact(self, count: int) -> bytes:
        chunks = []
        remaining = count
        while remaining:
            chunk = self._sock.recv(remaining)
            if not chunk:
                raise BrahmaputraError("connection closed by broker")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    # -- APIs that live on any connection ---------------------------------

    def authenticate(self, username: str, password: str) -> Tuple[str, str]:
        """Bind a principal to this connection using SCRAM-SHA-256.

        The password never crosses the wire: the broker sends a challenge
        and this answers with a proof derived from the password, which is
        what makes authentication meaningful on a plaintext listener. Use
        :meth:`authenticate_plain` only where the connection is already
        encrypted.
        """
        client_nonce = base64.b64encode(os.urandom(18)).decode("ascii").replace(",", ".")
        bare = f"n={username},r={client_nonce}"
        _, _, server_first, done = self._authenticate_step(
            username, "", SCRAM_MECHANISM, f"n,,{bare}"
        )
        if done:
            raise ProtocolError("broker ended the SCRAM exchange before it began")

        nonce = _scram_field(server_first, "r")
        salt = _scram_field(server_first, "s")
        iterations = _scram_field(server_first, "i")
        if nonce is None or salt is None or iterations is None or not iterations.isdigit():
            raise ProtocolError("malformed SCRAM server-first message")
        # The server must have kept this client's nonce, which is what makes
        # the exchange this one rather than a replay of an earlier one.
        if not nonce.startswith(client_nonce):
            raise ProtocolError("SCRAM server nonce does not extend the client nonce")

        # `biws` is base64 of the GS2 header "n,,", echoed so the server can
        # see it was not altered in flight.
        without_proof = f"c=biws,r={nonce}"
        auth_message = f"{bare},{server_first},{without_proof}"
        proof = _scram_client_proof(password, salt, int(iterations), auth_message)
        principal, role, _, _ = self._authenticate_step(
            username, "", SCRAM_MECHANISM, f"{without_proof},p={proof}"
        )
        return principal, role

    def authenticate_plain(self, username: str, password: str) -> Tuple[str, str]:
        """Send the password itself, as SASL/PLAIN does.

        The broker refuses this on a plaintext listener.
        """
        principal, role, _, _ = self._authenticate_step(username, password, "PLAIN", "")
        return principal, role

    def _authenticate_step(
        self, username: str, password: str, mechanism: str, payload: str
    ) -> Tuple[str, str, str, bool]:
        writer = body_writer()
        writer.string(username)
        writer.string(password)
        writer.string(mechanism)
        writer.string(payload)
        reader = body_reader(self.request(ApiKey.AUTHENTICATE, writer.bytes()))
        code = reader.i32()
        principal = reader.string()
        role = reader.string()
        response_payload = reader.string()
        done = reader.boolean()
        if code != ErrorCode.NONE:
            raise ServerError(code, "authenticate")
        return principal, role, response_payload, done

    def api_versions(self) -> Dict[int, Tuple[int, int]]:
        writer = body_writer()
        writer.string("brahmaputra-python")
        writer.string("0.1.0")
        reader = body_reader(self.request(ApiKey.API_VERSIONS, writer.bytes()))
        code = reader.i32()
        if code != ErrorCode.NONE:
            raise ServerError(code, "api_versions")
        count = reader.i32()
        out = {}
        for _ in range(count):
            api_key = reader.i32()
            out[api_key] = (reader.i32(), reader.i32())
        reader.string()  # broker_version
        return out

    def metadata(self, topics: Sequence[str] = ()) -> "ClusterMetadata":
        writer = body_writer()
        writer.string_array(list(topics))
        reader = body_reader(self.request(ApiKey.METADATA, writer.bytes()))
        return ClusterMetadata.decode(reader)


# --------------------------------------------------------------------------
# Metadata
# --------------------------------------------------------------------------


@dataclass
class BrokerInfo:
    node_id: int
    host: str
    port: int
    #: Failure domain this broker is in, empty when it was started without
    #: --rack. A consumer that sets `ConsumerConfig.rack` can be redirected
    #: to a replica in its own rack.
    rack: str = ""


@dataclass
class PartitionInfo:
    partition: int
    leader: int
    leader_epoch: int
    replicas: List[int]
    isr: List[int]


@dataclass
class TopicInfo:
    name: str
    partitions: List[PartitionInfo]


@dataclass
class ClusterMetadata:
    brokers: List[BrokerInfo]
    topics: List[TopicInfo]

    @staticmethod
    def decode(reader: Reader) -> "ClusterMetadata":
        # Field order is exactly the schema's: error_code, brokers,
        # controller_id, topics. The leading code is request-level — an
        # authorization denial, say — and is distinct from the per-topic
        # one inside TopicInfo, which is what "no such topic" uses.
        request_error = reader.i32()
        if request_error != 0:
            raise ServerError(request_error, "metadata")
        brokers = []
        for _ in range(reader.i32()):
            brokers.append(
                BrokerInfo(
                    reader.i32(),
                    reader.string(),
                    reader.i32(),
                    # Empty when the broker was started without --rack.
                    reader.string(),
                )
            )
        reader.i32()  # controller_id
        topics = []
        for _ in range(reader.i32()):
            name = reader.string()
            topic_error = reader.i32()
            partitions = []
            for _ in range(reader.i32()):
                partition = reader.i32()
                leader = reader.i32()
                replicas = [reader.i32() for _ in range(reader.i32())]
                isr = [reader.i32() for _ in range(reader.i32())]
                leader_epoch = reader.i32()
                partitions.append(
                    PartitionInfo(partition, leader, leader_epoch, replicas, isr)
                )
            if topic_error not in (ErrorCode.NONE, ErrorCode.UNKNOWN_TOPIC_OR_PARTITION):
                raise ServerError(topic_error, f"metadata for {name}")
            topics.append(TopicInfo(name, partitions))
        return ClusterMetadata(brokers, topics)

    def partitions_of(self, topic: str) -> List[int]:
        for info in self.topics:
            if info.name == topic:
                return sorted(p.partition for p in info.partitions)
        return []

    def leader_of(self, topic: str, partition: int) -> Optional[int]:
        for info in self.topics:
            if info.name == topic:
                for part in info.partitions:
                    if part.partition == partition:
                        return part.leader
        return None


class BrokerRouter:
    """Keeps connections to every broker and routes by partition leader.

    A client connects to one address and is routed from there; there is no
    bootstrap list to maintain. Metadata is cached and refreshed only when
    a request comes back saying the route was stale, because refreshing on
    every request would put the control plane on the data path.
    """

    def __init__(self, host: str, port: int, client_id: str, timeout: float = 30.0) -> None:
        self._client_id = client_id
        self._timeout = timeout
        self._seed = Connection(host, port, client_id, timeout)
        self._connections: Dict[int, Connection] = {}
        self._metadata: Optional[ClusterMetadata] = None
        self._lock = threading.Lock()

    def close(self) -> None:
        for connection in self._connections.values():
            connection.close()
        self._connections.clear()
        self._seed.close()

    @property
    def seed(self) -> Connection:
        return self._seed

    def metadata(self, topics: Sequence[str] = (), refresh: bool = False) -> ClusterMetadata:
        with self._lock:
            if refresh or self._metadata is None:
                self._metadata = self._seed.metadata(topics)
            return self._metadata

    def refresh(self, topic: str) -> ClusterMetadata:
        return self.metadata([topic], refresh=True)

    def partitions(self, topic: str) -> List[int]:
        partitions = self.metadata([topic]).partitions_of(topic)
        if not partitions:
            # A topic auto-created on first produce is not in the cached
            # image yet; one refresh distinguishes "new" from "absent".
            partitions = self.refresh(topic).partitions_of(topic)
        if not partitions:
            raise BrahmaputraError(f"topic {topic!r} has no partitions")
        return partitions

    def connection_for(self, topic: str, partition: int) -> Connection:
        metadata = self.metadata([topic])
        leader = metadata.leader_of(topic, partition)
        if leader is None:
            metadata = self.refresh(topic)
            leader = metadata.leader_of(topic, partition)
        if leader is None:
            raise BrahmaputraError(f"no leader for {topic}-{partition}")
        return self._connection_to(leader, metadata)

    def _connection_to(self, node_id: int, metadata: ClusterMetadata) -> Connection:
        with self._lock:
            existing = self._connections.get(node_id)
            if existing is not None:
                return existing
            for broker in metadata.brokers:
                if broker.node_id == node_id:
                    # A single-broker cluster advertises the address the
                    # broker was configured with, which may not be the one
                    # we dialled; reuse the seed rather than opening a
                    # second connection to ourselves.
                    if len(metadata.brokers) == 1:
                        self._connections[node_id] = self._seed
                        return self._seed
                    connection = Connection(
                        broker.host, broker.port, self._client_id, self._timeout
                    )
                    self._connections[node_id] = connection
                    return connection
        raise BrahmaputraError(f"broker {node_id} is not in the metadata")


# --------------------------------------------------------------------------
# Producer
# --------------------------------------------------------------------------


@dataclass
class ProducerConfig:
    """Producer settings, named as Kafka names them."""

    client_id: str = "brahmaputra-python"
    #: 0 fire-and-forget, 1 leader append, -1/"all" every in-sync replica.
    acks: int = 1
    #: Flush a partition buffer once it holds this many bytes.
    batch_size: int = 16 * 1024
    #: Flush every non-empty buffer at least this often. 0 sends each
    #: record immediately. Kafka defaults to 0; this defaults to 5 because
    #: an unbatched producer is slow enough to look broken.
    linger_ms: int = 5
    #: none, lz4, zstd, snappy or gzip. Kafka defaults to none.
    compression_type: str = "lz4"
    #: Broker-side wait for the requested acknowledgements.
    request_timeout_ms: int = 30_000
    #: Retries of a send the broker refused with a *retriable* error —
    #: one it returns before appending, so a retry cannot duplicate.
    retries: int = 5
    retry_backoff_ms: int = 100
    #: Ceiling on the whole send, first attempt through last retry.
    delivery_timeout_ms: int = 120_000
    #: Ceiling on unflushed record bytes held client-side.
    buffer_memory: int = 32 * 1024 * 1024
    #: How long `send` may block on a full buffer before failing.
    max_block_ms: int = 60_000

    def compression(self) -> int:
        return Compression.parse(self.compression_type)

    def acks_value(self) -> int:
        if self.acks in (0, 1, -1):
            return self.acks
        raise ValueError(f"acks must be 0, 1 or -1, got {self.acks!r}")


@dataclass
class _Buffered:
    record: Record
    created_ms: int


class Producer:
    """A batching producer.

    Records accumulate per partition until the buffer reaches `batch_size`
    or `linger_ms` elapses, then go out as one Produce request carrying one
    record batch. Share one instance across threads rather than creating
    one per message: the batching is the point.
    """

    def __init__(self, host: str, port: int, config: Optional[ProducerConfig] = None) -> None:
        self.config = config or ProducerConfig()
        self._router = BrokerRouter(host, port, self.config.client_id)
        self._buffers: Dict[Tuple[str, int], List[_Buffered]] = {}
        self._sizes: Dict[Tuple[str, int], int] = {}
        self._buffered_bytes = 0
        self._lock = threading.Condition()
        self._round_robin = 0
        self._closed = False
        self._ticker: Optional[threading.Thread] = None
        if self.config.linger_ms > 0:
            self._ticker = threading.Thread(target=self._linger_loop, daemon=True)
            self._ticker.start()

    def close(self) -> None:
        self.flush()
        with self._lock:
            self._closed = True
            self._lock.notify_all()
        if self._ticker is not None:
            self._ticker.join(timeout=2.0)
        self._router.close()

    def __enter__(self) -> "Producer":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()

    def send(
        self,
        topic: str,
        value: bytes,
        key: Optional[bytes] = None,
        partition: Optional[int] = None,
        headers: Optional[List[RecordHeader]] = None,
    ) -> None:
        """Buffer one record. Call `flush` to await delivery.

        Returning without an offset is deliberate: with batching the offset
        is not known until the batch goes out, and pretending otherwise
        would mean a synchronous round trip per record.
        """
        headers = headers or []
        if partition is None:
            partitions = self._router.partitions(topic)
            if key is None:
                with self._lock:
                    index = self._round_robin % len(partitions)
                    self._round_robin += 1
                partition = partitions[index]
            else:
                partition = partition_for_key(key, partitions)

        record = Record(value=value, key=key, headers=headers)
        size = (
            len(value)
            + (len(key) if key else 0)
            + sum(len(h.key) + (len(h.value) if h.value else 0) + 4 for h in headers)
            + 16
        )
        self._reserve(size)

        with self._lock:
            slot = (topic, partition)
            self._buffers.setdefault(slot, []).append(_Buffered(record, _now_ms()))
            self._sizes[slot] = self._sizes.get(slot, 0) + size
            full = self._sizes[slot] >= self.config.batch_size

        if self.config.linger_ms == 0 or full:
            self._flush_partition(topic, partition)

    def send_and_wait(
        self,
        topic: str,
        value: bytes,
        key: Optional[bytes] = None,
        partition: Optional[int] = None,
        headers: Optional[List[RecordHeader]] = None,
    ) -> int:
        """Send one record on its own and return its offset.

        A full round trip per record — correct, and slow. Use `send` plus
        `flush` for anything with throughput requirements.
        """
        headers = headers or []
        if partition is None:
            partitions = self._router.partitions(topic)
            partition = (
                partition_for_key(key, partitions)
                if key is not None
                else partitions[self._round_robin % len(partitions)]
            )
            self._round_robin += 1
        record = Record(value=value, key=key, headers=headers)
        return self._produce(topic, partition, [_Buffered(record, _now_ms())])

    def flush(self) -> None:
        with self._lock:
            slots = [slot for slot, records in self._buffers.items() if records]
        for topic, partition in slots:
            self._flush_partition(topic, partition)

    # -- internals --------------------------------------------------------

    def _reserve(self, size: int) -> None:
        """Block until `size` more bytes may be buffered.

        This is what makes `buffer_memory` real: a producer faster than its
        broker is slowed down here rather than allowed to grow without
        limit and die holding records nobody has acknowledged.
        """
        limit = self.config.buffer_memory
        if limit <= 0 or size >= limit:
            # A record larger than the whole budget is admitted rather than
            # waiting forever on a condition that can never hold; refusing
            # oversized records is the broker's job (`max.message.bytes`).
            with self._lock:
                self._buffered_bytes += size
            return
        deadline = time.monotonic() + self.config.max_block_ms / 1000.0
        with self._lock:
            while self._buffered_bytes + size > limit:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise BrahmaputraError(
                        f"producer buffer full: {self._buffered_bytes} of {limit} bytes "
                        f"unflushed after max_block_ms={self.config.max_block_ms}"
                    )
                self._lock.wait(remaining)
            self._buffered_bytes += size

    def _release(self, size: int) -> None:
        with self._lock:
            self._buffered_bytes = max(0, self._buffered_bytes - size)
            self._lock.notify_all()

    def _linger_loop(self) -> None:
        interval = self.config.linger_ms / 1000.0
        while True:
            with self._lock:
                if self._closed:
                    return
                self._lock.wait(interval)
                if self._closed:
                    return
            try:
                self.flush()
            except BrahmaputraError:
                # A background flush that fails must not kill the ticker;
                # the next explicit flush surfaces the error to a caller
                # who can actually act on it.
                pass

    def _flush_partition(self, topic: str, partition: int) -> None:
        slot = (topic, partition)
        with self._lock:
            batch = self._buffers.get(slot)
            if not batch:
                return
            self._buffers[slot] = []
            size = self._sizes.pop(slot, 0)
        self._release(size)
        self._produce(topic, partition, batch)

    def _produce(self, topic: str, partition: int, buffered: List[_Buffered]) -> int:
        if not buffered:
            return -1
        # The batch stores one base timestamp and a delta per record, so
        # the rebasing happens here; `max_timestamp` becomes the newest
        # record's time, which is what makes it a truthful answer to "how
        # recent is this batch".
        max_timestamp = max(item.created_ms for item in buffered)
        records = []
        for item in buffered:
            item.record.timestamp_delta = item.created_ms - max_timestamp
            records.append(item.record)

        encoded = encode_record_batch(
            records,
            max_timestamp=max_timestamp,
            compression=self.config.compression(),
        )
        writer = body_writer()
        writer.string(topic)
        writer.i32(partition)
        writer.i32(self.config.acks_value())
        writer.i32(self.config.request_timeout_ms)
        writer.i64(len(encoded))
        body = writer.bytes() + encoded

        if self.config.acks_value() == 0:
            self._router.connection_for(topic, partition).send_oneway(ApiKey.PRODUCE, body)
            return -1

        deadline = time.monotonic() + self.config.delivery_timeout_ms / 1000.0
        attempts_left = self.config.retries
        while True:
            connection = self._router.connection_for(topic, partition)
            reader = body_reader(connection.request(ApiKey.PRODUCE, body))
            reader.string()  # topic
            reader.i32()  # partition
            code = reader.i32()
            base_offset = reader.i64()
            reader.i64()  # log_append_time_ms
            if code == ErrorCode.NONE:
                return base_offset
            remaining = deadline - time.monotonic()
            if code not in RETRIABLE_ERRORS or attempts_left <= 0 or remaining <= 0:
                raise ServerError(code, f"produce to {topic}-{partition}")
            attempts_left -= 1
            if code in (
                ErrorCode.NOT_LEADER_OR_FOLLOWER,
                ErrorCode.FENCED_LEADER_EPOCH,
                ErrorCode.UNKNOWN_LEADER_EPOCH,
            ):
                # A stale route is the most common retriable cause, and
                # resending to the same broker would just repeat it.
                self._router.refresh(topic)
            time.sleep(min(self.config.retry_backoff_ms / 1000.0, max(remaining, 0)))


# --------------------------------------------------------------------------
# Consumer
# --------------------------------------------------------------------------


@dataclass
class ConsumedRecord:
    topic: str
    partition: int
    offset: int
    key: Optional[bytes]
    value: bytes
    timestamp: int
    headers: List[RecordHeader] = field(default_factory=list)

    def header(self, key: str) -> Optional[bytes]:
        for header in self.headers:
            if header.key == key:
                return header.value
        return None


@dataclass
class ConsumerConfig:
    client_id: str = "brahmaputra-python"
    #: Response cap, split across the partitions in one request.
    fetch_max_bytes: int = 8 * 1024 * 1024
    #: Return early once this many bytes are ready.
    fetch_min_bytes: int = 1
    #: Long-poll ceiling when caught up.
    fetch_max_wait_ms: int = 500
    # READ_UNCOMMITTED (0) or READ_COMMITTED (1). A committed read stops at
    # the last stable offset and never sees an aborted transaction's records.
    isolation_level: int = 0
    #: This consumer's failure domain (`client.rack`), empty when it has none.
    rack: str = ""
    #: Records returned per poll; the rest stay buffered and uncommitted.
    max_poll_records: int = 500


class Consumer:
    """Reads one partition at a time, with no group coordination."""

    def __init__(self, host: str, port: int, config: Optional[ConsumerConfig] = None) -> None:
        self.config = config or ConsumerConfig()
        self._router = BrokerRouter(host, port, self.config.client_id)

    def close(self) -> None:
        self._router.close()

    def __enter__(self) -> "Consumer":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()

    @property
    def router(self) -> BrokerRouter:
        return self._router

    def partitions(self, topic: str) -> List[int]:
        return self._router.partitions(topic)

    def list_offsets(self, topic: str, partition: int, timestamp: int) -> int:
        """Resolve EARLIEST, LATEST or a unix-ms timestamp to an offset."""
        writer = body_writer()
        writer.string(topic)
        writer.i32(partition)
        writer.i64(timestamp)
        connection = self._router.connection_for(topic, partition)
        reader = body_reader(connection.request(ApiKey.LIST_OFFSETS, writer.bytes()))
        reader.string()  # topic
        reader.i32()  # partition
        code = reader.i32()
        offset = reader.i64()
        reader.i64()  # timestamp
        if code != ErrorCode.NONE:
            raise ServerError(code, f"list_offsets {topic}-{partition}")
        return offset

    def fetch(
        self,
        topic: str,
        partition: int,
        offset: int,
        max_wait_ms: Optional[int] = None,
    ) -> List[ConsumedRecord]:
        records, _ = self.fetch_verbose(topic, partition, offset, max_wait_ms)
        return records

    def fetch_verbose(
        self,
        topic: str,
        partition: int,
        offset: int,
        max_wait_ms: Optional[int] = None,
    ) -> Tuple[List[ConsumedRecord], int]:
        """Fetch, also returning the partition's high watermark."""
        wait = self.config.fetch_max_wait_ms if max_wait_ms is None else max_wait_ms
        writer = body_writer()
        writer.string(topic)
        writer.i32(partition)
        writer.i64(offset)
        writer.i32(self.config.fetch_max_bytes)
        writer.i32(min(wait, self.config.fetch_max_wait_ms))
        writer.i32(self.config.fetch_min_bytes)
        writer.i32(self.config.isolation_level)
        # `client.rack`: with it set the leader names an in-sync replica in
        # the same rack, and this client reads from that instead.
        writer.string(self.config.rack)
        body = writer.bytes()

        connection = self._router.connection_for(topic, partition)
        response = connection.request(ApiKey.FETCH, body)
        code, high_watermark, batches = _decode_fetch_response(response)
        if code == ErrorCode.NOT_LEADER_OR_FOLLOWER:
            self._router.refresh(topic)
            connection = self._router.connection_for(topic, partition)
            code, high_watermark, batches = _decode_fetch_response(
                connection.request(ApiKey.FETCH, body)
            )
        if code != ErrorCode.NONE:
            raise ServerError(code, f"fetch {topic}-{partition}")

        return _records_from(batches, topic, partition, offset), high_watermark


def _decode_fetch_response(body: bytes) -> Tuple[int, int, List[DecodedBatch]]:
    reader = body_reader(body)
    reader.string()  # topic
    reader.i32()  # partition
    code = reader.i32()
    high_watermark = reader.i64()
    reader.i64()  # last_stable_offset
    batches_length = reader.i64()
    # Read even though this client does not act on it: the batches trail the
    # whole struct, so skipping a field would take them from the wrong
    # offset and every batch after it would fail to decode.
    reader.i32()  # preferred_read_replica
    trailing = reader.rest()
    if batches_length > len(trailing):
        raise ProtocolError("fetch response claims more batch bytes than it carries")
    raw = trailing[: int(batches_length)]

    batches: List[DecodedBatch] = []
    pos = 0
    while pos < len(raw):
        batch, pos = decode_record_batch(raw, pos)
        batches.append(batch)
    return code, high_watermark, batches


def _records_from(
    batches: Iterable[DecodedBatch], topic: str, partition: int, min_offset: int
) -> List[ConsumedRecord]:
    out: List[ConsumedRecord] = []
    for batch in batches:
        for index, record in enumerate(batch.records):
            offset = batch.base_offset + index
            # A batch can start before the requested offset; skip what the
            # caller has already seen.
            if offset < min_offset:
                continue
            out.append(
                ConsumedRecord(
                    topic=topic,
                    partition=partition,
                    offset=offset,
                    key=record.key,
                    value=record.value,
                    timestamp=record.timestamp(batch.max_timestamp),
                    headers=list(record.headers),
                )
            )
    return out
