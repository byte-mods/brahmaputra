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
from typing import Dict, Iterable, List, Optional, Sequence, Tuple, Union

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


#: Bound on one request/response round trip. It must exceed the longest
#: the broker may legitimately hold a request (a fetch long-poll, an
#: acks=all wait, a JoinGroup waiting out a rebalance), so it is generous;
#: its job is to turn a wedged broker into an error instead of a thread
#: blocked forever.
DEFAULT_REQUEST_TIMEOUT_S = 120.0


class BrokerConnectionError(BrahmaputraError):
    """The connection failed (I/O error, timeout, desync) and was closed."""


class Connection:
    """One TCP connection to one broker.

    A lock serialises request/response pairs, so there is at most one
    request in flight per connection; that is enough for a producer that
    batches, and it is what keeps a partition's appends in order.

    Any I/O failure, timeout or correlation mismatch leaves the byte stream
    at an unknown position — a partial frame may have been written, or a
    late response may still arrive — so the connection is closed and
    marked :attr:`broken` rather than reused. The router notices and
    redials.
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
        self.request_timeout = DEFAULT_REQUEST_TIMEOUT_S
        self._correlation = 0
        self._lock = threading.Lock()
        self._broken = False
        try:
            self._sock = socket.create_connection((host, port), timeout=timeout)
        except OSError as error:
            raise BrokerConnectionError(f"connect to {host}:{port}: {error}") from error
        # Responses are small and latency matters more than packet count;
        # without this every request pays Nagle plus the peer's delayed ACK.
        self._sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    @property
    def broken(self) -> bool:
        """True once this connection has failed and must not be reused."""
        return self._broken

    def set_request_timeout(self, seconds: Optional[float]) -> None:
        """How long one round trip may take before the connection is
        abandoned. None or <= 0 disables the bound."""
        with self._lock:
            self.request_timeout = seconds if seconds and seconds > 0 else None

    def close(self) -> None:
        self._broken = True
        try:
            self._sock.close()
        except OSError:
            pass

    def __enter__(self) -> "Connection":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()

    def _fail(self, error: BaseException) -> BrokerConnectionError:
        # Called with the lock held.
        self.close()
        if isinstance(error, BrokerConnectionError):
            return error
        return BrokerConnectionError(
            f"connection to {self.host}:{self.port} failed: {error!r}"
        )

    def _check_usable(self) -> None:
        if self._broken:
            raise BrokerConnectionError(
                f"connection to {self.host}:{self.port} is broken; the router will redial"
            )

    def request(self, api_key: int, body: bytes) -> bytes:
        with self._lock:
            self._check_usable()
            self._correlation = (self._correlation + 1) & 0x7FFFFFFF
            correlation_id = self._correlation
            deadline = (
                time.monotonic() + self.request_timeout if self.request_timeout else None
            )
            try:
                self._arm(deadline)
                self._sock.sendall(encode_frame(api_key, correlation_id, self.client_id, body))
                # Includes a timeout: the response may still be on its way,
                # and reading on from here would pair it with the next request.
                payload = self._read_frame(deadline)
                _key, got, response_body = decode_frame_payload(payload)
            except (OSError, BrahmaputraError) as error:
                raise self._fail(error) from error
            if got != correlation_id:
                # A response for a request we are no longer waiting on can
                # only mean the stream has desynchronised; continuing would
                # pair every later response with the wrong request.
                raise self._fail(ProtocolError(
                    f"correlation id mismatch: expected {correlation_id}, got {got}"
                ))
            return response_body

    def send_oneway(self, api_key: int, body: bytes) -> None:
        """Send without awaiting a response (`acks=0`)."""
        with self._lock:
            self._check_usable()
            self._correlation = (self._correlation + 1) & 0x7FFFFFFF
            deadline = (
                time.monotonic() + self.request_timeout if self.request_timeout else None
            )
            try:
                self._arm(deadline)
                self._sock.sendall(
                    encode_frame(api_key, self._correlation, self.client_id, body)
                )
            except OSError as error:
                raise self._fail(error) from error

    def _arm(self, deadline: Optional[float]) -> None:
        if deadline is None:
            self._sock.settimeout(None)
            return
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise socket.timeout("request timed out")
        self._sock.settimeout(remaining)

    def _read_frame(self, deadline: Optional[float]) -> bytes:
        header = self._read_exact(4, deadline)
        (length,) = struct.unpack(">i", header)
        if length < 0:
            raise ProtocolError(f"negative frame length {length}")
        return self._read_exact(length, deadline)

    def _read_exact(self, count: int, deadline: Optional[float]) -> bytes:
        buf = bytearray(count)
        view = memoryview(buf)
        got = 0
        while got < count:
            # socket timeouts apply per call, so re-arm with what is left of
            # the whole round trip's budget.
            self._arm(deadline)
            n = self._sock.recv_into(view[got:], count - got)
            if not n:
                raise BrokerConnectionError("connection closed by broker")
            got += n
        return bytes(buf)

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

    def api_versions(self) -> Tuple[Dict[int, Tuple[int, int]], str]:
        """Ask the broker what it speaks: ({api_key: (min, max)}, broker_version).

        This is the one call that works across a version mismatch, so it is
        what a client uses to decide whether it can talk to a broker at all.
        """
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
        broker_version = reader.string()
        return out, broker_version

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

    A connection that failed is replaced on its next use rather than kept:
    without that, one dropped socket — a broker restart, an idle timeout on
    a load balancer — would fail every later request for the life of the
    client.
    """

    def __init__(
        self,
        host: str,
        port: int,
        client_id: str,
        timeout: float = 30.0,
        request_timeout: Optional[float] = DEFAULT_REQUEST_TIMEOUT_S,
    ) -> None:
        self._client_id = client_id
        self._timeout = timeout
        self._request_timeout = request_timeout
        self._seed_address = (host, port)
        self._seed = self._dial(host, port)
        self._connections: Dict[int, Connection] = {}
        self._metadata: Optional[ClusterMetadata] = None
        self._lock = threading.RLock()

    def _dial(self, host: str, port: int) -> Connection:
        connection = Connection(host, port, self._client_id, self._timeout)
        connection.set_request_timeout(self._request_timeout)
        return connection

    def set_request_timeout(self, seconds: Optional[float]) -> None:
        """Bound one round trip on every connection held now or dialled
        later. None or <= 0 disables the bound. It must exceed the longest
        the broker may hold a request: a fetch long-poll, an acks=all wait,
        a JoinGroup rebalance."""
        with self._lock:
            self._request_timeout = seconds
            self._seed.set_request_timeout(seconds)
            for connection in self._connections.values():
                connection.set_request_timeout(seconds)

    def close(self) -> None:
        with self._lock:
            for connection in self._connections.values():
                if connection is not self._seed:
                    connection.close()
            self._connections.clear()
            self._seed.close()

    @property
    def seed(self) -> Connection:
        """The connection this router was opened with, redialled if broken."""
        with self._lock:
            return self._live_seed()

    def _live_seed(self) -> Connection:
        # Called with the lock held.
        if not self._seed.broken:
            return self._seed
        old = self._seed
        host, port = self._seed_address
        self._seed = self._dial(host, port)
        for node_id, cached in list(self._connections.items()):
            if cached is old:
                self._connections[node_id] = self._seed
        return self._seed

    def metadata(self, topics: Sequence[str] = (), refresh: bool = False) -> ClusterMetadata:
        with self._lock:
            if refresh or self._metadata is None:
                self._metadata = self._live_seed().metadata(topics)
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
        if leader is None or leader < 0:
            raise BrahmaputraError(f"no leader for {topic}-{partition}")
        return self._connection_to(leader, metadata)

    def _connection_to(self, node_id: int, metadata: ClusterMetadata) -> Connection:
        with self._lock:
            existing = self._connections.get(node_id)
            if existing is not None:
                if not existing.broken:
                    return existing
                del self._connections[node_id]
                if existing is not self._seed:
                    existing.close()
            for broker in metadata.brokers:
                if broker.node_id == node_id:
                    # A single-broker cluster advertises the address the
                    # broker was configured with, which may not be the one
                    # we dialled; reuse the seed rather than opening a
                    # second connection to ourselves.
                    if len(metadata.brokers) == 1:
                        seed = self._live_seed()
                        self._connections[node_id] = seed
                        return seed
                    connection = self._dial(broker.host, broker.port)
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
    #: 0 fire-and-forget, 1 leader append, -1 or "all" every in-sync replica.
    acks: Union[int, str] = 1
    #: Flush a partition buffer once it holds this many bytes.
    batch_size: int = 16 * 1024
    #: Flush every non-empty buffer at least this often. 0 sends each
    #: record immediately. Kafka defaults to 0; this defaults to 5 because
    #: an unbatched producer is slow enough to look broken.
    linger_ms: int = 5
    #: none, gzip, lz4, zstd or snappy. none and gzip are built in; the
    #: others need `register_codec` or their optional package.
    compression_type: str = "none"
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
    #: Connect timeout, in seconds.
    socket_timeout_s: float = 30.0
    #: Client-side bound on one request/response round trip, in seconds; a
    #: broker that stops answering costs an error, not a hang. Keep it above
    #: request_timeout_ms. None disables it.
    round_trip_timeout_s: Optional[float] = DEFAULT_REQUEST_TIMEOUT_S

    def compression(self) -> int:
        return Compression.parse(self.compression_type)

    def acks_value(self) -> int:
        """The wire value of `acks`: 0, 1 or -1 (Kafka's "all")."""
        acks = self.acks
        if isinstance(acks, str):
            acks = -1 if acks.strip().lower() == "all" else int(acks)
        if isinstance(acks, bool) or acks not in (0, 1, -1):
            raise ValueError(f"acks must be 0, 1, -1 or 'all', got {self.acks!r}")
        return acks


def _record_size(record: Record) -> int:
    """Bytes a buffered record is charged against `buffer_memory`.

    A tombstone (`value=None`) and a null header value count as zero bytes
    of payload, not as a crash.
    """
    size = 16 + len(record.value or b"") + len(record.key or b"")
    for header in record.headers:
        size += len(header.key.encode("utf-8")) + len(header.value or b"") + 4
    return size


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
        # Validated up front so a typo fails at construction, not on the
        # first flush from the background ticker where nobody sees it.
        self._codec = self.config.compression()
        self._acks = self.config.acks_value()
        self._router = BrokerRouter(
            host,
            port,
            self.config.client_id,
            self.config.socket_timeout_s,
            self.config.round_trip_timeout_s,
        )
        self._buffers: Dict[Tuple[str, int], List[_Buffered]] = {}
        self._sizes: Dict[Tuple[str, int], int] = {}
        self._buffered_bytes = 0
        self._lock = threading.Condition()
        self._round_robin = 0
        self._closed = False
        self._ticker: Optional[threading.Thread] = None
        self._background_error: Optional[BaseException] = None
        self._stop = threading.Event()
        self._send_locks: Dict[Tuple[str, int], threading.Lock] = {}
        if self.config.linger_ms > 0:
            self._ticker = threading.Thread(target=self._linger_loop, daemon=True)
            self._ticker.start()

    def close(self) -> None:
        """Flush, stop the linger ticker and release connections.

        Connections are released even when the final flush fails; the
        flush error is still raised.
        """
        try:
            self.flush()
        finally:
            with self._lock:
                self._closed = True
                self._lock.notify_all()
            self._stop.set()
            if self._ticker is not None:
                self._ticker.join(timeout=2.0)
            self._router.close()

    def __enter__(self) -> "Producer":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()

    @property
    def router(self) -> BrokerRouter:
        """The routing layer, for callers that need metadata."""
        return self._router

    def send(
        self,
        topic: str,
        value: Optional[bytes],
        key: Optional[bytes] = None,
        partition: Optional[int] = None,
        headers: Optional[List[RecordHeader]] = None,
        timestamp_ms: Optional[int] = None,
    ) -> None:
        """Buffer one record. Call `flush` to await delivery.

        `value=None` is a tombstone, distinct from `b""`. With `partition`
        set the partitioner is bypassed; otherwise a keyed record goes to
        `murmur2(key) % partitions` and an unkeyed one round-robins.
        `timestamp_ms` (unix milliseconds) overrides the wall-clock record
        timestamp.

        Returning without an offset is deliberate: with batching the offset
        is not known until the batch goes out, and pretending otherwise
        would mean a synchronous round trip per record.
        """
        headers = list(headers or [])
        if partition is None:
            partition = self._choose_partition(topic, key)

        record = Record(value=value, key=key, headers=headers)
        size = _record_size(record)
        self._reserve(size)

        with self._lock:
            slot = (topic, partition)
            self._buffers.setdefault(slot, []).append(
                _Buffered(record, _now_ms() if timestamp_ms is None else int(timestamp_ms))
            )
            self._sizes[slot] = self._sizes.get(slot, 0) + size
            full = self._sizes[slot] >= self.config.batch_size

        if self.config.linger_ms <= 0 or full:
            self._flush_partition(topic, partition)

    def send_and_wait(
        self,
        topic: str,
        value: Optional[bytes],
        key: Optional[bytes] = None,
        partition: Optional[int] = None,
        headers: Optional[List[RecordHeader]] = None,
        timestamp_ms: Optional[int] = None,
    ) -> int:
        """Send one record on its own and return its offset.

        A full round trip per record — correct, and slow. Use `send` plus
        `flush` for anything with throughput requirements. Returns -1 with
        `acks=0`, where no offset comes back. Records already buffered for
        the partition go first, so send order is kept.
        """
        if partition is None:
            partition = self._choose_partition(topic, key)
        record = Record(value=value, key=key, headers=list(headers or []))
        created = _now_ms() if timestamp_ms is None else int(timestamp_ms)
        with self._send_lock(topic, partition):
            self._take_and_send(topic, partition)
            return self._produce(topic, partition, [_Buffered(record, created)])

    def flush(self) -> None:
        """Send every buffered record and wait for acknowledgement.

        Also raises the failure of any background (linger) flush since the
        last call, because those records are gone and no other call would
        say so.
        """
        with self._lock:
            slots = [slot for slot, records in self._buffers.items() if records]
        first: Optional[BaseException] = None
        for topic, partition in slots:
            try:
                self._flush_partition(topic, partition)
            except Exception as error:  # noqa: BLE001
                first = first or error
        # Wait out batches the linger ticker has in flight, so "flushed"
        # means acknowledged and their failures are seen below.
        with self._lock:
            locks = list(self._send_locks.values())
        for send_lock in locks:
            with send_lock:
                pass
        with self._lock:
            background, self._background_error = self._background_error, None
        if first is not None:
            raise first
        if background is not None:
            raise background

    # -- internals --------------------------------------------------------

    def _choose_partition(self, topic: str, key: Optional[bytes]) -> int:
        partitions = self._router.partitions(topic)
        if key is not None:
            return partition_for_key(key, partitions)
        with self._lock:
            index = self._round_robin % len(partitions)
            self._round_robin += 1
        return partitions[index]

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
        # Waits on its own event, not on the buffer condition: that one is
        # notified on every release, which would turn the linger into a
        # flush-after-every-flush loop.
        while not self._stop.wait(interval):
            with self._lock:
                slots = [slot for slot, records in self._buffers.items() if records]
            for topic, partition in slots:
                try:
                    self._flush_partition(topic, partition)
                except Exception as error:  # noqa: BLE001
                    # A background flush that fails must not kill the
                    # ticker, nor stop other partitions flushing. Those
                    # records have left the buffer, so the error is the only
                    # trace of them: the next flush()/close() raises it.
                    with self._lock:
                        if self._background_error is None:
                            self._background_error = error

    def _send_lock(self, topic: str, partition: int) -> threading.Lock:
        slot = (topic, partition)
        with self._lock:
            send_lock = self._send_locks.get(slot)
            if send_lock is None:
                send_lock = self._send_locks[slot] = threading.Lock()
        return send_lock

    def _flush_partition(self, topic: str, partition: int) -> None:
        # Held across the round trip (and any retries): a partition has at
        # most one batch in flight, and batches leave in the order they were
        # taken. Without it the linger ticker and a send that fills a batch
        # could each take a batch for the same partition and race to the
        # connection, reordering the log.
        with self._send_lock(topic, partition):
            self._take_and_send(topic, partition)

    def _take_and_send(self, topic: str, partition: int) -> None:
        # Called with the partition's send lock held.
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
            compression=self._codec,
        )
        writer = body_writer()
        writer.string(topic)
        writer.i32(partition)
        writer.i32(self._acks)
        writer.i32(self.config.request_timeout_ms)
        writer.i64(len(encoded))
        body = writer.bytes() + encoded

        if self._acks == 0:
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
                try:
                    self._router.refresh(topic)
                except BrahmaputraError:
                    pass
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
    #: None for a tombstone, distinct from b"".
    value: Optional[bytes]
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
    #: Connect timeout, in seconds.
    socket_timeout_s: float = 30.0
    #: Client-side bound on one request/response round trip, in seconds.
    #: Must exceed fetch_max_wait_ms. None disables it.
    round_trip_timeout_s: Optional[float] = DEFAULT_REQUEST_TIMEOUT_S


class Consumer:
    """Reads one partition at a time, with no group coordination."""

    def __init__(self, host: str, port: int, config: Optional[ConsumerConfig] = None) -> None:
        self.config = config or ConsumerConfig()
        self._router = BrokerRouter(
            host,
            port,
            self.config.client_id,
            self.config.socket_timeout_s,
            self.config.round_trip_timeout_s,
        )

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
    if batches_length < 0 or batches_length > len(trailing):
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
