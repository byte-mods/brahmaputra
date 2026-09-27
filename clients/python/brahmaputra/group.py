"""Group-coordinated consumer.

The coordinator for a group is the leader of `__consumer_offsets`
partition `crc32c(group_id) % partitions`, so every group request goes to
that broker and nowhere else.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Sequence, Tuple

from .protocol import (
    ApiKey,
    BrahmaputraError,
    ErrorCode,
    NoOffsetForPartition,
    ServerError,
    body_reader,
    body_writer,
    crc32c,
)
from .client import (
    DEFAULT_REQUEST_TIMEOUT_S,
    EARLIEST,
    LATEST,
    ConsumedRecord,
    Consumer,
    ConsumerConfig,
    _now_ms,
)

OFFSETS_TOPIC = "__consumer_offsets"
#: Retries per coordinator request after a coordinator move or load.
COORDINATOR_ATTEMPTS = 4
#: Join+sync rounds before giving up on a group that will not stabilise.
JOIN_ATTEMPTS = 4


class AutoOffsetReset:
    """Where to start when a partition has no valid position.

    Either the group never committed one, or the committed one has fallen
    off the front of the log because retention deleted it. Both are the
    same situation to a consumer, so they take one policy.
    """

    #: Oldest record still retained. Reprocesses; never silently skips.
    EARLIEST = "earliest"
    #: The end. Skips whatever was missed; never reprocesses.
    LATEST = "latest"
    #: Refuse to guess and raise `NoOffsetForPartition`. The honest choice
    #: when neither reprocessing nor skipping is safe.
    NONE = "none"


class Assignor:
    RANGE = "range"
    ROUNDROBIN = "roundrobin"
    #: Keeps members on the partitions they already hold. Prefer this when
    #: consumers carry per-partition state, because every partition that
    #: moves throws that state away.
    STICKY = "sticky"


@dataclass
class GroupConfig:
    client_id: str = "brahmaputra-python"
    #: The coordinator evicts a member that stops heartbeating for this
    #: long. Kafka defaults to 45s; this defaults to 10s as the Rust
    #: client does.
    session_timeout_ms: int = 10_000
    #: How long the coordinator waits for members to rejoin.
    rebalance_timeout_ms: int = 3_000
    #: How often the background thread heartbeats. 0 means
    #: session_timeout_ms / 3, Kafka's rule of thumb.
    heartbeat_interval_ms: int = 0
    #: Longest gap between `poll` calls before this member is presumed
    #: stuck and leaves the group. Separate from the session timeout on
    #: purpose: heartbeats prove the process is alive, this proves the
    #: application is still consuming.
    max_poll_interval_ms: int = 300_000
    #: 0 disables auto-commit.
    auto_commit_interval_ms: int = 5_000
    auto_offset_reset: str = AutoOffsetReset.EARLIEST
    assignor: str = Assignor.RANGE
    #: Stable identity across restarts (KIP-345). A static member reclaims
    #: its own partitions instead of triggering two rebalances per restart.
    group_instance_id: str = ""
    max_poll_records: int = 500
    fetch_max_bytes: int = 8 * 1024 * 1024
    #: Connect and socket timeout, in seconds.
    socket_timeout_s: float = 30.0
    #: Client-side bound on one round trip, in seconds. Must exceed
    #: rebalance_timeout_ms, which a JoinGroup may legitimately wait out.
    round_trip_timeout_s: Optional[float] = DEFAULT_REQUEST_TIMEOUT_S


class GroupConsumer:
    """A consumer that shares a topic's partitions with its group.

    Single-threaded by design, matching Kafka's consumer: use one per
    thread and give each its own client id.
    """

    def __init__(
        self,
        host: str,
        port: int,
        group_id: str,
        config: Optional[GroupConfig] = None,
    ) -> None:
        self.group_id = group_id
        self.config = config or GroupConfig()
        self._consumer = Consumer(
            host,
            port,
            ConsumerConfig(
                client_id=self.config.client_id,
                fetch_max_bytes=self.config.fetch_max_bytes,
                max_poll_records=self.config.max_poll_records,
                socket_timeout_s=self.config.socket_timeout_s,
                round_trip_timeout_s=self.config.round_trip_timeout_s,
            ),
        )
        self._subscribed: List[str] = []
        self._member_id = ""
        self._generation = -1
        self._joined = False
        self._assignment: List[Tuple[str, int]] = []
        #: Next offset to *deliver* — what gets committed. Only advances
        #: over records handed to the caller.
        self._positions: Dict[Tuple[str, int], int] = {}
        #: Next offset to *fetch*. Runs ahead of `_positions` by exactly
        #: the records sitting in `_buffered`.
        self._fetch_positions: Dict[Tuple[str, int], int] = {}
        self._buffered: List[ConsumedRecord] = []
        self._last_poll_ms = _now_ms()
        #: True while `poll` runs. max_poll_interval_ms bounds the gap
        #: *between* polls — time the application spends processing — so a
        #: poll that is itself busy joining a slow rebalance must not count.
        self._in_poll = False
        self._last_commit_ms = _now_ms()
        #: Guards the fields the heartbeat thread shares: member id,
        #: generation, joined, and the poll timestamps.
        self._lock = threading.Lock()
        self._closed = False
        self._stop = threading.Event()
        self._heartbeat = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._heartbeat.start()

    # -- lifecycle --------------------------------------------------------

    def subscribe(self, topics: Sequence[str]) -> None:
        self._subscribed = list(topics)
        self._set_joined(False)

    @property
    def member_id(self) -> str:
        """The id the coordinator gave this member; empty before joining."""
        return self._membership()[0]

    @property
    def generation(self) -> int:
        """The generation this member last joined; -1 before joining."""
        return self._membership()[1]

    @property
    def assignment(self) -> List[Tuple[str, int]]:
        """The (topic, partition) pairs this member currently owns."""
        return list(self._assignment)

    def _membership(self) -> Tuple[str, int, bool]:
        with self._lock:
            return self._member_id, self._generation, self._joined

    def _set_joined(self, joined: bool) -> None:
        with self._lock:
            self._joined = joined

    def close(self) -> None:
        """Commit, leave the group, then stop.

        Leaving is what separates a clean shutdown from a crash. Without
        it the coordinator cannot tell the difference and must wait out
        `session_timeout_ms` before reassigning, so a rolling restart of N
        instances costs N session timeouts of stalled partitions.
        """
        with self._lock:
            self._closed = True
        self._stop.set()
        member_id, _, joined = self._membership()
        try:
            if joined:
                self.commit()
        except BrahmaputraError:
            pass
        try:
            if member_id:
                self._leave()
        except BrahmaputraError:
            # Best effort: the caller is shutting down, and failing here
            # costs only the session timeout it was trying to avoid.
            pass
        self._heartbeat.join(timeout=2.0)
        self._consumer.close()

    def __enter__(self) -> "GroupConsumer":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()

    # -- polling ----------------------------------------------------------

    def poll(self, timeout_ms: int = 1000) -> List[ConsumedRecord]:
        if not self._subscribed:
            raise BrahmaputraError("subscribe to at least one topic before polling")
        # Stamped on entry and again on return, and not enforced in between:
        # the interval bounds how long the *application* may go without
        # asking for records, and a poll that blocks — for its timeout, or
        # on a slow rebalance — is the consumer working normally.
        with self._lock:
            self._last_poll_ms = _now_ms()
            self._in_poll = True
        try:
            return self._poll(timeout_ms)
        finally:
            with self._lock:
                self._last_poll_ms = _now_ms()
                self._in_poll = False

    def _poll(self, timeout_ms: int) -> List[ConsumedRecord]:
        deadline = time.monotonic() + timeout_ms / 1000.0
        while True:
            # Checked every sweep, not only on entry: a rebalance the
            # heartbeat learns of mid-poll must stop this member fetching
            # partitions it may no longer own.
            if not self._membership()[2]:
                self._join()
            if self._buffered:
                return self._take_buffered()
            if not self._assignment:
                if time.monotonic() >= deadline:
                    return []
                time.sleep(0.05)
                continue

            got_any = False
            for topic, partition in list(self._assignment):
                remaining_ms = max(0, int((deadline - time.monotonic()) * 1000))
                offset = self._fetch_positions.get((topic, partition), 0)
                try:
                    records = self._consumer.fetch(
                        topic, partition, offset, min(remaining_ms, 500)
                    )
                except ServerError as error:
                    if error.code == ErrorCode.OFFSET_OUT_OF_RANGE:
                        # The committed offset fell off the log; restart
                        # where the policy says.
                        reset = self._reset_offset(topic, partition)
                        self._fetch_positions[(topic, partition)] = reset
                        self._positions[(topic, partition)] = reset
                        self._buffered = [
                            record
                            for record in self._buffered
                            if (record.topic, record.partition) != (topic, partition)
                        ]
                        continue
                    if error.code == ErrorCode.NOT_LEADER_OR_FOLLOWER:
                        self._consumer.router.refresh(topic)
                        continue
                    raise
                if records:
                    got_any = True
                    self._fetch_positions[(topic, partition)] = records[-1].offset + 1
                    self._buffered.extend(records)

            self._maybe_auto_commit()
            if self._buffered:
                return self._take_buffered()
            if not got_any and time.monotonic() >= deadline:
                return []

    def _take_buffered(self) -> List[ConsumedRecord]:
        limit = self.config.max_poll_records
        if limit <= 0:
            limit = len(self._buffered)
        delivered = self._buffered[:limit]
        self._buffered = self._buffered[limit:]
        for record in delivered:
            # The consumed position advances only over records actually
            # handed to the caller; committing what was merely fetched
            # would silently skip records nobody processed.
            self._positions[(record.topic, record.partition)] = record.offset + 1
        return delivered

    # -- offsets ----------------------------------------------------------

    def commit(self) -> None:
        """Commit the delivered positions. At-least-once: call after processing."""
        if not self._positions:
            return
        member_id, generation, _ = self._membership()
        writer = body_writer()
        writer.string(self.group_id)
        writer.i32(generation)
        writer.string(member_id)
        entries = sorted(self._positions.items())
        writer.i32(len(entries))
        for (topic, partition), offset in entries:
            writer.string(topic)
            writer.i32(partition)
            writer.i64(offset)
        reader = body_reader(self._coordinator_request(ApiKey.OFFSET_COMMIT, writer.bytes()))
        code = reader.i32()
        if code != ErrorCode.NONE:
            raise ServerError(code, "offset_commit")
        self._last_commit_ms = _now_ms()

    def committed(self, partitions: Sequence[Tuple[str, int]] = ()) -> Dict[Tuple[str, int], int]:
        writer = body_writer()
        writer.string(self.group_id)
        writer.i32(len(partitions))
        for topic, partition in partitions:
            writer.string(topic)
            writer.i32(partition)
        reader = body_reader(self._coordinator_request(ApiKey.OFFSET_FETCH, writer.bytes()))
        code = reader.i32()
        if code != ErrorCode.NONE:
            raise ServerError(code, "offset_fetch")
        out: Dict[Tuple[str, int], int] = {}
        for _ in range(reader.i32()):
            topic = reader.string()
            partition = reader.i32()
            out[(topic, partition)] = reader.i64()
        return out

    def _maybe_auto_commit(self) -> None:
        interval = self.config.auto_commit_interval_ms
        if interval <= 0 or not self._positions:
            return
        if _now_ms() - self._last_commit_ms < interval:
            return
        try:
            self.commit()
        except BrahmaputraError:
            # An auto-commit that fails is retried on the next poll; the
            # explicit `commit` is what a caller relies on.
            pass

    def _reset_offset(self, topic: str, partition: int) -> int:
        policy = self.config.auto_offset_reset
        if policy == AutoOffsetReset.EARLIEST:
            return self._consumer.list_offsets(topic, partition, EARLIEST)
        if policy == AutoOffsetReset.LATEST:
            return self._consumer.list_offsets(topic, partition, LATEST)
        if policy == AutoOffsetReset.NONE:
            raise NoOffsetForPartition(f"no committed offset for {topic}-{partition}")
        raise ValueError(f"unknown auto_offset_reset {policy!r}")

    # -- membership -------------------------------------------------------

    def _join(self) -> None:
        for _ in range(JOIN_ATTEMPTS):
            writer = body_writer()
            writer.string(self.group_id)
            writer.i32(self.config.session_timeout_ms)
            writer.i32(self.config.rebalance_timeout_ms)
            writer.string(self._membership()[0])
            writer.string_array(self._subscribed)
            writer.string(self.config.group_instance_id)

            reader = body_reader(self._coordinator_request(ApiKey.JOIN_GROUP, writer.bytes()))
            code = reader.i32()
            if code == ErrorCode.REBALANCE_IN_PROGRESS:
                time.sleep(0.1)
                continue
            if code == ErrorCode.UNKNOWN_MEMBER_ID:
                # The coordinator dropped this member (session expiry, or
                # removed while it waited): join again as a new one.
                with self._lock:
                    self._member_id = ""
                continue
            if code != ErrorCode.NONE:
                raise ServerError(code, "join_group")

            generation = reader.i32()
            member_id = reader.string()
            leader_id = reader.string()
            members = []
            for _ in range(reader.i32()):
                name = reader.string()
                topics = reader.string_array()
                held = []
                for _ in range(reader.i32()):
                    held.append((reader.string(), reader.i32()))
                members.append((name, topics, held))

            with self._lock:
                self._member_id = member_id
                self._generation = generation

            assignments = (
                self._compute_assignments(members) if member_id == leader_id else []
            )
            if self._sync(assignments):
                self._set_joined(True)
                return
        raise BrahmaputraError(
            f"consumer group failed to stabilise after {JOIN_ATTEMPTS} join attempts"
        )

    def _sync(self, assignments: List[Tuple[str, List[Tuple[str, int]]]]) -> bool:
        member_id, generation, _ = self._membership()
        writer = body_writer()
        writer.string(self.group_id)
        writer.i32(generation)
        writer.string(member_id)
        writer.i32(len(assignments))
        for member_id, partitions in assignments:
            writer.string(member_id)
            writer.i32(len(partitions))
            for topic, partition in partitions:
                writer.string(topic)
                writer.i32(partition)

        reader = body_reader(self._coordinator_request(ApiKey.SYNC_GROUP, writer.bytes()))
        code = reader.i32()
        if code in (ErrorCode.REBALANCE_IN_PROGRESS, ErrorCode.ILLEGAL_GENERATION):
            return False
        if code == ErrorCode.UNKNOWN_MEMBER_ID:
            with self._lock:
                self._member_id = ""
            return False
        if code != ErrorCode.NONE:
            raise ServerError(code, "sync_group")

        assignment = []
        for _ in range(reader.i32()):
            assignment.append((reader.string(), reader.i32()))
        self._apply_assignment(assignment)
        return True

    def _apply_assignment(self, assignment: List[Tuple[str, int]]) -> None:
        self._assignment = assignment
        owned = set(assignment)
        self._positions = {tp: off for tp, off in self._positions.items() if tp in owned}
        # Buffered records sit ahead of the consumed position and were
        # never delivered, so a new assignment simply drops them.
        self._buffered = []

        needed = [tp for tp in assignment if tp not in self._positions]
        if needed:
            committed = self.committed(needed)
            for topic, partition in needed:
                offset = committed.get((topic, partition), -1)
                if offset < 0:
                    offset = self._reset_offset(topic, partition)
                self._positions[(topic, partition)] = offset
        self._fetch_positions = dict(self._positions)

    def _compute_assignments(
        self, members: List[Tuple[str, List[str], List[Tuple[str, int]]]]
    ) -> List[Tuple[str, List[Tuple[str, int]]]]:
        topic_partitions: Dict[str, List[int]] = {}
        for _, topics, _ in members:
            for topic in topics:
                if topic not in topic_partitions:
                    topic_partitions[topic] = self._consumer.partitions(topic)

        member_list = [(member_id, topics) for member_id, topics, _ in members]
        previous = {member_id: held for member_id, _, held in members}

        if self.config.assignor == Assignor.RANGE:
            assignment = _range_assign(member_list, topic_partitions)
        elif self.config.assignor == Assignor.ROUNDROBIN:
            assignment = _roundrobin_assign(member_list, topic_partitions)
        elif self.config.assignor == Assignor.STICKY:
            assignment = _sticky_assign(member_list, topic_partitions, previous)
        else:
            raise ValueError(f"unknown assignor {self.config.assignor!r}")
        return sorted(assignment.items())

    def _leave(self) -> None:
        writer = body_writer()
        writer.string(self.group_id)
        writer.string(self._membership()[0])
        reader = body_reader(self._coordinator_request(ApiKey.LEAVE_GROUP, writer.bytes()))
        code = reader.i32()
        if code != ErrorCode.NONE:
            raise ServerError(code, "leave_group")
        self._set_joined(False)

    def _heartbeat_loop(self) -> None:
        # This loop enforces two independent deadlines, so it has to wake
        # often enough for the shorter of them. Deriving the tick from the
        # session timeout alone would leave a long session with a short
        # poll interval unchecked until long after it stalled.
        heartbeat_every = max(
            self.config.heartbeat_interval_ms or self.config.session_timeout_ms // 3, 1
        )
        poll_check_every = max(self.config.max_poll_interval_ms // 3, 1)
        interval = min(heartbeat_every, poll_check_every) / 1000.0
        left_for_slow_poll = False

        while not self._stop.wait(interval):
            try:
                left_for_slow_poll = self._heartbeat_tick(left_for_slow_poll)
            except Exception:  # noqa: BLE001
                # Transient (a dropped connection, a coordinator move):
                # a heartbeat thread that dies silently would get this
                # member evicted, so it retries on the next tick instead.
                continue

    def _heartbeat_tick(self, left_for_slow_poll: bool) -> bool:
        """One heartbeat-loop iteration; returns the new left_for_slow_poll."""
        with self._lock:
            if self._closed:
                return left_for_slow_poll
            idle_ms = _now_ms() - self._last_poll_ms
            in_poll = self._in_poll
            member_id, generation, joined = self._member_id, self._generation, self._joined
        if not joined or not member_id:
            return left_for_slow_poll

        if not in_poll and idle_ms >= self.config.max_poll_interval_ms:
            # The application has stopped consuming even though the process
            # is alive. Continuing to heartbeat would assert a liveness this
            # member no longer has, holding its partitions away from a
            # consumer that could progress.
            if not left_for_slow_poll:
                try:
                    self._leave()
                except BrahmaputraError:
                    pass
                self._set_joined(False)
            return True

        writer = body_writer()
        writer.string(self.group_id)
        writer.i32(generation)
        writer.string(member_id)
        reader = body_reader(self._coordinator_request(ApiKey.HEARTBEAT, writer.bytes()))
        if reader.i32() in (
            ErrorCode.REBALANCE_IN_PROGRESS,
            ErrorCode.UNKNOWN_MEMBER_ID,
            ErrorCode.ILLEGAL_GENERATION,
        ):
            # Rejoin on the next poll — but only if nothing changed since
            # the snapshot: a heartbeat for an old generation answering
            # after the member already rejoined must not send it round again.
            with self._lock:
                if self._generation == generation and self._member_id == member_id:
                    self._joined = False
        return False

    # -- coordinator routing ----------------------------------------------

    def _coordinator_partition(self) -> int:
        partitions = self._consumer.partitions(OFFSETS_TOPIC)
        return crc32c(self.group_id.encode("utf-8")) % len(partitions)

    def _coordinator_request(self, api_key: int, body: bytes) -> bytes:
        """Send to the group's coordinator, following moves and loads."""
        for _ in range(COORDINATOR_ATTEMPTS):
            partition = self._coordinator_partition()
            connection = self._consumer.router.connection_for(OFFSETS_TOPIC, partition)
            response = connection.request(api_key, body)
            code = _peek_error_code(response)
            if code == ErrorCode.COORDINATOR_LOAD_IN_PROGRESS:
                time.sleep(0.1)
                continue
            if code in (ErrorCode.NOT_COORDINATOR, ErrorCode.NOT_LEADER_OR_FOLLOWER):
                self._consumer.router.refresh(OFFSETS_TOPIC)
                continue
            return response
        raise BrahmaputraError(
            f"group coordinator unavailable after {COORDINATOR_ATTEMPTS} attempts"
        )


def _peek_error_code(body: bytes) -> int:
    """Read a response's leading error code without consuming the body.

    Every group response starts with one, which is what makes a generic
    coordinator-retry wrapper possible at all.
    """
    try:
        return body_reader(body).i32()
    except BrahmaputraError:
        return ErrorCode.NONE


# --------------------------------------------------------------------------
# Assignors
# --------------------------------------------------------------------------


def _empty(members) -> Dict[str, List[Tuple[str, int]]]:
    return {member_id: [] for member_id, _ in members}


def _range_assign(members, topic_partitions) -> Dict[str, List[Tuple[str, int]]]:
    """Contiguous ranges per topic; the first `n % members` take one extra."""
    assignment = _empty(members)
    for topic, partitions in sorted(topic_partitions.items()):
        subscribers = sorted(m for m, topics in members if topic in topics)
        if not subscribers:
            continue
        base, extra = divmod(len(partitions), len(subscribers))
        cursor = 0
        for index, member_id in enumerate(subscribers):
            count = base + (1 if index < extra else 0)
            for partition in partitions[cursor : cursor + count]:
                assignment[member_id].append((topic, partition))
            cursor += count
    return assignment


def _roundrobin_assign(members, topic_partitions) -> Dict[str, List[Tuple[str, int]]]:
    """Deal every partition around the circle of members sorted by id."""
    assignment = _empty(members)
    circle = sorted(members, key=lambda m: m[0])
    if not circle:
        return assignment
    cursor = 0
    for topic, partitions in sorted(topic_partitions.items()):
        for partition in partitions:
            start = cursor
            while True:
                member_id, topics = circle[cursor % len(circle)]
                cursor += 1
                if topic in topics:
                    assignment[member_id].append((topic, partition))
                    break
                if cursor - start >= len(circle):
                    break  # nobody subscribes to this topic
    return assignment


def _sticky_assign(members, topic_partitions, previous) -> Dict[str, List[Tuple[str, int]]]:
    """Keep members on what they hold; move only what balance requires.

    Mirrors the Rust implementation exactly, because members computing the
    assignment independently must agree — a leader running a different
    algorithm from its predecessor would reshuffle the whole group.
    """
    assignment = _empty(members)
    if not members:
        return assignment

    subscriptions = {member_id: set(topics) for member_id, topics in members}

    unassigned: List[Tuple[str, int]] = []
    claimed: Dict[Tuple[str, int], str] = {}
    for topic, partitions in sorted(topic_partitions.items()):
        for partition in partitions:
            tp = (topic, partition)
            holder = None
            for member_id, held in sorted(previous.items()):
                if tp in held and topic in subscriptions.get(member_id, ()):
                    holder = member_id
                    break
            if holder is None:
                unassigned.append(tp)
            else:
                claimed[tp] = holder

    eligible = sorted(
        member_id
        for member_id, topics in members
        if any(topic in topic_partitions for topic in topics)
    )
    if not eligible:
        return assignment

    total = sum(len(p) for p in topic_partitions.values())
    base, extra = divmod(total, len(eligible))
    quota = {
        member_id: base + (1 if index < extra else 0)
        for index, member_id in enumerate(eligible)
    }

    kept: Dict[str, List[Tuple[str, int]]] = {}
    for tp in sorted(claimed):
        member_id = claimed[tp]
        held = kept.setdefault(member_id, [])
        if len(held) < quota.get(member_id, 0):
            held.append(tp)
        else:
            unassigned.append(tp)

    for member_id, held in kept.items():
        if member_id in assignment:
            assignment[member_id] = held

    for tp in sorted(unassigned):
        topic = tp[0]
        taker = next(
            (
                member_id
                for member_id in eligible
                if topic in subscriptions.get(member_id, ())
                and len(assignment[member_id]) < quota.get(member_id, 0)
            ),
            None,
        )
        if taker is None:
            # Quotas exhausted (possible with uneven subscriptions): an
            # unassigned partition is a stalled partition, so fall back to
            # any subscribed member rather than dropping it.
            taker = next(
                (m for m in eligible if topic in subscriptions.get(m, ())),
                None,
            )
        if taker is not None:
            assignment[taker].append(tp)

    for held in assignment.values():
        held.sort()
    return assignment
