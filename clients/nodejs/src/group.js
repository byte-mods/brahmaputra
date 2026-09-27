'use strict';

/**
 * Group-coordinated consumer.
 *
 * The coordinator for a group is the leader of `__consumer_offsets`
 * partition `crc32c(group_id) % partitions`, so every group request goes
 * to that broker and nowhere else.
 */

const {
  ApiKey,
  BrahmaputraError,
  ErrorCode,
  NoOffsetForPartition,
  ServerError,
  bodyReader,
  bodyWriter,
  crc32c,
} = require('./protocol');

const {
  COORDINATOR_ATTEMPTS,
  Consumer,
  DEFAULT_REQUEST_TIMEOUT_MS,
  EARLIEST,
  JOIN_ATTEMPTS,
  LATEST,
  OFFSETS_TOPIC,
  nowMs,
  sleep,
} = require('./client');

/**
 * Where to start when a partition has no valid position — either the group
 * never committed one, or the committed one has fallen off the front of
 * the log because retention deleted it. Both are the same situation to a
 * consumer, so they take one policy.
 */
const AutoOffsetReset = Object.freeze({
  /** Oldest record still retained. Reprocesses; never silently skips. */
  EARLIEST: 'earliest',
  /** The end. Skips whatever was missed; never reprocesses. */
  LATEST: 'latest',
  /** Refuse to guess. The honest choice when neither is safe. */
  NONE: 'none',
});

const Assignor = Object.freeze({
  RANGE: 'range',
  ROUNDROBIN: 'roundrobin',
  /**
   * Keeps members on the partitions they already hold. Prefer this when
   * consumers carry per-partition state, because every partition that
   * moves throws that state away.
   */
  STICKY: 'sticky',
});

const defaultGroupConfig = () => ({
  clientId: 'brahmaputra-node',
  /**
   * The coordinator evicts a member that stops heartbeating for this long.
   * Kafka defaults to 45s; this defaults to 10s as the Rust client does.
   */
  sessionTimeoutMs: 10000,
  /**
   * How often the background timer heartbeats. 0 means sessionTimeoutMs/3,
   * Kafka's rule of thumb; keep it well below sessionTimeoutMs.
   */
  heartbeatIntervalMs: 0,
  rebalanceTimeoutMs: 3000,
  /**
   * Longest gap between poll() calls before this member is presumed stuck
   * and leaves. Separate from the session timeout on purpose: heartbeats
   * prove the process is alive, this proves the application is consuming.
   */
  maxPollIntervalMs: 300000,
  /** 0 disables auto-commit. */
  autoCommitIntervalMs: 5000,
  autoOffsetReset: AutoOffsetReset.EARLIEST,
  assignor: Assignor.RANGE,
  /**
   * Stable identity across restarts (KIP-345), so a rolling restart does
   * not rebalance twice per instance. Empty means a dynamic member.
   */
  groupInstanceId: '',
  maxPollRecords: 500,
  fetchMaxBytes: 8 * 1024 * 1024,
  /** Client-side bound on one round trip; keep it above rebalanceTimeoutMs. */
  socketTimeoutMs: DEFAULT_REQUEST_TIMEOUT_MS,
});

/**
 * A consumer that shares a topic's partitions with its group.
 *
 * Single-instance by design, matching Kafka's consumer: use one per
 * worker and give each its own client id.
 */
class GroupConsumer {
  constructor(consumer, groupId, config) {
    this.consumer = consumer;
    this.groupId = groupId;
    this.config = config;

    this.subscribed = [];
    this.memberId = '';
    this.generation = -1;
    this.joined = false;
    this.assignment = [];
    /** Next offset to *deliver* — what gets committed. */
    this.positions = new Map();
    /** Next offset to *fetch*; runs ahead of positions by the buffer. */
    this.fetchPositions = new Map();
    this.buffered = [];
    this.lastPollMs = nowMs();
    this.lastCommitMs = nowMs();
    this.closed = false;
    this.leftForSlowPoll = false;
    // True while poll() runs. max.poll.interval.ms bounds the gap *between*
    // polls — time the application spends processing — so a poll that is
    // itself busy joining a slow rebalance must not count against it.
    this.inPoll = false;
    this.ticking = false;

    // This timer enforces two independent deadlines, so it has to fire
    // often enough for the shorter of them. Deriving the tick from the
    // session timeout alone would leave a long session with a short poll
    // interval unchecked until long after it stalled.
    const heartbeatEvery = Math.max(
      config.heartbeatIntervalMs > 0
        ? config.heartbeatIntervalMs
        : Math.floor(config.sessionTimeoutMs / 3),
      1
    );
    const pollCheckEvery = Math.max(Math.floor(config.maxPollIntervalMs / 3), 1);
    this.timer = setInterval(() => {
      // One heartbeat at a time: a slow coordinator must not pile up
      // overlapping ticks that each send their own.
      if (this.ticking) return;
      this.ticking = true;
      this._tick()
        .catch(() => {})
        .finally(() => {
          this.ticking = false;
        });
    }, Math.min(heartbeatEvery, pollCheckEvery));
  }

  static async connect(host, port, groupId, overrides = {}) {
    const config = { ...defaultGroupConfig(), ...overrides };
    const consumer = await Consumer.connect(host, port, {
      clientId: config.clientId,
      fetchMaxBytes: config.fetchMaxBytes,
      maxPollRecords: config.maxPollRecords,
      socketTimeoutMs: config.socketTimeoutMs,
    });
    return new GroupConsumer(consumer, groupId, config);
  }

  subscribe(topics) {
    this.subscribed = [...topics];
    this.joined = false;
  }

  /**
   * Commit, leave the group, then stop.
   *
   * Leaving is what separates a clean shutdown from a crash. Without it
   * the coordinator cannot tell the difference and must wait out
   * sessionTimeoutMs before reassigning, so a rolling restart of N
   * instances costs N session timeouts of stalled partitions.
   */
  async close() {
    this.closed = true;
    clearInterval(this.timer);
    try {
      if (this.joined) await this.commit();
    } catch {
      // A failed final commit is reported by the next consumer resuming
      // from an older position, not by crashing the shutdown path.
    }
    try {
      if (this.memberId) await this._leave();
    } catch {
      // Best effort: failing here costs only the session timeout it was
      // trying to avoid.
    }
    this.consumer.close();
  }

  async poll(timeoutMs = 1000) {
    if (this.subscribed.length === 0) {
      throw new BrahmaputraError('subscribe to at least one topic before polling');
    }
    // Stamped on entry and again on return, and not enforced in between:
    // the interval bounds how long the *application* may go without asking
    // for records, and a poll that blocks — for its timeout, or on a slow
    // rebalance — is the consumer working normally.
    this.lastPollMs = nowMs();
    this.inPoll = true;
    try {
      return await this._poll(timeoutMs);
    } finally {
      this.inPoll = false;
      this.lastPollMs = nowMs();
    }
  }

  async _poll(timeoutMs) {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      // Checked every sweep, not only on entry: a rebalance the heartbeat
      // learns of mid-poll must stop this member fetching partitions it may
      // no longer own, rather than carrying on until the timeout.
      if (!this.joined) await this._join();
      if (this.buffered.length > 0) return this._takeBuffered();
      if (this.assignment.length === 0) {
        if (Date.now() >= deadline) return [];
        await sleep(50);
        continue;
      }

      let gotAny = false;
      for (const slot of this.assignment) {
        const remaining = Math.max(0, deadline - Date.now());
        const offset = this.fetchPositions.get(key(slot)) ?? 0n;
        let records;
        try {
          records = await this.consumer.fetch(
            slot.topic,
            slot.partition,
            offset,
            Math.min(remaining, 500)
          );
        } catch (error) {
          if (error instanceof ServerError) {
            if (error.code === ErrorCode.OFFSET_OUT_OF_RANGE) {
              // The committed offset fell off the log; restart where the
              // policy says.
              const reset = await this._resetOffset(slot.topic, slot.partition);
              this.fetchPositions.set(key(slot), reset);
              this.positions.set(key(slot), reset);
              this.buffered = this.buffered.filter(
                (record) => record.topic !== slot.topic || record.partition !== slot.partition
              );
              continue;
            }
            if (error.code === ErrorCode.NOT_LEADER_OR_FOLLOWER) {
              await this.consumer.router.refresh(slot.topic);
              continue;
            }
          }
          throw error;
        }
        if (records.length > 0) {
          gotAny = true;
          this.fetchPositions.set(key(slot), records[records.length - 1].offset + 1n);
          this.buffered.push(...records);
        }
      }

      await this._maybeAutoCommit();
      if (this.buffered.length > 0) return this._takeBuffered();
      if (!gotAny && Date.now() >= deadline) return [];
    }
  }

  _takeBuffered() {
    // 0 (or less) means no cap, as in the Go driver; slice(0, 0) would
    // otherwise hand back nothing forever while the buffer never drains.
    const limit =
      this.config.maxPollRecords > 0 ? this.config.maxPollRecords : this.buffered.length;
    const delivered = this.buffered.slice(0, limit);
    this.buffered = this.buffered.slice(limit);
    for (const record of delivered) {
      // The consumed position advances only over records actually handed
      // to the caller; committing what was merely fetched would silently
      // skip records nobody processed.
      this.positions.set(key(record), record.offset + 1n);
    }
    return delivered;
  }

  /** Commit delivered positions. At-least-once: call after processing. */
  async commit() {
    if (this.positions.size === 0) return;
    const entries = [...this.positions.entries()].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
    const writer = bodyWriter()
      .string(this.groupId)
      .int32(this.generation)
      .string(this.memberId)
      .int32(entries.length);
    for (const [slotKey, offset] of entries) {
      const [topic, partition] = splitKey(slotKey);
      writer.string(topic).int32(partition).int64(offset);
    }
    const reader = bodyReader(
      await this._coordinatorRequest(ApiKey.OFFSET_COMMIT, writer.bytes())
    );
    const code = reader.int32();
    if (code !== ErrorCode.NONE) throw new ServerError(code, 'offset_commit');
    this.lastCommitMs = nowMs();
  }

  /** Read committed offsets. An empty list asks for every partition. */
  async committed(partitions = []) {
    const writer = bodyWriter().string(this.groupId).int32(partitions.length);
    for (const slot of partitions) writer.string(slot.topic).int32(slot.partition);
    const reader = bodyReader(await this._coordinatorRequest(ApiKey.OFFSET_FETCH, writer.bytes()));
    const code = reader.int32();
    if (code !== ErrorCode.NONE) throw new ServerError(code, 'offset_fetch');
    const out = new Map();
    for (let count = reader.int32(); count > 0; count -= 1) {
      const topic = reader.string();
      const partition = reader.int32();
      out.set(`${topic} ${partition}`, reader.int64());
    }
    return out;
  }

  async _maybeAutoCommit() {
    const interval = this.config.autoCommitIntervalMs;
    if (interval <= 0 || this.positions.size === 0) return;
    if (nowMs() - this.lastCommitMs < interval) return;
    // An auto-commit that fails is retried on the next poll; the explicit
    // commit is what a caller relies on.
    await this.commit().catch(() => {});
  }

  async _resetOffset(topic, partition) {
    switch (this.config.autoOffsetReset) {
      case AutoOffsetReset.EARLIEST:
        return this.consumer.listOffsets(topic, partition, EARLIEST);
      case AutoOffsetReset.LATEST:
        return this.consumer.listOffsets(topic, partition, LATEST);
      case AutoOffsetReset.NONE:
        throw new NoOffsetForPartition(`no committed offset for ${topic}-${partition}`);
      default:
        throw new BrahmaputraError(`unknown autoOffsetReset ${this.config.autoOffsetReset}`);
    }
  }

  async _join() {
    for (let attempt = 0; attempt < JOIN_ATTEMPTS; attempt += 1) {
      const writer = bodyWriter()
        .string(this.groupId)
        .int32(this.config.sessionTimeoutMs)
        .int32(this.config.rebalanceTimeoutMs)
        .string(this.memberId)
        .stringArray(this.subscribed)
        .string(this.config.groupInstanceId);

      const reader = bodyReader(await this._coordinatorRequest(ApiKey.JOIN_GROUP, writer.bytes()));
      const code = reader.int32();
      if (code === ErrorCode.REBALANCE_IN_PROGRESS) {
        await sleep(100);
        continue;
      }
      if (code === ErrorCode.UNKNOWN_MEMBER_ID) {
        // The coordinator dropped this member (session expiry, or removed
        // while it waited): join again as a new one.
        this.memberId = '';
        continue;
      }
      if (code !== ErrorCode.NONE) throw new ServerError(code, 'join_group');

      const generation = reader.int32();
      const memberId = reader.string();
      const leaderId = reader.string();
      const members = [];
      for (let count = reader.int32(); count > 0; count -= 1) {
        const id = reader.string();
        const topics = reader.stringArray();
        const held = [];
        for (let hcount = reader.int32(); hcount > 0; hcount -= 1) {
          held.push({ topic: reader.string(), partition: reader.int32() });
        }
        members.push({ id, topics, held });
      }

      this.memberId = memberId;
      this.generation = generation;

      const assignments =
        memberId === leaderId ? await this._computeAssignments(members) : [];
      if (await this._sync(assignments)) {
        this.joined = true;
        return;
      }
    }
    throw new BrahmaputraError(
      `consumer group failed to stabilise after ${JOIN_ATTEMPTS} join attempts`
    );
  }

  async _sync(assignments) {
    const writer = bodyWriter()
      .string(this.groupId)
      .int32(this.generation)
      .string(this.memberId)
      .int32(assignments.length);
    for (const assignment of assignments) {
      writer.string(assignment.memberId).int32(assignment.partitions.length);
      for (const slot of assignment.partitions) {
        writer.string(slot.topic).int32(slot.partition);
      }
    }

    const reader = bodyReader(await this._coordinatorRequest(ApiKey.SYNC_GROUP, writer.bytes()));
    const code = reader.int32();
    if (code === ErrorCode.REBALANCE_IN_PROGRESS || code === ErrorCode.ILLEGAL_GENERATION) {
      return false;
    }
    if (code === ErrorCode.UNKNOWN_MEMBER_ID) {
      this.memberId = '';
      return false;
    }
    if (code !== ErrorCode.NONE) throw new ServerError(code, 'sync_group');

    const assignment = [];
    for (let count = reader.int32(); count > 0; count -= 1) {
      assignment.push({ topic: reader.string(), partition: reader.int32() });
    }
    await this._applyAssignment(assignment);
    return true;
  }

  async _applyAssignment(assignment) {
    this.assignment = assignment;
    const owned = new Set(assignment.map(key));
    for (const slotKey of [...this.positions.keys()]) {
      if (!owned.has(slotKey)) this.positions.delete(slotKey);
    }
    // Buffered records sit ahead of the consumed position and were never
    // delivered, so a new assignment simply drops them.
    this.buffered = [];

    const needed = assignment.filter((slot) => !this.positions.has(key(slot)));
    if (needed.length > 0) {
      const committed = await this.committed(needed);
      for (const slot of needed) {
        let offset = committed.get(key(slot));
        if (offset === undefined || offset < 0n) {
          offset = await this._resetOffset(slot.topic, slot.partition);
        }
        this.positions.set(key(slot), offset);
      }
    }
    this.fetchPositions = new Map(this.positions);
  }

  async _computeAssignments(members) {
    const topicPartitions = new Map();
    for (const member of members) {
      for (const topic of member.topics) {
        if (!topicPartitions.has(topic)) {
          topicPartitions.set(topic, await this.consumer.partitions(topic));
        }
      }
    }
    const previous = new Map(members.map((member) => [member.id, member.held]));
    const memberList = members.map((member) => ({ id: member.id, topics: member.topics }));

    let assignment;
    switch (this.config.assignor) {
      case Assignor.RANGE:
        assignment = rangeAssign(memberList, topicPartitions);
        break;
      case Assignor.ROUNDROBIN:
        assignment = roundRobinAssign(memberList, topicPartitions);
        break;
      case Assignor.STICKY:
        assignment = stickyAssign(memberList, topicPartitions, previous);
        break;
      default:
        throw new BrahmaputraError(`unknown assignor ${this.config.assignor}`);
    }
    return [...assignment.entries()]
      .map(([memberId, partitions]) => ({ memberId, partitions }))
      .sort((a, b) => (a.memberId < b.memberId ? -1 : 1));
  }

  async _leave() {
    const writer = bodyWriter().string(this.groupId).string(this.memberId);
    const reader = bodyReader(await this._coordinatorRequest(ApiKey.LEAVE_GROUP, writer.bytes()));
    const code = reader.int32();
    if (code !== ErrorCode.NONE) throw new ServerError(code, 'leave_group');
    this.joined = false;
  }

  async _tick() {
    if (this.closed || !this.joined || !this.memberId) return;

    const idleMs = nowMs() - this.lastPollMs;
    if (!this.inPoll && idleMs >= this.config.maxPollIntervalMs) {
      // The application has stopped consuming even though the process is
      // alive. Continuing to heartbeat would assert a liveness this member
      // no longer has, holding its partitions away from a consumer that
      // could make progress.
      if (!this.leftForSlowPoll) {
        await this._leave().catch(() => {});
        this.leftForSlowPoll = true;
        this.joined = false;
      }
      return;
    }
    this.leftForSlowPoll = false;

    const { generation, memberId } = this;
    const writer = bodyWriter().string(this.groupId).int32(generation).string(memberId);
    const reader = bodyReader(await this._coordinatorRequest(ApiKey.HEARTBEAT, writer.bytes()));
    const code = reader.int32();
    if (
      (code === ErrorCode.REBALANCE_IN_PROGRESS ||
        code === ErrorCode.UNKNOWN_MEMBER_ID ||
        code === ErrorCode.ILLEGAL_GENERATION) &&
      // Only if nothing changed meanwhile: an answer about a generation
      // this member has already moved past must not send it round again.
      this.generation === generation &&
      this.memberId === memberId
    ) {
      this.joined = false;
    }
  }

  async _coordinatorPartition() {
    const partitions = await this.consumer.partitions(OFFSETS_TOPIC);
    return crc32c(Buffer.from(this.groupId, 'utf8')) % partitions.length;
  }

  /** Send to the group's coordinator, following moves and waiting loads. */
  async _coordinatorRequest(apiKey, body) {
    for (let attempt = 0; attempt < COORDINATOR_ATTEMPTS; attempt += 1) {
      const partition = await this._coordinatorPartition();
      const connection = await this.consumer.router.connectionFor(OFFSETS_TOPIC, partition);
      const response = await connection.request(apiKey, body);
      const code = peekErrorCode(response);
      if (code === ErrorCode.COORDINATOR_LOAD_IN_PROGRESS) {
        await sleep(100);
        continue;
      }
      if (code === ErrorCode.NOT_COORDINATOR || code === ErrorCode.NOT_LEADER_OR_FOLLOWER) {
        await this.consumer.router.refresh(OFFSETS_TOPIC);
        continue;
      }
      return response;
    }
    throw new BrahmaputraError(
      `group coordinator unavailable after ${COORDINATOR_ATTEMPTS} attempts`
    );
  }
}

/**
 * Read a response's leading error code without consuming the body. Every
 * group response starts with one, which is what makes a generic
 * coordinator-retry wrapper possible at all.
 */
function peekErrorCode(body) {
  try {
    return bodyReader(body).int32();
  } catch {
    return ErrorCode.NONE;
  }
}

const key = (slot) => `${slot.topic} ${slot.partition}`;

/**
 * (topic, partition) order, partition compared as a number — the order the
 * Go and Rust drivers use. A string sort of `key()` would put "t 10" before
 * "t 2", and a sticky leader must decide exactly as the other drivers do or
 * a mixed-language group reshuffles every time leadership changes hands.
 */
function compareSlots(a, b) {
  if (a.topic !== b.topic) return a.topic < b.topic ? -1 : 1;
  return a.partition - b.partition;
}

function splitKey(slotKey) {
  const index = slotKey.lastIndexOf(' ');
  return [slotKey.slice(0, index), Number(slotKey.slice(index + 1))];
}

// ---------------------------------------------------------------------------
// Assignors
// ---------------------------------------------------------------------------

const emptyAssignment = (members) => new Map(members.map((member) => [member.id, []]));
const sortedTopics = (topicPartitions) => [...topicPartitions.keys()].sort();

/** Contiguous ranges per topic; the first (n % members) take one extra. */
function rangeAssign(members, topicPartitions) {
  const assignment = emptyAssignment(members);
  for (const topic of sortedTopics(topicPartitions)) {
    const partitions = topicPartitions.get(topic);
    const subscribers = members
      .filter((member) => member.topics.includes(topic))
      .map((member) => member.id)
      .sort();
    if (subscribers.length === 0) continue;
    const base = Math.floor(partitions.length / subscribers.length);
    const extra = partitions.length % subscribers.length;
    let cursor = 0;
    subscribers.forEach((memberId, index) => {
      const count = base + (index < extra ? 1 : 0);
      for (const partition of partitions.slice(cursor, cursor + count)) {
        assignment.get(memberId).push({ topic, partition });
      }
      cursor += count;
    });
  }
  return assignment;
}

/** Deal every partition around the circle of members sorted by id. */
function roundRobinAssign(members, topicPartitions) {
  const assignment = emptyAssignment(members);
  const circle = [...members].sort((a, b) => (a.id < b.id ? -1 : 1));
  if (circle.length === 0) return assignment;
  let cursor = 0;
  for (const topic of sortedTopics(topicPartitions)) {
    for (const partition of topicPartitions.get(topic)) {
      const start = cursor;
      for (;;) {
        const member = circle[cursor % circle.length];
        cursor += 1;
        if (member.topics.includes(topic)) {
          assignment.get(member.id).push({ topic, partition });
          break;
        }
        if (cursor - start >= circle.length) break; // nobody subscribes
      }
    }
  }
  return assignment;
}

/**
 * Keep members on what they hold; move only what balance requires.
 *
 * Mirrors the Rust implementation exactly, because members computing the
 * assignment independently must agree — a leader running a different
 * algorithm from its predecessor would reshuffle the whole group.
 */
function stickyAssign(members, topicPartitions, previous) {
  const assignment = emptyAssignment(members);
  if (members.length === 0) return assignment;

  const subscribes = (memberId, topic) => {
    const member = members.find((entry) => entry.id === memberId);
    return member ? member.topics.includes(topic) : false;
  };

  const unassigned = [];
  const claimed = new Map();
  const previousIds = [...previous.keys()].sort();
  for (const topic of sortedTopics(topicPartitions)) {
    for (const partition of topicPartitions.get(topic)) {
      const slot = { topic, partition };
      let holder = null;
      for (const memberId of previousIds) {
        const held = previous.get(memberId) || [];
        if (
          held.some((entry) => entry.topic === topic && entry.partition === partition) &&
          subscribes(memberId, topic)
        ) {
          holder = memberId;
          break;
        }
      }
      if (holder === null) unassigned.push(slot);
      else claimed.set(key(slot), holder);
    }
  }

  const eligible = members
    .filter((member) => member.topics.some((topic) => topicPartitions.has(topic)))
    .map((member) => member.id)
    .sort();
  if (eligible.length === 0) return assignment;

  const total = [...topicPartitions.values()].reduce((sum, list) => sum + list.length, 0);
  const base = Math.floor(total / eligible.length);
  const extra = total % eligible.length;
  const quota = new Map(
    eligible.map((memberId, index) => [memberId, base + (index < extra ? 1 : 0)])
  );

  const kept = new Map();
  const claimedSlots = [...claimed.keys()].map((slotKey) => {
    const [topic, partition] = splitKey(slotKey);
    return { topic, partition, slotKey };
  });
  claimedSlots.sort(compareSlots);
  for (const { topic, partition, slotKey } of claimedSlots) {
    const memberId = claimed.get(slotKey);
    const held = kept.get(memberId) || [];
    if (held.length < (quota.get(memberId) || 0)) {
      held.push({ topic, partition });
      kept.set(memberId, held);
    } else {
      unassigned.push({ topic, partition });
    }
  }
  for (const [memberId, held] of kept) {
    if (assignment.has(memberId)) assignment.set(memberId, held);
  }

  unassigned.sort(compareSlots);
  for (const slot of unassigned) {
    let taker = eligible.find(
      (memberId) =>
        subscribes(memberId, slot.topic) &&
        assignment.get(memberId).length < (quota.get(memberId) || 0)
    );
    if (!taker) {
      // Quotas exhausted (possible with uneven subscriptions): an
      // unassigned partition is a stalled partition, so fall back to any
      // subscribed member rather than dropping it.
      taker = eligible.find((memberId) => subscribes(memberId, slot.topic));
    }
    if (taker) assignment.get(taker).push(slot);
  }

  for (const held of assignment.values()) {
    held.sort(compareSlots);
  }
  return assignment;
}

module.exports = {
  Assignor,
  AutoOffsetReset,
  GroupConsumer,
  defaultGroupConfig,
  rangeAssign,
  roundRobinAssign,
  stickyAssign,
};
