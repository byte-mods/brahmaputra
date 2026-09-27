## Consumer groups: join/sync/heartbeat, assignors, offsets.

import std/[tables, locks, os, algorithm, sets, sequtils]
import ./protocol, ./conn, ./producer, ./consumer

const
  OffsetsTopic* = "__consumer_offsets"
    ## The internal topic whose partition leaders coordinate groups.
  coordinatorAttempts = 4
  joinAttempts = 4

  AutoOffsetResetEarliest* = "earliest"
    ## Start from the oldest retained record: reprocesses, never skips.
  AutoOffsetResetLatest* = "latest"
    ## Start from the end: skips what was missed, never reprocesses.
  AutoOffsetResetNone* = "none"
    ## Refuse to guess: raise `NoOffsetForPartitionError`.

  AssignorRange* = "range"
  AssignorRoundRobin* = "roundrobin"
  AssignorSticky* = "sticky"
    ## Keeps members on the partitions they already hold.

type
  GroupConfig* = object
    ## Named as Kafka names its consumer-group settings.
    clientId*: string
    sessionTimeoutMs*: int32
      ## `session.timeout.ms`: evicted after this long without a heartbeat.
      ## Kafka defaults to 45 s; this to 10 s as the Rust client does.
    heartbeatIntervalMs*: int
      ## `heartbeat.interval.ms`: how often this member heartbeats, and so
      ## how soon it notices a rebalance; 0 means a third of the session timeout.
    rebalanceTimeoutMs*: int32  ## How long the coordinator waits for rejoins.
    maxPollIntervalMs*: int
      ## `max.poll.interval.ms`: the longest gap between polls before this
      ## member is presumed stuck and leaves. Time spent inside `poll` does
      ## not count.
    autoCommitIntervalMs*: int  ## `auto.commit.interval.ms`; 0 disables auto-commit.
    autoOffsetReset*: string    ## `auto.offset.reset`: earliest, latest or none.
    assignor*: string           ## `partition.assignment.strategy`: range, roundrobin, sticky.
    groupInstanceId*: string    ## `group.instance.id` (static membership); empty = dynamic.
    maxPollRecords*: int        ## `max.poll.records`
    fetchMaxBytes*: int32       ## `fetch.max.bytes`
    dialTimeoutMs*: int
    socketTimeoutMs*: int

  MemberAssignment* = object
    memberId*: string
    partitions*: seq[TopicPartition]

  AssignorMember* = object
    id*: string
    topics*: seq[string]

  GroupConsumerObj = object
    groupId: string
    address: string
    config: GroupConfig
    consumer: Consumer          # the caller's thread only
    subscribed: seq[string]
    assignment: seq[TopicPartition]
    positions: Table[TopicPartition, int64]       # next offset to deliver (committed)
    fetchPositions: Table[TopicPartition, int64]  # next offset to fetch
    buffered: seq[ConsumedRecord]
    lastCommitMs: int64
    # Shared with the heartbeat thread; every access holds `lock`.
    lock: Lock
    memberId: string
    generation: int32
    joined: bool
    lastPollMs: int64
    inPoll: bool
    closed: bool
    heartbeatThread: Thread[ptr GroupConsumerObj]

  GroupConsumer* {.acyclic.} = ref GroupConsumerObj
    ## Shares a topic's partitions with the rest of its group.
    ##
    ## Use from one thread, as with Kafka's consumer. A background thread
    ## heartbeats on its own connection and enforces `maxPollIntervalMs`; it
    ## shares only the membership fields with the caller, under a lock.

proc defaultGroupConfig*(): GroupConfig =
  GroupConfig(clientId: DefaultClientId, sessionTimeoutMs: 10_000,
              rebalanceTimeoutMs: 3_000, maxPollIntervalMs: 300_000,
              autoCommitIntervalMs: 5_000, autoOffsetReset: AutoOffsetResetEarliest,
              assignor: AssignorRange, maxPollRecords: 500,
              fetchMaxBytes: 8 * 1024 * 1024, dialTimeoutMs: DefaultDialTimeoutMs,
              socketTimeoutMs: DefaultRequestTimeoutMs)

proc cmpSlot*(a, b: TopicPartition): int =
  ## Topic, then partition compared as an integer (never as a string).
  result = cmp(a.topic, b.topic)
  if result == 0: result = cmp(a.partition, b.partition)

proc sortSlots(slots: var seq[TopicPartition]) = slots.sort(cmpSlot)

# ---------------------------------------------------------------------------
# Coordinator routing (used by both threads, each with its own router)
# ---------------------------------------------------------------------------

proc coordinatorRequest(router: Router, groupId: string, apiKey: int16,
                        body: string): string =
  ## Sends to the group's coordinator, following moves and waiting out loads.
  for _ in 0 ..< coordinatorAttempts:
    let partitions = router.partitions(OffsetsTopic)
    let partition = int32(crc32c(groupId) mod uint32(partitions.len))
    let response = router.request(OffsetsTopic, partition, apiKey, body)
    let code = peekErrorCode(response)
    if code == ErrCoordinatorLoadInProgress:
      sleep(100)
      continue
    if code == ErrNotCoordinator or code == ErrNotLeaderOrFollower:
      try: router.refresh(OffsetsTopic)
      except CatchableError: discard
      continue
    return response
  raise newException(BrahmaputraError, "group coordinator unavailable after " &
                     $coordinatorAttempts & " attempts")

proc leaveRequest(router: Router, groupId, memberId: string) =
  var w = initBodyWriter()
  w.writeString(groupId)
  w.writeString(memberId)
  var r = initBodyReader(coordinatorRequest(router, groupId, ApiLeaveGroup, w.buf))
  let code = r.readInt32()
  if code != ErrNone:
    raise newServerError(code, "leave_group")

proc heartbeatLoop(g: ptr GroupConsumerObj) {.thread.} =
  # This loop enforces two independent deadlines, so it wakes often enough
  # for the shorter of them.
  let heartbeatEvery = if g.config.heartbeatIntervalMs > 0: g.config.heartbeatIntervalMs
                       else: int(g.config.sessionTimeoutMs) div 3
  let interval = int64(max(1, min(heartbeatEvery, g.config.maxPollIntervalMs div 3)))
  var router: Router = nil
  var nextBeat = monoMillis() + interval
  var leftForSlowPoll = false
  while true:
    var closed, inPoll, joined: bool
    var idleMs: int64
    var memberId: string
    var generation: int32
    withLock g.lock:
      closed = g.closed
      inPoll = g.inPoll
      joined = g.joined
      idleMs = nowMillis() - g.lastPollMs
      memberId = g.memberId
      generation = g.generation
    if closed: break
    let now = monoMillis()
    if now < nextBeat:
      sleep(int(min(nextBeat - now, 20)))
      continue
    nextBeat = now + interval
    if not joined or memberId.len == 0:
      continue
    try:
      if router == nil:
        router = newRouter(g.address, g.config.clientId, g.config.dialTimeoutMs,
                           g.config.socketTimeoutMs)
      if not inPoll and idleMs >= int64(g.config.maxPollIntervalMs):
        # The application stopped consuming although the process is alive.
        # Heartbeating on would hold its partitions away from a consumer
        # that could make progress, so leave; the next poll rejoins.
        if not leftForSlowPoll:
          leftForSlowPoll = true
          withLock g.lock:
            g.joined = false
          leaveRequest(router, g.groupId, memberId)
        continue
      leftForSlowPoll = false
      var w = initBodyWriter()
      w.writeString(g.groupId)
      w.writeInt32(generation)
      w.writeString(memberId)
      let code = peekErrorCode(coordinatorRequest(router, g.groupId, ApiHeartbeat, w.buf))
      if code in [ErrRebalanceInProgress, ErrUnknownMemberId, ErrIllegalGeneration]:
        # Only if nothing changed since the snapshot: a reply for an old
        # generation arriving after the member rejoined must not send it
        # round again.
        withLock g.lock:
          if g.generation == generation and g.memberId == memberId:
            g.joined = false
    except CatchableError:
      discard   # transient: the router redials, and the next tick retries
  if router != nil:
    try: router.close()
    except CatchableError: discard

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

proc raw(g: GroupConsumer): ptr GroupConsumerObj {.inline.} = cast[ptr GroupConsumerObj](g)

proc newGroupConsumer*(address, groupId: string, config = defaultGroupConfig()): GroupConsumer =
  ## Connects and starts the heartbeat thread. Call `subscribe`, then `poll`.
  var consumerConfig = defaultConsumerConfig()
  consumerConfig.clientId = config.clientId
  consumerConfig.fetchMaxBytes = config.fetchMaxBytes
  consumerConfig.maxPollRecords = config.maxPollRecords
  consumerConfig.dialTimeoutMs = config.dialTimeoutMs
  consumerConfig.socketTimeoutMs = config.socketTimeoutMs
  let consumer = newConsumer(address, consumerConfig)
  result = GroupConsumer(groupId: groupId, address: address, config: config,
                         consumer: consumer, generation: -1,
                         lastPollMs: nowMillis(), lastCommitMs: nowMillis())
  initLock(result.lock)
  createThread(result.heartbeatThread, heartbeatLoop, result.raw)

proc consumer*(g: GroupConsumer): Consumer = g.consumer
proc groupId*(g: GroupConsumer): string = g.groupId

proc membership*(g: GroupConsumer): tuple[memberId: string, generation: int32, joined: bool] =
  withLock g.lock:
    result = (g.memberId, g.generation, g.joined)

proc assignment*(g: GroupConsumer): seq[TopicPartition] = g.assignment

proc setJoined(g: GroupConsumer, joined: bool) =
  withLock g.lock:
    g.joined = joined

proc clearMemberId(g: GroupConsumer) =
  withLock g.lock:
    g.memberId = ""

proc subscribe*(g: GroupConsumer, topics: openArray[string]) =
  ## Sets the topics this member wants a share of.
  g.subscribed = @topics
  g.setJoined(false)

proc coordinator(g: GroupConsumer, apiKey: int16, body: string): string =
  coordinatorRequest(g.consumer.router, g.groupId, apiKey, body)

proc commit*(g: GroupConsumer) =
  ## Commits the delivered positions. At-least-once: call after processing.
  if g.positions.len == 0: return
  var slots: seq[TopicPartition]
  for slot in g.positions.keys: slots.add slot
  slots.sortSlots()
  let (memberId, generation, _) = g.membership()
  var w = initBodyWriter()
  w.writeString(g.groupId)
  w.writeInt32(generation)
  w.writeString(memberId)
  w.writeInt32(int32(slots.len))
  for slot in slots:
    w.writeString(slot.topic)
    w.writeInt32(slot.partition)
    w.writeInt64(g.positions[slot])
  var r = initBodyReader(g.coordinator(ApiOffsetCommit, w.buf))
  let code = r.readInt32()
  if code != ErrNone:
    raise newServerError(code, "offset_commit")
  g.lastCommitMs = nowMillis()

proc committed*(g: GroupConsumer, partitions: openArray[TopicPartition] = []):
    Table[TopicPartition, int64] =
  ## The group's committed offsets. Empty `partitions` asks for all of them.
  var w = initBodyWriter()
  w.writeString(g.groupId)
  w.writeInt32(int32(partitions.len))
  for slot in partitions:
    w.writeString(slot.topic)
    w.writeInt32(slot.partition)
  var r = initBodyReader(g.coordinator(ApiOffsetFetch, w.buf))
  let code = r.readInt32()
  if code != ErrNone:
    raise newServerError(code, "offset_fetch")
  for _ in 0 ..< r.readCount():
    let topic = r.readString()
    let partition = r.readInt32()
    result[(topic, partition)] = r.readInt64()

proc maybeAutoCommit(g: GroupConsumer) =
  let interval = g.config.autoCommitIntervalMs
  if interval <= 0 or g.positions.len == 0: return
  if nowMillis() - g.lastCommitMs < int64(interval): return
  # A failed auto-commit is retried on the next poll; an explicit commit
  # is what a caller relies on.
  try: g.commit()
  except CatchableError: discard

proc resetOffset(g: GroupConsumer, slot: TopicPartition): int64 =
  case g.config.autoOffsetReset
  of AutoOffsetResetEarliest: g.consumer.listOffsets(slot.topic, slot.partition, Earliest)
  of AutoOffsetResetLatest: g.consumer.listOffsets(slot.topic, slot.partition, Latest)
  of AutoOffsetResetNone:
    raise newException(NoOffsetForPartitionError, "no committed offset for partition " &
                       slot.topic & "-" & $slot.partition)
  else:
    raise newException(BrahmaputraError, "unknown auto.offset.reset \"" &
                       g.config.autoOffsetReset & "\"")

proc leave*(g: GroupConsumer) =
  ## Leaves the group now, so its partitions move without waiting out the
  ## session timeout. `close` does this for you.
  let (memberId, _, _) = g.membership()
  leaveRequest(g.consumer.router, g.groupId, memberId)
  g.setJoined(false)

# ---------------------------------------------------------------------------
# Assignors
# ---------------------------------------------------------------------------

proc subscribes(m: AssignorMember, topic: string): bool = topic in m.topics

proc emptyAssignment(members: openArray[AssignorMember]): OrderedTable[string, seq[TopicPartition]] =
  for m in members: result[m.id] = @[]

proc sortedTopics(topicPartitions: Table[string, seq[int32]]): seq[string] =
  for t in topicPartitions.keys: result.add t
  result.sort()

proc rangeAssign*(members: openArray[AssignorMember],
                  topicPartitions: Table[string, seq[int32]]): OrderedTable[string, seq[TopicPartition]] =
  ## Each subscribed member takes a contiguous range per topic; the first
  ## (partitions mod members) members take one extra.
  result = emptyAssignment(members)
  for topic in sortedTopics(topicPartitions):
    let partitions = topicPartitions[topic]
    var subscribers: seq[string]
    for m in members:
      if m.subscribes(topic): subscribers.add m.id
    subscribers.sort()
    if subscribers.len == 0: continue
    let base = partitions.len div subscribers.len
    let extra = partitions.len mod subscribers.len
    var cursor = 0
    for index, memberId in subscribers:
      let count = base + (if index < extra: 1 else: 0)
      for partition in partitions[cursor ..< cursor + count]:
        result[memberId].add (topic, partition)
      cursor += count

proc roundRobinAssign*(members: openArray[AssignorMember],
                       topicPartitions: Table[string, seq[int32]]): OrderedTable[string, seq[TopicPartition]] =
  ## Deals every partition around the circle of members sorted by id,
  ## skipping members not subscribed to a partition's topic.
  result = emptyAssignment(members)
  var circle = @members
  circle.sort(proc (a, b: AssignorMember): int = cmp(a.id, b.id))
  if circle.len == 0: return
  var cursor = 0
  for topic in sortedTopics(topicPartitions):
    for partition in topicPartitions[topic]:
      let start = cursor
      while true:
        let member = circle[cursor mod circle.len]
        inc cursor
        if member.subscribes(topic):
          result[member.id].add (topic, partition)
          break
        if cursor - start >= circle.len:
          break   # nobody subscribes to this topic

proc stickyAssign*(members: openArray[AssignorMember],
                   topicPartitions: Table[string, seq[int32]],
                   previous: Table[string, seq[TopicPartition]]): OrderedTable[string, seq[TopicPartition]] =
  ## Keeps members on what they hold and moves only what balance requires.
  ## Mirrors the Rust implementation so every leader computes the same result.
  result = emptyAssignment(members)
  if members.len == 0: return
  var byId = initTable[string, AssignorMember]()
  for m in members: byId[m.id] = m
  proc subscribes(memberId, topic: string): bool =
    memberId in byId and byId[memberId].subscribes(topic)

  var previousIds: seq[string]
  for id in previous.keys: previousIds.add id
  previousIds.sort()

  var unassigned: seq[TopicPartition]
  var claimed = initTable[TopicPartition, string]()
  for topic in sortedTopics(topicPartitions):
    for partition in topicPartitions[topic]:
      let slot: TopicPartition = (topic, partition)
      var holder = ""
      for memberId in previousIds:
        for held in previous[memberId]:
          if cmpSlot(held, slot) == 0 and subscribes(memberId, topic):
            holder = memberId
            break
        if holder.len > 0: break
      if holder.len == 0: unassigned.add slot
      else: claimed[slot] = holder

  var eligible: seq[string]
  for m in members:
    for topic in m.topics:
      if topic in topicPartitions:
        eligible.add m.id
        break
  eligible.sort()
  if eligible.len == 0: return

  var total = 0
  for partitions in topicPartitions.values: total += partitions.len
  let base = total div eligible.len
  let extra = total mod eligible.len
  var quota = initTable[string, int]()
  for index, memberId in eligible:
    quota[memberId] = base + (if index < extra: 1 else: 0)

  var claimedSlots: seq[TopicPartition]
  for slot in claimed.keys: claimedSlots.add slot
  claimedSlots.sortSlots()

  var kept = initTable[string, seq[TopicPartition]]()
  for slot in claimedSlots:
    let memberId = claimed[slot]
    if kept.getOrDefault(memberId).len < quota.getOrDefault(memberId):
      kept.mgetOrPut(memberId, @[]).add slot
    else:
      unassigned.add slot
  for memberId, held in kept:
    if memberId in result: result[memberId] = held

  unassigned.sortSlots()
  for slot in unassigned:
    var taker = ""
    for memberId in eligible:
      if subscribes(memberId, slot.topic) and result[memberId].len < quota[memberId]:
        taker = memberId
        break
    if taker.len == 0:
      # Quotas exhausted (uneven subscriptions): an unassigned partition is
      # a stalled partition, so any subscribed member takes it.
      for memberId in eligible:
        if subscribes(memberId, slot.topic):
          taker = memberId
          break
    if taker.len > 0:
      result[taker].add slot

  for memberId in toSeq(result.keys):
    result[memberId].sortSlots()

proc computeAssignment(g: GroupConsumer, members: seq[AssignorMember],
                       topicPartitions: Table[string, seq[int32]],
                       previous: Table[string, seq[TopicPartition]]): seq[MemberAssignment] =
  let assignment =
    case g.config.assignor
    of AssignorRange: rangeAssign(members, topicPartitions)
    of AssignorRoundRobin: roundRobinAssign(members, topicPartitions)
    of AssignorSticky: stickyAssign(members, topicPartitions, previous)
    else: raise newException(BrahmaputraError, "unknown assignor \"" & g.config.assignor & "\"")
  for memberId, partitions in assignment:
    result.add MemberAssignment(memberId: memberId, partitions: partitions)
  result.sort(proc (a, b: MemberAssignment): int = cmp(a.memberId, b.memberId))

# ---------------------------------------------------------------------------
# Membership
# ---------------------------------------------------------------------------

proc applyAssignment(g: GroupConsumer, assignment: seq[TopicPartition]) =
  g.assignment = assignment
  var owned = initHashSet[TopicPartition]()
  for slot in assignment: owned.incl slot
  for slot in toSeq(g.positions.keys):
    if slot notin owned: g.positions.del(slot)
  # Buffered records sit ahead of the consumed position and were never
  # delivered, so a new assignment simply drops them.
  g.buffered.setLen(0)
  var needed: seq[TopicPartition]
  for slot in assignment:
    if slot notin g.positions: needed.add slot
  if needed.len > 0:
    let committedOffsets = g.committed(needed)
    for slot in needed:
      var offset = committedOffsets.getOrDefault(slot, -1)
      if offset < 0:
        offset = g.resetOffset(slot)
      g.positions[slot] = offset
  g.fetchPositions.clear()
  for slot, offset in g.positions:
    g.fetchPositions[slot] = offset

proc sync(g: GroupConsumer, assignments: seq[MemberAssignment]): bool =
  let (memberId, generation, _) = g.membership()
  var w = initBodyWriter()
  w.writeString(g.groupId)
  w.writeInt32(generation)
  w.writeString(memberId)
  w.writeInt32(int32(assignments.len))
  for a in assignments:
    w.writeString(a.memberId)
    w.writeInt32(int32(a.partitions.len))
    for slot in a.partitions:
      w.writeString(slot.topic)
      w.writeInt32(slot.partition)
  var r = initBodyReader(g.coordinator(ApiSyncGroup, w.buf))
  let code = r.readInt32()
  if code == ErrRebalanceInProgress or code == ErrIllegalGeneration:
    return false
  if code == ErrUnknownMemberId:
    g.clearMemberId()
    return false
  if code != ErrNone:
    raise newServerError(code, "sync_group")
  var assignment: seq[TopicPartition]
  for _ in 0 ..< r.readCount():
    let topic = r.readString()
    assignment.add (topic, r.readInt32())
  g.applyAssignment(assignment)
  true

proc join(g: GroupConsumer) =
  for _ in 0 ..< joinAttempts:
    let (currentMemberId, _, _) = g.membership()
    var w = initBodyWriter()
    w.writeString(g.groupId)
    w.writeInt32(g.config.sessionTimeoutMs)
    w.writeInt32(g.config.rebalanceTimeoutMs)
    w.writeString(currentMemberId)
    w.writeStringArray(g.subscribed)
    w.writeString(g.config.groupInstanceId)
    var r = initBodyReader(g.coordinator(ApiJoinGroup, w.buf))
    let code = r.readInt32()
    if code == ErrRebalanceInProgress:
      sleep(100)
      continue
    if code == ErrUnknownMemberId:
      # The coordinator dropped this member: join again as a new one.
      g.clearMemberId()
      continue
    if code != ErrNone:
      raise newServerError(code, "join_group")
    let generation = r.readInt32()
    let memberId = r.readString()
    let leaderId = r.readString()
    var members: seq[AssignorMember]
    var previous = initTable[string, seq[TopicPartition]]()
    for _ in 0 ..< r.readCount():
      var member = AssignorMember(id: r.readString())
      member.topics = r.readStringArray()
      var held: seq[TopicPartition]
      for _ in 0 ..< r.readCount():
        let topic = r.readString()
        held.add (topic, r.readInt32())
      previous[member.id] = held
      members.add member
    withLock g.lock:
      g.memberId = memberId
      g.generation = generation

    var assignments: seq[MemberAssignment]
    if memberId == leaderId:
      var topicPartitions = initTable[string, seq[int32]]()
      for member in members:
        for topic in member.topics:
          if topic notin topicPartitions:
            topicPartitions[topic] = g.consumer.partitions(topic)
      assignments = g.computeAssignment(members, topicPartitions, previous)
    if g.sync(assignments):
      g.setJoined(true)
      return
  raise newException(BrahmaputraError, "consumer group failed to stabilise after " &
                     $joinAttempts & " join attempts")

proc takeBuffered(g: GroupConsumer): seq[ConsumedRecord] =
  var limit = g.config.maxPollRecords
  if limit <= 0 or limit > g.buffered.len: limit = g.buffered.len
  result = g.buffered[0 ..< limit]
  g.buffered = g.buffered[limit .. ^1]
  for record in result:
    # The consumed position advances only over records handed to the
    # caller; committing what was merely fetched would skip records.
    g.positions[(record.topic, record.partition)] = record.offset + 1

proc stampPoll(g: GroupConsumer, inPoll: bool) =
  withLock g.lock:
    g.lastPollMs = nowMillis()
    g.inPoll = inPoll

proc poll*(g: GroupConsumer, timeoutMs: int): seq[ConsumedRecord] =
  ## Returns up to `maxPollRecords` records, joining the group if needed.
  if g.subscribed.len == 0:
    raise newException(BrahmaputraError, "subscribe to at least one topic before polling")
  # Stamped on entry and on return, never enforced in between: the
  # interval bounds how long the application goes without asking for
  # records, and a poll that blocks is the consumer working normally.
  g.stampPoll(true)
  defer: g.stampPoll(false)

  let deadline = monoMillis() + int64(timeoutMs)
  while true:
    # Checked every sweep: a rebalance the heartbeat learns of mid-poll
    # must stop this member fetching partitions it may no longer own.
    if not g.membership().joined:
      g.join()
    if g.buffered.len > 0:
      return g.takeBuffered()
    if g.assignment.len == 0:
      if monoMillis() >= deadline: return @[]
      sleep(50)
      continue
    var gotAny = false
    for slot in g.assignment:
      let waitMs = int32(clamp(deadline - monoMillis(), 0'i64, 500'i64))
      let offset = g.fetchPositions.getOrDefault(slot, 0)
      var records: seq[ConsumedRecord]
      try:
        records = g.consumer.fetch(slot.topic, slot.partition, offset, waitMs)
      except ServerError as e:
        if e.code == ErrOffsetOutOfRange:
          # The committed offset fell off the log; restart per the policy.
          let reset = g.resetOffset(slot)
          g.fetchPositions[slot] = reset
          g.positions[slot] = reset
          continue
        if e.code == ErrNotLeaderOrFollower:
          try: g.consumer.router.refresh(slot.topic)
          except CatchableError: discard
          continue
        raise
      if records.len > 0:
        gotAny = true
        g.fetchPositions[slot] = records[^1].offset + 1
        g.buffered.add records
    g.maybeAutoCommit()
    if g.buffered.len > 0:
      return g.takeBuffered()
    if not gotAny and monoMillis() >= deadline:
      return @[]

proc close*(g: GroupConsumer) =
  ## Commits, leaves the group, then stops. Leaving is what lets the
  ## coordinator reassign at once instead of waiting out the session timeout.
  var alreadyClosed = false
  withLock g.lock:
    alreadyClosed = g.closed
    g.closed = true
  if alreadyClosed: return
  let (memberId, _, joined) = g.membership()
  if joined:
    try: g.commit()
    except CatchableError: discard
  if memberId.len > 0:
    try: g.leave()
    except CatchableError: discard
  joinThread(g.heartbeatThread)
  g.consumer.close()

