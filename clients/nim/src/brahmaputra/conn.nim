## Connections and leader routing.

import std/[os, net, nativesockets, posix, locks, atomics, tables, sequtils,
            algorithm, strutils, monotimes, times]
import ./protocol

const
  DefaultRequestTimeoutMs* = 120_000
    ## Bounds one request/response round trip on the socket. It must exceed
    ## the longest the broker may legitimately hold a request (a fetch
    ## long-poll, an acks=all wait, a JoinGroup waiting out a rebalance), so
    ## it is generous; its job is to turn a wedged broker into an error
    ## instead of a thread blocked forever.
  DefaultDialTimeoutMs* = 30_000
  DefaultClientId* = "brahmaputra-nim"

  Earliest*: int64 = -2  ## ListOffsets sentinel: the oldest retained offset.
  Latest*: int64 = -1    ## ListOffsets sentinel: the next offset to be written.

proc nowMillis*(): int64 =
  ## Wall-clock unix milliseconds.
  let t = getTime()
  t.toUnix * 1000 + int64(t.nanosecond div 1_000_000)

proc monoMillis*(): int64 =
  ## Monotonic milliseconds, for deadlines.
  getMonoTime().ticks div 1_000_000

# ---------------------------------------------------------------------------
# Conn
# ---------------------------------------------------------------------------

type
  ConnObj = object
    socket: Socket
    fd: SocketHandle
    address: string
    clientId: string
    timeoutMs: int
    lock: Lock
    next: int32
    brokenFlag: Atomic[bool]

  Conn* {.acyclic.} = ref ConnObj
    ## One TCP connection to one broker.
    ##
    ## A lock serialises request/response pairs, so there is at most one
    ## request in flight per connection. Any I/O failure, timeout or
    ## correlation mismatch leaves the byte stream at an unknown position, so
    ## the connection is closed and marked `broken` rather than reused; the
    ## `Router` notices and redials.

  ApiVersionRange* = object
    apiKey*: int32
    minVersion*: int32
    maxVersion*: int32

proc splitAddress*(address: string): tuple[host: string, port: int] =
  let colon = address.rfind(':')
  if colon <= 0:
    raise newException(ConnectionError, "address must be host:port, got \"" & address & "\"")
  var host = address[0 ..< colon]
  if host.startsWith("[") and host.endsWith("]"):
    host = host[1 .. ^2]
  try:
    result = (host, parseInt(address[colon + 1 .. ^1]))
  except ValueError:
    raise newException(ConnectionError, "bad port in \"" & address & "\"")

proc dial*(address: string, clientId = DefaultClientId,
           dialTimeoutMs = DefaultDialTimeoutMs,
           requestTimeoutMs = DefaultRequestTimeoutMs): Conn =
  ## Opens a connection to one broker at `host:port`.
  let (host, port) = splitAddress(address)
  let domain = if ':' in host: Domain.AF_INET6 else: Domain.AF_INET
  var sock = newSocket(domain, SockType.SOCK_STREAM, Protocol.IPPROTO_TCP, buffered = false)
  try:
    if dialTimeoutMs > 0:
      sock.connect(host, Port(port), dialTimeoutMs)
    else:
      sock.connect(host, Port(port))
    # Responses are small and latency matters more than packet count.
    sock.setSockOpt(OptNoDelay, true, level = posix.IPPROTO_TCP.cint)
  except CatchableError as e:
    sock.close()
    raise newException(ConnectionError, "dial " & address & ": " & e.msg)
  result = Conn(socket: sock, fd: sock.getFd, address: address,
                clientId: clientId, timeoutMs: requestTimeoutMs)
  initLock(result.lock)
  result.brokenFlag.store(false)

proc broken*(c: Conn): bool = c.brokenFlag.load
  ## Whether this connection failed and must not be reused.

proc address*(c: Conn): string = c.address

proc setRequestTimeout*(c: Conn, timeoutMs: int) =
  ## How long one round trip may take before the connection is abandoned.
  ## Zero or negative disables the bound.
  withLock c.lock:
    c.timeoutMs = timeoutMs

proc requestTimeout*(c: Conn): int = c.timeoutMs

proc failLocked(c: Conn) =
  if not c.brokenFlag.exchange(true):
    discard posix.shutdown(c.fd, SHUT_RDWR)
    c.socket.close()

proc close*(c: Conn) =
  ## Closes the connection. Safe to call more than once.
  withLock c.lock:
    c.failLocked()

proc remainingMs(c: Conn, started: int64): int =
  if c.timeoutMs <= 0: return -1
  let left = c.timeoutMs - int(monoMillis() - started)
  if left <= 0:
    raise newException(RequestTimeoutError, "request to " & c.address &
                       " timed out after " & $c.timeoutMs & " ms")
  left

proc waitReady(c: Conn, events: cshort, started: int64) =
  while true:
    let left = c.remainingMs(started)
    var pfd = TPollfd(fd: c.fd.cint, events: events, revents: 0)
    let rc = posix.poll(addr pfd, 1, cint(left))
    if rc > 0: return
    if rc == 0:
      discard c.remainingMs(started)   # raises once the deadline passed
      continue
    let err = osLastError()
    if err.int32 == EINTR: continue
    raise newException(ConnectionError, "poll " & c.address & ": " & osErrorMsg(err))

proc writeAll(c: Conn, data: string, started: int64) =
  var sent = 0
  while sent < data.len:
    c.waitReady(POLLOUT, started)
    let n = posix.send(c.fd, unsafeAddr data[sent], data.len - sent, MSG_NOSIGNAL)
    if n < 0:
      let err = osLastError()
      if err.int32 in [EINTR, EAGAIN, EWOULDBLOCK]: continue
      raise newException(ConnectionError, "write " & c.address & ": " & osErrorMsg(err))
    sent += n

proc readExact(c: Conn, buf: var string, size: int, started: int64) =
  buf.setLen(size)
  var got = 0
  while got < size:
    c.waitReady(POLLIN, started)
    let n = posix.recv(c.fd, addr buf[got], size - got, 0)
    if n == 0:
      raise newException(ConnectionError, "connection to " & c.address & " closed by peer")
    if n < 0:
      let err = osLastError()
      if err.int32 in [EINTR, EAGAIN, EWOULDBLOCK]: continue
      raise newException(ConnectionError, "read " & c.address & ": " & osErrorMsg(err))
    got += n

proc brokenError(c: Conn): ref ConnectionError =
  newException(ConnectionError, "connection to " & c.address &
               " is broken; the router will redial")

proc request*(c: Conn, apiKey: int16, body: string): string =
  ## Sends one request and returns the matching response body.
  withLock c.lock:
    if c.brokenFlag.load:
      raise c.brokenError()
    c.next = c.next +% 1'i32
    let correlationId = c.next
    let started = monoMillis()
    try:
      c.writeAll(encodeFrame(apiKey, correlationId, c.clientId, body), started)
      var header = ""
      c.readExact(header, 4, started)
      let length = cast[int32](readU32BE(header, 0))
      if length < 0 or length > MaxFrameBytes:
        raise newException(ConnectionError, "invalid frame length " & $length)
      var payload = ""
      c.readExact(payload, int(length), started)
      let (got, responseBody) = decodeFramePayload(payload)
      if got != correlationId:
        # The stream has desynchronised; continuing would pair every later
        # response with the wrong request.
        raise newException(ConnectionError, "correlation id mismatch: expected " &
                           $correlationId & ", got " & $got)
      result = responseBody
    except RequestTimeoutError as e:
      # The response may still be on its way; reading on would pair it
      # with the next request.
      c.failLocked()
      raise e
    except ConnectionError as e:
      c.failLocked()
      raise e
    except CatchableError as e:
      c.failLocked()
      raise newException(ConnectionError, e.msg)

proc sendOneway*(c: Conn, apiKey: int16, body: string) =
  ## Sends without awaiting a response (acks=0).
  withLock c.lock:
    if c.brokenFlag.load:
      raise c.brokenError()
    c.next = c.next +% 1'i32
    try:
      c.writeAll(encodeFrame(apiKey, c.next, c.clientId, body), monoMillis())
    except ConnectionError as e:
      c.failLocked()
      raise e
    except CatchableError as e:
      c.failLocked()
      raise newException(ConnectionError, e.msg)

proc apiVersions*(c: Conn): tuple[versions: seq[ApiVersionRange], brokerVersion: string] =
  ## Asks the broker what it speaks.
  var w = initBodyWriter()
  w.writeString(DefaultClientId)
  w.writeString("0.1.0")
  var r = initBodyReader(c.request(ApiApiVersions, w.buf))
  let code = r.readInt32()
  if code != ErrNone:
    raise newServerError(code, "api_versions")
  for _ in 0 ..< r.readCount():
    result.versions.add ApiVersionRange(apiKey: r.readInt32(),
      minVersion: r.readInt32(), maxVersion: r.readInt32())
  result.brokerVersion = r.readString()

# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------

type
  BrokerInfo* = object
    nodeId*: int32
    host*: string
    port*: int32
    rack*: string

  PartitionInfo* = object
    partition*: int32
    leader*: int32
    replicas*: seq[int32]
    isr*: seq[int32]
    leaderEpoch*: int32

  TopicInfo* = object
    name*: string
    partitions*: seq[PartitionInfo]

  ClusterMetadata* = object
    brokers*: seq[BrokerInfo]
    controllerId*: int32
    topics*: seq[TopicInfo]

proc partitionsOf*(m: ClusterMetadata, topic: string): seq[int32] =
  ## A topic's partition ids in ascending order.
  for t in m.topics:
    if t.name == topic:
      for p in t.partitions: result.add p.partition
      result.sort()
      return

proc leaderOf*(m: ClusterMetadata, topic: string, partition: int32): int32 =
  ## The broker id leading a partition, or -1.
  for t in m.topics:
    if t.name == topic:
      for p in t.partitions:
        if p.partition == partition: return p.leader
  -1

proc decodeMetadata(r: var BodyReader): ClusterMetadata =
  # error_code, brokers, controller_id, topics — the schema's order.
  let code = r.readInt32()
  if code != ErrNone:
    raise newServerError(code, "metadata")
  for _ in 0 ..< r.readCount():
    result.brokers.add BrokerInfo(nodeId: r.readInt32(), host: r.readString(),
                                  port: r.readInt32(), rack: r.readString())
  result.controllerId = r.readInt32()
  for _ in 0 ..< r.readCount():
    var topic = TopicInfo(name: r.readString())
    let topicError = r.readInt32()
    for _ in 0 ..< r.readCount():
      var info = PartitionInfo(partition: r.readInt32(), leader: r.readInt32())
      for _ in 0 ..< r.readCount(): info.replicas.add r.readInt32()
      for _ in 0 ..< r.readCount(): info.isr.add r.readInt32()
      info.leaderEpoch = r.readInt32()
      topic.partitions.add info
    if topicError != ErrNone and topicError != ErrUnknownTopicOrPartition:
      raise newServerError(topicError, "metadata for " & topic.name)
    result.topics.add topic

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

type
  RouterObj = object
    clientId: string
    seedAddress: string
    dialTimeoutMs: int
    requestTimeoutMs: int
    lock: Lock
    seed: Conn
    conns: Table[int32, Conn]
    metadata: ClusterMetadata
    hasMetadata: bool
    closed: bool

  Router* {.acyclic.} = ref RouterObj
    ## Keeps connections to every broker and routes by partition leader.
    ##
    ## Thread-safe: every operation runs under the router's lock, and a
    ## request is sent while holding it, so connections never escape to
    ## another thread. Metadata is cached and refreshed only when a request
    ## says the route was stale. A connection that failed is replaced on its
    ## next use (the seed included), so one dropped socket does not fail
    ## every later request.

proc newRouter*(address: string, clientId = DefaultClientId,
                dialTimeoutMs = DefaultDialTimeoutMs,
                requestTimeoutMs = DefaultRequestTimeoutMs): Router =
  let seed = dial(address, clientId, dialTimeoutMs, requestTimeoutMs)
  result = Router(clientId: clientId, seedAddress: address,
                  dialTimeoutMs: dialTimeoutMs, requestTimeoutMs: requestTimeoutMs,
                  seed: seed)
  initLock(result.lock)

proc close*(router: Router) =
  withLock router.lock:
    router.closed = true
    for conn in router.conns.values:
      if conn != router.seed: conn.close()
    router.conns.clear()
    router.seed.close()

proc checkOpen(router: Router) =
  if router.closed:
    raise newException(ConnectionError, "router is closed")

proc liveSeedLocked(router: Router): Conn =
  router.checkOpen()
  if not router.seed.broken:
    return router.seed
  let conn = dial(router.seedAddress, router.clientId, router.dialTimeoutMs,
                  router.requestTimeoutMs)
  let old = router.seed
  router.seed = conn
  for id in toSeq(router.conns.keys):
    if router.conns[id] == old:
      router.conns[id] = conn
  conn

proc seed*(router: Router): Conn =
  ## The connection this router was opened with, redialled if it failed.
  ## Use it from the thread that owns the router.
  withLock router.lock:
    result = router.liveSeedLocked()

proc mergeMetadata(router: Router, fresh: ClusterMetadata, all: bool) =
  if all or not router.hasMetadata:
    router.metadata = fresh
  else:
    router.metadata.brokers = fresh.brokers
    router.metadata.controllerId = fresh.controllerId
    for t in fresh.topics:
      var replaced = false
      for existing in router.metadata.topics.mitems:
        if existing.name == t.name:
          existing = t
          replaced = true
      if not replaced:
        router.metadata.topics.add t
  router.hasMetadata = true

proc metadataLocked(router: Router, topics: openArray[string], refresh: bool): ClusterMetadata =
  if not refresh and router.hasMetadata:
    var complete = true
    for t in topics:
      if router.metadata.partitionsOf(t).len == 0: complete = false
    if complete:
      return router.metadata
  let seed = router.liveSeedLocked()
  var w = initBodyWriter()
  w.writeStringArray(topics)
  var r = initBodyReader(seed.request(ApiMetadata, w.buf))
  let fresh = decodeMetadata(r)
  router.mergeMetadata(fresh, topics.len == 0)
  if topics.len == 0: fresh else: router.metadata

proc metadata*(router: Router, topics: openArray[string] = [],
               refresh = true): ClusterMetadata =
  ## Cluster metadata. An empty `topics` asks for every topic.
  withLock router.lock:
    router.checkOpen()
    result = router.metadataLocked(topics, refresh)

proc refresh*(router: Router, topic: string) =
  ## Re-reads one topic's routes after a request said they were stale.
  withLock router.lock:
    router.checkOpen()
    discard router.metadataLocked([topic], true)

proc partitionsLocked(router: Router, topic: string): seq[int32] =
  var m = router.metadataLocked([topic], false)
  result = m.partitionsOf(topic)
  if result.len == 0:
    # A topic auto-created on first reference is not in the cached image
    # yet; one refresh distinguishes "new" from "absent".
    m = router.metadataLocked([topic], true)
    result = m.partitionsOf(topic)
  if result.len == 0:
    raise newException(BrahmaputraError, "topic \"" & topic & "\" has no partitions")

proc partitions*(router: Router, topic: string): seq[int32] =
  ## A topic's partitions, creating it if the broker auto-creates topics.
  withLock router.lock:
    router.checkOpen()
    result = router.partitionsLocked(topic)

proc connForLocked(router: Router, topic: string, partition: int32): Conn =
  router.checkOpen()
  var m = router.metadataLocked([topic], false)
  var leader = m.leaderOf(topic, partition)
  if leader < 0:
    m = router.metadataLocked([topic], true)
    leader = m.leaderOf(topic, partition)
  if leader < 0:
    raise newException(BrahmaputraError, "no leader for " & topic & "-" & $partition)
  if leader in router.conns:
    let conn = router.conns[leader]
    if not conn.broken:
      return conn
    router.conns.del(leader)
    if conn != router.seed: conn.close()
  for broker in m.brokers:
    if broker.nodeId != leader: continue
    # A single-broker cluster advertises the address it was configured
    # with, which may not be the one we dialled; reuse the seed.
    if m.brokers.len == 1:
      let seed = router.liveSeedLocked()
      router.conns[leader] = seed
      return seed
    let conn = dial(broker.host & ":" & $broker.port, router.clientId,
                    router.dialTimeoutMs, router.requestTimeoutMs)
    router.conns[leader] = conn
    return conn
  raise newException(BrahmaputraError, "broker " & $leader & " is not in the metadata")

proc connFor*(router: Router, topic: string, partition: int32): Conn =
  ## The connection to a partition's leader. Use it from the thread that
  ## owns the router; `request` is the thread-safe form.
  withLock router.lock:
    result = router.connForLocked(topic, partition)

proc request*(router: Router, topic: string, partition: int32, apiKey: int16,
              body: string): string =
  ## Sends to a partition's leader and returns the response body.
  withLock router.lock:
    result = router.connForLocked(topic, partition).request(apiKey, body)

proc sendOneway*(router: Router, topic: string, partition: int32, apiKey: int16,
                 body: string) =
  withLock router.lock:
    router.connForLocked(topic, partition).sendOneway(apiKey, body)

proc seedRequest*(router: Router, apiKey: int16, body: string): string =
  withLock router.lock:
    result = router.liveSeedLocked().request(apiKey, body)

proc apiVersions*(router: Router): tuple[versions: seq[ApiVersionRange], brokerVersion: string] =
  withLock router.lock:
    result = router.liveSeedLocked().apiVersions()
