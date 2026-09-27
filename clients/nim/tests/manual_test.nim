## End-to-end suite for the Nim driver against a live broker — a port of the
## Go suite (clients/go/cmd/manualtest) with the same sections and checks.
##
##   brahmaputra-server --data-dir ./data --default-partitions 4
##   nim c -d:release --threads:on --mm:orc -r tests/manual_test.nim 127.0.0.1 9092
##
## Every check asserts a property of the system, not that a function ran.

import std/[os, strutils, times, tables, sets, net, nativesockets, posix, atomics, algorithm, sequtils]
import brahmaputra

var passed, failed: int

proc check(name: string, ok: bool, detail = "") =
  if ok:
    inc passed
    echo "  ok   ", name
  else:
    inc failed
    if detail.len > 0: echo "  FAIL ", name, ": ", detail
    else: echo "  FAIL ", name

proc section(title: string) = echo "\n", title

proc unique(prefix: string): string =
  let t = getTime()
  prefix & "-" & $((t.toUnix mod 1000) * 1_000_000 + int64(t.nanosecond div 1000))

template must(body: untyped): untyped =
  try:
    body
  except CatchableError as e:
    echo "  FATAL ", e.msg, " (", $e.name, ")"
    quit(2)

proc repeatStr(s: string, n: int): string =
  for _ in 0 ..< n: result.add s

proc noLinger(): ProducerConfig =
  result = defaultProducerConfig()
  result.lingerMs = 0

# ---------------------------------------------------------------------------
# A silent broker and a TCP proxy that can sever every connection. Each runs
# on one thread, multiplexing its sockets with poll(2).
# ---------------------------------------------------------------------------

var silentStop: Atomic[bool]

proc listenLocal(): Socket =
  result = newSocket(Domain.AF_INET, SockType.SOCK_STREAM, Protocol.IPPROTO_TCP)
  result.setSockOpt(OptReuseAddr, true)
  result.bindAddr(Port(0), "127.0.0.1")
  result.listen()

proc localPort(s: Socket): int = int(s.getLocalAddr()[1])

proc silentLoop(listenFd: SocketHandle) {.thread.} =
  ## Accepts and reads, never answers.
  var fds = @[listenFd]
  var buf = newString(65536)
  while not silentStop.load:
    var pfds = newSeq[TPollfd](fds.len)
    for i, fd in fds: pfds[i] = TPollfd(fd: fd.cint, events: POLLIN, revents: 0)
    if posix.poll(addr pfds[0], Tnfds(pfds.len), 20) <= 0: continue
    var closedFds: seq[SocketHandle]
    for i, p in pfds:
      if (p.revents and (POLLIN or POLLHUP or POLLERR)) == 0: continue
      if fds[i] == listenFd:
        let client = posix.accept(listenFd, nil, nil)
        if client.cint >= 0: fds.add client
      else:
        if posix.recv(fds[i], addr buf[0], buf.len, 0) <= 0:
          closedFds.add fds[i]
    for fd in closedFds:
      discard posix.close(fd)
      fds.delete(fds.find(fd))
  for fd in fds:
    if fd != listenFd: discard posix.close(fd)

var proxyStop: Atomic[bool]
var proxyDropRequested: Atomic[int]
var proxyDropDone: Atomic[int]

type ProxyArgs = tuple[listenFd: SocketHandle, host: string, port: int]

proc writeFully(fd: SocketHandle, data: string, n: int): bool =
  var sent = 0
  while sent < n:
    let w = posix.send(fd, unsafeAddr data[sent], n - sent, MSG_NOSIGNAL)
    if w <= 0: return false
    sent += w
  true

proc proxyLoop(args: ProxyArgs) {.thread.} =
  var pairs: seq[tuple[client, upstream: SocketHandle]]
  var upstreams: seq[Socket]
  var buf = newString(65536)
  proc dropAll() =
    for pair in pairs:
      discard posix.shutdown(pair.client, SHUT_RDWR)
      discard posix.close(pair.client)
    for s in upstreams: s.close()
    pairs.setLen(0)
    upstreams.setLen(0)
  while not proxyStop.load:
    let requested = proxyDropRequested.load
    if requested != proxyDropDone.load:
      dropAll()
      proxyDropDone.store(requested)
    var pfds = @[TPollfd(fd: args.listenFd.cint, events: POLLIN, revents: 0)]
    for pair in pairs:
      pfds.add TPollfd(fd: pair.client.cint, events: POLLIN, revents: 0)
      pfds.add TPollfd(fd: pair.upstream.cint, events: POLLIN, revents: 0)
    if posix.poll(addr pfds[0], Tnfds(pfds.len), 20) <= 0: continue
    if (pfds[0].revents and POLLIN) != 0:
      let client = posix.accept(args.listenFd, nil, nil)
      if client.cint >= 0:
        try:
          let upstream = newSocket(Domain.AF_INET, SockType.SOCK_STREAM, Protocol.IPPROTO_TCP)
          upstream.connect(args.host, Port(args.port))
          upstreams.add upstream
          pairs.add (client, upstream.getFd)
        except CatchableError:
          discard posix.close(client)
    var dead: seq[int]
    for i in 1 ..< pfds.len:
      if (pfds[i].revents and (POLLIN or POLLHUP or POLLERR)) == 0: continue
      let index = (i - 1) div 2
      let pair = pairs[index]
      let (src, dst) = if (i - 1) mod 2 == 0: (pair.client, pair.upstream)
                       else: (pair.upstream, pair.client)
      let n = posix.recv(src, addr buf[0], buf.len, 0)
      if (n <= 0 or not writeFully(dst, buf, n)) and index notin dead:
        dead.add index
    # A pair with one side gone is closed on both sides, as io.Copy does.
    for index in countdown(pairs.len - 1, 0):
      if index in dead:
        discard posix.shutdown(pairs[index].client, SHUT_RDWR)
        discard posix.close(pairs[index].client)
        upstreams[index].close()
        pairs.delete(index)
        upstreams.delete(index)
  dropAll()

type Proxy = object
  listener: Socket
  address: string
  thread: Thread[ProxyArgs]

proc newProxy(target: string): ref Proxy =
  let (host, port) = splitAddress(target)
  result = new Proxy
  result.listener = listenLocal()
  result.address = "127.0.0.1:" & $result.listener.localPort
  proxyStop.store(false)
  createThread(result.thread, proxyLoop, (result.listener.getFd, host, port))

proc dropAll(p: ref Proxy) =
  let want = proxyDropRequested.fetchAdd(1) + 1
  while proxyDropDone.load != want: sleep(5)
  sleep(50)

proc close(p: ref Proxy) =
  proxyStop.store(true)
  joinThread(p.thread)
  p.listener.close()

# A producer on another thread, as the Go suite's goroutine.
proc lateProducer(args: tuple[address, topic: string]) {.thread.} =
  sleep(2000)
  try:
    let producer = newProducer(args.address, noLinger())
    for i in 0 ..< 10:
      producer.send(args.topic, "j" & $i)
    producer.close()
  except CatchableError as e:
    echo "  late producer failed: ", e.msg

# ---------------------------------------------------------------------------
# Coverage: one check per client feature the Go suite's sections do not
# already exercise.
# ---------------------------------------------------------------------------

proc lz4Compress(data: string): string {.nimcall, gcsafe.} =
  ## The broker's lz4 payload: a little-endian uncompressed length, then a
  ## raw LZ4 block. This encoder writes literals only (valid, if uncompressed).
  let n = data.len
  for shift in [0, 8, 16, 24]: result.add char((n shr shift) and 0xff)
  if n >= 15:
    result.add '\xf0'
    var rest = n - 15
    while rest >= 255:
      result.add '\xff'
      rest -= 255
    result.add char(rest)
  else:
    result.add char(n shl 4)
  result.add data

proc lz4Decompress(data: string): string {.nimcall, gcsafe.} =
  var pos = 4
  proc length(n: int, pos: var int): int =
    result = n
    if n == 15:
      while true:
        let b = int(data[pos])
        inc pos
        result += b
        if b != 255: break
  while pos < data.len:
    let token = int(data[pos])
    inc pos
    let lits = length(token shr 4, pos)
    result.add data[pos ..< pos + lits]
    pos += lits
    if pos >= data.len: break
    let offset = int(data[pos]) or (int(data[pos + 1]) shl 8)
    pos += 2
    for _ in 0 ..< length(token and 15, pos) + 4:
      result.add result[result.len - offset]

# A broker that answers Metadata with itself as the only broker and refuses
# every produce: topic "fatal" with a non-retriable code, anything else with
# NOT_ENOUGH_REPLICAS (retriable). Counts produces and the acks/timeout they
# carried.
var fakeStop: Atomic[bool]
var fakeProduces: Atomic[int]
var fakeAcks: Atomic[int]
var fakeTimeout: Atomic[int]

proc fakeAnswer(api: int, req: string, port: int): string =
  var w = initBodyWriter()
  var r = initBodyReader(req)
  case api
  of 3:
    let topics = r.readStringArray()
    for v in [0'i32, 1, 0]: w.writeInt32(v)
    w.writeString("127.0.0.1")
    w.writeInt32(int32(port))
    w.writeString("")
    w.writeInt32(0)
    w.writeInt32(int32(topics.len))
    for t in topics:
      w.writeString(t)
      for v in [0'i32, 1, 0, 0, 1, 0, 1, 0, 0]: w.writeInt32(v)
  of 0:
    let topic = r.readString()
    let partition = r.readInt32()
    fakeAcks.store(int(r.readInt32()))
    fakeTimeout.store(int(r.readInt32()))
    discard fakeProduces.fetchAdd(1)
    w.writeString(topic)
    w.writeInt32(partition)
    w.writeInt32(if topic == "fatal": 87 else: 10)
    w.writeInt64(-1)
    w.writeInt64(-1)
  else:
    w.writeInt32(35)
  w.buf

proc fakeLoop(args: tuple[listenFd: SocketHandle, port: int]) {.thread.} =
  var fds = @[args.listenFd]
  var pending = @[""]
  var buf = newString(65536)
  while not fakeStop.load:
    var pfds = newSeq[TPollfd](fds.len)
    for i, fd in fds: pfds[i] = TPollfd(fd: fd.cint, events: POLLIN, revents: 0)
    if posix.poll(addr pfds[0], Tnfds(pfds.len), 20) <= 0: continue
    var closedFds: seq[SocketHandle]
    for i, p in pfds:
      if (p.revents and (POLLIN or POLLHUP or POLLERR)) == 0: continue
      if fds[i] == args.listenFd:
        let client = posix.accept(args.listenFd, nil, nil)
        if client.cint >= 0:
          fds.add client
          pending.add ""
        continue
      let n = posix.recv(fds[i], addr buf[0], buf.len, 0)
      if n <= 0:
        closedFds.add fds[i]
        continue
      pending[i].add buf[0 ..< n]
      while pending[i].len >= 4:
        let size = (int(pending[i][0]) shl 24) or (int(pending[i][1]) shl 16) or
                   (int(pending[i][2]) shl 8) or int(pending[i][3])
        if pending[i].len < 4 + size: break
        let payload = pending[i][4 ..< 4 + size]
        pending[i] = pending[i][4 + size .. ^1]
        let api = (int(payload[0]) shl 8) or int(payload[1])
        let clen = (int(payload[8]) shl 8) or int(payload[9])
        var body = ""
        try: body = fakeAnswer(api, payload[10 + clen .. ^1], args.port)
        except CatchableError: discard
        let resp = payload[0 ..< 10 + clen] & body
        var frame = ""
        for shift in [24, 16, 8, 0]: frame.add char((resp.len shr shift) and 0xff)
        frame.add resp
        discard writeFully(fds[i], frame, frame.len)
    for fd in closedFds:
      let index = fds.find(fd)
      discard posix.close(fd)
      fds.delete(index)
      pending.delete(index)
  for fd in fds:
    if fd != args.listenFd: discard posix.close(fd)

# A group member on its own thread, polling until told to stop and
# publishing how many partitions it holds.
var memberStop: Atomic[bool]
var memberAssigned: Atomic[int]

proc memberLoop(args: tuple[address, group, topic: string, sessionMs, heartbeatMs: int]) {.thread.} =
  try:
    var config = defaultGroupConfig()
    config.autoCommitIntervalMs = 0
    config.sessionTimeoutMs = int32(args.sessionMs)
    config.heartbeatIntervalMs = args.heartbeatMs
    let g = newGroupConsumer(args.address, args.group, config)
    g.subscribe([args.topic])
    while not memberStop.load:
      try: discard g.poll(200)
      except CatchableError: discard
      memberAssigned.store(g.assignment.len)
    g.close()
  except CatchableError as e:
    echo "  member thread failed: ", e.msg

proc elapsedMs(started: int64): int64 = monoMillis() - started

proc waitFor(cond: proc (): bool, ms: int, poller: GroupConsumer = nil): bool =
  let deadline = monoMillis() + int64(ms)
  while monoMillis() < deadline:
    if poller != nil:
      try: discard poller.poll(200)
      except CatchableError: discard
    else:
      sleep(20)
    if cond(): return true
  cond()

proc pollUntil(g: GroupConsumer, want, ms: int): seq[ConsumedRecord] =
  let deadline = monoMillis() + int64(ms)
  while result.len < want and monoMillis() < deadline:
    result.add must g.poll(300)

proc groupConfigWith(autoCommitMs = 0): GroupConfig =
  result = defaultGroupConfig()
  result.autoCommitIntervalMs = autoCommitMs

proc coverage(address: string) =
  section("producer settings")
  let c = must newConsumer(address)
  block:
    let topic = unique("nim-batchsize")
    var config = defaultProducerConfig()
    config.lingerMs = 60_000
    config.batchSize = 64
    let p = must newProducer(address, config)
    for i in 0 ..< 3: must p.sendTo(topic, 0, repeatStr("b", 100) & $i)
    let got = must c.fetch(topic, 0, 0, 1000)
    check("batch.size sends a full batch without waiting for linger", got.len == 3, "got " & $got.len)
    try: p.close() except CatchableError: discard
  block:
    let topic = unique("nim-linger")
    var config = defaultProducerConfig()
    config.lingerMs = 50
    config.batchSize = 1_048_576
    let p = must newProducer(address, config)
    must p.sendTo(topic, 0, "lingering")
    sleep(500)
    let got = must c.fetch(topic, 0, 0, 1000)
    check("linger.ms flushes a partial batch on its own", got.len == 1, "got " & $got.len)
    try: p.close() except CatchableError: discard
  block:
    let topic = unique("nim-sync")
    let p = must newProducer(address, noLinger())
    let stamp = 1_600_000_000_000'i64
    let first = must p.sendSync(topic, "one", partition = 2, timestamp = stamp)
    let second = must p.sendSync(topic, "two", partition = 2, timestamp = stamp + 1000)
    check("send_sync returns consecutive offsets", first == 0 and second == 1, $first & ", " & $second)
    let got = must c.fetch(topic, 2, 0, 1000)
    check("an explicit partition is honoured", got.len == 2, "partition 2 holds " & $got.len)
    var stamps: seq[int64]
    for r in got: stamps.add r.timestamp
    check("an explicit timestamp is stored exactly", stamps == @[stamp, stamp + 1000], $stamps)
    let rrTopic = unique("nim-roundrobin")
    let parts = must p.router.partitions(rrTopic)
    for i in 0 ..< 2 * parts.len: must p.send(rrTopic, "rr" & $i)
    must p.flush()
    var counts: seq[int]
    for part in parts: counts.add (must c.fetch(rrTopic, part, 0, 300)).len
    var even = true
    for n in counts: even = even and n == 2
    check("keyless records are spread round-robin", even, $counts)
    must p.close()
  block:
    # A codec the driver does not carry, registered by the application: a
    # valid LZ4 block of literals only, which the broker accepts as-is.
    registerCodec(compressionLz4, lz4Compress, lz4Decompress)
    let topic = unique("nim-lz4")
    var config = noLinger()
    config.compressionType = "lz4"
    let p = must newProducer(address, config)
    var want: seq[string]
    for i in 0 ..< 5:
      want.add repeatStr("registered codec payload ", 20) & $i
      must p.sendTo(topic, 0, want[^1])
    must p.close()
    let got = must c.fetch(topic, 0, 0, 1000)
    var values: seq[string]
    for r in got: values.add r.value.get("")
    check("a registered codec round-trips through the broker", values == want, "got " & $got.len)
  c.close()

  section("retries against a broker that refuses")
  block:
    let listener = listenLocal()
    let port = listener.localPort
    fakeStop.store(false)
    var fakeThread: Thread[tuple[listenFd: SocketHandle, port: int]]
    createThread(fakeThread, fakeLoop, (listener.getFd, port))
    let fakeAddress = "127.0.0.1:" & $port
    var config = noLinger()
    config.acks = -1
    config.requestTimeoutMs = 1234
    config.retries = 2
    config.retryBackoffMs = 150
    let p = must newProducer(fakeAddress, config)
    var started = monoMillis()
    var failedSend = false
    try: discard p.sendSync("retriable", "x", partition = 0)
    except CatchableError: failedSend = true
    var took = elapsedMs(started)
    let attempts = fakeProduces.load
    check("request.timeout.ms and acks reach the broker",
          attempts > 0 and fakeAcks.load == -1 and fakeTimeout.load == 1234,
          "acks=" & $fakeAcks.load & " timeout=" & $fakeTimeout.load)
    check("a retriable error is retried `retries` times", failedSend and attempts == 3,
          $attempts & " attempts")
    check("retry.backoff.ms spaces the retries", took >= 300, $took & " ms")
    fakeProduces.store(0)
    var fatal = false
    try: discard p.sendSync("fatal", "x", partition = 0)
    except CatchableError: fatal = true
    check("a non-retriable error is not retried", fatal and fakeProduces.load == 1,
          $fakeProduces.load & " attempts")
    try: p.close() except CatchableError: discard
    fakeProduces.store(0)
    var capped = noLinger()
    capped.retries = 1_000_000
    capped.retryBackoffMs = 50
    capped.deliveryTimeoutMs = 400
    let p2 = must newProducer(fakeAddress, capped)
    started = monoMillis()
    var cappedFailed = false
    try: discard p2.sendSync("retriable", "x", partition = 0)
    except CatchableError: cappedFailed = true
    took = elapsedMs(started)
    check("delivery.timeout.ms caps the retries", cappedFailed and took < 3000,
          $took & " ms, " & $fakeProduces.load & " attempts")
    try: p2.close() except CatchableError: discard
    fakeStop.store(true)
    joinThread(fakeThread)
    listener.close()

  section("consumer settings")
  block:
    let topic = unique("nim-fetchcfg")
    let p = must newProducer(address, noLinger())
    for i in 0 ..< 20: must p.sendTo(topic, 0, repeatStr("f", 1000) & $i)
    must p.close()
    let c = must newConsumer(address)
    let res = must c.fetchVerbose(topic, 0, 0, 500)
    check("fetch reports the high watermark", res.highWatermark == 20, $res.highWatermark)
    check("a default fetch returns every record", res.records.len == 20, "got " & $res.records.len)
    let meta = must c.router.metadata([topic], true)
    var brokers: seq[int32]
    for b in meta.brokers: brokers.add b.nodeId
    var leadersKnown = false
    for t in meta.topics:
      if t.name == topic:
        leadersKnown = t.partitions.len > 0
        for info in t.partitions: leadersKnown = leadersKnown and info.leader in brokers
    check("metadata names a live leader for every partition", leadersKnown, $meta.topics.len & " topics")
    c.close()
    var smallConfig = defaultConsumerConfig()
    smallConfig.fetchMaxBytes = 2500
    let small = must newConsumer(address, smallConfig)
    let got = must small.fetch(topic, 0, 0, 500)
    check("fetch.max.bytes caps a response", got.len >= 1 and got.len < 20, "got " & $got.len)
    small.close()
    var patientConfig = defaultConsumerConfig()
    patientConfig.fetchMinBytes = 10_000_000
    patientConfig.fetchMaxWaitMs = 400
    let patient = must newConsumer(address, patientConfig)
    let started = monoMillis()
    let tail = must patient.fetch(topic, 0, 19, 400)
    let waited = elapsedMs(started)
    check("fetch.min.bytes holds a fetch for up to fetch.max.wait.ms",
          tail.len == 1 and waited >= 300 and waited < 5000, $waited & " ms, " & $tail.len & " records")
    patient.close()
  block:
    let topic = unique("nim-bytime")
    let p = must newProducer(address, noLinger())
    let base = 1_700_000_000_000'i64
    for i in 0 ..< 3: must p.sendTo(topic, 0, "t" & $i, timestamp = base + int64(i) * 10_000)
    must p.close()
    let c = must newConsumer(address)
    let at = must c.listOffsets(topic, 0, base + 5000)
    check("list offsets by timestamp finds the first record at or after it", at == 1, $at)
    c.close()
  block:
    let topic = unique("nim-maxpoll")
    let p = must newProducer(address, noLinger())
    for i in 0 ..< 10: must p.sendTo(topic, 0, "m" & $i)
    must p.close()
    var config = groupConfigWith()
    config.maxPollRecords = 3
    let g = must newGroupConsumer(address, unique("nim-maxpoll-grp"), config)
    g.subscribe([topic])
    var sizes: seq[int]
    var total = 0
    let deadline = monoMillis() + 15_000
    while total < 10 and monoMillis() < deadline:
      var got: seq[ConsumedRecord]
      try: got = g.poll(300)
      except CatchableError: discard
      if got.len > 0:
        sizes.add got.len
        total += got.len
    var capped = true
    for n in sizes: capped = capped and n <= 3
    check("max.poll.records caps a poll", total == 10 and capped, $sizes)
    try: g.close() except CatchableError: discard

  section("consumer group settings")
  let p = must newProducer(address, noLinger())
  block:
    let t1 = unique("nim-multi-a")
    let t2 = unique("nim-multi-b")
    for i in 0 ..< 5:
      must p.send(t1, "a" & $i)
      must p.send(t2, "b" & $i)
    let g = must newGroupConsumer(address, unique("nim-multi-grp"), groupConfigWith())
    g.subscribe([t1, t2])
    let got = pollUntil(g, 10, 15_000)
    var perTopic = initCountTable[string]()
    for r in got: perTopic.inc r.topic
    check("a group consumes every subscribed topic", perTopic[t1] == 5 and perTopic[t2] == 5, $perTopic)
    try: g.close() except CatchableError: discard
  block:
    let topic = unique("nim-autocommit")
    for i in 0 ..< 6: must p.sendTo(topic, 0, "c" & $i)
    proc committedAfter(intervalMs: int): int64 =
      let g = must newGroupConsumer(address, unique("nim-auto-grp"), groupConfigWith(intervalMs))
      g.subscribe([topic])
      discard pollUntil(g, 6, 15_000)
      sleep(200)
      try: discard g.poll(300) except CatchableError: discard
      let committed = must g.committed([(topic, 0'i32)])
      result = committed.getOrDefault((topic, 0'i32), -1)
      try: g.close() except CatchableError: discard
    let auto = committedAfter(100)
    check("auto-commit records positions without an explicit commit", auto == 6, $auto)
    let manual = committedAfter(0)
    check("disabled auto-commit commits nothing", manual < 0, $manual)
  block:
    # Static membership: a second instance presenting the same
    # group.instance.id takes over the first one's partitions at once,
    # without a rebalance, while the first is still heartbeating.
    let topic = unique("nim-static")
    discard must p.router.partitions(topic)
    let group = unique("nim-static-grp")
    var config = groupConfigWith()
    config.groupInstanceId = "instance-1"
    let first = must newGroupConsumer(address, group, config)
    first.subscribe([topic])
    try: discard first.poll(2000) except CatchableError: discard
    let firstAssignment = first.assignment
    let second = must newGroupConsumer(address, group, config)
    second.subscribe([topic])
    let started = monoMillis()
    try: discard second.poll(200) except CatchableError: discard
    let took = elapsedMs(started)
    let secondAssignment = second.assignment
    check("a static member reclaims its partitions without a rebalance",
          firstAssignment.len == 4 and sorted(secondAssignment, cmpSlot) == sorted(firstAssignment, cmpSlot) and
            took < 2000,
          "first=" & $firstAssignment.len & " second=" & $secondAssignment.len & " " & $took & " ms")
    try: second.close() except CatchableError: discard
    try: first.close() except CatchableError: discard
  for silent in [false, true]:
    # LeaveGroup on close (30 s session, 200 ms heartbeat: the survivor takes
    # over within a heartbeat, not a session), then session.timeout.ms (a
    # member whose only route is a proxy that is shut goes silent without
    # leaving, and is evicted once its 2 s session lapses).
    let topic = unique(if silent: "nim-session" else: "nim-leave")
    discard must p.router.partitions(topic)
    let group = unique("nim-member-grp")
    let sessionMs = if silent: 2000 else: 30_000
    var config = groupConfigWith()
    config.sessionTimeoutMs = int32(sessionMs)
    config.heartbeatIntervalMs = 200
    let proxy = if silent: newProxy(address) else: nil
    let a = must newGroupConsumer(if silent: proxy.address else: address, group, config)
    a.subscribe([topic])
    memberStop.store(false)
    memberAssigned.store(0)
    var member: Thread[tuple[address, group, topic: string, sessionMs, heartbeatMs: int]]
    createThread(member, memberLoop, (address, group, topic, sessionMs, 200))
    let split = waitFor(proc (): bool = a.assignment.len == 2 and memberAssigned.load == 2, 20_000, a)
    if silent: proxy.close()
    else: must a.close()
    let started = monoMillis()
    let tookOver = waitFor(proc (): bool = memberAssigned.load == 4, 20_000)
    let took = elapsedMs(started)
    let detail = "split=" & $split & " took_over=" & $tookOver & " " & $took & " ms"
    if silent:
      check("a silent member is evicted after session.timeout.ms",
            split and tookOver and took >= 1000 and took < 12_000, detail)
    else:
      check("closing a member hands its partitions over within a heartbeat",
            split and tookOver and took < 5000, detail)
    memberStop.store(true)
    joinThread(member)
    if silent:
      try: a.close() except CatchableError: discard
  block:
    # Generation fencing: a member whose generation moved on cannot commit.
    let topic = unique("nim-fence")
    for i in 0 ..< 4: must p.send(topic, "f" & $i)
    let group = unique("nim-fence-grp")
    let a = must newGroupConsumer(address, group, groupConfigWith())
    a.subscribe([topic])
    discard pollUntil(a, 4, 10_000)
    let b = must newGroupConsumer(address, group, groupConfigWith())
    b.subscribe([topic])
    try: discard b.poll(500) except CatchableError: discard
    var fenced = false
    try: a.commit()
    except CatchableError: fenced = true
    check("a commit from a stale generation is refused", fenced, "commit succeeded")
    try: b.close() except CatchableError: discard
    try: a.close() except CatchableError: discard
  must p.close()

  section("assignors (unit)")
  block:
    let members = @[AssignorMember(id: "a", topics: @["t"]), AssignorMember(id: "b", topics: @["t"])]
    proc slots(ids: openArray[int]): seq[TopicPartition] =
      for i in ids: result.add ("t", int32(i))
    var all: seq[int32]
    for i in 0 .. 11: all.add int32(i)
    let sticky = stickyAssign(members, {"t": all}.toTable,
                              {"a": slots(toSeq(0 .. 11)), "b": newSeq[TopicPartition]()}.toTable)
    check("sticky keeps partitions in numeric order",
          sticky["a"] == slots(toSeq(0 .. 5)) and sticky["b"] == slots(toSeq(6 .. 11)), $sticky)
    let held = {"a": slots([1, 3]), "b": slots([0, 2])}.toTable
    let kept = stickyAssign(members, {"t": @[0'i32, 1, 2, 3]}.toTable, held)
    check("sticky keeps what members already hold", kept["a"] == held["a"] and kept["b"] == held["b"], $kept)

  section("decoder bounds")
  block:
    var w = initBodyWriter()
    w.writeInt32(-5)
    var negative = false
    try:
      var r = initBodyReader(w.buf)
      discard r.readString()
    except CatchableError: negative = true
    check("a negative length is an error", negative)
    var w2 = initBodyWriter()
    w2.writeInt32(1_000_000)
    w2.writeRaw("short")
    var oversized = false
    try:
      var r = initBodyReader(w2.buf)
      discard r.readString()
    except CatchableError: oversized = true
    check("a length past the end of the data is an error", oversized)
    var truncated = false
    try: discard decodeRecordBatch("\0\0\0\0\0\0\0\0\x7f\xff\xff\xff\0\0\0\0", 0)
    except CatchableError: truncated = true
    check("a batch longer than its bytes is an error", truncated)

# ---------------------------------------------------------------------------

proc main() =
  var address = "127.0.0.1:9092"
  let args = commandLineParams()
  if args.len >= 2: address = args[0] & ":" & args[1]
  elif args.len == 1: address = args[0]

  let payloads = block:
    var s: seq[string]
    for i in 0 ..< 50: s.add "record-" & $i
    s
  let topic = unique("nim-roundtrip")

  section("connection and metadata")
  block:
    let consumer = must newConsumer(address)
    var detail = ""
    var versions: seq[ApiVersionRange]
    var brokerVersion = ""
    try:
      (versions, brokerVersion) = consumer.router.seed.apiVersions()
    except CatchableError as e:
      detail = e.msg
    check("ApiVersions answers", detail.len == 0 and versions.len > 0, detail)
    check("broker reports a version", brokerVersion.len > 0, brokerVersion)
    let metadata = must consumer.router.metadata([], true)
    check("metadata lists brokers", metadata.brokers.len >= 1, $metadata.brokers.len & " brokers")
    consumer.close()

  section("produce and consume round trip")
  block:
    let producer = must newProducer(address, noLinger())
    for payload in payloads:
      must producer.sendTo(topic, 0, payload)
    must producer.flush()
    must producer.close()
  block:
    let consumer = must newConsumer(address)
    let got = must consumer.fetch(topic, 0, 0, 500)
    check("every record comes back", got.len == payloads.len, "got " & $got.len)
    var identical = got.len == payloads.len
    for i in 0 ..< got.len:
      if not identical: break
      if got[i].value != some(payloads[i]) or got[i].offset != int64(i): identical = false
    check("values byte-identical and offsets contiguous", identical)
    consumer.close()

  section("compression codecs")
  # Only none and gzip ship in the driver; lz4/zstd/snappy are opt-in via
  # registerCodec.
  for codec in ["none", "gzip"]:
    let codecTopic = unique("nim-" & codec)
    let body = repeatStr("the same line over and over. ", 40)
    var config = noLinger()
    config.compressionType = codec
    let producer = must newProducer(address, config)
    for i in 0 ..< 20:
      must producer.sendTo(codecTopic, 0, body & $char(ord('0') + i mod 10))
    must producer.flush()
    must producer.close()
    let consumer = must newConsumer(address)
    let got = must consumer.fetch(codecTopic, 0, 0, 500)
    check(codec & ": round trips", got.len == 20 and got[0].value.get("").startsWith(body),
          "got " & $got.len & " records")
    consumer.close()

  section("keys, partitioning and ordering")
  block:
    let keyTopic = unique("nim-keys")
    let producer = must newProducer(address, noLinger())
    let partitions = must producer.router.partitions(keyTopic)
    for i in 0 ..< 30:
      must producer.send(keyTopic, "v" & $i, some("user-7"))
    must producer.flush()
    must producer.close()

    let target = partitionForKey("user-7", partitions)
    let consumer = must newConsumer(address)
    let onTarget = must consumer.fetch(keyTopic, target, 0, 500)
    check("a key pins every record to one partition", onTarget.len == 30,
          "partition " & $target & " holds " & $onTarget.len & " of 30")
    var ordered = onTarget.len == 30
    for i in 0 ..< onTarget.len:
      if onTarget[i].value != some("v" & $i): ordered = false
    check("per-key order is preserved", ordered)
    var strays = 0
    for partition in partitions:
      if partition == target: continue
      strays += (must consumer.fetch(keyTopic, partition, 0, 200)).len
    check("no keyed record landed elsewhere", strays == 0, $strays & " strays")
    consumer.close()

  section("murmur2 agrees with the broker's partitioner")
  check("murmur2(\"\") is stable", murmur2("") == 275646681'u32, $murmur2(""))
  check("murmur2 is deterministic", murmur2("user-7") == murmur2("user-7"))
  check("different keys hash differently", murmur2("user-7") != murmur2("user-8"))

  section("record headers and timestamps")
  block:
    let headerTopic = unique("nim-headers")
    let before = nowMillis() - 1000
    let producer = must newProducer(address, noLinger())
    must producer.sendTo(headerTopic, 0, "annotated", headers = [
      header("trace-id", "abc-123"),
      header("content-type", "application/json"),
      nullHeader("tombstone-reason")])
    must producer.sendTo(headerTopic, 0, "plain")
    must producer.flush()
    must producer.close()
    let after = nowMillis() + 1000

    let consumer = must newConsumer(address)
    let got = must consumer.fetch(headerTopic, 0, 0, 500)
    check("both records arrive", got.len == 2, "got " & $got.len)
    if got.len == 2:
      let annotated = got[0]
      let plain = got[1]
      check("headers survive the round trip", annotated.headers.len == 3,
            $annotated.headers.len & " headers")
      check("header values are exact", annotated.header("trace-id") == some("abc-123"))
      check("a null header value stays null",
            annotated.headers.len == 3 and annotated.headers[2].value.isNone)
      check("a record with no headers gains none from its batch", plain.headers.len == 0,
            $plain.headers.len & " headers")
      var inWindow = true
      for record in got:
        if record.timestamp < before or record.timestamp > after: inWindow = false
      check("timestamps are real wall-clock values", inWindow,
            $got[0].timestamp & "," & $got[1].timestamp & " outside " & $before & ".." & $after)
    consumer.close()

  section("tombstones")
  block:
    let tombTopic = unique("nim-tombstones")
    let producer = must newProducer(address, noLinger())
    must producer.sendTo(tombTopic, 0, "set", some("k1"))
    must producer.sendTo(tombTopic, 0, "", some("k2"))
    # A null value is a deletion, and must stay distinguishable from the
    # empty value above all the way through the round trip.
    must producer.sendTo(tombTopic, 0, none(string), some("k3"))
    must producer.flush()
    must producer.close()

    let consumer = must newConsumer(address)
    let got = must consumer.fetch(tombTopic, 0, 0, 500)
    check("all three records arrive", got.len == 3, "got " & $got.len)
    if got.len == 3:
      check("an ordinary value round-trips", got[0].value == some("set"))
      check("an empty value is empty, not null", got[1].value == some(""), $got[1].value)
      check("a tombstone arrives as a null value", got[2].value.isNone, $got[2].value)
    consumer.close()

  section("offsets")
  block:
    let consumer = must newConsumer(address)
    let earliest = must consumer.listOffsets(topic, 0, Earliest)
    let latest = must consumer.listOffsets(topic, 0, Latest)
    check("earliest is 0 on a fresh topic", earliest == 0, $earliest)
    check("latest equals the record count", latest == 50, $latest)
    consumer.close()

  section("acks")
  for acks in [0'i32, 1, -1]:
    let acksTopic = unique("nim-acks" & $acks)
    var config = noLinger()
    config.acks = acks
    let producer = must newProducer(address, config)
    must producer.sendTo(acksTopic, 0, "durable")
    must producer.flush()
    must producer.close()
    sleep(400)
    let consumer = must newConsumer(address)
    let got = must consumer.fetch(acksTopic, 0, 0, 500)
    check("acks=" & $acks & " stores the record", got.len == 1, "got " & $got.len)
    consumer.close()

  section("consumer group: assignment, commit, resume")
  block:
    let groupTopic = unique("nim-group")
    let groupId = unique("nim-billing")
    let producer = must newProducer(address, noLinger())
    for i in 0 ..< 40:
      must producer.send(groupTopic, "g" & $i)
    must producer.flush()
    must producer.close()

    var groupConfig = defaultGroupConfig()
    groupConfig.autoCommitIntervalMs = 0
    let consumer = must newGroupConsumer(address, groupId, groupConfig)
    consumer.subscribe([groupTopic])
    var seen: seq[ConsumedRecord]
    let deadline = epochTime() + 30
    while seen.len < 40 and epochTime() < deadline:
      seen.add(must consumer.poll(500))
    check("the group consumes every record", seen.len == 40, "got " & $seen.len)
    var uniqueSeen = initHashSet[string]()
    for record in seen: uniqueSeen.incl $record.partition & "-" & $record.offset
    check("no record is delivered twice", uniqueSeen.len == seen.len)
    must consumer.commit()
    let committed = must consumer.committed()
    var total = 0'i64
    for offset in committed.values: total += offset
    check("commit records a position", total == 40, $total)
    must consumer.close()

    # A second consumer in the same group must resume, not replay.
    let rejoined = must newGroupConsumer(address, groupId, groupConfig)
    rejoined.subscribe([groupTopic])
    var replayed = 0
    let until = epochTime() + 5
    while epochTime() < until:
      try: replayed += rejoined.poll(300).len
      except CatchableError: discard
    check("a rejoining group resumes from its commit", replayed == 0,
          "replayed " & $replayed & " records it had already committed")
    must rejoined.close()

  section("auto.offset.reset")
  block:
    let resetTopic = unique("nim-reset")
    let producer = must newProducer(address, noLinger())
    for i in 0 ..< 10:
      must producer.send(resetTopic, "r" & $i)
    must producer.flush()
    must producer.close()

    var latestConfig = defaultGroupConfig()
    latestConfig.autoCommitIntervalMs = 0
    latestConfig.autoOffsetReset = AutoOffsetResetLatest
    let consumer = must newGroupConsumer(address, unique("nim-latest"), latestConfig)
    consumer.subscribe([resetTopic])
    var skipped = 0
    var until = epochTime() + 4
    while epochTime() < until:
      try: skipped += consumer.poll(300).len
      except CatchableError: discard
    check("latest skips records produced before the group existed", skipped == 0,
          "saw " & $skipped)
    must consumer.close()

    var noneConfig = defaultGroupConfig()
    noneConfig.autoCommitIntervalMs = 0
    noneConfig.autoOffsetReset = AutoOffsetResetNone
    let strict = must newGroupConsumer(address, unique("nim-none"), noneConfig)
    strict.subscribe([resetTopic])
    var raised = false
    until = epochTime() + 5
    while epochTime() < until and not raised:
      try: discard strict.poll(300)
      except NoOffsetForPartitionError: raised = true
      except CatchableError: discard
    check("none refuses to guess a position", raised)
    must strict.close()

  section("assignors")
  for assignor in [AssignorRange, AssignorRoundRobin, AssignorSticky]:
    let assignorTopic = unique("nim-" & assignor)
    let producer = must newProducer(address, noLinger())
    for i in 0 ..< 20:
      must producer.send(assignorTopic, "a" & $i)
    must producer.flush()
    must producer.close()
    var groupConfig = defaultGroupConfig()
    groupConfig.autoCommitIntervalMs = 0
    groupConfig.assignor = assignor
    let consumer = must newGroupConsumer(address, unique("nim-grp-" & assignor), groupConfig)
    consumer.subscribe([assignorTopic])
    var collected = 0
    let deadline = epochTime() + 20
    while collected < 20 and epochTime() < deadline:
      try: collected += consumer.poll(500).len
      except CatchableError: discard
    check(assignor & ": consumes every record", collected == 20, "got " & $collected)
    must consumer.close()

  section("bounded client buffer")
  block:
    let bufferTopic = unique("nim-buffer")
    var config = defaultProducerConfig()
    config.lingerMs = 10_000   # never flush on time during this check
    config.bufferMemory = 2048
    config.maxBlockMs = 300
    let producer = must newProducer(address, config)
    var blocked = false
    let record = repeatStr("x", 256)
    for i in 0 ..< 500:
      if blocked: break
      try: producer.sendTo(bufferTopic, 0, record)
      except BufferFullError as e: blocked = "buffer full" in e.msg
    check("a full buffer blocks and then reports", blocked)
    try: producer.close()
    except CatchableError: discard

  section("wire edge cases")
  block:
    let edgeTopic = unique("nim-edge")
    let producer = must newProducer(address, noLinger())
    var large = newString(1 shl 20)
    for i in 0 ..< large.len: large[i] = char((i * 7) and 0xFF)
    let unicodeKey = "ключ-✓-🔑"
    let unicodeValue = "значение — 数据 — 🚀"
    must producer.sendTo(edgeTopic, 0, large)
    must producer.sendTo(edgeTopic, 0, unicodeValue, some(unicodeKey),
                         [header("ünïcødé-🏷", "✓")])
    # An empty key and an empty header value are values, not nulls.
    must producer.sendTo(edgeTopic, 0, "empty-key", some(""),
                         [header("empty", ""), nullHeader("null")])
    must producer.sendTo(edgeTopic, 0, "null-key")
    must producer.close()

    let consumer = must newConsumer(address)
    var got: seq[ConsumedRecord]
    var offset = 0'i64
    while got.len < 4:
      var batch: seq[ConsumedRecord]
      try: batch = consumer.fetch(edgeTopic, 0, offset, 500)
      except CatchableError: break
      if batch.len == 0: break
      got.add batch
      offset = batch[^1].offset + 1
    check("edge records all arrive", got.len == 4, "got " & $got.len)
    if got.len == 4:
      check("a 1 MiB value round-trips byte-identical", got[0].value == some(large),
            $got[0].value.get("").len & " bytes")
      check("unicode key, value and header key round-trip",
            got[1].key == some(unicodeKey) and got[1].value == some(unicodeValue) and
            got[1].headers.len == 1 and got[1].headers[0].key == "ünïcødé-🏷")
      check("an empty key stays empty, not null", got[2].key == some(""), $got[2].key)
      check("an empty header value stays empty, not null",
            got[2].headers.len == 2 and got[2].headers[0].value == some("") and
            got[2].headers[1].value.isNone, $got[2].headers)
      check("a null key stays null", got[3].key.isNone, $got[3].key)
    consumer.close()

  section("ordering under linger flushes")
  block:
    let orderTopic = unique("nim-order")
    var config = defaultProducerConfig()
    config.lingerMs = 1
    config.batchSize = 256
    let producer = must newProducer(address, config)
    const total = 5000
    for i in 0 ..< total:
      must producer.sendTo(orderTopic, 0, $i)
    must producer.close()
    let consumer = must newConsumer(address)
    var values: seq[int]
    var offset = 0'i64
    while values.len < total:
      var batch: seq[ConsumedRecord]
      try: batch = consumer.fetch(orderTopic, 0, offset, 500)
      except CatchableError: break
      if batch.len == 0: break
      for record in batch: values.add parseInt(record.value.get("-1"))
      offset = batch[^1].offset + 1
    var inversions = 0
    for i in 1 ..< values.len:
      if values[i] < values[i - 1]: inc inversions
    check("every record of a partition arrives", values.len == total, "got " & $values.len)
    check("a partition's records keep send order", inversions == 0, $inversions & " inversions")
    consumer.close()

  section("background flush failures are reported")
  block:
    var config = defaultProducerConfig()
    config.lingerMs = 20
    let producer = must newProducer(address, config)
    # Partition 999 does not exist, so the linger thread's flush fails.
    var sendErr = ""
    try: producer.sendTo(unique("nim-bgfail"), 999, "lost")
    except CatchableError as e: sendErr = e.msg
    sleep(300)
    var flushErr = ""
    try: producer.flush()
    except CatchableError as e: flushErr = e.msg
    check("a failed linger flush surfaces on the next Flush",
          sendErr.len == 0 and flushErr.len > 0, "send=" & sendErr & " flush=" & flushErr)
    let started = epochTime()
    try: producer.close()
    except CatchableError: discard
    check("Close returns after a failed flush", epochTime() - started < 5.0, "hung")

  section("connection failures")
  block:
    # A broker that accepts and never answers must cost an error, not a
    # thread blocked forever.
    let silent = listenLocal()
    silentStop.store(false)
    var silentThread: Thread[SocketHandle]
    createThread(silentThread, silentLoop, silent.getFd)
    let conn = must dial("127.0.0.1:" & $silent.localPort, "nim-test", 1000)
    conn.setRequestTimeout(300)
    let started = epochTime()
    var requestErr = ""
    try: discard conn.apiVersions()
    except CatchableError as e: requestErr = e.msg
    check("a request to an unresponsive broker times out",
          requestErr.len > 0 and epochTime() - started < 3.0, requestErr)
    check("a timed-out connection is not reused", conn.broken)
    conn.close()
    silentStop.store(true)
    joinThread(silentThread)
    silent.close()

    # A connection the broker drops is redialled, not kept forever.
    let proxy = newProxy(address)
    let dropTopic = unique("nim-drop")
    let producer = must newProducer(proxy.address, noLinger())
    must producer.sendTo(dropTopic, 0, "before")
    proxy.dropAll()
    var recovered = "not attempted"
    for attempt in 0 ..< 3:
      if recovered.len == 0: break
      try:
        producer.sendTo(dropTopic, 0, "after")
        recovered = ""
      except CatchableError as e:
        recovered = e.msg
    check("a producer recovers after its connection drops", recovered.len == 0, recovered)
    try: producer.close()
    except CatchableError: discard
    let consumer = must newConsumer(proxy.address)
    discard must consumer.fetch(dropTopic, 0, 0, 100)
    proxy.dropAll()
    var fetchErr = "not attempted"
    var fetched: seq[ConsumedRecord]
    for attempt in 0 ..< 3:
      if fetchErr.len == 0: break
      try:
        fetched = consumer.fetch(dropTopic, 0, 0, 100)
        fetchErr = ""
      except CatchableError as e:
        fetchErr = e.msg
    check("a consumer recovers after its connection drops",
          fetchErr.len == 0 and fetched.len >= 1, fetchErr)
    consumer.close()
    proxy.close()

  section("consumer group: max.poll.interval and rejoin")
  block:
    let slowTopic = unique("nim-slow")
    let producer = must newProducer(address, noLinger())
    for i in 0 ..< 10:
      must producer.send(slowTopic, "s" & $i)
    var groupConfig = defaultGroupConfig()
    groupConfig.autoCommitIntervalMs = 0
    groupConfig.maxPollIntervalMs = 1500
    let consumer = must newGroupConsumer(address, unique("nim-slow-grp"), groupConfig)
    consumer.subscribe([slowTopic])
    var first: seq[ConsumedRecord]
    var deadline = epochTime() + 15
    while first.len < 10 and epochTime() < deadline:
      try: first.add consumer.poll(300)
      except CatchableError: break
    must consumer.commit()
    # Stall past max.poll.interval.ms: the member leaves the group.
    sleep(2500)
    for i in 10 ..< 20:
      must producer.send(slowTopic, "s" & $i)
    must producer.close()
    var second: seq[ConsumedRecord]
    var pollErr = ""
    deadline = epochTime() + 15
    while second.len < 10 and epochTime() < deadline:
      try: second.add consumer.poll(300)
      except CatchableError as e:
        pollErr = e.msg
        break
    check("a member that stalled rejoins on its next poll",
          first.len == 10 and second.len == 10 and pollErr.len == 0,
          "first=" & $first.len & " second=" & $second.len & " err=" & pollErr)
    must consumer.close()

  section("consumer group: time inside poll does not count against max.poll.interval")
  block:
    let joinTopic = unique("nim-inpoll")
    let producer = must newProducer(address, noLinger())
    discard must producer.router.partitions(joinTopic)
    var groupConfig = defaultGroupConfig()
    groupConfig.autoCommitIntervalMs = 0
    # Far shorter than the first poll below, which spends ~1s joining (the
    # broker's initial rebalance delay) and then waits for data.
    groupConfig.maxPollIntervalMs = 600
    let consumer = must newGroupConsumer(address, unique("nim-inpoll-grp"), groupConfig)
    consumer.subscribe([joinTopic])
    var late: Thread[tuple[address, topic: string]]
    createThread(late, lateProducer, (address, joinTopic))
    # One long poll: it joins, then waits for the records above.
    var got: seq[ConsumedRecord]
    var pollErr, commitErr = ""
    try: got = consumer.poll(4000)
    except CatchableError as e: pollErr = e.msg
    # Committed straight away, before another poll could quietly rejoin:
    # this fails if the member left the group mid-poll.
    try: consumer.commit()
    except CatchableError as e: commitErr = e.msg
    check("a member is still in its group after a long poll",
          pollErr.len == 0 and got.len > 0 and commitErr.len == 0,
          "got=" & $got.len & " poll=" & pollErr & " commit=" & commitErr)
    joinThread(late)
    must consumer.close()
    must producer.close()

  coverage(address)

  echo "\n", passed, " passed, ", failed, " failed"
  if failed > 0: quit(1)

main()
