# End-to-end suite for the Crystal driver against a live broker. A port of
# the Go suite (clients/go/cmd/manualtest) with the same sections and checks.
#
#   brahmaputra-server --data-dir ./data --default-partitions 4
#   crystal run test/manual_test.cr -- 127.0.0.1 9092
#
# Every check asserts a property of the system, not that a function ran.
require "../src/brahmaputra"

alias BP = Brahmaputra

HOST    = ARGV[0]? || "127.0.0.1"
PORT    = ARGV[1]? || "9092"
ADDRESS = "#{HOST}:#{PORT}"

class Tally
  class_property passed = 0
  class_property failed = 0
end

def check(name : String, ok : Bool, detail : String = "") : Nil
  if ok
    Tally.passed += 1
    puts "  ok   #{name}"
  else
    Tally.failed += 1
    puts detail.empty? ? "  FAIL #{name}" : "  FAIL #{name}: #{detail}"
  end
end

def section(title : String) : Nil
  puts "\n#{title}"
end

def unique(prefix : String) : String
  "#{prefix}-#{Time.utc.to_unix_ns % 1_000_000_000}"
end

# Aborts the suite on an unexpected error from setup code.
def must(&)
  yield
rescue ex
  puts "  FATAL #{ex.class}: #{ex.message}"
  exit 2
end

def producer(linger_ms = 0, &) : BP::Producer
  config = BP::ProducerConfig.new
  config.linger_ms = linger_ms
  yield config
  must { BP::Producer.new(ADDRESS, config) }
end

def producer(linger_ms = 0) : BP::Producer
  producer(linger_ms) { }
end

def consumer(address = ADDRESS) : BP::Consumer
  must { BP::Consumer.new(address) }
end

def group_config(&) : BP::GroupConfig
  config = BP::GroupConfig.new
  config.auto_commit_interval_ms = 0
  yield config
  config
end

def fetch_all(consumer : BP::Consumer, topic : String, partition : Int32, want : Int32) : Array(BP::ConsumedRecord)
  got = [] of BP::ConsumedRecord
  offset = 0_i64
  while got.size < want
    batch = begin
      consumer.fetch(topic, partition, offset, 500)
    rescue
      break
    end
    break if batch.empty?
    got.concat(batch)
    offset = batch.last.offset + 1
  end
  got
end

# Forwards TCP to the broker and can sever every live connection, which is
# how a broker restart or an idle timeout looks to a client.
class Proxy
  getter address : String

  def initialize(target : String)
    @server = TCPServer.new("127.0.0.1", 0)
    @address = "127.0.0.1:#{@server.local_address.port}"
    @live = [] of TCPSocket
    host, port = BP.parse_address(target)
    spawn do
      while client = (@server.accept? rescue nil)
        upstream = begin
          TCPSocket.new(host, port)
        rescue
          client.close
          next
        end
        @live << client << upstream
        pipe(client, upstream)
        pipe(upstream, client)
      end
    end
  end

  private def pipe(from : TCPSocket, to : TCPSocket) : Nil
    spawn do
      begin
        IO.copy(from, to)
      rescue
      ensure
        to.close rescue nil
      end
    end
  end

  def drop_all : Nil
    @live.each { |s| s.close rescue nil }
    @live.clear
    sleep 50.milliseconds
  end

  def close : Nil
    @server.close rescue nil
    drop_all
  end
end

section "connection and metadata"
begin
  c = consumer
  begin
    versions, broker_version = c.router.seed.api_versions
    check "ApiVersions answers", !versions.empty?
    check "broker reports a version", !broker_version.empty?, broker_version
  rescue ex
    check "ApiVersions answers", false, ex.message.to_s
    check "broker reports a version", false
  end
  metadata = must { c.router.metadata([] of String, refresh: true) }
  check "metadata lists brokers", metadata.brokers.size >= 1, "#{metadata.brokers.size} brokers"
  c.close
end

section "produce and consume round trip"
TOPIC    = unique("cr-roundtrip")
PAYLOADS = (0...50).map { |i| "record-#{i}".to_slice }
begin
  p = producer
  PAYLOADS.each { |payload| must { p.send_to(TOPIC, 0, payload) } }
  must { p.flush }
  must { p.close }
  c = consumer
  got = must { c.fetch(TOPIC, 0, 0_i64, 500) }
  check "every record comes back", got.size == PAYLOADS.size, "got #{got.size}"
  identical = got.size == PAYLOADS.size && got.each_with_index.all? { |r, i| r.value == PAYLOADS[i] && r.offset == i }
  check "values byte-identical and offsets contiguous", identical
  c.close
end

section "compression codecs"
# Only none and gzip ship in the driver; others are opt-in via
# Brahmaputra.register_codec.
%w(none gzip).each do |codec|
  codec_topic = unique("cr-#{codec}")
  body = "the same line over and over. " * 40
  p = producer { |cfg| cfg.compression_type = codec }
  20.times { |i| must { p.send_to(codec_topic, 0, body + ('0' + i % 10).to_s) } }
  must { p.flush }
  must { p.close }
  c = consumer
  got = must { c.fetch(codec_topic, 0, 0_i64, 500) }
  check "#{codec}: round trips", got.size == 20 && (got[0].value_string || "").starts_with?(body), "got #{got.size} records"
  c.close
end

section "keys, partitioning and ordering"
begin
  key_topic = unique("cr-keys")
  p = producer
  partitions = must { p.router.partitions(key_topic) }
  30.times { |i| must { p.send(key_topic, "v#{i}", "user-7") } }
  must { p.flush }
  must { p.close }
  target = BP.partition_for_key("user-7", partitions)
  c = consumer
  on_target = must { c.fetch(key_topic, target, 0_i64, 500) }
  check "a key pins every record to one partition", on_target.size == 30,
    "partition #{target} holds #{on_target.size} of 30"
  ordered = on_target.size == 30 && on_target.each_with_index.all? { |r, i| r.value_string == "v#{i}" }
  check "per-key order is preserved", ordered
  strays = partitions.reject(target).sum { |part| must { c.fetch(key_topic, part, 0_i64, 200) }.size }
  check "no keyed record landed elsewhere", strays == 0, "#{strays} strays"
  c.close
end

section "murmur2 agrees with the broker's partitioner"
check "murmur2(\"\") is stable", BP.murmur2(Bytes.empty) == 275646681_u32, BP.murmur2(Bytes.empty).to_s
check "murmur2 is deterministic", BP.murmur2("user-7") == BP.murmur2("user-7")
check "different keys hash differently", BP.murmur2("user-7") != BP.murmur2("user-8")

section "record headers and timestamps"
begin
  header_topic = unique("cr-headers")
  before = BP.now_ms - 1000
  p = producer
  must do
    p.send_to(header_topic, 0, "annotated", nil, [
      BP::Header.new("trace-id", "abc-123"),
      BP::Header.new("content-type", "application/json"),
      BP::Header.new("tombstone-reason", nil),
    ])
  end
  must { p.send_to(header_topic, 0, "plain") }
  must { p.flush }
  must { p.close }
  after = BP.now_ms + 1000
  c = consumer
  got = must { c.fetch(header_topic, 0, 0_i64, 500) }
  check "both records arrive", got.size == 2, "got #{got.size}"
  if got.size == 2
    annotated, plain = got[0], got[1]
    check "headers survive the round trip", annotated.headers.size == 3, "#{annotated.headers.size} headers"
    check "header values are exact", annotated.header("trace-id") == "abc-123".to_slice
    check "a null header value stays null", annotated.headers.size == 3 && annotated.headers[2].value.nil?
    check "a record with no headers gains none from its batch", plain.headers.empty?, "#{plain.headers.size} headers"
    in_window = got.all? { |r| r.timestamp >= before && r.timestamp <= after }
    check "timestamps are real wall-clock values", in_window,
      "#{got[0].timestamp},#{got[1].timestamp} outside #{before}..#{after}"
  end
  c.close
end

section "tombstones"
begin
  tomb_topic = unique("cr-tombstones")
  p = producer
  must { p.send_to(tomb_topic, 0, "set", "k1") }
  must { p.send_to(tomb_topic, 0, Bytes.empty, "k2") }
  # A nil value is a deletion, distinct from the empty value above.
  must { p.send_to(tomb_topic, 0, nil, "k3") }
  must { p.flush }
  must { p.close }
  c = consumer
  got = must { c.fetch(tomb_topic, 0, 0_i64, 500) }
  check "all three records arrive", got.size == 3, "got #{got.size}"
  if got.size == 3
    check "an ordinary value round-trips", got[0].value == "set".to_slice
    check "an empty value is empty, not null", !got[1].value.nil? && got[1].value.not_nil!.empty?, got[1].value.inspect
    check "a tombstone arrives as a null value", got[2].value.nil?, got[2].value.inspect
  end
  c.close
end

section "offsets"
begin
  c = consumer
  earliest = must { c.list_offsets(TOPIC, 0, BP::EARLIEST) }
  latest = must { c.list_offsets(TOPIC, 0, BP::LATEST) }
  check "earliest is 0 on a fresh topic", earliest == 0, earliest.to_s
  check "latest equals the record count", latest == 50, latest.to_s
  c.close
end

section "acks"
[0, 1, -1].each do |acks|
  acks_topic = unique("cr-acks#{acks}")
  p = producer { |cfg| cfg.acks = acks }
  must { p.send_to(acks_topic, 0, "durable") }
  must { p.flush }
  must { p.close }
  sleep 400.milliseconds
  c = consumer
  got = must { c.fetch(acks_topic, 0, 0_i64, 500) }
  check "acks=#{acks} stores the record", got.size == 1, "got #{got.size}"
  c.close
end

section "consumer group: assignment, commit, resume"
begin
  group_topic = unique("cr-group")
  group_id = unique("cr-billing")
  p = producer
  40.times { |i| must { p.send(group_topic, "g#{i}") } }
  must { p.flush }
  must { p.close }

  config = group_config { }
  gc = must { BP::GroupConsumer.new(ADDRESS, group_id, config) }
  gc.subscribe([group_topic])
  seen = [] of BP::ConsumedRecord
  deadline = Time.monotonic + 30.seconds
  while seen.size < 40 && Time.monotonic < deadline
    seen.concat(must { gc.poll(500.milliseconds) })
  end
  check "the group consumes every record", seen.size == 40, "got #{seen.size}"
  distinct = seen.map { |r| {r.partition, r.offset} }.uniq.size
  check "no record is delivered twice", distinct == seen.size
  must { gc.commit }
  committed = must { gc.committed }
  total = committed.values.sum
  check "commit records a position", total == 40, total.to_s
  must { gc.close }

  # A second consumer in the same group must resume, not replay.
  rejoined = must { BP::GroupConsumer.new(ADDRESS, group_id, config) }
  rejoined.subscribe([group_topic])
  replayed = [] of BP::ConsumedRecord
  until_t = Time.monotonic + 5.seconds
  while Time.monotonic < until_t
    replayed.concat(rejoined.poll(300.milliseconds)) rescue nil
  end
  check "a rejoining group resumes from its commit", replayed.empty?,
    "replayed #{replayed.size} records it had already committed"
  must { rejoined.close }
end

section "auto.offset.reset"
begin
  reset_topic = unique("cr-reset")
  p = producer
  10.times { |i| must { p.send(reset_topic, "r#{i}") } }
  must { p.flush }
  must { p.close }

  gc = must { BP::GroupConsumer.new(ADDRESS, unique("cr-latest"), group_config { |cfg| cfg.auto_offset_reset = "latest" }) }
  gc.subscribe([reset_topic])
  skipped = [] of BP::ConsumedRecord
  until_t = Time.monotonic + 4.seconds
  while Time.monotonic < until_t
    skipped.concat(gc.poll(300.milliseconds)) rescue nil
  end
  check "latest skips records produced before the group existed", skipped.empty?, "saw #{skipped.size}"
  must { gc.close }

  strict = must { BP::GroupConsumer.new(ADDRESS, unique("cr-none"), group_config { |cfg| cfg.auto_offset_reset = "none" }) }
  strict.subscribe([reset_topic])
  raised = false
  until_t = Time.monotonic + 5.seconds
  while Time.monotonic < until_t && !raised
    begin
      strict.poll(300.milliseconds)
    rescue ex : BP::NoOffsetForPartitionError
      raised = true
    rescue
    end
  end
  check "none refuses to guess a position", raised
  must { strict.close }
end

section "assignors"
%w(range roundrobin sticky).each do |assignor|
  assignor_topic = unique("cr-#{assignor}")
  p = producer
  20.times { |i| must { p.send(assignor_topic, "a#{i}") } }
  must { p.flush }
  must { p.close }
  gc = must do
    BP::GroupConsumer.new(ADDRESS, unique("cr-grp-#{assignor}"),
      group_config { |cfg| cfg.partition_assignment_strategy = assignor })
  end
  gc.subscribe([assignor_topic])
  collected = [] of BP::ConsumedRecord
  deadline = Time.monotonic + 20.seconds
  while collected.size < 20 && Time.monotonic < deadline
    collected.concat(gc.poll(500.milliseconds)) rescue nil
  end
  check "#{assignor}: consumes every record", collected.size == 20, "got #{collected.size}"
  must { gc.close }
end

section "bounded client buffer"
begin
  buffer_topic = unique("cr-buffer")
  p = producer(10_000) do |cfg| # never flush on time during this check
    cfg.buffer_memory = 2048
    cfg.max_block_ms = 300
  end
  blocked = false
  500.times do
    break if blocked
    begin
      p.send_to(buffer_topic, 0, "x" * 256)
    rescue ex : BP::BufferFullError
      blocked = (ex.message || "").includes?("buffer full")
    end
  end
  check "a full buffer blocks and then reports", blocked
  p.close rescue nil
end

section "wire edge cases"
begin
  edge_topic = unique("cr-edge")
  p = producer
  large = Bytes.new(1 << 20) { |i| (i &* 7).to_u8! }
  unicode_key = "ключ-✓-🔑".to_slice
  unicode_value = "значение — 数据 — 🚀".to_slice
  must { p.send_to(edge_topic, 0, large) }
  must { p.send_to(edge_topic, 0, unicode_value, unicode_key, [BP::Header.new("ünïcødé-🏷", "✓")]) }
  # An empty key and an empty header value are values, not nulls.
  must do
    p.send_to(edge_topic, 0, "empty-key", Bytes.empty,
      [BP::Header.new("empty", Bytes.empty), BP::Header.new("null", nil)])
  end
  must { p.send_to(edge_topic, 0, "null-key", nil) }
  must { p.close }

  c = consumer
  got = fetch_all(c, edge_topic, 0, 4)
  check "edge records all arrive", got.size == 4, "got #{got.size}"
  if got.size == 4
    check "a 1 MiB value round-trips byte-identical", got[0].value == large, "#{got[0].value.try(&.size)} bytes"
    check "unicode key, value and header key round-trip",
      got[1].key == unicode_key && got[1].value == unicode_value &&
      got[1].headers.size == 1 && got[1].headers[0].key == "ünïcødé-🏷"
    check "an empty key stays empty, not null", !got[2].key.nil? && got[2].key.not_nil!.empty?, got[2].key.inspect
    hs = got[2].headers
    check "an empty header value stays empty, not null",
      hs.size == 2 && !hs[0].value.nil? && hs[0].value.not_nil!.empty? && hs[1].value.nil?, hs.inspect
    check "a null key stays null", got[3].key.nil?, got[3].key.inspect
  end
  c.close
end

section "ordering under linger flushes"
begin
  order_topic = unique("cr-order")
  total = 5000
  p = producer(1) { |cfg| cfg.batch_size = 256 }
  total.times { |i| must { p.send_to(order_topic, 0, i.to_s) } }
  must { p.close }
  c = consumer
  values = fetch_all(c, order_topic, 0, total).map { |r| (r.value_string || "").to_i }
  inversions = (1...values.size).count { |i| values[i] < values[i - 1] }
  check "every record of a partition arrives", values.size == total, "got #{values.size}"
  check "a partition's records keep send order", inversions == 0, "#{inversions} inversions"
  c.close
end

section "background flush failures are reported"
begin
  p = producer(20)
  # Partition 999 does not exist, so the linger fiber's flush fails.
  send_error = begin
    p.send_to(unique("cr-bgfail"), 999, "lost")
    nil
  rescue ex
    ex
  end
  sleep 300.milliseconds
  flush_error = begin
    p.flush
    nil
  rescue ex
    ex
  end
  check "a failed linger flush surfaces on the next Flush", send_error.nil? && !flush_error.nil?,
    "send=#{send_error.inspect} flush=#{flush_error.inspect}"
  closed = Channel(Nil).new(1)
  spawn do
    p.close rescue nil
    closed.send(nil)
  end
  select
  when closed.receive
    check "Close returns after a failed flush", true
  when timeout(5.seconds)
    check "Close returns after a failed flush", false, "hung"
  end
end

section "connection failures"
begin
  # A broker that accepts and never answers must cost an error, not a
  # fiber blocked forever.
  silent = TCPServer.new("127.0.0.1", 0)
  held = [] of TCPSocket
  spawn do
    while client = (silent.accept? rescue nil)
      held << client
      spawn { IO.copy(client, IO::Memory.new) rescue nil }
    end
  end
  conn = must { BP::Connection.dial("127.0.0.1:#{silent.local_address.port}", "cr-test", 1.second) }
  conn.request_timeout = 300.milliseconds
  started = Time.monotonic
  request_error = begin
    conn.api_versions
    nil
  rescue ex
    ex
  end
  check "a request to an unresponsive broker times out",
    !request_error.nil? && Time.monotonic - started < 3.seconds, request_error.inspect
  check "a timed-out connection is not reused", conn.broken?
  conn.close
  silent.close
  held.each { |s| s.close rescue nil }

  # A connection the broker drops is redialled, not kept forever.
  proxy = Proxy.new(ADDRESS)
  drop_topic = unique("cr-drop")
  p = must { BP::Producer.new(proxy.address, BP::ProducerConfig.new { |cfg| cfg.linger_ms = 0 }) }
  must { p.send_to(drop_topic, 0, "before") }
  proxy.drop_all
  recovered = nil.as(Exception?)
  3.times do |attempt|
    begin
      p.send_to(drop_topic, 0, "after")
      recovered = nil
      break
    rescue ex
      recovered = ex
    end
  end
  check "a producer recovers after its connection drops", recovered.nil?, recovered.inspect
  p.close rescue nil

  c = must { BP::Consumer.new(proxy.address) }
  must { c.fetch(drop_topic, 0, 0_i64, 100) }
  proxy.drop_all
  fetch_error = nil.as(Exception?)
  fetched = [] of BP::ConsumedRecord
  3.times do
    begin
      fetched = c.fetch(drop_topic, 0, 0_i64, 100)
      fetch_error = nil
      break
    rescue ex
      fetch_error = ex
    end
  end
  check "a consumer recovers after its connection drops", fetch_error.nil? && fetched.size >= 1, fetch_error.inspect
  c.close
  proxy.close
end

section "consumer group: max.poll.interval and rejoin"
begin
  slow_topic = unique("cr-slow")
  p = producer
  10.times { |i| must { p.send(slow_topic, "s#{i}") } }
  gc = must { BP::GroupConsumer.new(ADDRESS, unique("cr-slow-grp"), group_config { |cfg| cfg.max_poll_interval_ms = 1500 }) }
  gc.subscribe([slow_topic])
  first = [] of BP::ConsumedRecord
  deadline = Time.monotonic + 15.seconds
  while first.size < 10 && Time.monotonic < deadline
    begin
      first.concat(gc.poll(300.milliseconds))
    rescue
      break
    end
  end
  must { gc.commit }
  # Stall past max.poll.interval.ms: the member leaves the group.
  sleep 2500.milliseconds
  (10...20).each { |i| must { p.send(slow_topic, "s#{i}") } }
  must { p.close }
  second = [] of BP::ConsumedRecord
  poll_error = nil.as(Exception?)
  deadline = Time.monotonic + 15.seconds
  while second.size < 10 && Time.monotonic < deadline
    begin
      second.concat(gc.poll(300.milliseconds))
    rescue ex
      poll_error = ex
      break
    end
  end
  check "a member that stalled rejoins on its next poll",
    first.size == 10 && second.size == 10 && poll_error.nil?,
    "first=#{first.size} second=#{second.size} err=#{poll_error.inspect}"
  must { gc.close }
end

section "consumer group: time inside poll does not count against max.poll.interval"
begin
  join_topic = unique("cr-inpoll")
  p = producer
  must { p.router.partitions(join_topic) }
  # Far shorter than the first poll below, which spends ~1s joining (the
  # broker's initial rebalance delay) and then waits for data.
  gc = must { BP::GroupConsumer.new(ADDRESS, unique("cr-inpoll-grp"), group_config { |cfg| cfg.max_poll_interval_ms = 600 }) }
  gc.subscribe([join_topic])
  spawn do
    sleep 2.seconds
    10.times { |i| p.send(join_topic, "j#{i}") rescue nil }
  end
  # One long poll: it joins, then waits for the records above.
  got = [] of BP::ConsumedRecord
  poll_error = nil.as(Exception?)
  begin
    got = gc.poll(4.seconds)
  rescue ex
    poll_error = ex
  end
  # Committed straight away, before another poll could quietly rejoin:
  # this fails if the member left the group mid-poll.
  commit_error = nil.as(Exception?)
  begin
    gc.commit
  rescue ex
    commit_error = ex
  end
  check "a member is still in its group after a long poll",
    poll_error.nil? && !got.empty? && commit_error.nil?,
    "got=#{got.size} poll=#{poll_error.inspect} commit=#{commit_error.inspect}"
  must { gc.close }
  must { p.close }
end

# ---------------------------------------------------------------------------
# Coverage: one check per client feature the Go suite's sections do not
# already exercise.
# ---------------------------------------------------------------------------

# The broker's lz4 payload: a little-endian uncompressed length, then a raw
# LZ4 block. This encoder writes literals only (valid, if uncompressed).
def lz4_compress(data : Bytes) : Bytes
  io = IO::Memory.new
  io.write_bytes(data.size.to_u32, IO::ByteFormat::LittleEndian)
  if data.size >= 15
    io.write_byte(0xf0_u8)
    rest = data.size - 15
    while rest >= 255
      io.write_byte(255_u8)
      rest -= 255
    end
    io.write_byte(rest.to_u8)
  else
    io.write_byte((data.size << 4).to_u8)
  end
  io.write(data)
  io.to_slice
end

def lz4_decompress(data : Bytes) : Bytes
  result = [] of UInt8
  pos = 4
  read_len = ->(n : Int32) do
    if n == 15
      loop do
        b = data[pos]
        pos += 1
        n += b
        break if b != 255
      end
    end
    n
  end
  while pos < data.size
    token = data[pos].to_i32
    pos += 1
    lits = read_len.call(token >> 4)
    result.concat(data[pos, lits].to_a)
    pos += lits
    break if pos >= data.size
    offset = data[pos].to_i32 | (data[pos + 1].to_i32 << 8)
    pos += 2
    (read_len.call(token & 15) + 4).times { result << result[result.size - offset] }
  end
  Slice.new(result.size) { |i| result[i] }
end

# A broker that answers Metadata with itself as the only broker and refuses
# every produce: topic "fatal" with a non-retriable code, anything else with
# NOT_ENOUGH_REPLICAS (retriable). Records {topic, acks, timeout} per produce.
class FakeBroker
  getter address : String
  getter produces = [] of {String, Int32, Int32}

  def initialize
    @server = TCPServer.new("127.0.0.1", 0)
    port = @server.local_address.port
    @address = "127.0.0.1:#{port}"
    spawn do
      while client = (@server.accept? rescue nil)
        spawn serve(client, port)
      end
    end
  end

  private def serve(client : TCPSocket, port : Int32) : Nil
    loop do
      size = client.read_bytes(Int32, IO::ByteFormat::BigEndian)
      payload = Bytes.new(size)
      client.read_fully(payload)
      api = IO::ByteFormat::BigEndian.decode(Int16, payload[0, 2])
      clen = IO::ByteFormat::BigEndian.decode(Int16, payload[8, 2]).to_i32
      body = answer(api, payload[10 + clen, size - 10 - clen], port)
      client.write_bytes(10 + clen + body.size, IO::ByteFormat::BigEndian)
      client.write(payload[0, 10 + clen])
      client.write(body)
      client.flush
    end
  rescue
    client.close rescue nil
  end

  private def answer(api : Int16, req : Bytes, port : Int32) : Bytes
    w = BP::Protocol::Writer.new
    r = BP::Protocol::Reader.new(req)
    case api
    when 3
      topics = r.string_array
      w.int32(0).int32(1).int32(0).string("127.0.0.1").int32(port).string("").int32(0).int32(topics.size)
      topics.each { |t| w.string(t).int32(0).int32(1).int32(0).int32(0).int32(1).int32(0).int32(1).int32(0).int32(0) }
    when 0
      topic = r.string
      partition = r.int32
      acks = r.int32
      timeout = r.int32
      @produces << {topic, acks, timeout}
      w.string(topic).int32(partition).int32(topic == "fatal" ? 87 : 10).int64(-1).int64(-1)
    else
      w.int32(35)
    end
    w.to_slice
  end

  def close : Nil
    @server.close rescue nil
  end
end

def fails(&) : Bool
  yield
  false
rescue
  true
end

def elapsed_ms(started : Time::Span) : Int64
  (Time.monotonic - started).total_milliseconds.to_i64
end

# Sizes of successive non-empty polls until `want` records or the deadline.
def poll_sizes(gc : BP::GroupConsumer, want : Int32, within : Time::Span) : Array(Int32)
  sizes = [] of Int32
  deadline = Time.monotonic + within
  while sizes.sum < want && Time.monotonic < deadline
    got = gc.poll(300.milliseconds) rescue [] of BP::ConsumedRecord
    sizes << got.size unless got.empty?
  end
  sizes
end

def poll_until(gc : BP::GroupConsumer, want : Int32, within : Time::Span) : Array(BP::ConsumedRecord)
  got = [] of BP::ConsumedRecord
  deadline = Time.monotonic + within
  while got.size < want && Time.monotonic < deadline
    got.concat(must { gc.poll(300.milliseconds) })
  end
  got
end

# Polls every group concurrently (a join blocks until every member has
# rejoined) until the block holds or the deadline passes.
def settle(groups : Array(BP::GroupConsumer), within : Time::Span, &done : -> Bool) : Bool
  deadline = Time.monotonic + within
  loop do
    finished = Channel(Nil).new(groups.size)
    groups.each do |g|
      spawn do
        g.poll(200.milliseconds) rescue nil
        finished.send(nil)
      end
    end
    groups.size.times { finished.receive }
    return true if done.call
    return false if Time.monotonic >= deadline
  end
end

def group_with(address = ADDRESS, group = unique("cr-grp"), &) : BP::GroupConsumer
  config = BP::GroupConfig.new
  config.auto_commit_interval_ms = 0
  yield config
  must { BP::GroupConsumer.new(address, group, config) }
end

def run_coverage : Nil
  section "producer settings"
  begin
    c = consumer
    topic = unique("cr-batchsize")
    p = producer(60_000) { |cfg| cfg.batch_size = 64 }
    3.times { |i| must { p.send_to(topic, 0, "b" * 100 + i.to_s) } }
    got = must { c.fetch(topic, 0, 0_i64, 1000) }
    check "batch.size sends a full batch without waiting for linger", got.size == 3, "got #{got.size}"
    p.close rescue nil

    topic = unique("cr-linger")
    p = producer(50) { |cfg| cfg.batch_size = 1_048_576 }
    must { p.send_to(topic, 0, "lingering") }
    sleep 500.milliseconds
    got = must { c.fetch(topic, 0, 0_i64, 1000) }
    check "linger.ms flushes a partial batch on its own", got.size == 1, "got #{got.size}"
    p.close rescue nil

    topic = unique("cr-sync")
    p = producer
    stamp = 1_600_000_000_000_i64
    first = must { p.send_sync(topic, "one", partition: 2, timestamp: stamp) }
    second = must { p.send_sync(topic, "two", partition: 2, timestamp: stamp + 1000) }
    check "send_sync returns consecutive offsets", first == 0 && second == 1, "#{first}, #{second}"
    got = must { c.fetch(topic, 2, 0_i64, 1000) }
    check "an explicit partition is honoured", got.size == 2, "partition 2 holds #{got.size}"
    check "an explicit timestamp is stored exactly", got.map(&.timestamp) == [stamp, stamp + 1000], got.map(&.timestamp).inspect
    rr_topic = unique("cr-roundrobin")
    parts = must { p.router.partitions(rr_topic) }
    (2 * parts.size).times { |i| must { p.send(rr_topic, "rr#{i}") } }
    must { p.flush }
    counts = parts.map { |part| must { c.fetch(rr_topic, part, 0_i64, 300) }.size }
    check "keyless records are spread round-robin", counts.all?(2), counts.inspect
    must { p.close }

    # A codec the driver does not carry, registered by the application: a
    # valid LZ4 block of literals only, which the broker accepts as-is.
    BP.register_codec(BP::Compression::Lz4, ->(b : Bytes) { lz4_compress(b) }, ->(b : Bytes) { lz4_decompress(b) })
    topic = unique("cr-lz4")
    body = ->(i : Int32) { "registered codec payload " * 20 + i.to_s }
    p = producer { |cfg| cfg.compression_type = "lz4" }
    5.times { |i| must { p.send_to(topic, 0, body.call(i)) } }
    must { p.close }
    got = must { c.fetch(topic, 0, 0_i64, 1000) }
    check "a registered codec round-trips through the broker",
      got.map(&.value_string) == (0...5).map { |i| body.call(i) }, "got #{got.size}"
    c.close
  end

  section "retries against a broker that refuses"
  begin
    fake = FakeBroker.new
    config = BP::ProducerConfig.new
    config.linger_ms = 0
    config.acks = -1
    config.request_timeout_ms = 1234
    config.retries = 2
    config.retry_backoff_ms = 150
    p = must { BP::Producer.new(fake.address, config) }
    started = Time.monotonic
    failed = fails { p.send_sync("retriable", "x", partition: 0) }
    took = elapsed_ms(started)
    attempts = fake.produces.dup
    check "request.timeout.ms and acks reach the broker",
      !attempts.empty? && attempts.all? { |a| a[1] == -1 && a[2] == 1234 }, attempts.inspect
    check "a retriable error is retried `retries` times", failed && attempts.size == 3, "#{attempts.size} attempts"
    check "retry.backoff.ms spaces the retries", took >= 300, "#{took} ms"
    fake.produces.clear
    fatal = fails { p.send_sync("fatal", "x", partition: 0) }
    check "a non-retriable error is not retried", fatal && fake.produces.size == 1, "#{fake.produces.size} attempts"
    p.close rescue nil
    fake.produces.clear
    config = BP::ProducerConfig.new
    config.linger_ms = 0
    config.retries = 1_000_000
    config.retry_backoff_ms = 50
    config.delivery_timeout_ms = 400
    p = must { BP::Producer.new(fake.address, config) }
    started = Time.monotonic
    capped = fails { p.send_sync("retriable", "x", partition: 0) }
    took = elapsed_ms(started)
    check "delivery.timeout.ms caps the retries", capped && took < 3000, "#{took} ms, #{fake.produces.size} attempts"
    p.close rescue nil
    fake.close
  end

  section "consumer settings"
  begin
    topic = unique("cr-fetchcfg")
    p = producer
    20.times { |i| must { p.send_to(topic, 0, "f" * 1000 + i.to_s) } }
    must { p.close }
    c = consumer
    records, hw = must { c.fetch_verbose(topic, 0, 0_i64, 500) }
    check "fetch reports the high watermark", hw == 20, hw.to_s
    check "a default fetch returns every record", records.size == 20, "got #{records.size}"
    meta = must { c.router.refresh(topic) }
    brokers = meta.brokers.map(&.node_id)
    infos = meta.topics.find { |t| t.name == topic }.try(&.partitions) || [] of BP::PartitionInfo
    check "metadata names a live leader for every partition",
      !infos.empty? && infos.all? { |i| brokers.includes?(i.leader) }, infos.inspect
    c.close
    small = must { BP::Consumer.new(ADDRESS, BP::ConsumerConfig.new { |cfg| cfg.fetch_max_bytes = 2500 }) }
    got = must { small.fetch(topic, 0, 0_i64, 500) }
    check "fetch.max.bytes caps a response", !got.empty? && got.size < 20, "got #{got.size}"
    small.close
    patient = must do
      BP::Consumer.new(ADDRESS, BP::ConsumerConfig.new { |cfg| cfg.fetch_min_bytes = 10_000_000; cfg.fetch_max_wait_ms = 400 })
    end
    started = Time.monotonic
    got = must { patient.fetch(topic, 0, 19_i64, 400) }
    waited = elapsed_ms(started)
    check "fetch.min.bytes holds a fetch for up to fetch.max.wait.ms",
      got.size == 1 && waited >= 300 && waited < 5000, "#{waited} ms, #{got.size} records"
    patient.close

    topic = unique("cr-bytime")
    p = producer
    base = 1_700_000_000_000_i64
    3.times { |i| must { p.send_to(topic, 0, "t#{i}", timestamp: base + i * 10_000) } }
    must { p.close }
    c = consumer
    at = must { c.list_offsets(topic, 0, base + 5000) }
    check "list offsets by timestamp finds the first record at or after it", at == 1, at.to_s
    c.close

    topic = unique("cr-maxpoll")
    p = producer
    10.times { |i| must { p.send_to(topic, 0, "m#{i}") } }
    must { p.close }
    gc = group_with(group: unique("cr-maxpoll-grp")) { |cfg| cfg.max_poll_records = 3 }
    gc.subscribe([topic])
    sizes = poll_sizes(gc, 10, 15.seconds)
    check "max.poll.records caps a poll", sizes.sum == 10 && sizes.all? { |n| n <= 3 }, sizes.inspect
    gc.close rescue nil
  end

  section "consumer group settings"
  begin
    p = producer
    t1 = unique("cr-multi-a")
    t2 = unique("cr-multi-b")
    5.times do |i|
      must { p.send(t1, "a#{i}") }
      must { p.send(t2, "b#{i}") }
    end
    gc = group_with { }
    gc.subscribe([t1, t2])
    got = poll_until(gc, 10, 15.seconds)
    per_topic = got.map(&.topic).tally
    check "a group consumes every subscribed topic", per_topic == {t1 => 5, t2 => 5}, per_topic.inspect
    gc.close rescue nil

    topic = unique("cr-autocommit")
    6.times { |i| must { p.send_to(topic, 0, "c#{i}") } }
    slot = BP::TopicPartition.new(topic, 0)
    committed_after = ->(interval : Int32) do
      g = group_with { |cfg| cfg.auto_commit_interval_ms = interval }
      g.subscribe([topic])
      poll_until(g, 6, 15.seconds)
      sleep 200.milliseconds
      g.poll(300.milliseconds) rescue nil
      committed = must { g.committed([slot]) }
      g.close rescue nil
      committed[slot]?
    end
    auto = committed_after.call(100)
    check "auto-commit records positions without an explicit commit", auto == 6, auto.inspect
    manual = committed_after.call(0)
    check "disabled auto-commit commits nothing", manual.nil? || manual < 0, manual.inspect

    # Static membership: a second instance presenting the same
    # group.instance.id takes over the first one's partitions at once,
    # without a rebalance, while the first is still heartbeating.
    topic = unique("cr-static")
    must { p.router.partitions(topic) }
    group = unique("cr-static-grp")
    first = group_with(group: group) { |cfg| cfg.group_instance_id = "instance-1" }
    first.subscribe([topic])
    first.poll(2.seconds) rescue nil
    first_assignment = first.assignment
    second = group_with(group: group) { |cfg| cfg.group_instance_id = "instance-1" }
    second.subscribe([topic])
    started = Time.monotonic
    second.poll(200.milliseconds) rescue nil
    took = elapsed_ms(started)
    second_assignment = second.assignment
    check "a static member reclaims its partitions without a rebalance",
      first_assignment.size == 4 && second_assignment.sort == first_assignment.sort && took < 2000,
      "first=#{first_assignment} second=#{second_assignment} #{took} ms"
    second.close rescue nil
    first.close rescue nil

    # LeaveGroup on close: with a 30 s session and a 200 ms heartbeat, the
    # survivor takes over within a heartbeat, not a session.
    topic = unique("cr-leave")
    must { p.router.partitions(topic) }
    group = unique("cr-leave-grp")
    a = group_with(group: group) { |cfg| cfg.session_timeout_ms = 30_000; cfg.heartbeat_interval_ms = 200 }
    b = group_with(group: group) { |cfg| cfg.session_timeout_ms = 30_000; cfg.heartbeat_interval_ms = 200 }
    a.subscribe([topic])
    b.subscribe([topic])
    split = settle([a, b], 20.seconds) { a.assignment.size == 2 && b.assignment.size == 2 }
    must { a.close }
    started = Time.monotonic
    took_over = settle([b], 15.seconds) { b.assignment.size == 4 }
    took = elapsed_ms(started)
    check "closing a member hands its partitions over within a heartbeat",
      split && took_over && took < 5000, "split=#{split} took_over=#{took_over} #{took} ms"
    b.close rescue nil

    # session.timeout.ms: a member that goes silent without leaving (its only
    # route to the broker is a proxy that is shut) is evicted once its session
    # lapses, and the survivor takes over.
    topic = unique("cr-session")
    must { p.router.partitions(topic) }
    group = unique("cr-session-grp")
    proxy = Proxy.new(ADDRESS)
    a = group_with(proxy.address, group) { |cfg| cfg.session_timeout_ms = 2000; cfg.heartbeat_interval_ms = 200 }
    b = group_with(group: group) { |cfg| cfg.session_timeout_ms = 2000; cfg.heartbeat_interval_ms = 200 }
    a.subscribe([topic])
    b.subscribe([topic])
    split = settle([a, b], 20.seconds) { a.assignment.size == 2 && b.assignment.size == 2 }
    proxy.close
    started = Time.monotonic
    took_over = settle([b], 20.seconds) { b.assignment.size == 4 }
    took = elapsed_ms(started)
    check "a silent member is evicted after session.timeout.ms",
      split && took_over && took >= 1000 && took < 12_000, "split=#{split} took_over=#{took_over} #{took} ms"
    b.close rescue nil
    a.close rescue nil

    # Generation fencing: a member whose generation moved on cannot commit.
    topic = unique("cr-fence")
    4.times { |i| must { p.send(topic, "f#{i}") } }
    group = unique("cr-fence-grp")
    a = group_with(group: group) { }
    a.subscribe([topic])
    poll_until(a, 4, 10.seconds)
    b = group_with(group: group) { }
    b.subscribe([topic])
    b.poll(500.milliseconds) rescue nil
    check "a commit from a stale generation is refused", fails { a.commit }, "commit succeeded"
    b.close rescue nil
    a.close rescue nil
    must { p.close }
  end

  section "assignors (unit)"
  begin
    members = [BP::Assignors::Member.new("a", ["t"]), BP::Assignors::Member.new("b", ["t"])]
    slots = ->(r : Array(Int32)) { r.map { |i| BP::TopicPartition.new("t", i) } }
    sticky = BP::Assignors.sticky(members, {"t" => (0..11).to_a}, {"a" => slots.call((0..11).to_a), "b" => [] of BP::TopicPartition})
    check "sticky keeps partitions in numeric order",
      sticky["a"] == slots.call((0..5).to_a) && sticky["b"] == slots.call((6..11).to_a), sticky.inspect
    held = {"a" => slots.call([1, 3]), "b" => slots.call([0, 2])}
    kept = BP::Assignors.sticky(members, {"t" => [0, 1, 2, 3]}, held)
    check "sticky keeps what members already hold", kept == held, kept.inspect
  end

  section "decoder bounds"
  begin
    negative = BP::Protocol::Writer.new.int32(-5).to_slice
    check "a negative length is an error", fails { BP::Protocol::Reader.new(negative).string }
    oversized = BP::Protocol::Writer.new.int32(1_000_000).raw("short".to_slice).to_slice
    check "a length past the end of the data is an error", fails { BP::Protocol::Reader.new(oversized).string }
    batch = Bytes[0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 0xff, 0xff, 0xff, 0, 0, 0, 0]
    check "a batch longer than its bytes is an error", fails { BP::RecordBatch.decode(batch, 0) }
  end
end

run_coverage

puts "\n#{Tally.passed} passed, #{Tally.failed} failed"
exit(Tally.failed > 0 ? 1 : 0)
