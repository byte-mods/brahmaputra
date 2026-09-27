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

puts "\n#{Tally.passed} passed, #{Tally.failed} failed"
exit(Tally.failed > 0 ? 1 : 0)
