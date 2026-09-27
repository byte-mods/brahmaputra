# frozen_string_literal: true

# End-to-end suite for the Ruby driver against a live broker.
#
#   brahmaputra-server --data-dir ./data --default-partitions 4
#   ruby -Ilib test/manual_test.rb [HOST] [PORT]
#
# Every check asserts a property of the system, not that a method ran:
# records come back byte-identical, keys pin partitions, headers survive,
# offsets are contiguous. It is a port of clients/go/cmd/manualtest.

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))
require "brahmaputra"
require "socket"

HOST = ARGV[0] || "127.0.0.1"
PORT = (ARGV[1] || "9092").to_i
BOOTSTRAP = "#{HOST}:#{PORT}"

$passed = 0
$failed = 0

def check(name, ok, detail = "")
  if ok
    $passed += 1
    puts "  ok   #{name}"
  else
    $failed += 1
    puts detail.to_s.empty? ? "  FAIL #{name}" : "  FAIL #{name}: #{detail}"
  end
end

def section(title) = puts("\n#{title}")

def unique(prefix) = "#{prefix}-#{Process.clock_gettime(Process::CLOCK_REALTIME, :nanosecond) % 1_000_000_000}"

def must
  yield
rescue StandardError => e
  puts "  FATAL #{e.class}: #{e.message}"
  puts e.backtrace.first(5).map { |line| "        #{line}" }
  exit 2
end

def now_ms = Process.clock_gettime(Process::CLOCK_REALTIME, :millisecond)
def monotonic = Process.clock_gettime(Process::CLOCK_MONOTONIC)

def producer(**config) = must { Brahmaputra::Producer.new("bootstrap.servers" => BOOTSTRAP, "linger.ms" => 0, **config.transform_keys(&:to_s)) }
def consumer = must { Brahmaputra::Consumer.new("bootstrap.servers" => BOOTSTRAP) }

def group(group_id, **config)
  must do
    Brahmaputra::GroupConsumer.new("bootstrap.servers" => BOOTSTRAP, "group.id" => group_id,
                                   **config.transform_keys(&:to_s))
  end
end

section("connection and metadata")
begin
  c = consumer
  versions, broker_version = begin
    c.router.seed.api_versions
  rescue StandardError => e
    [[], e.message]
  end
  check("ApiVersions answers", !versions.empty?, broker_version)
  check("broker reports a version", !broker_version.to_s.empty?, broker_version)
  metadata = must { c.router.metadata(nil, refresh: true) }
  check("metadata lists brokers", metadata.brokers.size >= 1, "#{metadata.brokers.size} brokers")
  c.close
end

section("produce and consume round trip")
topic = unique("rb-roundtrip")
payloads = Array.new(50) { |i| "record-#{i}" }
begin
  p = producer
  payloads.each { |payload| must { p.send(topic, payload, partition: 0) } }
  must { p.flush }
  must { p.close }

  c = consumer
  got = must { c.fetch(topic, 0, 0, 500) }
  check("every record comes back", got.size == payloads.size, "got #{got.size}")
  identical = got.size == payloads.size &&
              got.each_with_index.all? { |record, i| record.value == payloads[i].b && record.offset == i }
  check("values byte-identical and offsets contiguous", identical)
  c.close
end

section("compression codecs")
# Only none and gzip ship in the driver; lz4/zstd/snappy are opt-in via
# Brahmaputra.register_codec.
%w[none gzip].each do |codec|
  codec_topic = unique("rb-#{codec}")
  body = "the same line over and over. " * 40
  p = producer("compression.type" => codec)
  20.times { |i| must { p.send(codec_topic, body + ("0".ord + i % 10).chr, partition: 0) } }
  must { p.flush }
  must { p.close }

  c = consumer
  got = must { c.fetch(codec_topic, 0, 0, 500) }
  check("#{codec}: round trips", got.size == 20 && got[0].value.start_with?(body), "got #{got.size} records")
  c.close
end

section("keys, partitioning and ordering")
begin
  key_topic = unique("rb-keys")
  p = producer
  partitions = must { p.partitions_for(key_topic) }
  30.times { |i| must { p.send(key_topic, "v#{i}", key: "user-7") } }
  must { p.flush }
  must { p.close }

  target = Brahmaputra.partition_for_key("user-7", partitions)
  c = consumer
  on_target = must { c.fetch(key_topic, target, 0, 500) }
  check("a key pins every record to one partition", on_target.size == 30,
        "partition #{target} holds #{on_target.size} of 30")
  ordered = on_target.size == 30 && on_target.each_with_index.all? { |record, i| record.value == "v#{i}" }
  check("per-key order is preserved", ordered)
  strays = (partitions - [target]).sum { |partition| must { c.fetch(key_topic, partition, 0, 200) }.size }
  check("no keyed record landed elsewhere", strays.zero?, "#{strays} strays")
  c.close
end

section("murmur2 agrees with the broker's partitioner")
check("murmur2(\"\") is stable", Brahmaputra.murmur2("") == 275_646_681, Brahmaputra.murmur2("").to_s)
check("murmur2 is deterministic", Brahmaputra.murmur2("user-7") == Brahmaputra.murmur2("user-7"))
check("different keys hash differently", Brahmaputra.murmur2("user-7") != Brahmaputra.murmur2("user-8"))

section("record headers and timestamps")
begin
  header_topic = unique("rb-headers")
  before = now_ms - 1000
  p = producer
  must do
    p.send(header_topic, "annotated", partition: 0, headers: [
      Brahmaputra::RecordHeader.new("trace-id", "abc-123"),
      Brahmaputra::RecordHeader.new("content-type", "application/json"),
      Brahmaputra::RecordHeader.new("tombstone-reason", nil)
    ])
  end
  must { p.send(header_topic, "plain", partition: 0) }
  must { p.flush }
  must { p.close }
  after = now_ms + 1000

  c = consumer
  got = must { c.fetch(header_topic, 0, 0, 500) }
  check("both records arrive", got.size == 2, "got #{got.size}")
  if got.size == 2
    annotated, plain = got
    check("headers survive the round trip", annotated.headers.size == 3, "#{annotated.headers.size} headers")
    check("header values are exact", annotated.header("trace-id") == "abc-123".b)
    check("a null header value stays null", annotated.headers.size == 3 && annotated.headers[2].value.nil?)
    check("a record with no headers gains none from its batch", plain.headers.empty?,
          "#{plain.headers.size} headers")
    in_window = got.all? { |record| record.timestamp.between?(before, after) }
    check("timestamps are real wall-clock values", in_window,
          "#{got[0].timestamp},#{got[1].timestamp} outside #{before}..#{after}")
  end
  c.close
end

section("tombstones")
begin
  tomb_topic = unique("rb-tombstones")
  p = producer
  must { p.send(tomb_topic, "set", key: "k1", partition: 0) }
  must { p.send(tomb_topic, "", key: "k2", partition: 0) }
  # A nil value is a deletion, and must stay distinguishable from the empty
  # value above all the way through the round trip.
  must { p.send(tomb_topic, nil, key: "k3", partition: 0) }
  must { p.flush }
  must { p.close }

  c = consumer
  got = must { c.fetch(tomb_topic, 0, 0, 500) }
  check("all three records arrive", got.size == 3, "got #{got.size}")
  if got.size == 3
    check("an ordinary value round-trips", got[0].value == "set".b)
    check("an empty value is empty, not null", !got[1].value.nil? && got[1].value.empty?, got[1].value.inspect)
    check("a tombstone arrives as a null value", got[2].value.nil?, got[2].value.inspect)
  end
  c.close
end

section("offsets")
begin
  c = consumer
  earliest = must { c.list_offsets(topic, 0, Brahmaputra::EARLIEST) }
  latest = must { c.list_offsets(topic, 0, Brahmaputra::LATEST) }
  check("earliest is 0 on a fresh topic", earliest.zero?, earliest.to_s)
  check("latest equals the record count", latest == 50, latest.to_s)
  c.close
end

section("acks")
[0, 1, -1].each do |acks|
  acks_topic = unique("rb-acks#{acks}")
  p = producer("acks" => acks)
  must { p.send(acks_topic, "durable", partition: 0) }
  must { p.flush }
  must { p.close }
  sleep 0.4

  c = consumer
  got = must { c.fetch(acks_topic, 0, 0, 500) }
  check("acks=#{acks} stores the record", got.size == 1, "got #{got.size}")
  c.close
end

section("consumer group: assignment, commit, resume")
begin
  group_topic = unique("rb-group")
  group_id = unique("rb-billing")
  p = producer
  40.times { |i| must { p.send(group_topic, "g#{i}") } }
  must { p.flush }
  must { p.close }

  g = group(group_id, "auto.commit.interval.ms" => 0)
  g.subscribe([group_topic])
  seen = []
  deadline = monotonic + 30
  seen.concat(must { g.poll(500) }) while seen.size < 40 && monotonic < deadline
  check("the group consumes every record", seen.size == 40, "got #{seen.size}")
  distinct = seen.map { |record| [record.partition, record.offset] }.uniq
  check("no record is delivered twice", distinct.size == seen.size)

  must { g.commit }
  committed = must { g.committed }
  total = committed.values.sum
  check("commit records a position", total == 40, total.to_s)
  must { g.close }

  # A second member of the same group must resume, not replay.
  rejoined = group(group_id, "auto.commit.interval.ms" => 0)
  rejoined.subscribe([group_topic])
  replayed = []
  until_time = monotonic + 5
  while monotonic < until_time
    begin
      replayed.concat(rejoined.poll(300))
    rescue Brahmaputra::Error
      nil
    end
  end
  check("a rejoining group resumes from its commit", replayed.empty?,
        "replayed #{replayed.size} records it had already committed")
  must { rejoined.close }
end

section("auto.offset.reset")
begin
  reset_topic = unique("rb-reset")
  p = producer
  10.times { |i| must { p.send(reset_topic, "r#{i}") } }
  must { p.flush }
  must { p.close }

  g = group(unique("rb-latest"), "auto.commit.interval.ms" => 0, "auto.offset.reset" => "latest")
  g.subscribe([reset_topic])
  skipped = []
  until_time = monotonic + 4
  while monotonic < until_time
    begin
      skipped.concat(g.poll(300))
    rescue Brahmaputra::Error
      nil
    end
  end
  check("latest skips records produced before the group existed", skipped.empty?, "saw #{skipped.size}")
  must { g.close }

  strict = group(unique("rb-none"), "auto.commit.interval.ms" => 0, "auto.offset.reset" => "none")
  strict.subscribe([reset_topic])
  raised = false
  until_time = monotonic + 5
  while monotonic < until_time && !raised
    begin
      strict.poll(300)
    rescue Brahmaputra::NoOffsetForPartitionError
      raised = true
    rescue Brahmaputra::Error => e
      raised = e.message.include?("no committed offset")
    end
  end
  check("none refuses to guess a position", raised)
  must { strict.close }
end

section("assignors")
Brahmaputra::Assignors::ALL.each do |assignor|
  assignor_topic = unique("rb-#{assignor}")
  p = producer
  20.times { |i| must { p.send(assignor_topic, "a#{i}") } }
  must { p.flush }
  must { p.close }

  g = group(unique("rb-grp-#{assignor}"), "auto.commit.interval.ms" => 0,
                                          "partition.assignment.strategy" => assignor)
  g.subscribe([assignor_topic])
  collected = []
  deadline = monotonic + 20
  while collected.size < 20 && monotonic < deadline
    begin
      collected.concat(g.poll(500))
    rescue Brahmaputra::Error
      nil
    end
  end
  check("#{assignor}: consumes every record", collected.size == 20, "got #{collected.size}")
  must { g.close }
end

section("bounded client buffer")
begin
  buffer_topic = unique("rb-buffer")
  # linger.ms is long enough that nothing flushes on time during this check.
  p = producer("linger.ms" => 10_000, "buffer.memory" => 2048, "max.block.ms" => 300)
  blocked = false
  500.times do
    p.send(buffer_topic, "x" * 256, partition: 0)
  rescue Brahmaputra::BufferFullError => e
    blocked = e.message.include?("buffer full")
    break
  end
  check("a full buffer blocks and then reports", blocked)
  begin
    p.close
  rescue StandardError
    nil
  end
end

section("wire edge cases")
begin
  edge_topic = unique("rb-edge")
  p = producer
  large = Array.new(1 << 20) { |i| (i * 7) & 0xFF }.pack("C*")
  unicode_key = "ключ-✓-🔑"
  unicode_value = "значение — 数据 — 🚀"
  must { p.send(edge_topic, large, partition: 0).value }
  must do
    p.send(edge_topic, unicode_value, key: unicode_key, partition: 0,
                                      headers: [Brahmaputra::RecordHeader.new("ünïcødé-🏷", "✓")]).value
  end
  # An empty key and an empty header value are values, not nulls.
  must do
    p.send(edge_topic, "empty-key", key: "", partition: 0,
                                    headers: [Brahmaputra::RecordHeader.new("empty", ""),
                                              Brahmaputra::RecordHeader.new("null", nil)]).value
  end
  must { p.send(edge_topic, "null-key", partition: 0).value }
  must { p.close }

  c = consumer
  got = []
  offset = 0
  while got.size < 4
    batch = begin
      c.fetch(edge_topic, 0, offset, 500)
    rescue Brahmaputra::Error
      []
    end
    break if batch.empty?

    got.concat(batch)
    offset = batch.last.offset + 1
  end
  check("edge records all arrive", got.size == 4, "got #{got.size}")
  if got.size == 4
    check("a 1 MiB value round-trips byte-identical", got[0].value == large, "#{got[0].value&.bytesize} bytes")
    check("unicode key, value and header key round-trip",
          got[1].key == unicode_key.b && got[1].value == unicode_value.b &&
            got[1].headers.size == 1 && got[1].headers[0].key == "ünïcødé-🏷")
    check("an empty key stays empty, not null", !got[2].key.nil? && got[2].key.empty?, got[2].key.inspect)
    check("an empty header value stays empty, not null",
          got[2].headers.size == 2 && !got[2].headers[0].value.nil? &&
            got[2].headers[0].value.empty? && got[2].headers[1].value.nil?,
          got[2].headers.inspect)
    check("a null key stays null", got[3].key.nil?, got[3].key.inspect)
  end
  c.close
end

section("ordering under linger flushes")
begin
  order_topic = unique("rb-order")
  p = producer("linger.ms" => 1, "batch.size" => 256)
  total = 5000
  total.times { |i| must { p.send(order_topic, i.to_s, partition: 0) } }
  must { p.close }
  c = consumer
  values = []
  offset = 0
  while values.size < total
    batch = begin
      c.fetch(order_topic, 0, offset, 500)
    rescue Brahmaputra::Error
      []
    end
    break if batch.empty?

    values.concat(batch.map { |record| record.value.to_i })
    offset = batch.last.offset + 1
  end
  inversions = values.each_cons(2).count { |a, b| b < a }
  check("every record of a partition arrives", values.size == total, "got #{values.size}")
  check("a partition's records keep send order", inversions.zero?, "#{inversions} inversions")
  c.close
end

section("background flush failures are reported")
begin
  p = producer("linger.ms" => 20)
  # Partition 999 does not exist, so the sender thread's linger flush fails.
  send_error = nil
  begin
    p.send(unique("rb-bgfail"), "lost", partition: 999)
  rescue StandardError => e
    send_error = e
  end
  sleep 0.3
  flush_error = nil
  begin
    p.flush
  rescue StandardError => e
    flush_error = e
  end
  check("a failed linger flush surfaces on the next Flush", send_error.nil? && !flush_error.nil?,
        "send=#{send_error.inspect} flush=#{flush_error.inspect}")
  closer = Thread.new do
    p.close
  rescue StandardError
    nil
  end
  check("Close returns after a failed flush", !closer.join(5).nil?, "hung")
end

# Forwards TCP to the broker and can sever every live connection, which is
# how a broker restart or an idle timeout looks to a client.
class Proxy
  attr_reader :address

  def initialize(host, port)
    @server = TCPServer.new("127.0.0.1", 0)
    @address = "127.0.0.1:#{@server.addr[1]}"
    @lock = Mutex.new
    @live = []
    @thread = Thread.new do
      loop do
        client = @server.accept
        upstream = begin
          TCPSocket.new(host, port)
        rescue SystemCallError
          client.close
          next
        end
        @lock.synchronize { @live.push(client, upstream) }
        pipe(client, upstream)
        pipe(upstream, client)
      end
    rescue IOError, SystemCallError
      nil
    end
  end

  def drop_all
    @lock.synchronize do
      @live.each { |socket| socket.close rescue nil } # rubocop:disable Style/RescueModifier
      @live.clear
    end
    sleep 0.05
  end

  def close
    @server.close
    drop_all
  end

  private

  def pipe(from, to)
    Thread.new do
      IO.copy_stream(from, to)
    rescue IOError, SystemCallError
      nil
    ensure
      to.close rescue nil # rubocop:disable Style/RescueModifier
    end
  end
end

section("connection failures")
begin
  # A broker that accepts and never answers must cost an error, not a
  # thread blocked forever.
  silent = TCPServer.new("127.0.0.1", 0)
  held = []
  acceptor = Thread.new do
    loop { held << silent.accept }
  rescue IOError, SystemCallError
    nil
  end
  conn = must { Brahmaputra::Connection.new("127.0.0.1", silent.addr[1], client_id: "rb-test", connect_timeout_ms: 1000) }
  conn.request_timeout_ms = 300
  started = monotonic
  request_error = nil
  begin
    conn.api_versions
  rescue StandardError => e
    request_error = e
  end
  check("a request to an unresponsive broker times out", !request_error.nil? && monotonic - started < 3,
        request_error.inspect)
  check("a timed-out connection is not reused", conn.broken?)
  conn.close
  silent.close
  acceptor.join(1)
  held.each(&:close)

  # A connection the broker drops is redialled, not kept forever.
  proxy = Proxy.new(HOST, PORT)
  drop_topic = unique("rb-drop")
  p = must { Brahmaputra::Producer.new("bootstrap.servers" => proxy.address, "linger.ms" => 0) }
  must { p.send(drop_topic, "before", partition: 0).value }
  proxy.drop_all
  recovered = RuntimeError.new("not attempted")
  3.times do
    break if recovered.nil?

    begin
      p.send(drop_topic, "after", partition: 0).value
      recovered = nil
    rescue StandardError => e
      recovered = e
    end
  end
  check("a producer recovers after its connection drops", recovered.nil?, recovered.inspect)
  begin
    p.close
  rescue StandardError
    nil
  end
  c = must { Brahmaputra::Consumer.new("bootstrap.servers" => proxy.address) }
  must { c.fetch(drop_topic, 0, 0, 100) }
  proxy.drop_all
  fetch_error = RuntimeError.new("not attempted")
  fetched = []
  3.times do
    break if fetch_error.nil?

    begin
      fetched = c.fetch(drop_topic, 0, 0, 100)
      fetch_error = nil
    rescue StandardError => e
      fetch_error = e
    end
  end
  check("a consumer recovers after its connection drops", fetch_error.nil? && fetched.size >= 1,
        fetch_error.inspect)
  c.close
  proxy.close
end

section("consumer group: max.poll.interval and rejoin")
begin
  slow_topic = unique("rb-slow")
  p = producer
  10.times { |i| must { p.send(slow_topic, "s#{i}").value } }
  g = group(unique("rb-slow-grp"), "auto.commit.interval.ms" => 0, "max.poll.interval.ms" => 1500)
  g.subscribe([slow_topic])
  first = []
  deadline = monotonic + 15
  while first.size < 10 && monotonic < deadline
    begin
      first.concat(g.poll(300))
    rescue Brahmaputra::Error
      break
    end
  end
  must { g.commit }
  # Stall past max.poll.interval.ms: the member leaves the group.
  sleep 2.5
  (10...20).each { |i| must { p.send(slow_topic, "s#{i}").value } }
  must { p.close }
  second = []
  poll_error = nil
  deadline = monotonic + 15
  while second.size < 10 && monotonic < deadline
    begin
      second.concat(g.poll(300))
    rescue Brahmaputra::Error => e
      poll_error = e
      break
    end
  end
  check("a member that stalled rejoins on its next poll",
        first.size == 10 && second.size == 10 && poll_error.nil?,
        "first=#{first.size} second=#{second.size} err=#{poll_error.inspect}")
  must { g.close }
end

section("consumer group: time inside poll does not count against max.poll.interval")
begin
  join_topic = unique("rb-inpoll")
  p = producer
  must { p.partitions_for(join_topic) }
  # Far shorter than the first poll below, which spends ~1s joining (the
  # broker's initial rebalance delay) and then waits for data.
  g = group(unique("rb-inpoll-grp"), "auto.commit.interval.ms" => 0, "max.poll.interval.ms" => 600)
  g.subscribe([join_topic])
  feeder = Thread.new do
    sleep 2
    10.times do |i|
      p.send(join_topic, "j#{i}")
    rescue StandardError
      nil
    end
  end
  # One long poll: it joins, then waits for the records above.
  got = []
  poll_error = nil
  begin
    got = g.poll(4000)
  rescue StandardError => e
    poll_error = e
  end
  # Committed straight away, before another poll could quietly rejoin: this
  # fails if the member left the group mid-poll.
  commit_error = nil
  begin
    g.commit
  rescue StandardError => e
    commit_error = e
  end
  check("a member is still in its group after a long poll",
        poll_error.nil? && !got.empty? && commit_error.nil?,
        "got=#{got.size} poll=#{poll_error.inspect} commit=#{commit_error.inspect}")
  feeder.join
  must { g.close }
  must { p.close }
end

puts "\n#{$passed} passed, #{$failed} failed"
exit(1) if $failed.positive?
