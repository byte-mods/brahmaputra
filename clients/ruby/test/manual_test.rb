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

# ---------------------------------------------------------------------------
# Checks beyond the Go suite's 54: one per feature of the client contract
# that those do not already exercise.
# ---------------------------------------------------------------------------

def elapsed_ms(since) = ((monotonic - since) * 1000).round

# Polls until want records arrive or limit_ms passes.
def poll_until(consumer, want, limit_ms, largest: nil)
  seen = []
  deadline = monotonic + limit_ms / 1000.0
  while seen.size < want && monotonic < deadline
    batch = begin
      consumer.poll(300)
    rescue StandardError
      []
    end
    largest[0] = [largest[0], batch.size].max if largest
    seen.concat(batch)
  end
  seen
end

def committed_total(consumer) = consumer.committed.values.sum

# A proxy that understands frames. It forwards every request to the broker
# except Produce, which it can answer itself with an error code for the next
# `failures` requests -- how a leader move or an under-replicated partition
# looks to a producer -- and it records what each Produce asked for.
class FaultProxy
  attr_reader :address, :produces, :last_acks, :last_timeout_ms

  def initialize(host, port)
    @server = TCPServer.new("127.0.0.1", 0)
    @address = "127.0.0.1:#{@server.addr[1]}"
    @lock = Mutex.new
    @live = []
    @failures = 0
    @code = 0
    @produces = 0
    @last_acks = nil
    @last_timeout_ms = nil
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
        Thread.new { serve(client, upstream) }
      end
    rescue IOError, SystemCallError
      nil
    end
  end

  # Answer the next count Produce requests with code.
  def fail_produces(count, code)
    @lock.synchronize do
      @failures = count
      @code = code
      @produces = 0
    end
  end

  def close
    @server.close
    @lock.synchronize do
      @live.each { |socket| socket.close rescue nil } # rubocop:disable Style/RescueModifier
      @live.clear
    end
  end

  private

  def read_frame(socket)
    header = socket.read(4)
    return nil if header.nil? || header.bytesize < 4

    payload = socket.read(header.unpack1("N"))
    payload && header + payload
  end

  def serve(client, upstream)
    while (frame = read_frame(client))
      api_key = frame.byteslice(4, 2).unpack1("s>")
      expect_reply = true
      if api_key == Brahmaputra::Protocol::ApiKey::PRODUCE
        correlation_id, body = Brahmaputra::Protocol.decode_frame_payload(frame.byteslice(4, frame.bytesize - 4))
        reader = Brahmaputra::Protocol.body_reader(body)
        topic = reader.string
        partition = reader.int32
        acks = reader.int32
        timeout_ms = reader.int32
        expect_reply = !acks.zero?
        code = @lock.synchronize do
          @produces += 1
          @last_acks = acks
          @last_timeout_ms = timeout_ms
          if @failures.positive?
            @failures -= 1
            @code
          else
            0
          end
        end
        unless code.zero?
          reply = Brahmaputra::Protocol.body_writer.string(topic).int32(partition).int32(code)
                                       .int64(-1).int64(-1).bytes
          client.write(Brahmaputra::Protocol.encode_frame(api_key, correlation_id, "", reply))
          next
        end
      end
      upstream.write(frame)
      next unless expect_reply

      reply = read_frame(upstream)
      break if reply.nil?

      client.write(reply)
    end
  rescue IOError, SystemCallError, Brahmaputra::Error
    nil
  ensure
    [client, upstream].each { |socket| socket.close rescue nil } # rubocop:disable Style/RescueModifier
  end
end

section("producer: explicit partition, timestamp and synchronous send")
begin
  t = unique("rb-sync")
  p = producer
  offsets = Array.new(3) { |i| must { p.send_sync(t, "sync-#{i}", partition: 0).offset } }
  check("send_sync returns each record's offset", offsets == [0, 1, 2], offsets.inspect)
  must { p.send(t, "stamped", partition: 2, timestamp: 1_600_000_000_123) }
  must { p.close }
  c = consumer
  on_two = must { c.fetch(t, 2, 0, 500) }
  check("an explicit partition is honoured", on_two.size == 1 && must { c.fetch(t, 0, 0, 500) }.size == 3,
        "partition 2 holds #{on_two.size}")
  check("an explicit timestamp survives the round trip",
        on_two.size == 1 && on_two[0].timestamp == 1_600_000_000_123, on_two.map(&:timestamp).inspect)
  c.close
end

section("producer: round-robin for records without a key")
begin
  t = unique("rb-rr")
  p = producer
  partitions = must { p.partitions_for(t) }
  8.times { |i| must { p.send(t, "rr#{i}") } }
  must { p.close }
  c = consumer
  counts = partitions.map { |partition| must { c.fetch(t, partition, 0, 300) }.size }
  check("unkeyed records are spread evenly over every partition", counts == [2, 2, 2, 2], counts.inspect)
  c.close
end

section("producer: batch.size, linger.ms and close")
begin
  c = consumer
  full = unique("rb-batchfull")
  eager = producer("linger.ms" => 60_000, "batch.size" => 64)
  must { eager.send(full, "b" * 100, partition: 0) }
  sleep 0.3
  check("a batch that reaches batch.size is sent without waiting for linger.ms",
        must { c.fetch(full, 0, 0, 300) }.size == 1)

  lingering = unique("rb-linger")
  lazy = producer("linger.ms" => 100, "batch.size" => 1 << 20)
  must { lazy.send(lingering, "waits", partition: 0) }
  held_back = must { c.fetch(lingering, 0, 0, 0) }.empty?
  sleep 0.8
  check("linger.ms holds a partial batch, then sends it in the background",
        held_back && must { c.fetch(lingering, 0, 0, 300) }.size == 1,
        held_back ? "never sent" : "sent before linger.ms")

  closing = unique("rb-close")
  closer = producer("linger.ms" => 60_000, "batch.size" => 1 << 20)
  5.times { |i| must { closer.send(closing, "c#{i}", partition: 0) } }
  must { closer.close }
  check("close flushes what is still buffered", must { c.fetch(closing, 0, 0, 300) }.size == 5)
  must { eager.close }
  must { lazy.close }
  c.close
end

section("producer: retries, request.timeout.ms and delivery.timeout.ms")
begin
  proxy = FaultProxy.new(HOST, PORT)
  t = unique("rb-retry")
  base = { "bootstrap.servers" => proxy.address, "linger.ms" => 0, "acks" => "all",
           "request.timeout.ms" => 4321 }
  p = must { Brahmaputra::Producer.new(base.merge("retries" => 3, "retry.backoff.ms" => 50)) }
  must { p.partitions_for(t) }
  proxy.fail_produces(2, Brahmaputra::ErrorCode::NOT_LEADER_OR_FOLLOWER)
  error = nil
  begin
    p.send(t, "persistent", partition: 0).value
  rescue StandardError => e
    error = e
  end
  c = consumer
  check("a retriable error is retried until the send succeeds",
        error.nil? && proxy.produces == 3 && must { c.fetch(t, 0, 0, 300) }.size == 1,
        "attempts=#{proxy.produces} #{error.inspect}")
  check("request.timeout.ms and acks travel on the produce request",
        proxy.last_timeout_ms == 4321 && proxy.last_acks == -1, "#{proxy.last_timeout_ms}/#{proxy.last_acks}")
  c.close

  bounded = must { Brahmaputra::Producer.new(base.merge("retries" => 2, "retry.backoff.ms" => 150)) }
  must { bounded.partitions_for(t) }
  proxy.fail_produces(1000, Brahmaputra::ErrorCode::NOT_LEADER_OR_FOLLOWER)
  started = monotonic
  failed_right = false
  begin
    bounded.send(t, "doomed", partition: 0).value
  rescue Brahmaputra::ServerError => e
    failed_right = e.code == Brahmaputra::ErrorCode::NOT_LEADER_OR_FOLLOWER
  end
  took = elapsed_ms(started)
  check("retries are bounded and spaced by retry.backoff.ms",
        failed_right && proxy.produces == 3 && took >= 300, "attempts=#{proxy.produces} took #{took}ms")

  proxy.fail_produces(1000, Brahmaputra::ErrorCode::INVALID_REQUEST)
  begin
    bounded.send(t, "malformed", partition: 0).value
  rescue StandardError
    nil
  end
  check("a non-retriable error is not retried", proxy.produces == 1, "attempts=#{proxy.produces}")
  begin
    bounded.close
  rescue StandardError
    nil
  end

  capped = must do
    Brahmaputra::Producer.new(base.merge("retries" => 1000, "retry.backoff.ms" => 50, "delivery.timeout.ms" => 400))
  end
  must { capped.partitions_for(t) }
  proxy.fail_produces(100_000, Brahmaputra::ErrorCode::NOT_LEADER_OR_FOLLOWER)
  started = monotonic
  gave_up = false
  begin
    capped.send(t, "late", partition: 0).value
  rescue Brahmaputra::Error
    gave_up = true
  end
  took = elapsed_ms(started)
  check("delivery.timeout.ms caps the whole retry loop", gave_up && took < 3000,
        "took #{took}ms, attempts=#{proxy.produces}")
  proxy.fail_produces(0, 0)
  begin
    capped.close
  rescue StandardError
    nil
  end
  must { p.close }
  proxy.close
end

section("compression: registering a codec")
begin
  refused = begin
    Brahmaputra::Producer.new("bootstrap.servers" => BOOTSTRAP, "compression.type" => "snappy").close
    false
  rescue ArgumentError, Brahmaputra::Error
    true
  end
  check("an unregistered codec is refused up front", refused)

  # A toy reversible codec: enough to prove the hook is used on both the
  # produce and the fetch path. The broker stores batches as-is.
  flip = ->(data) { data.b.reverse.bytes.map { |byte| byte ^ 0x5a }.pack("C*") }
  Brahmaputra.register_codec("snappy", compress: flip, decompress: flip)
  t = unique("rb-codec")
  p = producer("compression.type" => "snappy")
  must { p.send(t, "through a registered codec", key: "k", partition: 0, headers: { "h" => "v" }) }
  must { p.close }
  c = consumer
  got = must { c.fetch(t, 0, 0, 300) }
  check("a registered codec compresses on produce and decompresses on fetch",
        got.size == 1 && got[0].value == "through a registered codec".b && got[0].key == "k".b &&
        got[0].headers.size == 1)
  c.close
  record = Brahmaputra::Protocol::BatchRecord.new("k", "v", 0, [])
  encoded = Brahmaputra::Protocol.encode_record_batch([record], now_ms, Brahmaputra::Protocol::Compression::SNAPPY)
  decoded, = Brahmaputra::Protocol.decode_record_batch(encoded, 0)
  check("a batch encoded with it decodes offline", decoded.records.size == 1 && decoded.records[0].value == "v")
end

section("consumer: fetch limits, watermark, offsets by time, metadata")
begin
  t = unique("rb-fetch")
  p = producer
  base = 1_700_000_000_000
  # One batch per record: the broker resolves a timestamp to a batch.
  20.times { |i| must { p.send(t, ("a".ord + i).chr * 1000, partition: 0, timestamp: base + i * 1000).value } }
  must { p.close }

  limited = must { Brahmaputra::Consumer.new("bootstrap.servers" => BOOTSTRAP, "fetch.max.bytes" => 2500) }
  capped = must { limited.fetch(t, 0, 0, 300) }
  check("fetch.max.bytes caps a response", !capped.empty? && capped.size < 20, "#{capped.size} records")
  limited.close

  c = consumer
  result = must { c.fetch_verbose(t, 0, 0, 300) }
  check("the high watermark is reported", result.high_watermark == 20, result.high_watermark.to_s)

  waiter = must do
    Brahmaputra::Consumer.new("bootstrap.servers" => BOOTSTRAP, "fetch.max.wait.ms" => 400, "fetch.min.bytes" => 1)
  end
  started = monotonic
  none = must { waiter.fetch(t, 0, 20, 10_000) }
  took = elapsed_ms(started)
  check("fetch.max.wait.ms bounds a long poll at the end of the log", none.empty? && took >= 250 && took < 3000,
        "#{took}ms")
  waiter.close

  by_time = must { c.list_offsets(t, 0, base + 5000) }
  between = must { c.list_offsets(t, 0, base + 5500) }
  check("list offsets by timestamp finds the first record at or after it", by_time == 5 && between == 6,
        "#{by_time},#{between}")

  metadata = must { c.router.metadata([t], refresh: true) }
  partitions = metadata.partitions_of(t)
  check("metadata lists a topic's partitions and their leaders",
        partitions.size == 4 && partitions.all? { |partition| metadata.leader_of(t, partition) >= 0 },
        "#{partitions.size} partitions")
  c.close

  g = group(unique("rb-maxpoll"), "enable.auto.commit" => false, "max.poll.records" => 3)
  g.subscribe([t])
  largest = [0]
  seen = poll_until(g, 20, 20_000, largest: largest)
  check("max.poll.records caps every poll", seen.size == 20 && largest[0] == 3,
        "#{seen.size} records, largest poll #{largest[0]}")
  must { g.close }
end

section("decoding is bounds-checked")
begin
  negative = begin
    Brahmaputra::Protocol.body_reader(Brahmaputra::Protocol.body_writer.int32(-5).bytes).string
    false
  rescue Brahmaputra::ProtocolError
    true
  end
  check("a negative length is an error, not a read", negative)
  oversized = begin
    Brahmaputra::Protocol.body_reader(Brahmaputra::Protocol.body_writer.int32(1 << 30).bytes).string
    false
  rescue Brahmaputra::ProtocolError
    true
  end
  record = Brahmaputra::Protocol::BatchRecord.new("k", "v", 0, [])
  batch = Brahmaputra::Protocol.encode_record_batch([record], now_ms)
  batch.setbyte(8, 0x7f) # batch_length far past the buffer
  truncated = begin
    Brahmaputra::Protocol.decode_record_batch(batch, 0)
    false
  rescue Brahmaputra::ProtocolError
    true
  end
  check("an oversized length is an error, not a read", oversized && truncated)
end

section("consumer groups: auto commit, several topics, heartbeats")
begin
  t1 = unique("rb-multi-a")
  t2 = unique("rb-multi-b")
  p = producer
  6.times do |i|
    must { p.send(t1, "a#{i}") }
    must { p.send(t2, "b#{i}") }
  end
  must { p.close }

  g = group(unique("rb-multi"), "enable.auto.commit" => true, "auto.commit.interval.ms" => 200)
  g.subscribe([t1, t2])
  seen = poll_until(g, 12, 20_000)
  check("one member subscribed to two topics consumes both",
        seen.size == 12 && seen.map(&:topic).uniq.size == 2, "#{seen.size} records")
  sleep 0.3
  begin
    g.poll(300)
  rescue StandardError
    nil
  end
  total = must { committed_total(g) }
  check("enable.auto.commit commits on poll after auto.commit.interval.ms", total == 12, "committed #{total}")
  must { g.close }

  idle = unique("rb-idle")
  p = producer
  must { p.send(idle, "x") }
  must { p.close }
  quiet = group(unique("rb-heartbeat"), "enable.auto.commit" => false, "session.timeout.ms" => 1500,
                                         "heartbeat.interval.ms" => 300)
  quiet.subscribe([idle])
  poll_until(quiet, 1, 15_000)
  generation = quiet.generation
  sleep 4 # no poll: only heartbeats keep it in
  error = nil
  begin
    quiet.commit
  rescue StandardError => e
    error = e
  end
  check("heartbeats keep an idle member in its group past session.timeout.ms",
        error.nil? && quiet.generation == generation, error.inspect)
  must { quiet.close }
end

section("consumer groups: fencing, rejoin, leave and static membership")
begin
  t = unique("rb-fence")
  p = producer
  8.times { |i| must { p.send(t, "f#{i}") } }

  group_id = unique("rb-fence-grp")
  config = { "enable.auto.commit" => false, "max.poll.interval.ms" => 60_000, "heartbeat.interval.ms" => 200 }
  first = group(group_id, **config)
  first.subscribe([t])
  poll_until(first, 8, 15_000)

  # The coordinator forgets this member behind its back, as it does when a
  # session expires.
  leave = Brahmaputra::Protocol.body_writer.string(group_id).string(first.member_id).bytes
  must { first.consumer.router.seed.request(Brahmaputra::Protocol::ApiKey::LEAVE_GROUP, leave) }
  old_member = first.member_id
  old_generation = first.generation
  sleep 1 # a heartbeat learns UNKNOWN_MEMBER_ID
  4.times { |i| must { p.send(t, "f#{8 + i}") } }
  after = poll_until(first, 4, 15_000)
  commit_error = nil
  begin
    first.commit
  rescue StandardError => e
    commit_error = e
  end
  check("a member the coordinator forgot rejoins on its next poll",
        after.size == 4 && first.generation > old_generation && commit_error.nil?,
        "#{after.size} records, #{old_member}@#{old_generation} -> #{first.member_id}@#{first.generation} " \
        "#{commit_error.inspect}")

  # A second member joins; the first sits out the rebalance and its
  # generation goes stale.
  stale_generation = first.generation
  second = group(group_id, **config)
  second.subscribe([t])
  poll_until(second, 1000, 8000)
  fenced_code = nil
  begin
    first.commit
  rescue Brahmaputra::ServerError => e
    fenced_code = e.code
  end
  check("a commit from a stale generation is fenced",
        [Brahmaputra::ErrorCode::ILLEGAL_GENERATION, Brahmaputra::ErrorCode::UNKNOWN_MEMBER_ID].include?(fenced_code),
        "generation #{stale_generation} -> code #{fenced_code.inspect}")
  must { second.close }
  must { first.close }

  # Close sends LeaveGroup: the next member gets every partition at once
  # instead of waiting out a long session.
  slow = config.merge("session.timeout.ms" => 30_000, "rebalance.timeout.ms" => 30_000)
  leaver = group("#{group_id}-leave", **slow)
  leaver.subscribe([t])
  poll_until(leaver, 12, 15_000)
  must { leaver.close }
  successor = group("#{group_id}-leave", **slow)
  successor.subscribe([t])
  started = monotonic
  2.times { |i| must { p.send(t, "f#{12 + i}") } }
  handed_over = poll_until(successor, 2, 15_000)
  took = elapsed_ms(started)
  check("close leaves the group so partitions move without a session timeout",
        handed_over.size == 2 && successor.assignment.size == 4 && took < 10_000,
        "#{handed_over.size} records after #{took}ms")
  must { successor.close }

  static_group = unique("rb-static")
  static = config.merge("group.instance.id" => "rb-instance-1")
  original = group(static_group, **static)
  original.subscribe([t])
  poll_until(original, 14, 15_000)
  original_member = original.member_id
  original_generation = original.generation
  restarted = group(static_group, **static)
  restarted.subscribe([t])
  poll_until(restarted, 1000, 3000)
  check("a static member reclaims its member id without a rebalance",
        !original_member.empty? && restarted.member_id == original_member &&
        restarted.generation == original_generation,
        "#{original_member}@#{original_generation} vs #{restarted.member_id}@#{restarted.generation}")
  must { restarted.close }
  must { original.close }
  must { p.close }
end

section("assignors: sticky keeps what members hold")
begin
  tp = ->(partition) { Brahmaputra::TopicPartition.new("t", partition) }
  members = [["m1", ["t"]], ["m2", ["t"]]]
  topics = { "t" => (0..11).to_a }
  previous = { "m1" => [tp[2], tp[10], tp[11]], "m2" => [tp[0], tp[1]] }
  sticky = Brahmaputra::Assignors.assign("sticky", members, topics, previous)
  check("sticky leaves every held partition where it was",
        [2, 10, 11].all? { |n| sticky["m1"].include?(tp[n]) } && [0, 1].all? { |n| sticky["m2"].include?(tp[n]) } &&
        sticky["m1"].size == 6 && sticky["m2"].size == 6)
  ids = sticky["m1"].map(&:partition)
  check("sticky orders partitions as numbers, not strings", ids.size > 1 && ids == ids.sort && ids.uniq == ids,
        ids.inspect)
  range = Brahmaputra::Assignors.assign("range", members, topics)
  rr = Brahmaputra::Assignors.assign("roundrobin", members, topics)
  check("range and roundrobin split twelve partitions six and six",
        range["m1"].size == 6 && range["m2"].size == 6 && rr["m1"].size == 6 && rr["m1"][1].partition == 2)
end

puts "\n#{$passed} passed, #{$failed} failed"
exit(1) if $failed.positive?
