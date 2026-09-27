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

puts "\n#{$passed} passed, #{$failed} failed"
exit(1) if $failed.positive?
