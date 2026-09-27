# End-to-end suite for the Elixir driver against a live broker.
#
#     brahmaputra-server --data-dir ./data --default-partitions 4
#     mix run e2e/manual_test.exs [HOST] [PORT]
#
# Every check asserts a property of the system, not that a function ran:
# records come back byte-identical, keys pin partitions, headers survive,
# offsets are contiguous. Exits non-zero on any failure.

defmodule ManualTest do
  alias Brahmaputra.{ConsumedRecord, Consumer, GroupConsumer, Producer, Router}

  def check(name, ok, detail \\ "") do
    if ok do
      Process.put(:passed, Process.get(:passed, 0) + 1)
      IO.puts("  ok   #{name}")
    else
      Process.put(:failed, Process.get(:failed, 0) + 1)
      if detail != "", do: IO.puts("  FAIL #{name}: #{detail}"), else: IO.puts("  FAIL #{name}")
    end
  end

  def section(title), do: IO.puts("\n#{title}")

  def unique(prefix), do: "#{prefix}-#{rem(System.system_time(:nanosecond), 1_000_000_000)}"

  def must({:ok, value}), do: value
  def must({:ok, a, b}), do: {a, b}
  def must(:ok), do: :ok

  def must({:error, error}) do
    IO.puts("  FATAL #{format(error)}")
    System.halt(2)
  end

  def format(error) when is_exception(error), do: Exception.message(error)
  def format(error), do: inspect(error)

  def now_ms, do: System.system_time(:millisecond)
  def mono_ms, do: System.monotonic_time(:millisecond)

  def producer(host, port, opts \\ []),
    do: must(Producer.start_link(host, port, Keyword.merge([linger_ms: 0], opts)))

  def consumer(host, port), do: must(Consumer.connect(host, port))

  # Polls until `count` records arrive or `ms` pass.
  def poll_until(group, count, ms, poll_ms) do
    deadline = mono_ms() + ms
    poll_loop(group, count, deadline, poll_ms, [])
  end

  defp poll_loop(group, count, deadline, poll_ms, acc) do
    if length(acc) >= count or mono_ms() >= deadline do
      acc
    else
      case GroupConsumer.poll(group, poll_ms) do
        {:ok, records} -> poll_loop(group, count, deadline, poll_ms, acc ++ records)
        {:error, e} -> must({:error, e})
      end
    end
  end

  # Polls for the full window, ignoring errors (as the Go suite does).
  def poll_for(group, ms, poll_ms) do
    deadline = mono_ms() + ms
    Stream.repeatedly(fn -> GroupConsumer.poll(group, poll_ms) end)
    |> Enum.reduce_while([], fn result, acc ->
      acc =
        case result do
          {:ok, records} -> acc ++ records
          _ -> acc
        end

      if mono_ms() >= deadline, do: {:halt, acc}, else: {:cont, acc}
    end)
  end

  def run(host, port) do
    section("connection and metadata")
    c = consumer(host, port)
    versions = Consumer.api_versions(c)

    check(
      "ApiVersions answers",
      match?({:ok, [_ | _], _}, versions),
      inspect(versions)
    )

    broker_version =
      case versions do
        {:ok, _, v} -> v
        _ -> ""
      end

    check("broker reports a version", broker_version != "", broker_version)
    metadata = must(Router.metadata(Consumer.router(c), [], true))

    check(
      "metadata lists brokers",
      length(metadata.brokers) >= 1,
      "#{length(metadata.brokers)} brokers"
    )

    Consumer.close(c)

    section("produce and consume round trip")
    topic = unique("ex-roundtrip")
    payloads = for i <- 0..49, do: "record-#{i}"
    p = producer(host, port)
    for payload <- payloads, do: must(Producer.send(p, topic, payload, partition: 0))
    must(Producer.flush(p))
    must(Producer.close(p))

    c = consumer(host, port)
    got = must(Consumer.fetch(c, topic, 0, 0, 500))
    check("every record comes back", length(got) == length(payloads), "got #{length(got)}")

    identical =
      length(got) == length(payloads) and
        got
        |> Enum.zip(payloads)
        |> Enum.with_index()
        |> Enum.all?(fn {{record, payload}, i} -> record.value == payload and record.offset == i end)

    check("values byte-identical and offsets contiguous", identical)
    Consumer.close(c)

    section("compression codecs")
    # Only none and gzip ship in the driver; lz4/zstd/snappy are opt-in via
    # Brahmaputra.register_codec/3.
    for codec <- ["none", "gzip"] do
      codec_topic = unique("ex-#{codec}")
      body = String.duplicate("the same line over and over. ", 40)
      p = producer(host, port, compression_type: codec)

      for i <- 0..19,
          do: must(Producer.send(p, codec_topic, body <> <<?0 + rem(i, 10)>>, partition: 0))

      must(Producer.flush(p))
      must(Producer.close(p))

      c = consumer(host, port)
      got = must(Consumer.fetch(c, codec_topic, 0, 0, 500))

      check(
        "#{codec}: round trips",
        length(got) == 20 and String.starts_with?(hd(got).value, body),
        "got #{length(got)} records"
      )

      Consumer.close(c)
    end

    section("keys, partitioning and ordering")
    key_topic = unique("ex-keys")
    p = producer(host, port)
    partitions = must(Router.partitions(Producer.router(p), key_topic))
    for i <- 0..29, do: must(Producer.send(p, key_topic, "v#{i}", key: "user-7"))
    must(Producer.flush(p))
    must(Producer.close(p))

    target = Brahmaputra.partition_for_key("user-7", partitions)
    c = consumer(host, port)
    on_target = must(Consumer.fetch(c, key_topic, target, 0, 500))

    check(
      "a key pins every record to one partition",
      length(on_target) == 30,
      "partition #{target} holds #{length(on_target)} of 30"
    )

    ordered =
      length(on_target) == 30 and
        on_target |> Enum.with_index() |> Enum.all?(fn {r, i} -> r.value == "v#{i}" end)

    check("per-key order is preserved", ordered)

    strays =
      partitions
      |> Enum.reject(&(&1 == target))
      |> Enum.map(fn part -> length(must(Consumer.fetch(c, key_topic, part, 0, 200))) end)
      |> Enum.sum()

    check("no keyed record landed elsewhere", strays == 0, "#{strays} strays")
    Consumer.close(c)

    section("murmur2 agrees with the broker's partitioner")
    check("murmur2(\"\") is stable", Brahmaputra.murmur2("") == 275_646_681, "#{Brahmaputra.murmur2("")}")

    check(
      "murmur2 is deterministic",
      Brahmaputra.murmur2("user-7") == Brahmaputra.murmur2("user-7")
    )

    check(
      "different keys hash differently",
      Brahmaputra.murmur2("user-7") != Brahmaputra.murmur2("user-8")
    )

    section("record headers and timestamps")
    header_topic = unique("ex-headers")
    before = now_ms() - 1000
    p = producer(host, port)

    must(
      Producer.send(p, header_topic, "annotated",
        partition: 0,
        headers: [
          {"trace-id", "abc-123"},
          {"content-type", "application/json"},
          {"tombstone-reason", nil}
        ]
      )
    )

    must(Producer.send(p, header_topic, "plain", partition: 0))
    must(Producer.flush(p))
    must(Producer.close(p))
    after_ms = now_ms() + 1000

    c = consumer(host, port)
    got = must(Consumer.fetch(c, header_topic, 0, 0, 500))
    check("both records arrive", length(got) == 2, "got #{length(got)}")

    case got do
      [annotated, plain] ->
        check(
          "headers survive the round trip",
          length(annotated.headers) == 3,
          "#{length(annotated.headers)} headers"
        )

        check("header values are exact", ConsumedRecord.header(annotated, "trace-id") == "abc-123")

        check(
          "a null header value stays null",
          length(annotated.headers) == 3 and elem(Enum.at(annotated.headers, 2), 1) == nil
        )

        check(
          "a record with no headers gains none from its batch",
          plain.headers == [],
          "#{length(plain.headers)} headers"
        )

        in_window = Enum.all?(got, &(&1.timestamp >= before and &1.timestamp <= after_ms))

        check(
          "timestamps are real wall-clock values",
          in_window,
          "#{annotated.timestamp},#{plain.timestamp} outside #{before}..#{after_ms}"
        )

      _ ->
        :ok
    end

    Consumer.close(c)

    section("tombstones")
    tomb_topic = unique("ex-tombstones")
    p = producer(host, port)
    must(Producer.send(p, tomb_topic, "set", key: "k1", partition: 0))
    must(Producer.send(p, tomb_topic, "", key: "k2", partition: 0))
    # A nil value is a deletion, and must stay distinguishable from the empty
    # value above all the way through the round trip.
    must(Producer.send(p, tomb_topic, nil, key: "k3", partition: 0))
    must(Producer.flush(p))
    must(Producer.close(p))

    c = consumer(host, port)
    got = must(Consumer.fetch(c, tomb_topic, 0, 0, 500))
    check("all three records arrive", length(got) == 3, "got #{length(got)}")

    case got do
      [a, b, t] ->
        check("an ordinary value round-trips", a.value == "set")
        check("an empty value is empty, not null", b.value == "", inspect(b.value))
        check("a tombstone arrives as a null value", t.value == nil, inspect(t.value))

      _ ->
        :ok
    end

    Consumer.close(c)

    section("offsets")
    c = consumer(host, port)
    earliest = must(Consumer.list_offsets(c, topic, 0, :earliest))
    latest = must(Consumer.list_offsets(c, topic, 0, :latest))
    check("earliest is 0 on a fresh topic", earliest == 0, "#{earliest}")
    check("latest equals the record count", latest == 50, "#{latest}")
    Consumer.close(c)

    section("acks")

    for acks <- [0, 1, -1] do
      acks_topic = unique("ex-acks#{acks}")
      p = producer(host, port, acks: acks)
      must(Producer.send(p, acks_topic, "durable", partition: 0))
      must(Producer.flush(p))
      must(Producer.close(p))
      Process.sleep(400)

      c = consumer(host, port)
      got = must(Consumer.fetch(c, acks_topic, 0, 0, 500))
      check("acks=#{acks} stores the record", length(got) == 1, "got #{length(got)}")
      Consumer.close(c)
    end

    section("consumer group: assignment, commit, resume")
    group_topic = unique("ex-group")
    group_id = unique("ex-billing")
    p = producer(host, port)
    for i <- 0..39, do: must(Producer.send(p, group_topic, "g#{i}"))
    must(Producer.flush(p))
    must(Producer.close(p))

    g = must(GroupConsumer.start_link(host, port, group_id, auto_commit_interval_ms: 0))
    must(GroupConsumer.subscribe(g, [group_topic]))
    seen = poll_until(g, 40, 30_000, 500)
    check("the group consumes every record", length(seen) == 40, "got #{length(seen)}")

    distinct = seen |> Enum.map(&{&1.partition, &1.offset}) |> Enum.uniq() |> length()
    check("no record is delivered twice", distinct == length(seen))

    must(GroupConsumer.commit(g))
    committed = must(GroupConsumer.committed(g, []))
    total = committed |> Map.values() |> Enum.sum()
    check("commit records a position", total == 40, "#{total}")
    GroupConsumer.close(g)

    # A second consumer in the same group must resume, not replay.
    rejoined = must(GroupConsumer.start_link(host, port, group_id, auto_commit_interval_ms: 0))
    must(GroupConsumer.subscribe(rejoined, [group_topic]))
    replayed = poll_for(rejoined, 5_000, 300)

    check(
      "a rejoining group resumes from its commit",
      replayed == [],
      "replayed #{length(replayed)} records it had already committed"
    )

    GroupConsumer.close(rejoined)

    section("auto.offset.reset")
    reset_topic = unique("ex-reset")
    p = producer(host, port)
    for i <- 0..9, do: must(Producer.send(p, reset_topic, "r#{i}"))
    must(Producer.flush(p))
    must(Producer.close(p))

    g =
      must(
        GroupConsumer.start_link(host, port, unique("ex-latest"),
          auto_commit_interval_ms: 0,
          auto_offset_reset: :latest
        )
      )

    must(GroupConsumer.subscribe(g, [reset_topic]))
    skipped = poll_for(g, 4_000, 300)

    check(
      "latest skips records produced before the group existed",
      skipped == [],
      "saw #{length(skipped)}"
    )

    GroupConsumer.close(g)

    strict =
      must(
        GroupConsumer.start_link(host, port, unique("ex-none"),
          auto_commit_interval_ms: 0,
          auto_offset_reset: :none
        )
      )

    must(GroupConsumer.subscribe(strict, [reset_topic]))
    deadline = mono_ms() + 5_000

    raised =
      Enum.reduce_while(Stream.repeatedly(fn -> nil end), false, fn _, _ ->
        case GroupConsumer.poll(strict, 300) do
          {:error, %Brahmaputra.NoOffsetForPartitionError{}} ->
            {:halt, true}

          {:error, e} ->
            if String.contains?(format(e), "no committed offset"),
              do: {:halt, true},
              else: if(mono_ms() >= deadline, do: {:halt, false}, else: {:cont, false})

          _ ->
            if mono_ms() >= deadline, do: {:halt, false}, else: {:cont, false}
        end
      end)

    check("none refuses to guess a position", raised)
    GroupConsumer.close(strict)

    section("assignors")

    for assignor <- [:range, :roundrobin, :sticky] do
      assignor_topic = unique("ex-#{assignor}")
      p = producer(host, port)
      for i <- 0..19, do: must(Producer.send(p, assignor_topic, "a#{i}"))
      must(Producer.flush(p))
      must(Producer.close(p))

      g =
        must(
          GroupConsumer.start_link(host, port, unique("ex-grp-#{assignor}"),
            auto_commit_interval_ms: 0,
            partition_assignment_strategy: assignor
          )
        )

      must(GroupConsumer.subscribe(g, [assignor_topic]))

      collected =
        Enum.reduce_while(Stream.repeatedly(fn -> nil end), {[], mono_ms() + 20_000}, fn _,
                                                                                        {acc, dl} ->
          acc =
            case GroupConsumer.poll(g, 500) do
              {:ok, records} -> acc ++ records
              _ -> acc
            end

          if length(acc) >= 20 or mono_ms() >= dl, do: {:halt, {acc, dl}}, else: {:cont, {acc, dl}}
        end)
        |> elem(0)

      check("#{assignor}: consumes every record", length(collected) == 20, "got #{length(collected)}")
      GroupConsumer.close(g)
    end

    section("bounded client buffer")
    buffer_topic = unique("ex-buffer")
    # linger 10s: never flush on time during this check
    p = producer(host, port, linger_ms: 10_000, buffer_memory: 2048, max_block_ms: 300)
    chunk = String.duplicate("x", 256)

    blocked =
      Enum.reduce_while(1..500, false, fn _, _ ->
        case Producer.send(p, buffer_topic, chunk, partition: 0) do
          {:error, e} -> {:halt, String.contains?(format(e), "buffer full")}
          :ok -> {:cont, false}
        end
      end)

    check("a full buffer blocks and then reports", blocked)

    passed = Process.get(:passed, 0)
    failed = Process.get(:failed, 0)
    IO.puts("\n#{passed} passed, #{failed} failed")
    System.halt(if failed > 0, do: 1, else: 0)
  end
end

{host, port} =
  case System.argv() do
    [host, port | _] -> {host, String.to_integer(port)}
    [host] -> {host, 9092}
    [] -> {"127.0.0.1", 9092}
  end

ManualTest.run(host, port)
