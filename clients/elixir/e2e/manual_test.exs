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

    run_parity(host, port)

    passed = Process.get(:passed, 0)
    failed = Process.get(:failed, 0)
    IO.puts("\n#{passed} passed, #{failed} failed")
    System.halt(if failed > 0, do: 1, else: 0)
  end

  # Reads a partition from 0 until `count` records arrive or a fetch comes
  # back empty.
  def fetch_all(c, topic, partition, count), do: fetch_all(c, topic, partition, count, 0, [])

  defp fetch_all(c, topic, partition, count, offset, acc) do
    if length(acc) >= count do
      acc
    else
      case Consumer.fetch(c, topic, partition, offset, 500) do
        {:ok, [_ | _] = batch} ->
          fetch_all(c, topic, partition, count, List.last(batch).offset + 1, acc ++ batch)

        _ ->
          acc
      end
    end
  end

  def run_parity(host, port) do
    section("wire edge cases")
    edge_topic = unique("ex-edge")
    p = producer(host, port)
    large = for i <- 0..(1024 * 1024 - 1), into: <<>>, do: <<rem(i * 7, 256)>>
    unicode_key = "ключ-✓-🔑"
    unicode_value = "значение — 数据 — 🚀"
    must(Producer.send(p, edge_topic, large, partition: 0))

    must(
      Producer.send(p, edge_topic, unicode_value,
        partition: 0,
        key: unicode_key,
        headers: [{"ünïcødé-🏷", "✓"}]
      )
    )

    # An empty key and an empty header value are values, not nulls.
    must(
      Producer.send(p, edge_topic, "empty-key",
        partition: 0,
        key: "",
        headers: [{"empty", ""}, {"null", nil}]
      )
    )

    must(Producer.send(p, edge_topic, "null-key", partition: 0))
    must(Producer.close(p))

    c = consumer(host, port)
    got = fetch_all(c, edge_topic, 0, 4)
    check("edge records all arrive", length(got) == 4, "got #{length(got)}")

    case got do
      [big, uni, empty, null] ->
        check(
          "a 1 MiB value round-trips byte-identical",
          big.value == large,
          "#{byte_size(big.value || "")} bytes"
        )

        check(
          "unicode key, value and header key round-trip",
          uni.key == unicode_key and uni.value == unicode_value and
            match?([{"ünïcødé-🏷", _}], uni.headers)
        )

        check("an empty key stays empty, not null", empty.key == "", inspect(empty.key))

        check(
          "an empty header value stays empty, not null",
          empty.headers == [{"empty", ""}, {"null", nil}],
          inspect(empty.headers)
        )

        check("a null key stays null", null.key == nil, inspect(null.key))

      _ ->
        :ok
    end

    Consumer.close(c)

    section("ordering under linger flushes")
    order_topic = unique("ex-order")
    p = producer(host, port, linger_ms: 1, batch_size: 256)
    total = 5000
    for i <- 0..(total - 1), do: must(Producer.send(p, order_topic, Integer.to_string(i), partition: 0))
    must(Producer.close(p))

    c = consumer(host, port)
    values = c |> fetch_all(order_topic, 0, total) |> Enum.map(&String.to_integer(&1.value))

    inversions =
      values |> Enum.chunk_every(2, 1, :discard) |> Enum.count(fn [a, b] -> b < a end)

    check("every record of a partition arrives", length(values) == total, "got #{length(values)}")
    check("a partition's records keep send order", inversions == 0, "#{inversions} inversions")
    Consumer.close(c)

    section("background flush failures are reported")
    p = must(Producer.start_link(host, port, linger_ms: 20))
    # Partition 999 does not exist, so the linger timer's flush fails.
    send_result = Producer.send(p, unique("ex-bgfail"), "lost", partition: 999)
    Process.sleep(300)
    flush_result = Producer.flush(p)

    check(
      "a failed linger flush surfaces on the next Flush",
      send_result == :ok and match?({:error, _}, flush_result),
      "send=#{inspect(send_result)} flush=#{inspect(flush_result)}"
    )

    closer = Task.async(fn -> Producer.close(p) end)

    case Task.yield(closer, 5_000) do
      {:ok, _} ->
        check("Close returns after a failed flush", true)

      _ ->
        Task.shutdown(closer, :brutal_kill)
        check("Close returns after a failed flush", false, "hung")
    end

    section("connection failures")
    # A broker that accepts and never answers must cost an error, not a
    # process blocked forever.
    {:ok, silent} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, silent_port} = :inet.port(silent)

    silent_acceptor =
      spawn(fn ->
        Stream.repeatedly(fn -> :gen_tcp.accept(silent) end)
        |> Enum.take_while(&match?({:ok, _}, &1))
      end)

    conn =
      must(Brahmaputra.Connection.start_link("127.0.0.1", silent_port, client_id: "ex-test", connect_timeout: 1_000))

    :ok = Brahmaputra.Connection.set_request_timeout(conn, 300)
    started = mono_ms()
    request_result = Brahmaputra.Connection.api_versions(conn)

    check(
      "a request to an unresponsive broker times out",
      match?({:error, _}, request_result) and mono_ms() - started < 3_000,
      inspect(request_result)
    )

    check("a timed-out connection is not reused", Brahmaputra.Connection.broken?(conn))
    Brahmaputra.Connection.close(conn)
    :gen_tcp.close(silent)
    Process.exit(silent_acceptor, :kill)

    # A connection the broker drops is redialled, not kept forever.
    proxy = Proxy.start(host, port)
    drop_topic = unique("ex-drop")
    p = producer("127.0.0.1", proxy.port)
    must(Producer.send(p, drop_topic, "before", partition: 0))
    Proxy.drop_all(proxy)

    recovered =
      Enum.reduce_while(1..3, {:error, :not_attempted}, fn _, _ ->
        case Producer.send(p, drop_topic, "after", partition: 0) do
          :ok -> {:halt, :ok}
          err -> {:cont, err}
        end
      end)

    check("a producer recovers after its connection drops", recovered == :ok, inspect(recovered))
    Producer.close(p)

    c = consumer("127.0.0.1", proxy.port)
    must(Consumer.fetch(c, drop_topic, 0, 0, 100))
    Proxy.drop_all(proxy)

    fetched =
      Enum.reduce_while(1..3, {:error, :not_attempted}, fn _, _ ->
        case Consumer.fetch(c, drop_topic, 0, 0, 100) do
          {:ok, records} -> {:halt, {:ok, records}}
          err -> {:cont, err}
        end
      end)

    check(
      "a consumer recovers after its connection drops",
      match?({:ok, [_ | _]}, fetched),
      inspect(fetched, limit: 3)
    )

    Consumer.close(c)
    Proxy.close(proxy)

    section("consumer group: max.poll.interval and rejoin")
    slow_topic = unique("ex-slow")
    p = producer(host, port)
    for i <- 0..9, do: must(Producer.send(p, slow_topic, "s#{i}"))

    g =
      must(
        GroupConsumer.start_link(host, port, unique("ex-slow-grp"),
          auto_commit_interval_ms: 0,
          max_poll_interval_ms: 1500
        )
      )

    must(GroupConsumer.subscribe(g, [slow_topic]))
    {first, _} = poll_collect(g, 10, 15_000, 300)
    must(GroupConsumer.commit(g))
    # Stall past max.poll.interval.ms: the member leaves the group.
    Process.sleep(2500)
    for i <- 10..19, do: must(Producer.send(p, slow_topic, "s#{i}"))
    must(Producer.close(p))
    {second, poll_err} = poll_collect(g, 10, 15_000, 300)

    check(
      "a member that stalled rejoins on its next poll",
      length(first) == 10 and length(second) == 10 and poll_err == nil,
      "first=#{length(first)} second=#{length(second)} err=#{inspect(poll_err)}"
    )

    GroupConsumer.close(g)

    section("consumer group: time inside poll does not count against max.poll.interval")
    join_topic = unique("ex-inpoll")
    p = producer(host, port)
    must(Router.partitions(Producer.router(p), join_topic))

    # Far shorter than the first poll below, which spends ~1s joining (the
    # broker's initial rebalance delay) and then waits for data.
    g =
      must(
        GroupConsumer.start_link(host, port, unique("ex-inpoll-grp"),
          auto_commit_interval_ms: 0,
          max_poll_interval_ms: 600
        )
      )

    must(GroupConsumer.subscribe(g, [join_topic]))

    sender =
      Task.async(fn ->
        Process.sleep(2_000)
        for i <- 0..9, do: Producer.send(p, join_topic, "j#{i}")
      end)

    # One long poll: it joins, then waits for the records above.
    poll_result = GroupConsumer.poll(g, 4_000)
    # Committed straight away, before another poll could quietly rejoin:
    # this fails if the member left the group mid-poll.
    commit_result = GroupConsumer.commit(g)

    got_count =
      case poll_result do
        {:ok, records} -> length(records)
        _ -> 0
      end

    check(
      "a member is still in its group after a long poll",
      match?({:ok, _}, poll_result) and got_count > 0 and commit_result == :ok,
      "got=#{got_count} poll=#{inspect(poll_result, limit: 2)} commit=#{inspect(commit_result)}"
    )

    Task.await(sender, 10_000)
    GroupConsumer.close(g)
    must(Producer.close(p))
  end

  # Polls until `count` records or `ms` pass, stopping at the first error.
  def poll_collect(group, count, ms, poll_ms) do
    deadline = mono_ms() + ms

    Stream.repeatedly(fn -> nil end)
    |> Enum.reduce_while({[], nil}, fn _, {acc, _} ->
      if length(acc) >= count or mono_ms() >= deadline do
        {:halt, {acc, nil}}
      else
        case GroupConsumer.poll(group, poll_ms) do
          {:ok, records} -> {:cont, {acc ++ records, nil}}
          {:error, e} -> {:halt, {acc, e}}
        end
      end
    end)
  end
end

defmodule Proxy do
  @moduledoc false
  # Forwards TCP to the broker and can sever every live connection, which is
  # how a broker restart or an idle timeout looks to a client.

  defstruct [:port, :listener, :registry, :acceptor]

  def start(target_host, target_port) do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(listener)
    {:ok, registry} = Agent.start(fn -> [] end)

    acceptor =
      spawn(fn -> accept_loop(listener, registry, String.to_charlist(target_host), target_port) end)

    %__MODULE__{port: port, listener: listener, registry: registry, acceptor: acceptor}
  end

  defp accept_loop(listener, registry, host, port) do
    case :gen_tcp.accept(listener) do
      {:ok, client} ->
        case :gen_tcp.connect(host, port, [:binary, active: false]) do
          {:ok, upstream} ->
            a = spawn(fn -> pump(client, upstream) end)
            b = spawn(fn -> pump(upstream, client) end)
            Agent.update(registry, &[{client, upstream, a, b} | &1])

          _ ->
            :gen_tcp.close(client)
        end

        accept_loop(listener, registry, host, port)

      _ ->
        :ok
    end
  end

  defp pump(from, to) do
    case :gen_tcp.recv(from, 0) do
      {:ok, data} ->
        :gen_tcp.send(to, data)
        pump(from, to)

      _ ->
        :gen_tcp.close(to)
    end
  end

  def drop_all(proxy) do
    for {client, upstream, a, b} <- Agent.get_and_update(proxy.registry, &{&1, []}) do
      :gen_tcp.shutdown(client, :read_write)
      :gen_tcp.shutdown(upstream, :read_write)
      :gen_tcp.close(client)
      :gen_tcp.close(upstream)
      Process.exit(a, :kill)
      Process.exit(b, :kill)
    end

    Process.sleep(50)
  end

  def close(proxy) do
    :gen_tcp.close(proxy.listener)
    drop_all(proxy)
    Process.exit(proxy.acceptor, :kill)
    Agent.stop(proxy.registry)
  end
end

{host, port} =
  case System.argv() do
    [host, port | _] -> {host, String.to_integer(port)}
    [host] -> {host, 9092}
    [] -> {"127.0.0.1", 9092}
  end

ManualTest.run(host, port)
