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
    Coverage.run(host, port)

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

defmodule Coverage do
  @moduledoc false
  # One check per client feature the sections above do not already exercise:
  # producer and consumer settings, group options, and decoder bounds.

  import ManualTest, only: [check: 2, check: 3, section: 1, unique: 1, must: 1, format: 1, mono_ms: 0]
  alias Brahmaputra.{Assignor, Consumer, GroupConsumer, Producer, Protocol, Router}

  def run(host, port) do
    producer_settings(host, port)
    retry_settings()
    consumer_settings(host, port)
    group_settings(host, port)
    decoder_bounds()
  end

  defp producer(host, port, opts), do: must(Producer.start_link(host, port, opts))

  defp producer_settings(host, port) do
    section("producer settings")
    c = must(Consumer.connect(host, port))

    # batch.size: a partition that fills its batch is sent at once, even
    # though linger would hold it for a minute.
    topic = unique("ex-batchsize")
    p = producer(host, port, linger_ms: 60_000, batch_size: 64)
    for i <- 0..2, do: must(Producer.send(p, topic, String.duplicate("b", 100) <> "#{i}", partition: 0))
    got = must(Consumer.fetch(c, topic, 0, 0, 1_000))
    check("batch.size sends a full batch without waiting for linger", length(got) == 3, "got #{length(got)}")
    Producer.close(p)

    # linger.ms: a partial batch goes out on its own once linger passes.
    topic = unique("ex-linger")
    p = producer(host, port, linger_ms: 50, batch_size: 1_048_576)
    must(Producer.send(p, topic, "lingering", partition: 0))
    Process.sleep(500)
    got = must(Consumer.fetch(c, topic, 0, 0, 1_000))
    check("linger.ms flushes a partial batch on its own", length(got) == 1, "got #{length(got)}")
    Producer.close(p)

    # Synchronous send, explicit partition and explicit timestamps.
    topic = unique("ex-sync")
    p = producer(host, port, linger_ms: 0)
    stamp = 1_600_000_000_000
    first = must(Producer.send_sync(p, topic, "one", partition: 2, timestamp: stamp))
    second = must(Producer.send_sync(p, topic, "two", partition: 2, timestamp: stamp + 1_000))
    check("send_sync returns consecutive offsets", first == 0 and second == 1, "#{first}, #{second}")
    got = must(Consumer.fetch(c, topic, 2, 0, 1_000))
    check("an explicit partition is honoured", length(got) == 2, "partition 2 holds #{length(got)}")

    check(
      "an explicit timestamp is stored exactly",
      Enum.map(got, & &1.timestamp) == [stamp, stamp + 1_000],
      inspect(Enum.map(got, & &1.timestamp))
    )

    # Keyless records are dealt round-robin across every partition.
    topic = unique("ex-roundrobin")
    partitions = must(Router.partitions(Producer.router(p), topic))
    for i <- 1..(2 * length(partitions)), do: must(Producer.send(p, topic, "rr#{i}"))
    must(Producer.flush(p))
    counts = Enum.map(partitions, fn part -> length(must(Consumer.fetch(c, topic, part, 0, 300))) end)
    check("keyless records are spread round-robin", Enum.all?(counts, &(&1 == 2)), inspect(counts))
    Producer.close(p)

    # A codec the driver does not carry, registered by the application. The
    # encoder emits a valid LZ4 block of literals only, which the broker
    # (lz4_flex) accepts and stores as-is.
    :ok = Brahmaputra.register_codec(:lz4, &Lz4.compress/1, &Lz4.decompress/1)
    topic = unique("ex-lz4")
    body = String.duplicate("registered codec payload ", 20)
    p = producer(host, port, linger_ms: 0, compression_type: "lz4")
    for i <- 0..4, do: must(Producer.send(p, topic, body <> "#{i}", partition: 0))
    Producer.close(p)
    got = must(Consumer.fetch(c, topic, 0, 0, 1_000))

    check(
      "a registered codec round-trips through the broker",
      Enum.map(got, & &1.value) == for(i <- 0..4, do: body <> "#{i}"),
      "got #{length(got)}"
    )

    Consumer.close(c)
  end

  defp retry_settings do
    section("retries against a broker that refuses")
    fake = FakeBroker.start()

    # request.timeout.ms and acks travel in the produce request itself.
    p =
      must(
        Producer.start_link("127.0.0.1", fake.port,
          linger_ms: 0,
          acks: -1,
          request_timeout_ms: 1_234,
          retries: 2,
          retry_backoff_ms: 150
        )
      )

    started = mono_ms()
    result = Producer.send_sync(p, "retriable", "x", partition: 0)
    elapsed = mono_ms() - started
    attempts = FakeBroker.produces(fake)

    check(
      "request.timeout.ms and acks reach the broker",
      attempts != [] and Enum.all?(attempts, &(&1.acks == -1 and &1.timeout == 1_234)),
      inspect(attempts)
    )

    check(
      "a retriable error is retried `retries` times",
      match?({:error, _}, result) and length(attempts) == 3,
      "#{length(attempts)} attempts, #{inspect(result)}"
    )

    check("retry.backoff.ms spaces the retries", elapsed >= 300, "#{elapsed} ms")

    FakeBroker.reset(fake)
    result = Producer.send_sync(p, "fatal", "x", partition: 0)
    attempts = FakeBroker.produces(fake)

    check(
      "a non-retriable error is not retried",
      match?({:error, _}, result) and length(attempts) == 1,
      "#{length(attempts)} attempts"
    )

    Producer.close(p)

    FakeBroker.reset(fake)

    p =
      must(
        Producer.start_link("127.0.0.1", fake.port,
          linger_ms: 0,
          retries: 1_000_000,
          retry_backoff_ms: 50,
          delivery_timeout_ms: 400
        )
      )

    started = mono_ms()
    result = Producer.send_sync(p, "retriable", "x", partition: 0)
    elapsed = mono_ms() - started

    check(
      "delivery.timeout.ms caps the retries",
      match?({:error, _}, result) and elapsed < 3_000,
      "#{elapsed} ms, #{length(FakeBroker.produces(fake))} attempts"
    )

    Producer.close(p)
    FakeBroker.stop(fake)
  end

  defp consumer_settings(host, port) do
    section("consumer settings")
    topic = unique("ex-fetchcfg")
    p = producer(host, port, linger_ms: 0)
    # linger 0: every send is its own batch.
    for i <- 0..19, do: must(Producer.send(p, topic, String.duplicate("f", 1_000) <> "#{i}", partition: 0))
    Producer.close(p)

    c = must(Consumer.connect(host, port))
    {records, hw} = must(Consumer.fetch_verbose(c, topic, 0, 0, 500))
    check("fetch reports the high watermark", hw == 20, "#{hw}")
    check("a default fetch returns every record", length(records) == 20, "got #{length(records)}")

    meta = must(Router.metadata(Consumer.router(c), [topic], true))
    brokers = MapSet.new(meta.brokers, & &1.node_id)
    infos = Map.get(meta.topics, topic, [])

    check(
      "metadata names a live leader for every partition",
      infos != [] and Enum.all?(infos, &MapSet.member?(brokers, &1.leader)),
      inspect(infos)
    )

    Consumer.close(c)

    small = must(Consumer.connect(host, port, fetch_max_bytes: 2_500))
    got = must(Consumer.fetch(small, topic, 0, 0, 500))

    check(
      "fetch.max.bytes caps a response",
      length(got) >= 1 and length(got) < 20,
      "got #{length(got)}"
    )

    Consumer.close(small)

    patient = must(Consumer.connect(host, port, fetch_min_bytes: 10_000_000, fetch_max_wait_ms: 400))
    started = mono_ms()
    got = must(Consumer.fetch(patient, topic, 0, 19, 400))
    waited = mono_ms() - started

    check(
      "fetch.min.bytes holds a fetch for up to fetch.max.wait.ms",
      length(got) == 1 and waited >= 300 and waited < 5_000,
      "#{waited} ms, #{length(got)} records"
    )

    Consumer.close(patient)

    # List offsets by timestamp: the first record at or after it.
    topic = unique("ex-bytime")
    p = producer(host, port, linger_ms: 0)
    base = 1_700_000_000_000
    for i <- 0..2, do: must(Producer.send(p, topic, "t#{i}", partition: 0, timestamp: base + i * 10_000))
    Producer.close(p)
    c = must(Consumer.connect(host, port))
    at = must(Consumer.list_offsets(c, topic, 0, base + 5_000))
    check("list offsets by timestamp finds the first record at or after it", at == 1, "#{at}")
    Consumer.close(c)

    # max.poll.records caps what one poll hands over.
    topic = unique("ex-maxpoll")
    p = producer(host, port, linger_ms: 0)
    for i <- 0..9, do: must(Producer.send(p, topic, "m#{i}", partition: 0))
    Producer.close(p)

    g =
      must(
        GroupConsumer.start_link(host, port, unique("ex-maxpoll-grp"),
          auto_commit_interval_ms: 0,
          max_poll_records: 3
        )
      )

    must(GroupConsumer.subscribe(g, [topic]))
    sizes = poll_sizes(g, 10, 15_000)

    check(
      "max.poll.records caps a poll",
      Enum.sum(sizes) == 10 and Enum.all?(sizes, &(&1 <= 3)),
      inspect(sizes)
    )

    GroupConsumer.close(g)
  end

  defp poll_sizes(g, want, ms) do
    deadline = mono_ms() + ms

    Stream.repeatedly(fn -> nil end)
    |> Enum.reduce_while([], fn _, acc ->
      if Enum.sum(acc) >= want or mono_ms() >= deadline do
        {:halt, acc}
      else
        case GroupConsumer.poll(g, 300) do
          {:ok, []} -> {:cont, acc}
          {:ok, records} -> {:cont, acc ++ [length(records)]}
          _ -> {:cont, acc}
        end
      end
    end)
  end

  defp group_settings(host, port) do
    section("consumer group settings")
    # Several topics in one subscription.
    t1 = unique("ex-multi-a")
    t2 = unique("ex-multi-b")
    p = producer(host, port, linger_ms: 0)
    for i <- 0..4, do: must(Producer.send(p, t1, "a#{i}"))
    for i <- 0..4, do: must(Producer.send(p, t2, "b#{i}"))

    g = must(GroupConsumer.start_link(host, port, unique("ex-multi-grp"), auto_commit_interval_ms: 0))
    must(GroupConsumer.subscribe(g, [t1, t2]))
    got = ManualTest.poll_until(g, 10, 15_000, 300)
    topics = got |> Enum.map(& &1.topic) |> Enum.frequencies()

    check(
      "a group consumes every subscribed topic",
      topics == %{t1 => 5, t2 => 5},
      inspect(topics)
    )

    GroupConsumer.close(g)

    # enable.auto.commit: positions are committed by poll itself.
    topic = unique("ex-autocommit")
    for i <- 0..5, do: must(Producer.send(p, topic, "c#{i}", partition: 0))
    group = unique("ex-auto-grp")
    g = must(GroupConsumer.start_link(host, port, group, auto_commit_interval_ms: 100))
    must(GroupConsumer.subscribe(g, [topic]))
    ManualTest.poll_until(g, 6, 15_000, 300)
    Process.sleep(200)
    GroupConsumer.poll(g, 300)
    committed = must(GroupConsumer.committed(g, [{topic, 0}]))

    check(
      "auto-commit records positions without an explicit commit",
      Map.get(committed, {topic, 0}) == 6,
      inspect(committed)
    )

    GroupConsumer.close(g)

    group = unique("ex-noauto-grp")

    g =
      must(
        GroupConsumer.start_link(host, port, group,
          enable_auto_commit: false,
          auto_commit_interval_ms: 100
        )
      )

    must(GroupConsumer.subscribe(g, [topic]))
    ManualTest.poll_until(g, 6, 15_000, 300)
    Process.sleep(200)
    GroupConsumer.poll(g, 300)
    committed = must(GroupConsumer.committed(g, [{topic, 0}]))

    check(
      "disabled auto-commit commits nothing",
      Map.get(committed, {topic, 0}, -1) < 0,
      inspect(committed)
    )

    GroupConsumer.close(g)

    # Static membership: a second instance presenting the same
    # group.instance.id takes over the first one's partitions at once,
    # without a rebalance, even though the first is still heartbeating.
    topic = unique("ex-static")
    must(Router.partitions(Producer.router(p), topic))
    group = unique("ex-static-grp")
    opts = [auto_commit_interval_ms: 0, group_instance_id: "instance-1"]
    first = must(GroupConsumer.start_link(host, port, group, opts))
    must(GroupConsumer.subscribe(first, [topic]))
    GroupConsumer.poll(first, 2_000)
    first_assignment = GroupConsumer.assignment(first)
    second = must(GroupConsumer.start_link(host, port, group, opts))
    must(GroupConsumer.subscribe(second, [topic]))
    started = mono_ms()
    GroupConsumer.poll(second, 200)
    took = mono_ms() - started
    second_assignment = GroupConsumer.assignment(second)

    check(
      "a static member reclaims its partitions without a rebalance",
      length(first_assignment) == 4 and Enum.sort(second_assignment) == Enum.sort(first_assignment) and
        took < 2_000,
      "first=#{inspect(first_assignment)} second=#{inspect(second_assignment)} #{took} ms"
    )

    GroupConsumer.close(second)
    Process.unlink(first)
    Process.exit(first, :kill)

    # LeaveGroup on close: the survivor takes over without waiting out the
    # leaver's session.
    topic = unique("ex-leave")
    must(Router.partitions(Producer.router(p), topic))
    group = unique("ex-leave-grp")
    # A long session and a short heartbeat: without LeaveGroup the survivor
    # would wait 30 s; with it, the next heartbeat (200 ms) learns of it.
    opts = [auto_commit_interval_ms: 0, session_timeout_ms: 30_000, heartbeat_interval_ms: 200]
    a = must(GroupConsumer.start_link(host, port, group, opts))
    b = must(GroupConsumer.start_link(host, port, group, opts))
    must(GroupConsumer.subscribe(a, [topic]))
    must(GroupConsumer.subscribe(b, [topic]))
    split = settle([a, b], fn -> Enum.map([a, b], &length(GroupConsumer.assignment(&1))) == [2, 2] end, 20_000)
    GroupConsumer.close(a)
    started = mono_ms()
    took_over = settle([b], fn -> length(GroupConsumer.assignment(b)) == 4 end, 15_000)
    took = mono_ms() - started

    check(
      "closing a member hands its partitions over within a heartbeat",
      split and took_over and took < 5_000,
      "split=#{split} took_over=#{took_over} #{took} ms"
    )

    GroupConsumer.close(b)

    # session.timeout.ms: a member that goes silent without leaving (here,
    # killed) is evicted once its session lapses, and the survivor takes over.
    topic = unique("ex-session")
    must(Router.partitions(Producer.router(p), topic))
    group = unique("ex-session-grp")
    opts = [auto_commit_interval_ms: 0, session_timeout_ms: 2_000, heartbeat_interval_ms: 200]
    a = must(GroupConsumer.start_link(host, port, group, opts))
    b = must(GroupConsumer.start_link(host, port, group, opts))
    must(GroupConsumer.subscribe(a, [topic]))
    must(GroupConsumer.subscribe(b, [topic]))
    split = settle([a, b], fn -> Enum.map([a, b], &length(GroupConsumer.assignment(&1))) == [2, 2] end, 20_000)
    Process.unlink(a)
    Process.exit(a, :kill)
    started = mono_ms()
    took_over = settle([b], fn -> length(GroupConsumer.assignment(b)) == 4 end, 20_000)
    took = mono_ms() - started

    check(
      "a silent member is evicted after session.timeout.ms",
      split and took_over and took >= 1_000 and took < 12_000,
      "split=#{split} took_over=#{took_over} #{took} ms"
    )

    GroupConsumer.close(b)

    # Generation fencing: a member whose generation moved on cannot commit.
    topic = unique("ex-fence")
    for i <- 0..3, do: must(Producer.send(p, topic, "f#{i}"))
    group = unique("ex-fence-grp")
    a = must(GroupConsumer.start_link(host, port, group, auto_commit_interval_ms: 0))
    must(GroupConsumer.subscribe(a, [topic]))
    ManualTest.poll_until(a, 4, 10_000, 300)
    b = must(GroupConsumer.start_link(host, port, group, auto_commit_interval_ms: 0))
    must(GroupConsumer.subscribe(b, [topic]))
    # b's join starts a new generation; a, not polling, is left behind.
    GroupConsumer.poll(b, 500)
    fenced = GroupConsumer.commit(a)
    check("a commit from a stale generation is refused", match?({:error, _}, fenced), inspect(fenced))
    GroupConsumer.close(b)
    GroupConsumer.close(a)

    Producer.close(p)

    section("assignors (unit)")
    members = [%{id: "a", topics: ["t"]}, %{id: "b", topics: ["t"]}]
    twelve = %{"t" => Enum.to_list(0..11)}
    previous = %{"a" => for(i <- 0..11, do: {"t", i}), "b" => []}
    sticky = Assignor.assign(:sticky, members, twelve, previous)

    check(
      "sticky keeps partitions in numeric order",
      sticky["a"] == for(i <- 0..5, do: {"t", i}) and sticky["b"] == for(i <- 6..11, do: {"t", i}),
      inspect(sticky)
    )

    held = %{"a" => [{"t", 1}, {"t", 3}], "b" => [{"t", 0}, {"t", 2}]}
    kept = Assignor.assign(:sticky, members, %{"t" => [0, 1, 2, 3]}, held)
    check("sticky keeps what members already hold", kept == held, inspect(kept))
  end

  # Polls every group in turn until `done` holds or `ms` pass.
  defp settle(groups, done, ms) do
    deadline = mono_ms() + ms

    Stream.repeatedly(fn -> nil end)
    |> Enum.reduce_while(false, fn _, _ ->
      # In parallel: a join blocks until every member has rejoined.
      groups |> Enum.map(&Task.async(fn -> GroupConsumer.poll(&1, 200) end)) |> Enum.each(&Task.await(&1, 30_000))

      cond do
        done.() -> {:halt, true}
        mono_ms() >= deadline -> {:halt, false}
        true -> {:cont, false}
      end
    end)
  end

  defp decoder_bounds do
    section("decoder bounds")
    body = Protocol.body([Protocol.w_int32(-5)])
    negative = Consumer.guard(fn -> {:ok, body |> Protocol.open_body() |> Protocol.r_string()} end)
    check("a negative length is an error", match?({:error, _}, negative), inspect(negative))

    body = Protocol.body([Protocol.w_int32(1_000_000), "short"])
    oversized = Consumer.guard(fn -> {:ok, body |> Protocol.open_body() |> Protocol.r_string()} end)
    check("a length past the end of the data is an error", match?({:error, _}, oversized), inspect(oversized))

    batch = <<0::64, 0x7FFFFFFF::32, 0::32>>
    truncated = Consumer.guard(fn -> {:ok, Protocol.decode_record_batches(batch)} end)
    check("a batch longer than its bytes is an error", match?({:error, _}, truncated), inspect(truncated))

    _ = format(:ok)
  end
end

defmodule Lz4 do
  @moduledoc false
  # The broker's lz4 payload: little-endian uncompressed length, then a raw
  # LZ4 block. The encoder writes literals only (valid, if uncompressed).

  import Bitwise

  def compress(data) do
    n = byte_size(data)
    token = if n >= 15, do: <<0xF0>> <> ext(n - 15), else: <<n <<< 4>>
    <<n::little-32>> <> token <> data
  end

  defp ext(k) when k >= 255, do: <<255>> <> ext(k - 255)
  defp ext(k), do: <<k>>

  def decompress(<<_size::little-32, block::binary>>), do: block(block, <<>>)

  defp block(<<>>, out), do: out

  defp block(<<token, rest::binary>>, out) do
    {lits, rest} = len(token >>> 4, rest)
    <<literal::binary-size(lits), rest::binary>> = rest
    out = out <> literal

    case rest do
      <<>> ->
        out

      <<offset::little-16, rest::binary>> ->
        {mlen, rest} = len(token &&& 15, rest)
        block(rest, copy(out, offset, mlen + 4))
    end
  end

  defp len(15, rest), do: more(rest, 15)
  defp len(n, rest), do: {n, rest}

  defp more(<<255, rest::binary>>, acc), do: more(rest, acc + 255)
  defp more(<<b, rest::binary>>, acc), do: {acc + b, rest}

  defp copy(out, _offset, 0), do: out
  defp copy(out, offset, n), do: copy(out <> binary_part(out, byte_size(out) - offset, 1), offset, n - 1)
end

defmodule FakeBroker do
  @moduledoc false
  # Answers Metadata with itself as the only broker and refuses every
  # produce: topic "fatal" with a non-retriable code, anything else with
  # NOT_ENOUGH_REPLICAS (retriable). Records each produce it sees.

  import Brahmaputra.Protocol
  defstruct [:port, :listener, :log, :acceptor]

  def start do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, packet: 4, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(listener)
    {:ok, log} = Agent.start(fn -> [] end)
    acceptor = spawn(fn -> accept(listener, log, port) end)
    %__MODULE__{port: port, listener: listener, log: log, acceptor: acceptor}
  end

  def produces(fake), do: fake.log |> Agent.get(& &1) |> Enum.reverse()
  def reset(fake), do: Agent.update(fake.log, fn _ -> [] end)

  def stop(fake) do
    :gen_tcp.close(fake.listener)
    Process.exit(fake.acceptor, :kill)
    Agent.stop(fake.log)
  end

  defp accept(listener, log, port) do
    case :gen_tcp.accept(listener) do
      {:ok, socket} ->
        pid = spawn(fn -> serve(socket, log, port) end)
        :gen_tcp.controlling_process(socket, pid)
        accept(listener, log, port)

      _ ->
        :ok
    end
  end

  defp serve(socket, log, port) do
    case :gen_tcp.recv(socket, 0) do
      {:ok, <<api_key::big-16, _version::16, _corr::32, clen::big-16, _::binary-size(clen), req::binary>> = frame} ->
        header = binary_part(frame, 0, 10 + clen)
        :gen_tcp.send(socket, [header, answer(api_key, req, log, port)])
        serve(socket, log, port)

      _ ->
        :gen_tcp.close(socket)
    end
  end

  defp answer(3, req, _log, port) do
    {topics, _} = req |> open_body() |> r_string_array()

    body([
      w_int32(0),
      w_int32(1),
      [w_int32(0), w_string("127.0.0.1"), w_int32(port), w_string("")],
      w_int32(0),
      w_int32(length(topics))
      | Enum.map(topics, fn t ->
          [w_string(t), w_int32(0), w_int32(1),
           [w_int32(0), w_int32(0), w_int32(1), w_int32(0), w_int32(1), w_int32(0), w_int32(0)]]
        end)
    ])
  end

  defp answer(0, req, log, _port) do
    {topic, rest} = req |> open_body() |> r_string()
    {partition, rest} = r_int32(rest)
    {acks, rest} = r_int32(rest)
    {timeout, _} = r_int32(rest)
    Agent.update(log, &[%{topic: topic, acks: acks, timeout: timeout} | &1])
    code = if topic == "fatal", do: 87, else: 10
    body([w_string(topic), w_int32(partition), w_int32(code), w_int64(-1), w_int64(-1)])
  end

  defp answer(_api, _req, _log, _port), do: body([w_int32(35)])
end

{host, port} =
  case System.argv() do
    [host, port | _] -> {host, String.to_integer(port)}
    [host] -> {host, 9092}
    [] -> {"127.0.0.1", 9092}
  end

ManualTest.run(host, port)
