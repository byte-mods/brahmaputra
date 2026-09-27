defmodule Brahmaputra.Producer do
  @moduledoc """
  Batches records per partition and sends each batch as one Produce request.

  A GenServer: share one across processes rather than starting one per
  message — the batching is the point. Linger is a `Process.send_after`
  timer; a send that would overflow `buffer_memory` is parked (the caller
  blocks) until a flush frees space or `max_block_ms` passes.

  Options (Kafka's names, as atoms):

    * `:client_id` — default `"brahmaputra-elixir"`
    * `:acks` — `0` fire-and-forget, `1` leader append (default), `-1`/`:all`
      every in-sync replica
    * `:batch_size` — flush a partition once it holds this many bytes (16 KiB)
    * `:linger_ms` — flush every non-empty buffer at least this often. `0`
      sends each record immediately. Defaults to 5 (Kafka: 0) because an
      unbatched producer is slow enough to look broken.
    * `:compression_type` — `"none"`, `"gzip"`, or a codec registered with
      `Brahmaputra.register_codec/3` (`"lz4"`, `"zstd"`, `"snappy"`)
    * `:request_timeout_ms` — broker-side wait for acknowledgements (30 000)
    * `:retries` — resends of a batch the broker refused with a retriable
      error, one returned before appending (5)
    * `:retry_backoff_ms` — wait between retries (100)
    * `:delivery_timeout_ms` — caps a send, first attempt to last retry (120 000)
    * `:buffer_memory` — cap on unflushed record bytes held client-side (32 MiB)
    * `:max_block_ms` — how long `send/4` may block on a full buffer (60 000)
    * `:connect_timeout` — ms (30 000)
  """
  use GenServer

  alias Brahmaputra.{Connection, Error, Protocol, Router, Util}
  import Protocol

  @defaults [
    client_id: "brahmaputra-elixir",
    acks: 1,
    batch_size: 16 * 1024,
    linger_ms: 5,
    compression_type: "none",
    request_timeout_ms: 30_000,
    retries: 5,
    retry_backoff_ms: 100,
    delivery_timeout_ms: 120_000,
    buffer_memory: 32 * 1024 * 1024,
    max_block_ms: 60_000,
    connect_timeout: 30_000
  ]

  def default_config, do: Map.new(@defaults)

  # -- API --------------------------------------------------------------------

  @doc "Connects to a seed broker and starts the linger timer."
  def start_link(host, port, opts \\ []), do: Util.start_linked(__MODULE__, {host, port, opts})

  @doc """
  Buffers one record; call `flush/1` to await delivery.

  `value` may be `nil`, a tombstone (distinct from `""`). Options:

    * `:key` — binary or nil; keyed records go to `murmur2(key) % partitions`
    * `:partition` — explicit partition, bypassing the partitioner
    * `:headers` — list of `{key, value | nil}`, order and repeats preserved
    * `:timestamp` — unix ms; defaults to now

  Returning without an offset is deliberate: with batching the offset is not
  known until the batch goes out. Use `send_sync/4` when you need one.
  """
  def send(producer, topic, value, opts \\ []),
    do: GenServer.call(producer, {:send, topic, value, opts}, :infinity)

  @doc "Sends one record on its own and returns `{:ok, offset}`. A round trip per record."
  def send_sync(producer, topic, value, opts \\ []),
    do: GenServer.call(producer, {:send_sync, topic, value, opts}, :infinity)

  @doc "Sends every buffered record and waits for acknowledgement."
  def flush(producer), do: GenServer.call(producer, :flush, :infinity)

  @doc "The producer's router, for callers that need metadata."
  def router(producer), do: GenServer.call(producer, :router, :infinity)

  @doc "Flushes, stops the linger timer and releases connections."
  def close(producer) do
    GenServer.call(producer, :close, :infinity)
  catch
    :exit, _ -> :ok
  end

  # -- Server -----------------------------------------------------------------

  @impl true
  def init({host, port, opts}) do
    config = Map.merge(default_config(), Map.new(opts))
    config = %{config | acks: if(config.acks == :all, do: -1, else: config.acks)}

    with {:ok, codec} <- parse_compression(config.compression_type),
         {:ok, router} <-
           Router.start_link(host, port,
             client_id: config.client_id,
             connect_timeout: config.connect_timeout
           ) do
      state = %{
        config: config,
        codec: codec,
        router: router,
        buffers: %{},
        sizes: %{},
        buffered_bytes: 0,
        round_robin: 0,
        waiters: :queue.new(),
        last_error: nil
      }

      {:ok, schedule_linger(state)}
    else
      {:error, error} -> {:stop, error}
    end
  end

  @impl true
  def handle_call({:send, topic, value, opts}, from, state) do
    case choose_partition(state, topic, opts) do
      {:ok, partition, state} ->
        record = make_record(value, opts)
        slot = {topic, partition}
        size = record_size(record)
        limit = state.config.buffer_memory

        cond do
          # A record larger than the whole budget is admitted rather than
          # waiting forever on a condition that can never hold; refusing
          # oversized records is the broker's job.
          limit <= 0 or size >= limit or
              (:queue.is_empty(state.waiters) and state.buffered_bytes + size <= limit) ->
            {result, state} = admit(state, slot, record, size)
            {:reply, result, state}

          true ->
            # This is what makes buffer_memory real: a producer faster than
            # its broker is slowed down here rather than allowed to grow.
            ref = make_ref()
            timer = Process.send_after(self(), {:max_block, ref}, state.config.max_block_ms)
            waiter = %{ref: ref, timer: timer, from: from, slot: slot, record: record, size: size}
            {:noreply, %{state | waiters: :queue.in(waiter, state.waiters)}}
        end

      {:error, error, state} ->
        {:reply, {:error, error}, state}
    end
  end

  def handle_call({:send_sync, topic, value, opts}, _from, state) do
    case choose_partition(state, topic, opts) do
      {:ok, partition, state} ->
        {:reply, produce(state, topic, partition, [make_record(value, opts)]), state}

      {:error, error, state} ->
        {:reply, {:error, error}, state}
    end
  end

  def handle_call(:flush, _from, state) do
    {result, state} = flush_all(state)

    # A background (linger) flush that failed is surfaced here, to a caller
    # who can act on it.
    {result, state} =
      case {result, state.last_error} do
        {:ok, nil} -> {:ok, state}
        {:ok, error} -> {{:error, error}, %{state | last_error: nil}}
        _ -> {result, %{state | last_error: nil}}
      end

    {:reply, result, state}
  end

  def handle_call(:router, _from, state), do: {:reply, state.router, state}

  def handle_call(:close, _from, state) do
    {result, state} = flush_all(state)

    for waiter <- :queue.to_list(state.waiters) do
      Process.cancel_timer(waiter.timer)
      GenServer.reply(waiter.from, {:error, %Error{message: "producer closed"}})
    end

    Router.close(state.router)
    {:stop, :normal, result, %{state | waiters: :queue.new()}}
  end

  @impl true
  def handle_info(:linger, state) do
    # A background flush that fails must not kill the timer; the next
    # explicit flush reports it.
    {result, state} = flush_all(state)

    state =
      case result do
        {:error, error} -> %{state | last_error: error}
        :ok -> state
      end

    {:noreply, schedule_linger(state)}
  end

  def handle_info({:max_block, ref}, state) do
    {expired, rest} =
      state.waiters |> :queue.to_list() |> Enum.split_with(&(&1.ref == ref))

    for waiter <- expired do
      GenServer.reply(
        waiter.from,
        {:error,
         %Error{
           message:
             "producer buffer full: #{state.buffered_bytes} of #{state.config.buffer_memory} " <>
               "bytes unflushed after max.block.ms=#{state.config.max_block_ms}"
         }}
      )
    end

    {:noreply, %{state | waiters: :queue.from_list(rest)}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # -- buffering ----------------------------------------------------------------

  defp schedule_linger(%{config: %{linger_ms: linger}} = state) when linger > 0 do
    Process.send_after(self(), :linger, linger)
    state
  end

  defp schedule_linger(state), do: state

  defp make_record(value, opts) do
    %{
      key: Keyword.get(opts, :key),
      value: value,
      headers: Keyword.get(opts, :headers, []),
      created_ms: Keyword.get(opts, :timestamp) || Util.now_ms()
    }
  end

  defp record_size(record) do
    Enum.reduce(record.headers, bin_size(record.value) + bin_size(record.key) + 16, fn {k, v}, acc ->
      acc + byte_size(k) + bin_size(v) + 4
    end)
  end

  defp bin_size(nil), do: 0
  defp bin_size(bin), do: byte_size(bin)

  defp choose_partition(state, topic, opts) do
    case Keyword.get(opts, :partition) do
      p when is_integer(p) ->
        {:ok, p, state}

      nil ->
        case Router.partitions(state.router, topic) do
          {:ok, partitions} ->
            case Keyword.get(opts, :key) do
              nil ->
                index = rem(state.round_robin, length(partitions))
                {:ok, Enum.at(partitions, index), %{state | round_robin: state.round_robin + 1}}

              key ->
                {:ok, partition_for_key(key, partitions), state}
            end

          {:error, error} ->
            {:error, error, state}
        end
    end
  end

  defp admit(state, slot, record, size) do
    state = %{
      state
      | buffers: Map.update(state.buffers, slot, [record], &[record | &1]),
        sizes: Map.update(state.sizes, slot, size, &(&1 + size)),
        buffered_bytes: state.buffered_bytes + size
    }

    if state.config.linger_ms == 0 or state.sizes[slot] >= state.config.batch_size do
      flush_partition(state, slot)
    else
      {:ok, state}
    end
  end

  defp flush_all(state) do
    state.buffers
    |> Map.keys()
    |> Enum.sort()
    |> Enum.reduce({:ok, state}, fn slot, {result, state} ->
      {r, state} = flush_partition(state, slot)
      {if(result == :ok, do: r, else: result), state}
    end)
  end

  defp flush_partition(state, {topic, partition} = slot) do
    case Map.get(state.buffers, slot, []) do
      [] ->
        {:ok, state}

      reversed ->
        size = Map.get(state.sizes, slot, 0)

        state = %{
          state
          | buffers: Map.delete(state.buffers, slot),
            sizes: Map.delete(state.sizes, slot),
            buffered_bytes: max(state.buffered_bytes - size, 0)
        }

        result =
          case produce(state, topic, partition, Enum.reverse(reversed)) do
            {:ok, _offset} -> :ok
            {:error, error} -> {:error, error}
          end

        {result, drain_waiters(state)}
    end
  end

  # Admits parked senders, oldest first, while they fit.
  defp drain_waiters(state) do
    case :queue.out(state.waiters) do
      {{:value, waiter}, rest} ->
        if state.buffered_bytes + waiter.size <= state.config.buffer_memory do
          Process.cancel_timer(waiter.timer)
          {result, state} = admit(%{state | waiters: rest}, waiter.slot, waiter.record, waiter.size)
          GenServer.reply(waiter.from, result)
          drain_waiters(state)
        else
          state
        end

      {:empty, _} ->
        state
    end
  end

  # -- produce ------------------------------------------------------------------

  defp produce(state, topic, partition, records) do
    # The batch stores one max timestamp and a delta per record, so the
    # rebasing happens here; max_timestamp is the newest record's time.
    max_ts = records |> Enum.map(& &1.created_ms) |> Enum.max()

    batch_records =
      Enum.map(records, fn r ->
        %{key: r.key, value: r.value, headers: r.headers, timestamp_delta: r.created_ms - max_ts}
      end)

    config = state.config

    try do
      encoded = encode_record_batch(batch_records, max_ts, state.codec)

      req =
        body([
          w_string(topic),
          w_int32(partition),
          w_int32(config.acks),
          w_int32(config.request_timeout_ms),
          w_int64(byte_size(encoded)),
          encoded
        ])

      if config.acks == 0 do
        with {:ok, conn} <- Router.conn_for(state.router, topic, partition),
             :ok <- Connection.send_oneway(conn, api(:produce), req) do
          {:ok, -1}
        end
      else
        deadline = Util.mono_ms() + config.delivery_timeout_ms
        attempt(state, topic, partition, req, config.retries, deadline)
      end
    rescue
      e in Error -> {:error, e}
    end
  end

  defp attempt(state, topic, partition, req, attempts_left, deadline) do
    config = state.config
    timeout = config.request_timeout_ms + 5_000

    with {:ok, resp} <- Router.request(state.router, topic, partition, api(:produce), req, timeout) do
      rest = open_body(resp)
      {_topic, rest} = r_string(rest)
      {_partition, rest} = r_int32(rest)
      {code, rest} = r_int32(rest)
      {base_offset, _rest} = r_int64(rest)

      cond do
        code == 0 ->
          {:ok, base_offset}

        not retriable?(code) or attempts_left <= 0 or Util.mono_ms() > deadline ->
          {:error, server_error(code, "produce to #{topic}-#{partition}")}

        true ->
          # A stale route is the most common retriable cause, and resending
          # to the same broker would just repeat it.
          if code in [6, 8, 9], do: Router.refresh(state.router, topic)
          Process.sleep(config.retry_backoff_ms)
          attempt(state, topic, partition, req, attempts_left - 1, deadline)
      end
    end
  end
end
