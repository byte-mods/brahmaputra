defmodule Brahmaputra.ConsumedRecord do
  @moduledoc """
  One record delivered to the application. `timestamp` is absolute unix
  milliseconds, already resolved against its batch. `value` is `nil` for a
  tombstone, which is distinct from an empty binary. `headers` is an ordered
  list of `{key, value | nil}`.
  """
  defstruct [:topic, :partition, :offset, :key, :value, :timestamp, headers: []]

  @type t :: %__MODULE__{}

  @doc "The first value stored under `key`, or nil."
  def header(%__MODULE__{headers: headers}, key) do
    case List.keyfind(headers, key, 0) do
      {_, value} -> value
      nil -> nil
    end
  end
end

defmodule Brahmaputra.Consumer do
  @moduledoc """
  Reads partitions directly, with no group coordination.

  A consumer is a plain struct around a `Brahmaputra.Router` process: its
  functions run in the caller, and the router and connection processes
  serialise what goes on the wire.

  Options (Kafka's names, as atoms):

    * `:client_id` — default `"brahmaputra-elixir"`
    * `:fetch_max_bytes` — caps one response (default 8 MiB)
    * `:fetch_min_bytes` — return early once this many bytes are ready (1)
    * `:fetch_max_wait_ms` — long-poll ceiling when caught up (500)
    * `:max_poll_records` — used by the group consumer (500)
    * `:isolation_level` — `:read_uncommitted` (default) or `:read_committed`
    * `:client_rack` — this consumer's failure domain, `""` for none
    * `:connect_timeout` — ms (30 000)
  """

  alias Brahmaputra.{ConsumedRecord, Error, Protocol, Router}
  import Protocol

  defstruct [:router, :config]

  @defaults [
    client_id: "brahmaputra-elixir",
    fetch_max_bytes: 8 * 1024 * 1024,
    fetch_min_bytes: 1,
    fetch_max_wait_ms: 500,
    max_poll_records: 500,
    isolation_level: :read_uncommitted,
    client_rack: "",
    connect_timeout: 30_000
  ]

  def default_config, do: Map.new(@defaults)

  @doc "Connects to a seed broker."
  def connect(host, port, opts \\ []) do
    config = Map.merge(default_config(), Map.new(opts))

    case Router.start_link(host, port,
           client_id: config.client_id,
           connect_timeout: config.connect_timeout
         ) do
      {:ok, router} -> {:ok, %__MODULE__{router: router, config: config}}
      {:error, error} -> {:error, error}
    end
  end

  def close(%__MODULE__{router: router}), do: Router.close(router)

  def router(%__MODULE__{router: router}), do: router

  def partitions(%__MODULE__{router: router}, topic), do: Router.partitions(router, topic)

  @doc """
  Asks the broker which API versions it speaks, returning
  `{:ok, [{api_key, min, max}], broker_version}`. This is the one call that
  works across a version mismatch.
  """
  def api_versions(%__MODULE__{router: router, config: config}) do
    with {:ok, seed} <- Router.seed(router) do
      Brahmaputra.Connection.api_versions(seed, config.client_id)
    end
  end

  @doc """
  Resolves `:earliest`, `:latest` or a unix-ms timestamp to an offset.
  """
  def list_offsets(%__MODULE__{router: router}, topic, partition, timestamp) do
    ts =
      case timestamp do
        :earliest -> -2
        :latest -> -1
        ts when is_integer(ts) -> ts
      end

    req = body([w_string(topic), w_int32(partition), w_int64(ts)])

    with {:ok, resp} <- Router.request(router, topic, partition, api(:list_offsets), req) do
      guard(fn ->
        rest = open_body(resp)
        {_topic, rest} = r_string(rest)
        {_partition, rest} = r_int32(rest)
        {code, rest} = r_int32(rest)
        {offset, _rest} = r_int64(rest)

        if code != 0,
          do: {:error, server_error(code, "list_offsets #{topic}-#{partition}")},
          else: {:ok, offset}
      end)
    end
  end

  @doc "Reads one partition from `offset`. Returns `{:ok, [ConsumedRecord]}`."
  def fetch(consumer, topic, partition, offset, max_wait_ms \\ 500) do
    with {:ok, records, _hw} <- fetch_verbose(consumer, topic, partition, offset, max_wait_ms) do
      {:ok, records}
    end
  end

  @doc "Like `fetch/5`, also returning the partition's high watermark."
  def fetch_verbose(%__MODULE__{router: router, config: config}, topic, partition, offset, max_wait_ms \\ 500) do
    max_wait_ms = min(max_wait_ms, config.fetch_max_wait_ms)
    isolation = if config.isolation_level == :read_committed, do: 1, else: 0

    req =
      body([
        w_string(topic),
        w_int32(partition),
        w_int64(offset),
        w_int32(config.fetch_max_bytes),
        w_int32(max_wait_ms),
        w_int32(config.fetch_min_bytes),
        w_int32(isolation),
        # client.rack: with it set the leader may name a same-rack replica.
        w_string(config.client_rack)
      ])

    timeout = max_wait_ms + 30_000

    result =
      with {:ok, resp} <- Router.request(router, topic, partition, api(:fetch), req, timeout) do
        decode_fetch(resp)
      end

    result =
      case result do
        {:ok, 6, _, _} ->
          # Stale route: refresh once and retry against the new leader.
          with {:ok, _} <- Router.refresh(router, topic),
               {:ok, resp} <- Router.request(router, topic, partition, api(:fetch), req, timeout) do
            decode_fetch(resp)
          end

        other ->
          other
      end

    case result do
      {:ok, 0, hw, batches} ->
        records =
          for batch <- batches,
              {record, index} <- Enum.with_index(batch.records),
              record_offset <- [batch.base_offset + index],
              # A batch can start before the requested offset; skip what the
              # caller has already seen.
              record_offset >= offset do
            %ConsumedRecord{
              topic: topic,
              partition: partition,
              offset: record_offset,
              key: record.key,
              value: record.value,
              timestamp: batch.max_timestamp + record.timestamp_delta,
              headers: record.headers
            }
          end

        {:ok, records, hw}

      {:ok, code, _, _} ->
        {:error, server_error(code, "fetch #{topic}-#{partition}")}

      {:error, error} ->
        {:error, error}
    end
  end

  defp decode_fetch(resp) do
    guard(fn ->
      rest = open_body(resp)
      {_topic, rest} = r_string(rest)
      {_partition, rest} = r_int32(rest)
      {code, rest} = r_int32(rest)
      {hw, rest} = r_int64(rest)
      {_last_stable, rest} = r_int64(rest)
      {batches_length, rest} = r_int64(rest)
      # Read even though unused: the batches trail the whole struct, so
      # skipping a field would take them from the wrong offset.
      {_preferred_read_replica, rest} = r_int32(rest)

      if batches_length < 0 or batches_length > byte_size(rest) do
        raise Error, message: "fetch response claims more batch bytes than it carries"
      end

      raw = binary_part(rest, 0, batches_length)
      batches = if code == 0, do: decode_record_batches(raw), else: []
      {:ok, code, hw, batches}
    end)
  end

  @doc false
  def guard(fun) do
    fun.()
  rescue
    e in Error -> {:error, e}
  catch
    {:server_error, e} -> {:error, e}
  end
end
