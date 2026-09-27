defmodule Brahmaputra.Router do
  @moduledoc """
  Keeps a connection to every broker and routes by partition leader.

  Metadata is cached and refreshed only when a request comes back saying the
  route was stale, because refreshing per request would put the control
  plane on the data path.
  """
  use GenServer

  alias Brahmaputra.{Connection, Error, Protocol}
  import Protocol

  # -- API --------------------------------------------------------------------

  @doc """
  Starts a router with a seed connection to `host:port`. `opts`:
  `:client_id`, `:connect_timeout`.
  """
  def start_link(host, port, opts \\ []) do
    Brahmaputra.Util.start_linked(__MODULE__, {host, port, opts})
  end

  def close(router) do
    GenServer.stop(router, :normal)
  catch
    :exit, _ -> :ok
  end

  @doc "The seed connection this router was opened with."
  def seed(router), do: GenServer.call(router, :seed, :infinity)

  @doc """
  Cluster metadata for `topics` (`[]` means every topic). Served from cache
  unless `refresh` is true.
  """
  def metadata(router, topics \\ [], refresh \\ true),
    do: GenServer.call(router, {:metadata, topics, refresh}, :infinity)

  def refresh(router, topic), do: metadata(router, [topic], true)

  @doc "A topic's partition ids in ascending order."
  def partitions(router, topic), do: GenServer.call(router, {:partitions, topic}, :infinity)

  @doc "The connection to a partition's leader."
  def conn_for(router, topic, partition),
    do: GenServer.call(router, {:conn_for, topic, partition}, :infinity)

  @doc "Sends a request to a partition's leader."
  def request(router, topic, partition, api_key, body, timeout \\ 60_000) do
    with {:ok, conn} <- conn_for(router, topic, partition) do
      Connection.request(conn, api_key, body, timeout)
    end
  end

  # -- metadata helpers --------------------------------------------------------

  def partitions_of(%{topics: topics}, topic) do
    case Map.get(topics, topic) do
      nil -> []
      infos -> infos |> Enum.map(& &1.partition) |> Enum.sort()
    end
  end

  def leader_of(%{topics: topics}, topic, partition) do
    with infos when is_list(infos) <- Map.get(topics, topic),
         %{leader: leader} <- Enum.find(infos, &(&1.partition == partition)) do
      leader
    else
      _ -> -1
    end
  end

  @doc false
  def decode_metadata(body) do
    # Field order is exactly the schema's: error_code, brokers,
    # controller_id, topics. The leading code is request-level (an
    # authorization denial, say); "no such topic" is per topic.
    rest = open_body(body)
    {code, rest} = r_int32(rest)
    if code != 0, do: throw({:server_error, server_error(code, "metadata")})

    {brokers, rest} =
      r_array(rest, fn data ->
        {node_id, data} = r_int32(data)
        {host, data} = r_string(data)
        {port, data} = r_int32(data)
        {rack, data} = r_string(data)
        {%{node_id: node_id, host: host, port: port, rack: rack}, data}
      end)

    {_controller_id, rest} = r_int32(rest)

    {topics, _rest} =
      r_array(rest, fn data ->
        {name, data} = r_string(data)
        {topic_error, data} = r_int32(data)

        {partitions, data} =
          r_array(data, fn data ->
            {partition, data} = r_int32(data)
            {leader, data} = r_int32(data)
            {replicas, data} = r_array(data, &r_int32/1)
            {isr, data} = r_array(data, &r_int32/1)
            {leader_epoch, data} = r_int32(data)

            {%{partition: partition, leader: leader, replicas: replicas, isr: isr,
               leader_epoch: leader_epoch}, data}
          end)

        if topic_error not in [0, 1],
          do: throw({:server_error, server_error(topic_error, "metadata for #{name}")})

        {{name, partitions}, data}
      end)

    %{brokers: brokers, topics: Map.new(topics, fn {n, ps} -> {n, ps} end)}
  end

  # -- Server -----------------------------------------------------------------

  @impl true
  def init({host, port, opts}) do
    Process.flag(:trap_exit, true)
    state = %{host: host, port: port, opts: opts, seed: nil, conns: %{}, metadata: nil}

    case dial(state, host, port) do
      {:ok, seed} -> {:ok, %{state | seed: seed}}
      {:error, error} -> {:stop, error}
    end
  end

  @impl true
  def handle_call(:seed, _from, state) do
    case ensure_seed(state) do
      {:ok, state} -> {:reply, {:ok, state.seed}, state}
      {:error, error} -> {:reply, {:error, error}, state}
    end
  end

  def handle_call({:metadata, topics, refresh}, _from, state) do
    case get_metadata(state, topics, refresh) do
      {:ok, metadata, state} -> {:reply, {:ok, metadata}, state}
      {:error, error, state} -> {:reply, {:error, error}, state}
    end
  end

  def handle_call({:partitions, topic}, _from, state) do
    with {:ok, metadata, state} <- get_metadata(state, [topic], false),
         {:ok, partitions, state} <- partitions_refreshing(state, metadata, topic) do
      {:reply, {:ok, partitions}, state}
    else
      {:error, error, state} -> {:reply, {:error, error}, state}
    end
  end

  def handle_call({:conn_for, topic, partition}, _from, state) do
    case do_conn_for(state, topic, partition) do
      {:ok, conn, state} -> {:reply, {:ok, conn}, state}
      {:error, error, state} -> {:reply, {:error, error}, state}
    end
  end

  @impl true
  def handle_info({:EXIT, pid, _reason}, state) do
    # A connection died (the broker closed it or a request failed). Forget
    # it; the next request to that broker redials.
    conns = state.conns |> Enum.reject(fn {_, c} -> c == pid end) |> Map.new()
    seed = if state.seed == pid, do: nil, else: state.seed
    {:noreply, %{state | conns: conns, seed: seed}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    [state.seed | Map.values(state.conns)]
    |> Enum.filter(&is_pid/1)
    |> Enum.uniq()
    |> Enum.each(&Connection.close/1)

    :ok
  end

  # -- internals ----------------------------------------------------------------

  defp dial(state, host, port) do
    Connection.start_link(host, port,
      client_id: Keyword.get(state.opts, :client_id, "brahmaputra-elixir"),
      connect_timeout: Keyword.get(state.opts, :connect_timeout, 30_000)
    )
    |> case do
      {:ok, pid} -> {:ok, pid}
      {:error, %Error{} = e} -> {:error, e}
      {:error, other} -> {:error, %Error{message: "connect failed: #{inspect(other)}"}}
    end
  end

  defp ensure_seed(%{seed: seed} = state) when is_pid(seed) do
    if Process.alive?(seed), do: {:ok, state}, else: ensure_seed(%{state | seed: nil})
  end

  defp ensure_seed(state) do
    case dial(state, state.host, state.port) do
      {:ok, seed} -> {:ok, %{state | seed: seed}}
      {:error, error} -> {:error, error}
    end
  end

  defp get_metadata(%{metadata: cached} = state, _topics, false) when cached != nil,
    do: {:ok, cached, state}

  defp get_metadata(state, topics, _refresh) do
    with {:ok, state} <- ensure_seed(state),
         {:ok, body} <- Connection.request(state.seed, api(:metadata), body([w_string_array(topics)])) do
      try do
        fresh = decode_metadata(body)
        # Merge, so a refresh for one topic does not forget the others.
        topics_map =
          if state.metadata, do: Map.merge(state.metadata.topics, fresh.topics), else: fresh.topics

        metadata = %{fresh | topics: topics_map}
        {:ok, metadata, %{state | metadata: metadata}}
      rescue
        e in Error -> {:error, e, state}
      catch
        {:server_error, e} -> {:error, e, state}
      end
    else
      {:error, error} -> {:error, error, state}
    end
  end

  defp partitions_refreshing(state, metadata, topic) do
    case partitions_of(metadata, topic) do
      [] ->
        # A topic auto-created on first reference is not in the cached image
        # yet; one refresh distinguishes "new" from "absent".
        with {:ok, metadata, state} <- get_metadata(state, [topic], true) do
          case partitions_of(metadata, topic) do
            [] -> {:error, %Error{message: "topic #{inspect(topic)} has no partitions"}, state}
            ps -> {:ok, ps, state}
          end
        end

      ps ->
        {:ok, ps, state}
    end
  end

  defp do_conn_for(state, topic, partition) do
    with {:ok, metadata, state} <- get_metadata(state, [topic], false),
         {:ok, leader, metadata, state} <- leader_refreshing(state, metadata, topic, partition) do
      case Map.get(state.conns, leader) do
        pid when is_pid(pid) ->
          if Process.alive?(pid),
            do: {:ok, pid, state},
            else: open_leader(%{state | conns: Map.delete(state.conns, leader)}, metadata, leader)

        nil ->
          open_leader(state, metadata, leader)
      end
    end
  end

  defp leader_refreshing(state, metadata, topic, partition) do
    case leader_of(metadata, topic, partition) do
      leader when leader >= 0 ->
        {:ok, leader, metadata, state}

      _ ->
        with {:ok, metadata, state} <- get_metadata(state, [topic], true) do
          case leader_of(metadata, topic, partition) do
            leader when leader >= 0 -> {:ok, leader, metadata, state}
            _ -> {:error, %Error{message: "no leader for #{topic}-#{partition}"}, state}
          end
        end
    end
  end

  defp open_leader(state, metadata, leader) do
    case Enum.find(metadata.brokers, &(&1.node_id == leader)) do
      nil ->
        {:error, %Error{message: "broker #{leader} is not in the metadata"}, state}

      # A single-broker cluster advertises the address it was configured
      # with, which may not be the one we dialled; reuse the seed rather
      # than opening a second connection to ourselves.
      _broker when length(metadata.brokers) == 1 ->
        case ensure_seed(state) do
          {:ok, state} -> {:ok, state.seed, %{state | conns: Map.put(state.conns, leader, state.seed)}}
          {:error, e} -> {:error, e, state}
        end

      broker ->
        case dial(state, broker.host, broker.port) do
          {:ok, conn} -> {:ok, conn, %{state | conns: Map.put(state.conns, leader, conn)}}
          {:error, e} -> {:error, e, state}
        end
    end
  end
end
