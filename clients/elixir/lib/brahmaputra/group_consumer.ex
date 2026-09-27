defmodule Brahmaputra.GroupConsumer do
  @moduledoc """
  Shares a topic's partitions with the rest of its consumer group.

  A GenServer. Heartbeats run on a `Process.send_after` timer and, because
  a poll occupies the process, are also sent inline from the poll loop
  whenever one is due.

  Options (Kafka's names, as atoms):

    * `:client_id` — default `"brahmaputra-elixir"`
    * `:session_timeout_ms` — the coordinator evicts a member that stops
      heartbeating for this long (10 000; Kafka: 45 000)
    * `:rebalance_timeout_ms` — how long the coordinator waits for rejoins (3 000)
    * `:max_poll_interval_ms` — longest gap between polls before this member
      is presumed stuck and leaves the group (300 000)
    * `:enable_auto_commit` — default true
    * `:auto_commit_interval_ms` — default 5 000; 0 also disables auto-commit
    * `:auto_offset_reset` — `:earliest` (default), `:latest` or `:none`
    * `:partition_assignment_strategy` — `:range` (default), `:roundrobin`
      or `:sticky`
    * `:group_instance_id` — stable identity across restarts (static
      membership, KIP-345); nil for a dynamic member
    * `:max_poll_records` — default 500
    * `:fetch_max_bytes`, `:fetch_min_bytes`, `:fetch_max_wait_ms`,
      `:isolation_level`, `:client_rack`, `:connect_timeout` — as for
      `Brahmaputra.Consumer`
  """
  use GenServer

  alias Brahmaputra.{
    Assignor,
    ConsumedRecord,
    Consumer,
    Error,
    NoOffsetForPartitionError,
    Protocol,
    Router,
    ServerError,
    Util
  }

  import Protocol

  @offsets_topic "__consumer_offsets"
  @coordinator_attempts 4
  @join_attempts 4

  @defaults [
    client_id: "brahmaputra-elixir",
    session_timeout_ms: 10_000,
    rebalance_timeout_ms: 3_000,
    max_poll_interval_ms: 300_000,
    enable_auto_commit: true,
    auto_commit_interval_ms: 5_000,
    auto_offset_reset: :earliest,
    partition_assignment_strategy: :range,
    group_instance_id: nil,
    max_poll_records: 500,
    fetch_max_bytes: 8 * 1024 * 1024,
    fetch_min_bytes: 1,
    fetch_max_wait_ms: 500,
    isolation_level: :read_uncommitted,
    client_rack: "",
    connect_timeout: 30_000
  ]

  def default_config, do: Map.new(@defaults)

  # -- API --------------------------------------------------------------------

  @doc "Connects to a seed broker and starts the heartbeat timer."
  def start_link(host, port, group_id, opts \\ []),
    do: Util.start_linked(__MODULE__, {host, port, group_id, opts})

  @doc "Sets the topics this member wants a share of."
  def subscribe(group, topics), do: GenServer.call(group, {:subscribe, topics}, :infinity)

  @doc """
  Returns `{:ok, records}` with up to `max_poll_records` records, joining the
  group first if needed. Waits up to `timeout_ms` when nothing is available.
  """
  def poll(group, timeout_ms), do: GenServer.call(group, {:poll, timeout_ms}, :infinity)

  @doc """
  Commits the delivered positions. At-least-once: call it after processing,
  not before.
  """
  def commit(group), do: GenServer.call(group, :commit, :infinity)

  @doc """
  The group's committed offsets as `%{{topic, partition} => offset}`. An
  empty list asks for every partition the group holds.
  """
  def committed(group, partitions \\ []),
    do: GenServer.call(group, {:committed, partitions}, :infinity)

  @doc "This member's current `[{topic, partition}]`."
  def assignment(group), do: GenServer.call(group, :assignment, :infinity)

  @doc """
  Commits, leaves the group, then stops.

  Leaving is what separates a clean shutdown from a crash: without it the
  coordinator must wait out the session timeout before reassigning.
  """
  def close(group) do
    GenServer.call(group, :close, :infinity)
  catch
    :exit, _ -> :ok
  end

  # -- Server -----------------------------------------------------------------

  @impl true
  def init({host, port, group_id, opts}) do
    config = Map.merge(default_config(), Map.new(opts))

    consumer_opts =
      Map.take(config, [
        :client_id,
        :fetch_max_bytes,
        :fetch_min_bytes,
        :fetch_max_wait_ms,
        :max_poll_records,
        :isolation_level,
        :client_rack,
        :connect_timeout
      ])
      |> Map.to_list()

    case Consumer.connect(host, port, consumer_opts) do
      {:ok, consumer} ->
        # The timer enforces two independent deadlines, so it wakes often
        # enough for the shorter of them.
        interval =
          max(1, min(div(config.session_timeout_ms, 3), div(config.max_poll_interval_ms, 3)))

        state = %{
          group_id: group_id,
          config: config,
          consumer: consumer,
          subscribed: [],
          member_id: "",
          generation: -1,
          joined: false,
          assignment: [],
          # Next offset to *deliver*, which is what gets committed.
          positions: %{},
          # Next offset to *fetch*; runs ahead of positions by exactly the
          # records sitting in `buffered`.
          fetch_positions: %{},
          buffered: [],
          last_poll_ms: Util.mono_ms(),
          last_commit_ms: Util.mono_ms(),
          last_heartbeat_ms: Util.mono_ms(),
          tick_ms: interval,
          left_for_slow_poll: false
        }

        Process.send_after(self(), :tick, interval)
        {:ok, state}

      {:error, error} ->
        {:stop, error}
    end
  end

  @impl true
  def handle_call({:subscribe, topics}, _from, state) do
    {:reply, :ok, %{state | subscribed: topics, joined: false}}
  end

  def handle_call({:poll, timeout_ms}, _from, state) do
    {result, state} = do_poll(state, timeout_ms)
    # Stamped on entry and again on return, and never enforced in between
    # (the timer's message waits while a poll runs): the interval bounds how
    # long the *application* goes without asking for records, and a poll
    # that blocks — for its timeout, or on a slow rebalance — is the
    # consumer working normally.
    {:reply, result, %{state | last_poll_ms: Util.mono_ms()}}
  end

  def handle_call(:commit, _from, state) do
    {result, state} = do_commit(state)
    {:reply, result, state}
  end

  def handle_call({:committed, partitions}, _from, state) do
    {:reply, fetch_committed(state, partitions), state}
  end

  def handle_call(:assignment, _from, state), do: {:reply, state.assignment, state}

  def handle_call(:close, _from, state) do
    state = if state.joined, do: elem(do_commit(state), 1), else: state
    # Best effort: failing here costs only the session timeout.
    state = if state.member_id != "", do: elem(leave(state), 1), else: state
    Consumer.close(state.consumer)
    {:stop, :normal, :ok, state}
  end

  @impl true
  def handle_info(:tick, state) do
    Process.send_after(self(), :tick, state.tick_ms)
    {:noreply, heartbeat_tick(state)}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # -- poll ---------------------------------------------------------------------

  defp do_poll(%{subscribed: []} = state, _timeout),
    do: {{:error, %Error{message: "subscribe to at least one topic before polling"}}, state}

  defp do_poll(state, timeout_ms) do
    state = %{state | last_poll_ms: Util.mono_ms(), left_for_slow_poll: false}
    deadline = Util.mono_ms() + timeout_ms

    case ensure_joined(state) do
      {:ok, state} -> poll_loop(state, deadline)
      {:error, error, state} -> {{:error, error}, state}
    end
  end

  defp ensure_joined(%{joined: true} = state), do: {:ok, state}
  defp ensure_joined(state), do: join(state, @join_attempts)

  defp poll_loop(state, deadline) do
    state = maybe_heartbeat(state)

    with {:ok, state} <- ensure_joined(state) do
      cond do
        state.buffered != [] ->
          take_buffered(state)

        state.assignment == [] ->
          if Util.mono_ms() >= deadline do
            {{:ok, []}, state}
          else
            Process.sleep(50)
            poll_loop(state, deadline)
          end

        true ->
          case fetch_assigned(state, deadline) do
            {:ok, got_any, state} ->
              state = maybe_auto_commit(state)

              cond do
                state.buffered != [] -> take_buffered(state)
                not got_any and Util.mono_ms() >= deadline -> {{:ok, []}, state}
                true -> poll_loop(state, deadline)
              end

            {:error, error, state} ->
              {{:error, error}, state}
          end
      end
    else
      {:error, error, state} -> {{:error, error}, state}
    end
  end

  defp fetch_assigned(state, deadline) do
    Enum.reduce_while(state.assignment, {:ok, false, state}, fn {topic, partition} = slot,
                                                                 {:ok, got_any, state} ->
      wait = deadline |> Kernel.-(Util.mono_ms()) |> max(0) |> min(500)
      offset = Map.fetch!(state.fetch_positions, slot)

      case Consumer.fetch(state.consumer, topic, partition, offset, wait) do
        {:ok, []} ->
          {:cont, {:ok, got_any, state}}

        {:ok, records} ->
          state = %{
            state
            | fetch_positions: Map.put(state.fetch_positions, slot, List.last(records).offset + 1),
              buffered: state.buffered ++ records
          }

          {:cont, {:ok, true, state}}

        {:error, %ServerError{code: 2}} ->
          # The committed offset fell off the log; restart where the policy says.
          case reset_offset(state, topic, partition) do
            {:ok, reset} ->
              {:cont,
               {:ok, got_any,
                %{
                  state
                  | fetch_positions: Map.put(state.fetch_positions, slot, reset),
                    positions: Map.put(state.positions, slot, reset)
                }}}

            {:error, error} ->
              {:halt, {:error, error, state}}
          end

        {:error, %ServerError{code: 6}} ->
          Router.refresh(state.consumer.router, topic)
          {:cont, {:ok, got_any, state}}

        {:error, error} ->
          {:halt, {:error, error, state}}
      end
    end)
  end

  defp take_buffered(state) do
    limit = state.config.max_poll_records
    limit = if limit <= 0, do: length(state.buffered), else: limit
    {delivered, rest} = Enum.split(state.buffered, limit)

    # The consumed position advances only over records actually handed to
    # the caller; committing what was merely fetched would skip records
    # nobody processed.
    positions =
      Enum.reduce(delivered, state.positions, fn %ConsumedRecord{} = r, acc ->
        Map.put(acc, {r.topic, r.partition}, r.offset + 1)
      end)

    {{:ok, delivered}, %{state | buffered: rest, positions: positions}}
  end

  defp reset_offset(state, topic, partition) do
    case state.config.auto_offset_reset do
      reset when reset in [:earliest, "earliest"] ->
        Consumer.list_offsets(state.consumer, topic, partition, :earliest)

      reset when reset in [:latest, "latest"] ->
        Consumer.list_offsets(state.consumer, topic, partition, :latest)

      reset when reset in [:none, "none"] ->
        {:error, %NoOffsetForPartitionError{topic: topic, partition: partition}}

      other ->
        {:error, %Error{message: "unknown auto.offset.reset #{inspect(other)}"}}
    end
  end

  # -- commits ------------------------------------------------------------------

  defp do_commit(%{positions: positions} = state) when map_size(positions) == 0,
    do: {:ok, state}

  defp do_commit(state) do
    slots = state.positions |> Map.keys() |> Enum.sort()

    req =
      body([
        w_string(state.group_id),
        w_int32(state.generation),
        w_string(state.member_id),
        w_int32(length(slots))
        | Enum.map(slots, fn {t, p} = slot ->
            [w_string(t), w_int32(p), w_int64(state.positions[slot])]
          end)
      ])

    with {:ok, resp} <- coordinator_request(state, api(:offset_commit), req),
         {:ok, code} <- Consumer.guard(fn -> {:ok, resp |> open_body() |> r_int32() |> elem(0)} end) do
      case code do
        0 ->
          {:ok, %{state | last_commit_ms: Util.mono_ms()}}

        # Generation fencing: this member's view of the group is stale. The
        # commit is refused and the next poll rejoins.
        code when code in [13, 14, 16] ->
          {{:error, server_error(code, "offset_commit")}, %{state | joined: false}}

        code ->
          {{:error, server_error(code, "offset_commit")}, state}
      end
    else
      {:error, error} -> {{:error, error}, state}
    end
  end

  defp fetch_committed(state, partitions) do
    req =
      body([
        w_string(state.group_id),
        w_int32(length(partitions))
        | Enum.map(partitions, fn {t, p} -> [w_string(t), w_int32(p)] end)
      ])

    with {:ok, resp} <- coordinator_request(state, api(:offset_fetch), req) do
      Consumer.guard(fn ->
        {code, rest} = resp |> open_body() |> r_int32()
        if code != 0, do: throw({:server_error, server_error(code, "offset_fetch")})

        {entries, _} =
          r_array(rest, fn data ->
            {t, data} = r_string(data)
            {p, data} = r_int32(data)
            {o, data} = r_int64(data)
            {{{t, p}, o}, data}
          end)

        {:ok, Map.new(entries)}
      end)
    end
  end

  defp maybe_auto_commit(state) do
    interval = state.config.auto_commit_interval_ms

    if state.config.enable_auto_commit and interval > 0 and map_size(state.positions) > 0 and
         Util.mono_ms() - state.last_commit_ms >= interval do
      # A failed auto-commit is retried on the next poll; the explicit
      # commit/1 is what a caller relies on.
      elem(do_commit(state), 1)
    else
      state
    end
  end

  # -- membership ---------------------------------------------------------------

  defp join(state, 0),
    do:
      {:error,
       %Error{message: "consumer group failed to stabilise after #{@join_attempts} join attempts"},
       state}

  defp join(state, attempts) do
    req =
      body([
        w_string(state.group_id),
        w_int32(state.config.session_timeout_ms),
        w_int32(state.config.rebalance_timeout_ms),
        w_string(state.member_id),
        w_string_array(state.subscribed),
        w_string(state.config.group_instance_id || "")
      ])

    with {:ok, resp} <- coordinator_request(state, api(:join_group), req),
         {:ok, decoded} <- Consumer.guard(fn -> {:ok, decode_join(resp)} end) do
      case decoded do
        {14, _} ->
          Process.sleep(100)
          join(state, attempts - 1)

        # The coordinator dropped this member (session expiry, or removed
        # while it waited): join again as a new one.
        {13, _} ->
          join(%{state | member_id: ""}, attempts - 1)

        {0, {generation, member_id, leader_id, members}} ->
          state = %{state | member_id: member_id, generation: generation}

          assignments =
            if member_id == leader_id, do: lead(state, members), else: {:ok, []}

          with {:ok, assignments} <- assignments,
               {:ok, synced, state} <- sync(state, assignments) do
            if synced,
              do: {:ok, %{state | joined: true, last_heartbeat_ms: Util.mono_ms()}},
              else: join(state, attempts - 1)
          else
            {:error, error} -> {:error, error, state}
            {:error, error, state} -> {:error, error, state}
          end

        {code, _} ->
          {:error, server_error(code, "join_group"), state}
      end
    else
      {:error, error} -> {:error, error, state}
    end
  end

  defp decode_join(resp) do
    {code, rest} = resp |> open_body() |> r_int32()

    if code != 0 do
      {code, nil}
    else
      {generation, rest} = r_int32(rest)
      {member_id, rest} = r_string(rest)
      {leader_id, rest} = r_string(rest)

      {members, _} =
        r_array(rest, fn data ->
          {id, data} = r_string(data)
          {topics, data} = r_string_array(data)

          {held, data} =
            r_array(data, fn d ->
              {t, d} = r_string(d)
              {p, d} = r_int32(d)
              {{t, p}, d}
            end)

          {%{id: id, topics: topics, held: held}, data}
        end)

      {0, {generation, member_id, leader_id, members}}
    end
  end

  defp lead(state, members) do
    topics = members |> Enum.flat_map(& &1.topics) |> Enum.uniq()

    topic_partitions =
      Enum.reduce_while(topics, {:ok, %{}}, fn topic, {:ok, acc} ->
        case Router.partitions(state.consumer.router, topic) do
          {:ok, ps} -> {:cont, {:ok, Map.put(acc, topic, ps)}}
          {:error, e} -> {:halt, {:error, e}}
        end
      end)

    with {:ok, topic_partitions} <- topic_partitions do
      previous = Map.new(members, &{&1.id, &1.held})
      strategy = normalize_strategy(state.config.partition_assignment_strategy)

      case Assignor.assign(strategy, members, topic_partitions, previous) do
        {:error, message} -> {:error, %Error{message: message}}
        assignment -> {:ok, assignment |> Enum.sort_by(&elem(&1, 0))}
      end
    end
  end

  defp normalize_strategy(s) when is_binary(s), do: String.to_atom(s)
  defp normalize_strategy(s), do: s

  defp sync(state, assignments) do
    req =
      body([
        w_string(state.group_id),
        w_int32(state.generation),
        w_string(state.member_id),
        w_int32(length(assignments))
        | Enum.map(assignments, fn {member, slots} ->
            [
              w_string(member),
              w_int32(length(slots))
              | Enum.map(slots, fn {t, p} -> [w_string(t), w_int32(p)] end)
            ]
          end)
      ])

    with {:ok, resp} <- coordinator_request(state, api(:sync_group), req),
         {:ok, decoded} <-
           Consumer.guard(fn ->
             {code, rest} = resp |> open_body() |> r_int32()

             if code == 0 do
               {slots, _} =
                 r_array(rest, fn d ->
                   {t, d} = r_string(d)
                   {p, d} = r_int32(d)
                   {{t, p}, d}
                 end)

               {:ok, {0, slots}}
             else
               {:ok, {code, nil}}
             end
           end) do
      case decoded do
        {code, _} when code in [14, 16] ->
          {:ok, false, state}

        {13, _} ->
          {:ok, false, %{state | member_id: ""}}

        {0, slots} ->
          case apply_assignment(state, slots) do
            {:ok, state} -> {:ok, true, state}
            {:error, error, state} -> {:error, error, state}
          end

        {code, _} ->
          {:error, server_error(code, "sync_group"), state}
      end
    else
      {:error, error} -> {:error, error, state}
    end
  end

  defp apply_assignment(state, assignment) do
    owned = MapSet.new(assignment)
    positions = state.positions |> Enum.filter(fn {slot, _} -> slot in owned end) |> Map.new()
    # Buffered records were never delivered, so a new assignment drops them.
    state = %{state | assignment: assignment, positions: positions, buffered: []}
    needed = Enum.reject(assignment, &Map.has_key?(positions, &1))

    result =
      if needed == [] do
        {:ok, positions}
      else
        with {:ok, committed} <- fetch_committed(state, needed) do
          Enum.reduce_while(needed, {:ok, positions}, fn {t, p} = slot, {:ok, acc} ->
            case Map.get(committed, slot) do
              offset when is_integer(offset) and offset >= 0 ->
                {:cont, {:ok, Map.put(acc, slot, offset)}}

              _ ->
                case reset_offset(state, t, p) do
                  {:ok, offset} -> {:cont, {:ok, Map.put(acc, slot, offset)}}
                  {:error, e} -> {:halt, {:error, e}}
                end
            end
          end)
        end
      end

    case result do
      {:ok, positions} -> {:ok, %{state | positions: positions, fetch_positions: positions}}
      {:error, error} -> {:error, error, state}
    end
  end

  defp leave(state) do
    req = body([w_string(state.group_id), w_string(state.member_id)])

    with {:ok, resp} <- coordinator_request(state, api(:leave_group), req),
         {:ok, code} <- Consumer.guard(fn -> {:ok, resp |> open_body() |> r_int32() |> elem(0)} end) do
      state = %{state | joined: false}
      if code == 0, do: {:ok, state}, else: {{:error, server_error(code, "leave_group")}, state}
    else
      {:error, error} -> {{:error, error}, state}
    end
  end

  # -- heartbeat ----------------------------------------------------------------

  defp heartbeat_every(state), do: max(1, div(state.config.session_timeout_ms, 3))

  defp maybe_heartbeat(state) do
    if state.joined and state.member_id != "" and
         Util.mono_ms() - state.last_heartbeat_ms >= heartbeat_every(state),
       do: heartbeat(state),
       else: state
  end

  defp heartbeat_tick(state) do
    idle = Util.mono_ms() - state.last_poll_ms

    cond do
      not state.joined or state.member_id == "" ->
        state

      idle >= state.config.max_poll_interval_ms ->
        # The application has stopped consuming though the process is alive.
        # Heartbeating on would hold its partitions away from a consumer that
        # could make progress, so leave instead.
        if state.left_for_slow_poll do
          state
        else
          {_, state} = leave(state)
          %{state | left_for_slow_poll: true, joined: false, member_id: ""}
        end

      Util.mono_ms() - state.last_heartbeat_ms >= heartbeat_every(state) ->
        heartbeat(state)

      true ->
        state
    end
  end

  defp heartbeat(state) do
    req = body([w_string(state.group_id), w_int32(state.generation), w_string(state.member_id)])
    state = %{state | last_heartbeat_ms: Util.mono_ms()}

    case coordinator_request(state, api(:heartbeat), req) do
      {:ok, resp} ->
        # A rebalance, an eviction or a newer generation: rejoin on next poll.
        if peek_error_code(resp) in [13, 14, 16], do: %{state | joined: false}, else: state

      # transient: retry next tick
      {:error, _} ->
        state
    end
  end

  # -- coordinator routing --------------------------------------------------------

  # Sends to the group's coordinator, following moves and waiting out loads.
  defp coordinator_request(state, api_key, req), do: coordinator_request(state, api_key, req, 0)

  defp coordinator_request(_state, _api_key, _req, @coordinator_attempts),
    do:
      {:error,
       %Error{message: "group coordinator unavailable after #{@coordinator_attempts} attempts"}}

  defp coordinator_request(state, api_key, req, attempt) do
    router = state.consumer.router

    with {:ok, partitions} <- Router.partitions(router, @offsets_topic),
         partition = rem(crc32c(state.group_id), length(partitions)),
         {:ok, resp} <- Router.request(router, @offsets_topic, partition, api_key, req) do
      case peek_error_code(resp) do
        17 ->
          Process.sleep(100)
          coordinator_request(state, api_key, req, attempt + 1)

        code when code in [15, 6] ->
          Router.refresh(router, @offsets_topic)
          coordinator_request(state, api_key, req, attempt + 1)

        _ ->
          {:ok, resp}
      end
    end
  end
end
