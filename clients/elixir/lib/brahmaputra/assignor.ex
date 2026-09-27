defmodule Brahmaputra.Assignor do
  @moduledoc """
  Partition assignment strategies, run by the group leader.

  Each takes `members` (a list of `%{id: member_id, topics: [topic]}`) and
  `topic_partitions` (`%{topic => [partition]}`) and returns
  `%{member_id => [{topic, partition}]}`.

    * `:range` — each member gets a contiguous range per topic; the first
      `partitions rem members` members take one extra.
    * `:roundrobin` — every partition dealt around the circle of members
      sorted by id.
    * `:sticky` — members keep what they held and only what balance requires
      moves. Prefer it when consumers carry per-partition state.

  These mirror the broker's Rust implementation exactly, because members
  computing assignments independently must agree.
  """

  def assign(:range, members, topic_partitions, _previous), do: range(members, topic_partitions)

  def assign(:roundrobin, members, topic_partitions, _previous),
    do: roundrobin(members, topic_partitions)

  def assign(:sticky, members, topic_partitions, previous),
    do: sticky(members, topic_partitions, previous)

  def assign(other, _, _, _), do: {:error, "unknown assignor #{inspect(other)}"}

  defp empty(members), do: Map.new(members, &{&1.id, []})

  defp subscribes?(member, topic), do: topic in member.topics

  def range(members, topic_partitions) do
    topic_partitions
    |> Map.keys()
    |> Enum.sort()
    |> Enum.reduce(empty(members), fn topic, acc ->
      partitions = topic_partitions[topic]
      subscribers = members |> Enum.filter(&subscribes?(&1, topic)) |> Enum.map(& &1.id) |> Enum.sort()

      case subscribers do
        [] ->
          acc

        _ ->
          base = div(length(partitions), length(subscribers))
          extra = rem(length(partitions), length(subscribers))

          {acc, _} =
            subscribers
            |> Enum.with_index()
            |> Enum.reduce({acc, partitions}, fn {id, index}, {acc, remaining} ->
              count = if index < extra, do: base + 1, else: base
              {taken, remaining} = Enum.split(remaining, count)
              {Map.update!(acc, id, &(&1 ++ Enum.map(taken, fn p -> {topic, p} end))), remaining}
            end)

          acc
      end
    end)
  end

  def roundrobin(members, topic_partitions) do
    circle = members |> Enum.sort_by(& &1.id) |> List.to_tuple()
    n = tuple_size(circle)

    if n == 0 do
      %{}
    else
      slots =
        for topic <- topic_partitions |> Map.keys() |> Enum.sort(),
            p <- topic_partitions[topic],
            do: {topic, p}

      {acc, _} =
        Enum.reduce(slots, {empty(members), 0}, fn {topic, _} = slot, {acc, cursor} ->
          deal(acc, circle, n, slot, topic, cursor, cursor)
        end)

      acc
    end
  end

  defp deal(acc, circle, n, slot, topic, start, cursor) do
    member = elem(circle, rem(cursor, n))
    cursor = cursor + 1

    cond do
      subscribes?(member, topic) -> {Map.update!(acc, member.id, &(&1 ++ [slot])), cursor}
      # nobody subscribes to this topic
      cursor - start >= n -> {acc, cursor}
      true -> deal(acc, circle, n, slot, topic, start, cursor)
    end
  end

  def sticky(members, topic_partitions, previous) do
    assignment = empty(members)
    by_id = Map.new(members, &{&1.id, &1})

    subscribes_id? = fn id, topic ->
      case by_id[id] do
        nil -> false
        m -> subscribes?(m, topic)
      end
    end

    if members == [] do
      assignment
    else
      previous_ids = previous |> Map.keys() |> Enum.sort()

      all_slots =
        for topic <- topic_partitions |> Map.keys() |> Enum.sort(),
            p <- topic_partitions[topic],
            do: {topic, p}

      {unassigned, claimed} =
        Enum.reduce(all_slots, {[], %{}}, fn {topic, _} = slot, {un, cl} ->
          holder =
            Enum.find(previous_ids, fn id ->
              slot in Map.get(previous, id, []) and subscribes_id?.(id, topic)
            end)

          if holder, do: {un, Map.put(cl, slot, holder)}, else: {[slot | un], cl}
        end)

      unassigned = Enum.reverse(unassigned)

      eligible =
        members
        |> Enum.filter(fn m -> Enum.any?(m.topics, &Map.has_key?(topic_partitions, &1)) end)
        |> Enum.map(& &1.id)
        |> Enum.sort()

      if eligible == [] do
        assignment
      else
        total = topic_partitions |> Map.values() |> Enum.map(&length/1) |> Enum.sum()
        base = div(total, length(eligible))
        extra = rem(total, length(eligible))

        quota =
          eligible
          |> Enum.with_index()
          |> Map.new(fn {id, i} -> {id, if(i < extra, do: base + 1, else: base)} end)

        {kept, unassigned} =
          claimed
          |> Map.keys()
          |> Enum.sort()
          |> Enum.reduce({%{}, unassigned}, fn slot, {kept, un} ->
            id = claimed[slot]
            held = Map.get(kept, id, [])

            if length(held) < Map.get(quota, id, 0),
              do: {Map.put(kept, id, held ++ [slot]), un},
              else: {kept, un ++ [slot]}
          end)

        assignment =
          Enum.reduce(kept, assignment, fn {id, held}, acc ->
            if Map.has_key?(acc, id), do: Map.put(acc, id, held), else: acc
          end)

        assignment =
          unassigned
          |> Enum.sort()
          |> Enum.reduce(assignment, fn {topic, _} = slot, acc ->
            taker =
              Enum.find(eligible, fn id ->
                subscribes_id?.(id, topic) and length(acc[id]) < quota[id]
              end) ||
                # Quotas exhausted (possible with uneven subscriptions): an
                # unassigned partition is a stalled partition, so fall back
                # to any subscribed member rather than dropping it.
                Enum.find(eligible, &subscribes_id?.(&1, topic))

            if taker, do: Map.update!(acc, taker, &(&1 ++ [slot])), else: acc
          end)

        Map.new(assignment, fn {id, slots} -> {id, Enum.sort(slots)} end)
      end
    end
  end
end
