using System;
using System.Collections.Generic;
using System.Linq;

namespace Brahmaputra;

/// <summary><c>partition.assignment.strategy</c>.</summary>
public enum PartitionAssignmentStrategy
{
    /// <summary>Each member gets a contiguous range of each topic's partitions.</summary>
    Range,

    /// <summary>Partitions are dealt around the members one at a time.</summary>
    RoundRobin,

    /// <summary>
    /// Members keep the partitions they already hold and only what balance
    /// requires moves. Prefer it when consumers carry per-partition state.
    /// </summary>
    Sticky,
}

/// <summary>One group member as the leader sees it when assigning.</summary>
/// <param name="MemberId">The member id.</param>
/// <param name="Topics">Topics it subscribes to.</param>
/// <param name="Owned">Partitions it held in the previous generation.</param>
public sealed record GroupMemberSubscription(string MemberId, IReadOnlyList<string> Topics, IReadOnlyList<TopicPartition> Owned);

/// <summary>
/// The assignment algorithms. Every member computes identically, so these
/// mirror the Rust client exactly.
/// </summary>
public static class PartitionAssignors
{
    /// <summary>Runs a strategy over members and each topic's partition ids.</summary>
    public static Dictionary<string, List<TopicPartition>> Assign(
        PartitionAssignmentStrategy strategy,
        IReadOnlyList<GroupMemberSubscription> members,
        IReadOnlyDictionary<string, IReadOnlyList<int>> topicPartitions) => strategy switch
        {
            PartitionAssignmentStrategy.Range => Range(members, topicPartitions),
            PartitionAssignmentStrategy.RoundRobin => RoundRobin(members, topicPartitions),
            PartitionAssignmentStrategy.Sticky => Sticky(members, topicPartitions),
            _ => throw new ArgumentOutOfRangeException(nameof(strategy)),
        };

    private static Dictionary<string, List<TopicPartition>> Empty(IEnumerable<GroupMemberSubscription> members) =>
        members.ToDictionary(m => m.MemberId, _ => new List<TopicPartition>());

    private static List<string> SortedTopics(IReadOnlyDictionary<string, IReadOnlyList<int>> topicPartitions) =>
        topicPartitions.Keys.OrderBy(t => t, StringComparer.Ordinal).ToList();

    /// <summary>
    /// Each subscribed member gets a contiguous range per topic; the first
    /// (partitions % members) members take one extra.
    /// </summary>
    public static Dictionary<string, List<TopicPartition>> Range(
        IReadOnlyList<GroupMemberSubscription> members, IReadOnlyDictionary<string, IReadOnlyList<int>> topicPartitions)
    {
        var assignment = Empty(members);
        foreach (string topic in SortedTopics(topicPartitions))
        {
            var partitions = topicPartitions[topic];
            var subscribers = members.Where(m => m.Topics.Contains(topic)).Select(m => m.MemberId)
                .OrderBy(id => id, StringComparer.Ordinal).ToList();
            if (subscribers.Count == 0) continue;
            int baseCount = partitions.Count / subscribers.Count;
            int extra = partitions.Count % subscribers.Count;
            int cursor = 0;
            for (int index = 0; index < subscribers.Count; index++)
            {
                int count = baseCount + (index < extra ? 1 : 0);
                for (int i = cursor; i < cursor + count; i++)
                    assignment[subscribers[index]].Add(new TopicPartition(topic, partitions[i]));
                cursor += count;
            }
        }
        return assignment;
    }

    /// <summary>
    /// Deals every partition around the circle of members sorted by id,
    /// skipping members not subscribed to a partition's topic.
    /// </summary>
    public static Dictionary<string, List<TopicPartition>> RoundRobin(
        IReadOnlyList<GroupMemberSubscription> members, IReadOnlyDictionary<string, IReadOnlyList<int>> topicPartitions)
    {
        var assignment = Empty(members);
        var circle = members.OrderBy(m => m.MemberId, StringComparer.Ordinal).ToList();
        if (circle.Count == 0) return assignment;
        int cursor = 0;
        foreach (string topic in SortedTopics(topicPartitions))
        {
            foreach (int partition in topicPartitions[topic])
            {
                int start = cursor;
                while (true)
                {
                    var member = circle[cursor % circle.Count];
                    cursor++;
                    if (member.Topics.Contains(topic))
                    {
                        assignment[member.MemberId].Add(new TopicPartition(topic, partition));
                        break;
                    }
                    if (cursor - start >= circle.Count) break; // nobody subscribes
                }
            }
        }
        return assignment;
    }

    /// <summary>Keeps members on what they hold and moves only what balance requires.</summary>
    public static Dictionary<string, List<TopicPartition>> Sticky(
        IReadOnlyList<GroupMemberSubscription> members, IReadOnlyDictionary<string, IReadOnlyList<int>> topicPartitions)
    {
        var assignment = Empty(members);
        if (members.Count == 0) return assignment;
        var byId = members.ToDictionary(m => m.MemberId);
        bool Subscribes(string memberId, string topic) => byId.TryGetValue(memberId, out var m) && m.Topics.Contains(topic);

        var previousIds = members.Select(m => m.MemberId).OrderBy(id => id, StringComparer.Ordinal).ToList();
        var unassigned = new List<TopicPartition>();
        var claimed = new Dictionary<TopicPartition, string>();
        foreach (string topic in SortedTopics(topicPartitions))
        {
            foreach (int partition in topicPartitions[topic])
            {
                var slot = new TopicPartition(topic, partition);
                string? holder = previousIds.FirstOrDefault(id => byId[id].Owned.Contains(slot) && Subscribes(id, topic));
                if (holder == null) unassigned.Add(slot);
                else claimed[slot] = holder;
            }
        }

        var eligible = members.Where(m => m.Topics.Any(topicPartitions.ContainsKey)).Select(m => m.MemberId)
            .OrderBy(id => id, StringComparer.Ordinal).ToList();
        if (eligible.Count == 0) return assignment;

        int total = topicPartitions.Values.Sum(p => p.Count);
        int baseQuota = total / eligible.Count;
        int extra = total % eligible.Count;
        var quota = new Dictionary<string, int>();
        for (int index = 0; index < eligible.Count; index++) quota[eligible[index]] = baseQuota + (index < extra ? 1 : 0);

        var kept = new Dictionary<string, List<TopicPartition>>();
        foreach (var slot in claimed.Keys.OrderBy(s => s))
        {
            string memberId = claimed[slot];
            if (!kept.TryGetValue(memberId, out var list)) kept[memberId] = list = new List<TopicPartition>();
            if (list.Count < quota.GetValueOrDefault(memberId)) list.Add(slot);
            else unassigned.Add(slot);
        }
        foreach (var (memberId, held) in kept)
            if (assignment.ContainsKey(memberId)) assignment[memberId] = held;

        unassigned.Sort();
        foreach (var slot in unassigned)
        {
            string? taker = eligible.FirstOrDefault(id => Subscribes(id, slot.Topic) && assignment[id].Count < quota[id]);
            // Quotas exhausted (possible with uneven subscriptions): an
            // unassigned partition is a stalled partition, so fall back to any
            // subscribed member rather than dropping it.
            taker ??= eligible.FirstOrDefault(id => Subscribes(id, slot.Topic));
            if (taker != null) assignment[taker].Add(slot);
        }

        foreach (var list in assignment.Values) list.Sort();
        return assignment;
    }
}
