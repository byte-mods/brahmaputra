/**
 * Partition assignors run by the group leader: range, roundrobin, sticky.
 *
 * Each mirrors the Rust implementation exactly, because members computing
 * an assignment independently must agree.
 */
module brahmaputra.assignor;

import brahmaputra.protocol : BrahmaputraException, TopicPartition;

import std.algorithm.searching : canFind;
import std.algorithm.sorting : sort;

/// Kafka's `partition.assignment.strategy` names.
enum string ASSIGNOR_RANGE = "range";
/// ditto
enum string ASSIGNOR_ROUND_ROBIN = "roundrobin";
/// Keeps members on the partitions they already hold; prefer it when
/// consumers carry per-partition state.
enum string ASSIGNOR_STICKY = "sticky";

/// One group member as the assignor sees it.
struct AssignorMember
{
    string id;
    string[] topics;

    bool subscribes(string topic) const
    {
        return topics.canFind(topic);
    }
}

/// A member's share of the partitions.
struct MemberAssignment
{
    string memberId;
    TopicPartition[] partitions;
}

/// Runs the named assignor and returns assignments sorted by member id.
MemberAssignment[] computeAssignment(string assignor, AssignorMember[] members,
    int[][string] topicPartitions, TopicPartition[][string] previous)
{
    TopicPartition[][string] assignment;
    switch (assignor)
    {
    case ASSIGNOR_RANGE:
        assignment = rangeAssign(members, topicPartitions);
        break;
    case ASSIGNOR_ROUND_ROBIN:
        assignment = roundRobinAssign(members, topicPartitions);
        break;
    case ASSIGNOR_STICKY:
        assignment = stickyAssign(members, topicPartitions, previous);
        break;
    default:
        throw new BrahmaputraException("unknown assignor " ~ assignor);
    }
    auto ids = assignment.keys;
    ids.sort();
    MemberAssignment[] out_;
    foreach (id; ids)
        out_ ~= MemberAssignment(id, assignment[id]);
    return out_;
}

private TopicPartition[][string] emptyAssignment(const AssignorMember[] members)
{
    TopicPartition[][string] out_;
    foreach (ref member; members)
        out_[member.id] = null;
    return out_;
}

private string[] sortedTopics(int[][string] topicPartitions)
{
    auto topics = topicPartitions.keys;
    topics.sort();
    return topics;
}

/// Each subscribed member gets a contiguous range per topic; the first
/// (partitions % members) members take one extra.
TopicPartition[][string] rangeAssign(AssignorMember[] members, int[][string] topicPartitions)
{
    auto assignment = emptyAssignment(members);
    foreach (topic; sortedTopics(topicPartitions))
    {
        auto partitions = topicPartitions[topic];
        string[] subscribers;
        foreach (ref member; members)
            if (member.subscribes(topic))
                subscribers ~= member.id;
        subscribers.sort();
        if (subscribers.length == 0)
            continue;
        const base = partitions.length / subscribers.length;
        const extra = partitions.length % subscribers.length;
        size_t cursor = 0;
        foreach (index, memberId; subscribers)
        {
            const count = base + (index < extra ? 1 : 0);
            foreach (partition; partitions[cursor .. cursor + count])
                assignment[memberId] ~= TopicPartition(topic, partition);
            cursor += count;
        }
    }
    return assignment;
}

/// Deals every partition around the circle of members sorted by id,
/// skipping members not subscribed to a partition's topic.
TopicPartition[][string] roundRobinAssign(AssignorMember[] members,
    int[][string] topicPartitions)
{
    auto assignment = emptyAssignment(members);
    auto circle = members.dup;
    circle.sort!((a, b) => a.id < b.id);
    if (circle.length == 0)
        return assignment;
    size_t cursor = 0;
    foreach (topic; sortedTopics(topicPartitions))
    {
        foreach (partition; topicPartitions[topic])
        {
            const start = cursor;
            while (true)
            {
                auto member = circle[cursor % circle.length];
                cursor++;
                if (member.subscribes(topic))
                {
                    assignment[member.id] ~= TopicPartition(topic, partition);
                    break;
                }
                if (cursor - start >= circle.length)
                    break; // nobody subscribes to this topic
            }
        }
    }
    return assignment;
}

/**
 * Keeps members on what they hold and moves only what balance requires.
 * Partitions are compared as (topic, integer partition), never as strings.
 */
TopicPartition[][string] stickyAssign(AssignorMember[] members,
    int[][string] topicPartitions, TopicPartition[][string] previous)
{
    auto assignment = emptyAssignment(members);
    if (members.length == 0)
        return assignment;

    bool subscribes(string memberId, string topic)
    {
        foreach (ref member; members)
            if (member.id == memberId)
                return member.subscribes(topic);
        return false;
    }

    auto previousIds = previous.keys;
    previousIds.sort();

    TopicPartition[] unassigned;
    string[TopicPartition] claimed;
    foreach (topic; sortedTopics(topicPartitions))
    {
        foreach (partition; topicPartitions[topic])
        {
            const slot = TopicPartition(topic, partition);
            string holder;
            outer: foreach (memberId; previousIds)
            {
                foreach (ref held; previous[memberId])
                {
                    if (held == slot && subscribes(memberId, topic))
                    {
                        holder = memberId;
                        break outer;
                    }
                }
            }
            if (holder.length == 0)
                unassigned ~= slot;
            else
                claimed[slot] = holder;
        }
    }

    string[] eligible;
    foreach (ref member; members)
    {
        foreach (topic; member.topics)
        {
            if (topic in topicPartitions)
            {
                eligible ~= member.id;
                break;
            }
        }
    }
    eligible.sort();
    if (eligible.length == 0)
        return assignment;

    size_t total = 0;
    foreach (partitions; topicPartitions.byValue)
        total += partitions.length;
    const base = total / eligible.length;
    const extra = total % eligible.length;
    size_t[string] quota;
    foreach (index, memberId; eligible)
        quota[memberId] = base + (index < extra ? 1 : 0);

    auto claimedSlots = claimed.keys;
    claimedSlots.sort();

    TopicPartition[][string] kept;
    foreach (slot; claimedSlots)
    {
        const memberId = claimed[slot];
        if (kept.get(memberId, null).length < quota.get(memberId, 0))
            kept[memberId] ~= slot;
        else
            unassigned ~= slot;
    }
    foreach (memberId, held; kept)
        if (memberId in assignment)
            assignment[memberId] = held;

    unassigned.sort();
    foreach (slot; unassigned)
    {
        string taker;
        foreach (memberId; eligible)
        {
            if (subscribes(memberId, slot.topic)
                    && assignment[memberId].length < quota[memberId])
            {
                taker = memberId;
                break;
            }
        }
        if (taker.length == 0)
        {
            // Quotas exhausted (uneven subscriptions): an unassigned
            // partition is a stalled one, so any subscriber takes it.
            foreach (memberId; eligible)
            {
                if (subscribes(memberId, slot.topic))
                {
                    taker = memberId;
                    break;
                }
            }
        }
        if (taker.length > 0)
            assignment[taker] ~= slot;
    }

    foreach (memberId; assignment.keys)
        assignment[memberId].sort();
    return assignment;
}

unittest
{
    auto members = [AssignorMember("a", ["t"]), AssignorMember("b", ["t"])];
    int[][string] tp = ["t": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]];
    auto r = rangeAssign(members, tp);
    assert(r["a"].length == 6 && r["b"].length == 5);
    auto rr = roundRobinAssign(members, tp);
    assert(rr["a"][1] == TopicPartition("t", 2));
    // Sticky keeps what b held, and orders partition 10 after 9 (integer,
    // not string, comparison).
    TopicPartition[][string] previous = ["b": [TopicPartition("t", 10), TopicPartition("t", 9)]];
    auto s = stickyAssign(members, tp, previous);
    assert(s["b"].canFind(TopicPartition("t", 10)) && s["b"].canFind(TopicPartition("t", 9)));
    assert(s["b"][$ - 1] == TopicPartition("t", 10));
    assert(s["a"].length + s["b"].length == 11);
}
