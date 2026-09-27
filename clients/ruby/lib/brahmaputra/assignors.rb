# frozen_string_literal: true

module Brahmaputra
  TopicPartition = Struct.new(:topic, :partition) do
    include Comparable

    def <=>(other) = [topic, partition] <=> [other.topic, other.partition]
    def to_s = "#{topic}-#{partition}"
  end

  # Partition assignment strategies, run by the group leader. Each takes
  # members as [[member_id, [topics]], ...] and topic_partitions as
  # {topic => [partition ids]} and returns {member_id => [TopicPartition]}.
  #
  # They mirror the Rust and Go implementations exactly: members computing
  # an assignment independently must agree, or a new leader reshuffles the
  # whole group.
  module Assignors
    RANGE = "range"
    ROUNDROBIN = "roundrobin"
    # Keeps members on the partitions they already hold. Prefer this when
    # consumers carry per-partition state.
    STICKY = "sticky"
    ALL = [RANGE, ROUNDROBIN, STICKY].freeze

    module_function

    def assign(strategy, members, topic_partitions, previous = {})
      case strategy.to_s
      when RANGE then range(members, topic_partitions)
      when ROUNDROBIN, "round_robin" then round_robin(members, topic_partitions)
      when STICKY then sticky(members, topic_partitions, previous)
      else raise ArgumentError, "unknown partition.assignment.strategy #{strategy.inspect} (#{ALL.join(', ')})"
      end
    end

    def empty(members) = members.to_h { |id, _| [id, []] }

    # Contiguous ranges per topic; the first (n % members) take one extra.
    def range(members, topic_partitions)
      assignment = empty(members)
      topic_partitions.keys.sort.each do |topic|
        partitions = topic_partitions[topic]
        subscribers = members.select { |_, topics| topics.include?(topic) }.map(&:first).sort
        next if subscribers.empty?

        base, extra = partitions.size.divmod(subscribers.size)
        cursor = 0
        subscribers.each_with_index do |member_id, index|
          count = base + (index < extra ? 1 : 0)
          partitions[cursor, count].each { |partition| assignment[member_id] << TopicPartition.new(topic, partition) }
          cursor += count
        end
      end
      assignment
    end

    # Deal every partition around the circle of members sorted by id.
    def round_robin(members, topic_partitions)
      assignment = empty(members)
      circle = members.sort_by(&:first)
      return assignment if circle.empty?

      cursor = 0
      topic_partitions.keys.sort.each do |topic|
        topic_partitions[topic].each do |partition|
          start = cursor
          loop do
            member_id, topics = circle[cursor % circle.size]
            cursor += 1
            if topics.include?(topic)
              assignment[member_id] << TopicPartition.new(topic, partition)
              break
            end
            break if cursor - start >= circle.size # nobody subscribes
          end
        end
      end
      assignment
    end

    # Keep members on what they hold; move only what balance requires.
    def sticky(members, topic_partitions, previous)
      assignment = empty(members)
      return assignment if members.empty?

      subscriptions = members.to_h
      subscribes = ->(member_id, topic) { (subscriptions[member_id] || []).include?(topic) }

      unassigned = []
      claimed = {}
      previous_ids = previous.keys.sort
      topic_partitions.keys.sort.each do |topic|
        topic_partitions[topic].each do |partition|
          slot = TopicPartition.new(topic, partition)
          holder = previous_ids.find do |member_id|
            (previous[member_id] || []).include?(slot) && subscribes.call(member_id, topic)
          end
          holder ? claimed[slot] = holder : unassigned << slot
        end
      end

      eligible = members.select { |_, topics| topics.any? { |topic| topic_partitions.key?(topic) } }
                        .map(&:first).sort
      return assignment if eligible.empty?

      total = topic_partitions.values.sum(&:size)
      base, extra = total.divmod(eligible.size)
      quota = eligible.each_with_index.to_h { |member_id, index| [member_id, base + (index < extra ? 1 : 0)] }

      kept = Hash.new { |hash, key| hash[key] = [] }
      claimed.keys.sort.each do |slot|
        member_id = claimed[slot]
        if kept[member_id].size < quota.fetch(member_id, 0)
          kept[member_id] << slot
        else
          unassigned << slot
        end
      end
      kept.each { |member_id, held| assignment[member_id] = held if assignment.key?(member_id) }

      unassigned.sort.each do |slot|
        taker = eligible.find do |member_id|
          subscribes.call(member_id, slot.topic) && assignment[member_id].size < quota.fetch(member_id, 0)
        end
        # Quotas exhausted (possible with uneven subscriptions): an
        # unassigned partition is a stalled partition, so fall back to any
        # subscribed member rather than dropping it.
        taker ||= eligible.find { |member_id| subscribes.call(member_id, slot.topic) }
        assignment[taker] << slot if taker
      end
      assignment.each_value(&:sort!)
      assignment
    end
  end
end
