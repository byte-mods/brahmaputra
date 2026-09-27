module Brahmaputra
  # Partition assignment strategies. Mirrors the Go and Rust drivers
  # exactly: members computing an assignment independently must agree.
  module Assignors
    record Member, id : String, topics : Array(String) do
      def subscribes?(topic : String) : Bool
        topics.includes?(topic)
      end
    end

    alias Assignment = Hash(String, Array(TopicPartition))

    def self.assign(strategy : String, members : Array(Member),
                    topic_partitions : Hash(String, Array(Int32)),
                    previous : Hash(String, Array(TopicPartition))) : Assignment
      case strategy
      when "range"      then range(members, topic_partitions)
      when "roundrobin" then round_robin(members, topic_partitions)
      when "sticky"     then sticky(members, topic_partitions, previous)
      else                   raise ConfigError.new("unknown assignor #{strategy.inspect}")
      end
    end

    private def self.empty(members : Array(Member)) : Assignment
      out = Assignment.new
      members.each { |m| out[m.id] = [] of TopicPartition }
      out
    end

    # Each subscribed member gets a contiguous range per topic; the first
    # (partitions % members) take one extra.
    def self.range(members : Array(Member), topic_partitions : Hash(String, Array(Int32))) : Assignment
      assignment = empty(members)
      topic_partitions.keys.sort!.each do |topic|
        partitions = topic_partitions[topic]
        subscribers = members.select(&.subscribes?(topic)).map(&.id).sort!
        next if subscribers.empty?
        base = partitions.size // subscribers.size
        extra = partitions.size % subscribers.size
        cursor = 0
        subscribers.each_with_index do |member_id, index|
          count = base + (index < extra ? 1 : 0)
          partitions[cursor, count].each { |p| assignment[member_id] << TopicPartition.new(topic, p) }
          cursor += count
        end
      end
      assignment
    end

    # Deals partitions around the circle of members sorted by id, skipping
    # members not subscribed to a partition's topic.
    def self.round_robin(members : Array(Member), topic_partitions : Hash(String, Array(Int32))) : Assignment
      assignment = empty(members)
      circle = members.sort_by(&.id)
      return assignment if circle.empty?
      cursor = 0
      topic_partitions.keys.sort!.each do |topic|
        topic_partitions[topic].each do |partition|
          start = cursor
          loop do
            member = circle[cursor % circle.size]
            cursor += 1
            if member.subscribes?(topic)
              assignment[member.id] << TopicPartition.new(topic, partition)
              break
            end
            break if cursor - start >= circle.size
          end
        end
      end
      assignment
    end

    # Keeps members on what they hold and moves only what balance requires.
    def self.sticky(members : Array(Member), topic_partitions : Hash(String, Array(Int32)),
                    previous : Hash(String, Array(TopicPartition))) : Assignment
      assignment = empty(members)
      return assignment if members.empty?
      by_id = members.to_h { |m| {m.id, m} }
      subscribes = ->(member_id : String, topic : String) { by_id[member_id]?.try(&.subscribes?(topic)) || false }

      previous_ids = previous.keys.sort!
      unassigned = [] of TopicPartition
      claimed = {} of TopicPartition => String
      topic_partitions.keys.sort!.each do |topic|
        topic_partitions[topic].each do |partition|
          slot = TopicPartition.new(topic, partition)
          holder = previous_ids.find { |id| previous[id].includes?(slot) && subscribes.call(id, topic) }
          if holder
            claimed[slot] = holder
          else
            unassigned << slot
          end
        end
      end

      eligible = members.select { |m| m.topics.any? { |t| topic_partitions.has_key?(t) } }.map(&.id).sort!
      return assignment if eligible.empty?

      total = topic_partitions.values.sum(&.size)
      base = total // eligible.size
      extra = total % eligible.size
      quota = {} of String => Int32
      eligible.each_with_index { |id, i| quota[id] = base + (i < extra ? 1 : 0) }

      kept = {} of String => Array(TopicPartition)
      claimed.keys.sort!.each do |slot|
        member_id = claimed[slot]
        held = kept[member_id] ||= [] of TopicPartition
        if held.size < (quota[member_id]? || 0)
          held << slot
        else
          unassigned << slot
        end
      end
      kept.each { |id, held| assignment[id] = held if assignment.has_key?(id) }

      unassigned.sort!.each do |slot|
        taker = eligible.find { |id| subscribes.call(id, slot.topic) && assignment[id].size < quota[id] }
        # Quotas exhausted (uneven subscriptions): an unassigned partition
        # is a stalled one, so fall back to any subscriber.
        taker ||= eligible.find { |id| subscribes.call(id, slot.topic) }
        assignment[taker] << slot if taker
      end
      assignment.each_value(&.sort!)
      assignment
    end
  end
end
