module Brahmaputra
  # The internal topic whose partition leaders coordinate consumer groups.
  OFFSETS_TOPIC = "__consumer_offsets"

  # Consumer-group settings, named as Kafka names them.
  class GroupConfig
    property client_id : String = "brahmaputra-crystal"
    # `session.timeout.ms`: the coordinator evicts a member that stops
    # heartbeating for this long.
    property session_timeout_ms : Int32 = 10_000
    # `rebalance.timeout.ms`: how long the coordinator waits for rejoins.
    property rebalance_timeout_ms : Int32 = 3_000
    # `max.poll.interval.ms`: the longest gap *between* polls before this
    # member is presumed stuck and leaves. Time inside poll never counts.
    property max_poll_interval_ms : Int32 = 300_000
    # `auto.commit.interval.ms`; 0 disables auto-commit
    # (`enable.auto.commit=false`).
    property auto_commit_interval_ms : Int32 = 5_000
    # `auto.offset.reset`: "earliest", "latest" or "none".
    property auto_offset_reset : String = "earliest"
    # `partition.assignment.strategy`: "range", "roundrobin" or "sticky".
    property partition_assignment_strategy : String = "range"
    # `group.instance.id` for static membership; empty means dynamic.
    property group_instance_id : String = ""
    property max_poll_records : Int32 = 500
    property fetch_max_bytes : Int32 = 8 * 1024 * 1024
    property fetch_min_bytes : Int32 = 1
    property connect_timeout_ms : Int32 = 30_000
    property socket_timeout_ms : Int32 = 120_000

    def initialize
    end

    def initialize(&)
      yield self
    end
  end

  # Shares its subscribed topics' partitions with the rest of its group.
  #
  # Use one per fiber, as with Kafka's consumer. A background fiber
  # heartbeats and enforces max.poll.interval.ms.
  class GroupConsumer
    COORDINATOR_ATTEMPTS = 4
    JOIN_ATTEMPTS        = 4

    getter group_id : String
    getter config : GroupConfig
    getter consumer : Consumer

    def initialize(bootstrap : String, @group_id : String, @config : GroupConfig = GroupConfig.new)
      consumer_config = ConsumerConfig.new
      consumer_config.client_id = @config.client_id
      consumer_config.fetch_max_bytes = @config.fetch_max_bytes
      consumer_config.fetch_min_bytes = @config.fetch_min_bytes
      consumer_config.max_poll_records = @config.max_poll_records
      consumer_config.connect_timeout_ms = @config.connect_timeout_ms
      consumer_config.socket_timeout_ms = @config.socket_timeout_ms
      @consumer = Consumer.new(bootstrap, consumer_config)
      @subscribed = [] of String
      @mutex = Mutex.new
      @member_id = ""
      @generation = -1
      @joined = false
      @assignment = [] of TopicPartition
      # Next offset to *deliver* — what gets committed.
      @positions = {} of TopicPartition => Int64
      # Next offset to *fetch*; runs ahead of positions by what is buffered.
      @fetch_positions = {} of TopicPartition => Int64
      @buffered = Deque(ConsumedRecord).new
      @last_poll = Time.monotonic
      @in_poll = false
      @last_commit = Time.monotonic
      @closed = false
      @stop = Channel(Nil).new
      @done = Channel(Nil).new(1)
      spawn(name: "brahmaputra-heartbeat") { heartbeat_loop }
    end

    def member_id : String
      @mutex.synchronize { @member_id }
    end

    def generation : Int32
      @mutex.synchronize { @generation }
    end

    def assignment : Array(TopicPartition)
      @assignment.dup
    end

    # Sets the topics this member wants a share of; rejoins on next poll.
    def subscribe(topics : Array(String)) : Nil
      @subscribed = topics.dup
      @mutex.synchronize { @joined = false }
    end

    private def membership : {String, Int32, Bool}
      @mutex.synchronize { {@member_id, @generation, @joined} }
    end

    # Commits, leaves the group so partitions move at once rather than
    # after session.timeout.ms, then stops.
    def close : Nil
      return if @closed
      @closed = true
      @stop.close
      member_id, _, joined = membership
      if joined
        begin
          commit
        rescue Error
        end
      end
      unless member_id.empty?
        # Best effort: failing costs only the session timeout it avoids.
        begin
          leave
        rescue Error
        end
      end
      select
      when @done.receive?
      when timeout(2.seconds)
      end
      @consumer.close
    end

    # Returns up to max.poll.records records, joining the group if needed.
    def poll(timeout : Time::Span) : Array(ConsumedRecord)
      raise ConfigError.new("subscribe to at least one topic before polling") if @subscribed.empty?
      raise Error.new("consumer is closed") if @closed
      # Stamped on entry and on exit and never enforced in between: the
      # interval bounds time the *application* spends between polls.
      @mutex.synchronize do
        @last_poll = Time.monotonic
        @in_poll = true
      end
      begin
        poll_inner(timeout)
      ensure
        @mutex.synchronize do
          @last_poll = Time.monotonic
          @in_poll = false
        end
      end
    end

    private def poll_inner(timeout : Time::Span) : Array(ConsumedRecord)
      deadline = Time.monotonic + timeout
      loop do
        # Checked every sweep: a rebalance the heartbeat learns of mid-poll
        # must stop this member fetching partitions it may no longer own.
        _, _, joined = membership
        join unless joined
        return take_buffered unless @buffered.empty?
        if @assignment.empty?
          return [] of ConsumedRecord if Time.monotonic >= deadline
          sleep 50.milliseconds
          next
        end

        got_any = false
        @assignment.each do |slot|
          remaining = (deadline - Time.monotonic).total_milliseconds.to_i32
          wait_ms = remaining.clamp(0, 500)
          offset = @fetch_positions[slot]? || 0_i64
          records = begin
            @consumer.fetch(slot.topic, slot.partition, offset, wait_ms)
          rescue ex : ServerError
            case ex.code
            when ErrorCode::OFFSET_OUT_OF_RANGE
              # The position fell off the log; restart where policy says.
              reset = reset_offset(slot.topic, slot.partition)
              @fetch_positions[slot] = reset
              @positions[slot] = reset
              next
            when ErrorCode::NOT_LEADER_OR_FOLLOWER
              @consumer.router.refresh(slot.topic) rescue nil
              next
            else
              raise ex
            end
          end
          unless records.empty?
            got_any = true
            @fetch_positions[slot] = records.last.offset + 1
            records.each { |r| @buffered << r }
          end
        end

        maybe_auto_commit
        return take_buffered unless @buffered.empty?
        return [] of ConsumedRecord if !got_any && Time.monotonic >= deadline
      end
    end

    private def take_buffered : Array(ConsumedRecord)
      limit = @config.max_poll_records
      limit = @buffered.size if limit <= 0 || limit > @buffered.size
      delivered = Array(ConsumedRecord).new(limit) { @buffered.shift }
      # The committed position advances only over records handed to the
      # caller; committing what was merely fetched would skip records.
      delivered.each { |r| @positions[TopicPartition.new(r.topic, r.partition)] = r.offset + 1 }
      delivered
    end

    # Commits delivered positions. At-least-once: call after processing.
    def commit : Nil
      return if @positions.empty?
      slots = @positions.keys.sort!
      member_id, generation, _ = membership
      w = Protocol::Writer.new.string(@group_id).int32(generation).string(member_id).int32(slots.size)
      slots.each { |s| w.string(s.topic).int32(s.partition).int64(@positions[s]) }
      r = Protocol::Reader.new(coordinator_request(Protocol::OFFSET_COMMIT, w.to_slice))
      code = r.int32
      raise ServerError.new(code, "offset_commit") unless code == ErrorCode::NONE
      @last_commit = Time.monotonic
    end

    # The group's committed offsets. An empty list asks for every
    # partition the group has committed.
    def committed(partitions : Array(TopicPartition) = [] of TopicPartition) : Hash(TopicPartition, Int64)
      w = Protocol::Writer.new.string(@group_id).int32(partitions.size)
      partitions.each { |s| w.string(s.topic).int32(s.partition) }
      r = Protocol::Reader.new(coordinator_request(Protocol::OFFSET_FETCH, w.to_slice))
      code = r.int32
      raise ServerError.new(code, "offset_fetch") unless code == ErrorCode::NONE
      out = {} of TopicPartition => Int64
      r.count.times do
        topic = r.string
        partition = r.int32
        out[TopicPartition.new(topic, partition)] = r.int64
      end
      out
    end

    private def maybe_auto_commit : Nil
      interval = @config.auto_commit_interval_ms
      return if interval <= 0 || @positions.empty?
      return if Time.monotonic - @last_commit < interval.milliseconds
      # A failed auto-commit is retried on the next poll.
      commit rescue nil
    end

    private def reset_offset(topic : String, partition : Int32) : Int64
      case @config.auto_offset_reset
      when "earliest" then @consumer.list_offsets(topic, partition, EARLIEST)
      when "latest"   then @consumer.list_offsets(topic, partition, LATEST)
      when "none"     then raise NoOffsetForPartitionError.new(topic, partition)
      else                 raise ConfigError.new("unknown auto.offset.reset #{@config.auto_offset_reset.inspect}")
      end
    end

    # -------------------------------------------------------------------
    # Membership
    # -------------------------------------------------------------------

    private def join : Nil
      JOIN_ATTEMPTS.times do
        current_member, _, _ = membership
        w = Protocol::Writer.new
          .string(@group_id).int32(@config.session_timeout_ms).int32(@config.rebalance_timeout_ms)
          .string(current_member).string_array(@subscribed).string(@config.group_instance_id)
        r = Protocol::Reader.new(coordinator_request(Protocol::JOIN_GROUP, w.to_slice))
        code = r.int32
        if code == ErrorCode::REBALANCE_IN_PROGRESS
          sleep 100.milliseconds
          next
        end
        if code == ErrorCode::UNKNOWN_MEMBER_ID
          # Dropped by the coordinator: join again as a new member.
          @mutex.synchronize { @member_id = "" }
          next
        end
        raise ServerError.new(code, "join_group") unless code == ErrorCode::NONE

        generation = r.int32
        member_id = r.string
        leader_id = r.string
        members = [] of Assignors::Member
        previous = {} of String => Array(TopicPartition)
        r.count.times do
          id = r.string
          topics = r.string_array
          held = Array(TopicPartition).new(r.count) { TopicPartition.new(r.string, r.int32) }
          members << Assignors::Member.new(id, topics)
          previous[id] = held
        end
        @mutex.synchronize do
          @member_id = member_id
          @generation = generation
        end

        assignments = [] of {String, Array(TopicPartition)}
        if member_id == leader_id
          topic_partitions = {} of String => Array(Int32)
          members.each do |m|
            m.topics.each do |topic|
              topic_partitions[topic] ||= @consumer.partitions(topic)
            end
          end
          computed = Assignors.assign(@config.partition_assignment_strategy, members, topic_partitions, previous)
          assignments = computed.keys.sort!.map { |id| {id, computed[id]} }
        end

        if sync(assignments)
          @mutex.synchronize { @joined = true }
          return
        end
      end
      raise Error.new("consumer group failed to stabilise after #{JOIN_ATTEMPTS} join attempts")
    end

    private def sync(assignments : Array({String, Array(TopicPartition)})) : Bool
      member_id, generation, _ = membership
      w = Protocol::Writer.new.string(@group_id).int32(generation).string(member_id).int32(assignments.size)
      assignments.each do |id, slots|
        w.string(id).int32(slots.size)
        slots.each { |s| w.string(s.topic).int32(s.partition) }
      end
      r = Protocol::Reader.new(coordinator_request(Protocol::SYNC_GROUP, w.to_slice))
      code = r.int32
      return false if code == ErrorCode::REBALANCE_IN_PROGRESS || code == ErrorCode::ILLEGAL_GENERATION
      if code == ErrorCode::UNKNOWN_MEMBER_ID
        @mutex.synchronize { @member_id = "" }
        return false
      end
      raise ServerError.new(code, "sync_group") unless code == ErrorCode::NONE
      assignment = Array(TopicPartition).new(r.count) { TopicPartition.new(r.string, r.int32) }
      apply_assignment(assignment)
      true
    end

    private def apply_assignment(assignment : Array(TopicPartition)) : Nil
      @assignment = assignment
      owned = assignment.to_set
      @positions.reject! { |slot, _| !owned.includes?(slot) }
      # Buffered records were never delivered, so a new assignment drops them.
      @buffered.clear
      needed = assignment.reject { |slot| @positions.has_key?(slot) }
      unless needed.empty?
        committed = committed(needed)
        needed.each do |slot|
          offset = committed[slot]?
          offset = reset_offset(slot.topic, slot.partition) if offset.nil? || offset < 0
          @positions[slot] = offset
        end
      end
      @fetch_positions = @positions.dup
    end

    private def leave : Nil
      member_id, _, _ = membership
      w = Protocol::Writer.new.string(@group_id).string(member_id)
      r = Protocol::Reader.new(coordinator_request(Protocol::LEAVE_GROUP, w.to_slice))
      code = r.int32
      raise ServerError.new(code, "leave_group") unless code == ErrorCode::NONE
      @mutex.synchronize { @joined = false }
    end

    private def heartbeat_loop : Nil
      # Wakes often enough for the shorter of the two deadlines it enforces.
      heartbeat_every = Math.max(@config.session_timeout_ms // 3, 1)
      poll_check_every = Math.max(@config.max_poll_interval_ms // 3, 1)
      interval = Math.min(heartbeat_every, poll_check_every).milliseconds
      left_for_slow_poll = false
      loop do
        select
        when @stop.receive?
          break
        when timeout(interval)
        end
        break if @closed
        idle, in_poll = @mutex.synchronize { {Time.monotonic - @last_poll, @in_poll} }
        member_id, generation, joined = membership
        next if !joined || member_id.empty?

        if !in_poll && idle >= @config.max_poll_interval_ms.milliseconds
          # The application stopped consuming though the process lives;
          # heartbeating would hold its partitions away from a consumer
          # that could make progress.
          unless left_for_slow_poll
            leave rescue nil
            left_for_slow_poll = true
            @mutex.synchronize { @joined = false }
          end
          next
        end
        left_for_slow_poll = false

        begin
          w = Protocol::Writer.new.string(@group_id).int32(generation).string(member_id)
          code = Protocol::Reader.new(coordinator_request(Protocol::HEARTBEAT, w.to_slice)).int32
          if code.in?(ErrorCode::REBALANCE_IN_PROGRESS, ErrorCode::UNKNOWN_MEMBER_ID, ErrorCode::ILLEGAL_GENERATION)
            # Only if nothing changed since the snapshot: a stale reply for
            # an old generation must not send a rejoined member round again.
            @mutex.synchronize do
              @joined = false if @generation == generation && @member_id == member_id
            end
          end
        rescue Error
          # transient: retry next tick
        end
      end
    ensure
      @done.send(nil) rescue nil
    end

    # -------------------------------------------------------------------
    # Coordinator routing
    # -------------------------------------------------------------------

    private def coordinator_partition : Int32
      partitions = @consumer.partitions(OFFSETS_TOPIC)
      (Protocol.crc32c(@group_id.to_slice) % partitions.size.to_u32).to_i32
    end

    # Sends to the group's coordinator, following moves and waiting out loads.
    private def coordinator_request(api_key : Int16, body : Bytes) : Bytes
      COORDINATOR_ATTEMPTS.times do
        conn = @consumer.router.connection_for(OFFSETS_TOPIC, coordinator_partition)
        response = conn.request(api_key, body)
        case Protocol::Reader.peek_error_code(response)
        when ErrorCode::COORDINATOR_LOAD_IN_PROGRESS
          sleep 100.milliseconds
          next
        when ErrorCode::NOT_COORDINATOR, ErrorCode::NOT_LEADER_OR_FOLLOWER
          @consumer.router.refresh(OFFSETS_TOPIC) rescue nil
          next
        end
        return response
      end
      raise Error.new("group coordinator unavailable after #{COORDINATOR_ATTEMPTS} attempts")
    end
  end
end
