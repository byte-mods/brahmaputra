# frozen_string_literal: true

module Brahmaputra
  # A consumer that shares its topics' partitions with the rest of its group.
  #
  # The coordinator for a group is the leader of __consumer_offsets
  # partition crc32c(group.id) % partitions, so every group request goes to
  # that broker. A background thread heartbeats for as long as the member
  # is joined, and leaves the group if the application stops calling #poll
  # for max.poll.interval.ms.
  #
  # #poll, #commit and #close are meant to be called from one thread, as
  # with Kafka's consumer: use one GroupConsumer per worker.
  class GroupConsumer
    OFFSETS_TOPIC = "__consumer_offsets"
    COORDINATOR_ATTEMPTS = 4
    JOIN_ATTEMPTS = 4

    DEFAULTS = {
      "group.id" => nil,
      # Kafka defaults to 45s; this defaults to 10s as the Rust client does.
      "session.timeout.ms" => 10_000,
      "rebalance.timeout.ms" => 3_000,
      "heartbeat.interval.ms" => nil, # default: session.timeout.ms / 3
      # Longest gap between #poll calls before this member is presumed stuck
      # and leaves the group.
      "max.poll.interval.ms" => 300_000,
      "enable.auto.commit" => true,
      "auto.commit.interval.ms" => 5_000,
      # earliest | latest | none
      "auto.offset.reset" => "earliest",
      # range | roundrobin | sticky
      "partition.assignment.strategy" => Assignors::RANGE,
      # Static membership (KIP-345); empty means a dynamic member.
      "group.instance.id" => "",
      "max.poll.records" => 500,
      "fetch.max.bytes" => 8 * 1024 * 1024,
      "fetch.min.bytes" => 1,
      "fetch.max.wait.ms" => 500,
      "isolation.level" => "read_uncommitted",
      "client.rack" => ""
    }.freeze

    AUTO_OFFSET_RESETS = %w[earliest latest none].freeze

    attr_reader :config, :consumer, :group_id

    # GroupConsumer.new("bootstrap.servers" => "127.0.0.1:9092", "group.id" => "billing")
    def initialize(config = nil, **overrides)
      @config = Config.build(DEFAULTS, config, overrides)
      @group_id = @config["group.id"].to_s
      raise ArgumentError, "group.id is required" if @group_id.empty?
      unless AUTO_OFFSET_RESETS.include?(@config["auto.offset.reset"].to_s)
        raise ArgumentError, "auto.offset.reset must be one of #{AUTO_OFFSET_RESETS.join(', ')}"
      end
      unless Assignors::ALL.include?(@config["partition.assignment.strategy"].to_s)
        raise ArgumentError, "partition.assignment.strategy must be one of #{Assignors::ALL.join(', ')}"
      end

      consumer_keys = Consumer::DEFAULTS.keys + Config::COMMON.keys
      @consumer = Consumer.new(@config.slice(*consumer_keys))
      @lock = Monitor.new
      @tick = @lock.new_cond
      @subscribed = []
      @member_id = ""
      @generation = -1
      @joined = false
      @assignment = []
      # Next offset to *deliver*: what gets committed.
      @positions = {}
      # Next offset to *fetch*; runs ahead of @positions by the buffer.
      @fetch_positions = {}
      @buffered = []
      @last_poll = monotonic
      @in_poll = false
      @last_commit = monotonic
      @closed = false
      @heartbeat_error = nil
      @heartbeat = Thread.new { heartbeat_loop }
      @heartbeat.report_on_exception = false
    end

    def member_id = @lock.synchronize { @member_id }
    def generation = @lock.synchronize { @generation }
    def assignment = @lock.synchronize { @assignment.dup }

    def subscribe(topics)
      topics = Array(topics).map(&:to_s)
      @lock.synchronize do
        @subscribed = topics
        @joined = false
      end
      self
    end

    def subscription = @subscribed.dup

    # Up to max.poll.records records, joining the group first if needed.
    def poll(timeout_ms = 1000)
      raise Error, "consumer is closed" if @closed
      raise Error, "subscribe to at least one topic before polling" if @subscribed.empty?

      # The interval bounds how long the *application* may go between polls.
      # Time spent inside poll (a slow join, a long wait for data) is the
      # consumer working, so it is excluded: stamped on entry and exit, and
      # not enforced at all while a poll is running.
      @lock.synchronize do
        @last_poll = monotonic
        @in_poll = true
      end
      begin
        poll_loop(monotonic + timeout_ms / 1000.0)
      ensure
        @lock.synchronize do
          @last_poll = monotonic
          @in_poll = false
        end
      end
    end

    private def poll_loop(deadline)
      loop do
        join unless joined?
        return take_buffered unless @buffered.empty?

        if @assignment.empty?
          return [] if monotonic >= deadline

          sleep 0.05
          next
        end

        got_any = false
        @assignment.each do |slot|
          remaining_ms = [((deadline - monotonic) * 1000).to_i, 0].max
          offset = @fetch_positions.fetch(slot, 0)
          begin
            records = @consumer.fetch(slot.topic, slot.partition, offset, [remaining_ms, 500].min)
          rescue ServerError => e
            case e.code
            when ErrorCode::OFFSET_OUT_OF_RANGE
              # The position fell off the log; restart where the policy says.
              reset = reset_offset(slot)
              @fetch_positions[slot] = reset
              @positions[slot] = reset
              @buffered.reject! { |record| record.topic == slot.topic && record.partition == slot.partition }
              next
            when ErrorCode::NOT_LEADER_OR_FOLLOWER
              @consumer.router.refresh(slot.topic)
              next
            else raise
            end
          end
          next if records.empty?

          got_any = true
          @fetch_positions[slot] = records.last.offset + 1
          @buffered.concat(records)
        end

        maybe_auto_commit
        return take_buffered unless @buffered.empty?
        return [] if !got_any && monotonic >= deadline
      end
    end

    # Commit delivered positions (or the given {TopicPartition => offset}).
    # At-least-once: call after processing, never before.
    def commit(offsets = nil)
      offsets ||= @positions
      return if offsets.empty?

      generation, member_id = @lock.synchronize { [@generation, @member_id] }
      writer = Protocol.body_writer.string(@group_id).int32(generation).string(member_id).int32(offsets.size)
      offsets.sort_by { |slot, _| slot }.each do |slot, offset|
        writer.string(slot.topic).int32(slot.partition).int64(offset)
      end
      reader = Protocol.body_reader(coordinator_request(Protocol::ApiKey::OFFSET_COMMIT, writer.bytes))
      code = reader.int32
      unless code == ErrorCode::NONE
        # A fenced generation means a rebalance happened under us.
        mark_rejoin if [ErrorCode::ILLEGAL_GENERATION, ErrorCode::UNKNOWN_MEMBER_ID,
                        ErrorCode::REBALANCE_IN_PROGRESS].include?(code)
        raise ServerError.new(code, "offset_commit")
      end
      @last_commit = monotonic
      nil
    end

    # Committed offsets as {TopicPartition => offset}; nil asks for every
    # partition the group has committed. -1 means none.
    def committed(partitions = nil)
      partitions = Array(partitions)
      writer = Protocol.body_writer.string(@group_id).int32(partitions.size)
      partitions.each { |slot| writer.string(slot.topic).int32(slot.partition) }
      reader = Protocol.body_reader(coordinator_request(Protocol::ApiKey::OFFSET_FETCH, writer.bytes))
      code = reader.int32
      raise ServerError.new(code, "offset_fetch") unless code == ErrorCode::NONE

      Array.new(reader.int32) { [TopicPartition.new(reader.string, reader.int32), reader.int64] }.to_h
    end

    # Commit, leave the group, then stop.
    #
    # Leaving is what separates a clean shutdown from a crash: without it the
    # coordinator must wait out session.timeout.ms before reassigning.
    def close
      return if @closed

      @lock.synchronize do
        @closed = true
        @tick.broadcast
      end
      @heartbeat.join(5)
      begin
        commit if joined?
      rescue StandardError
        nil # reported by the next member resuming from an older position
      end
      begin
        leave unless member_id.empty?
      rescue StandardError
        nil # costs only the session timeout this was trying to avoid
      end
      @consumer.close
      nil
    end

    def closed? = @closed

    private

    def joined? = @lock.synchronize { @joined }

    def mark_rejoin = @lock.synchronize { @joined = false }

    def take_buffered
      delivered = @buffered.shift(@config["max.poll.records"])
      # The consumed position advances only over records actually handed
      # to the caller; committing what was merely fetched would skip
      # records nobody processed.
      delivered.each { |record| @positions[TopicPartition.new(record.topic, record.partition)] = record.offset + 1 }
      delivered
    end

    def maybe_auto_commit
      return unless @config["enable.auto.commit"]

      interval = @config["auto.commit.interval.ms"].to_i
      return if interval <= 0 || @positions.empty?
      return if (monotonic - @last_commit) * 1000 < interval

      begin
        commit
      rescue Error
        nil # retried on the next poll; explicit commit is what callers rely on
      end
    end

    def reset_offset(slot)
      case @config["auto.offset.reset"].to_s
      when "earliest" then @consumer.list_offsets(slot.topic, slot.partition, EARLIEST)
      when "latest" then @consumer.list_offsets(slot.topic, slot.partition, LATEST)
      else raise NoOffsetForPartitionError, "no committed offset for #{slot} and auto.offset.reset=none"
      end
    end

    def join
      JOIN_ATTEMPTS.times do
        member_id = @lock.synchronize { @member_id }
        body = Protocol.body_writer
                       .string(@group_id)
                       .int32(@config["session.timeout.ms"])
                       .int32(@config["rebalance.timeout.ms"])
                       .string(member_id)
                       .string_array(@subscribed)
                       .string(@config["group.instance.id"].to_s)
                       .bytes
        reader = Protocol.body_reader(coordinator_request(Protocol::ApiKey::JOIN_GROUP, body,
                                                          timeout_ms: join_timeout_ms))
        code = reader.int32
        case code
        when ErrorCode::NONE then nil
        when ErrorCode::REBALANCE_IN_PROGRESS
          sleep 0.1
          next
        when ErrorCode::UNKNOWN_MEMBER_ID
          # Evicted (or left); join again as a new member.
          @lock.synchronize { @member_id = "" }
          next
        else raise ServerError.new(code, "join_group")
        end

        generation = reader.int32
        new_member_id = reader.string
        leader_id = reader.string
        members = Array.new(reader.int32) do
          id = reader.string
          topics = reader.string_array
          held = Array.new(reader.int32) { TopicPartition.new(reader.string, reader.int32) }
          [id, topics, held]
        end
        @lock.synchronize do
          @member_id = new_member_id
          @generation = generation
        end

        assignments = new_member_id == leader_id ? compute_assignments(members) : {}
        return if sync(generation, new_member_id, assignments)
      end
      raise Error, "consumer group failed to stabilise after #{JOIN_ATTEMPTS} join attempts"
    end

    def join_timeout_ms = @config["request.timeout.ms"] + @config["rebalance.timeout.ms"]

    def compute_assignments(members)
      topic_partitions = {}
      members.each do |_, topics, _|
        topics.each { |topic| topic_partitions[topic] ||= @consumer.partitions(topic) }
      end
      previous = members.to_h { |id, _, held| [id, held] }
      Assignors.assign(@config["partition.assignment.strategy"],
                       members.map { |id, topics, _| [id, topics] }, topic_partitions, previous)
    end

    def sync(generation, member_id, assignments)
      writer = Protocol.body_writer.string(@group_id).int32(generation).string(member_id).int32(assignments.size)
      assignments.sort_by(&:first).each do |assignee, partitions|
        writer.string(assignee).int32(partitions.size)
        partitions.each { |slot| writer.string(slot.topic).int32(slot.partition) }
      end
      reader = Protocol.body_reader(coordinator_request(Protocol::ApiKey::SYNC_GROUP, writer.bytes,
                                                        timeout_ms: join_timeout_ms))
      code = reader.int32
      return false if [ErrorCode::REBALANCE_IN_PROGRESS, ErrorCode::ILLEGAL_GENERATION].include?(code)
      if code == ErrorCode::UNKNOWN_MEMBER_ID
        # Evicted between join and sync; rejoin as a new member.
        @lock.synchronize { @member_id = "" }
        return false
      end
      raise ServerError.new(code, "sync_group") unless code == ErrorCode::NONE

      assigned = Array.new(reader.int32) { TopicPartition.new(reader.string, reader.int32) }
      apply_assignment(assigned)
      @lock.synchronize { @joined = true }
      true
    end

    def apply_assignment(assigned)
      @lock.synchronize { @assignment = assigned }
      @positions.select! { |slot, _| assigned.include?(slot) }
      # Buffered records were never delivered, so a new assignment drops them.
      @buffered.clear

      needed = assigned.reject { |slot| @positions.key?(slot) }
      unless needed.empty?
        found = committed(needed)
        needed.each do |slot|
          offset = found[slot]
          offset = reset_offset(slot) if offset.nil? || offset.negative?
          @positions[slot] = offset
        end
      end
      @fetch_positions = @positions.dup
    end

    def leave
      member_id = @lock.synchronize { @member_id }
      body = Protocol.body_writer.string(@group_id).string(member_id).bytes
      reader = Protocol.body_reader(coordinator_request(Protocol::ApiKey::LEAVE_GROUP, body))
      code = reader.int32
      @lock.synchronize do
        @joined = false
        # A dynamic member's id dies with its membership; a static member
        # keeps its identity through group.instance.id instead.
        @member_id = ""
        @generation = -1
      end
      raise ServerError.new(code, "leave_group") unless code == ErrorCode::NONE
    end

    # --- heartbeat thread --------------------------------------------------

    def heartbeat_loop
      # This loop enforces two deadlines, so it wakes often enough for the
      # shorter of them.
      heartbeat_ms = (@config["heartbeat.interval.ms"] || @config["session.timeout.ms"] / 3).to_i
      poll_check_ms = @config["max.poll.interval.ms"] / 3
      interval = [[heartbeat_ms, poll_check_ms].min, 1].max / 1000.0
      left_for_slow_poll = false

      loop do
        state = @lock.synchronize do
          @tick.wait(interval)
          break nil if @closed

          [@joined, @member_id, @generation, @in_poll ? 0 : monotonic - @last_poll]
        end
        break if state.nil?

        joined, member_id, generation, idle = state
        next if !joined || member_id.empty?

        begin
          if idle * 1000 >= @config["max.poll.interval.ms"]
            # The process is alive but the application stopped consuming.
            # Heartbeating on would hold partitions away from a member that
            # could make progress.
            leave unless left_for_slow_poll
            left_for_slow_poll = true
            next
          end
          left_for_slow_poll = false
          body = Protocol.body_writer.string(@group_id).int32(generation).string(member_id).bytes
          code = Protocol.body_reader(coordinator_request(Protocol::ApiKey::HEARTBEAT, body)).int32
          if [ErrorCode::REBALANCE_IN_PROGRESS, ErrorCode::UNKNOWN_MEMBER_ID,
              ErrorCode::ILLEGAL_GENERATION].include?(code)
            @lock.synchronize do
              if @generation == generation
                @joined = false
                # The coordinator forgot this member; its id is dead.
                @member_id = "" if code == ErrorCode::UNKNOWN_MEMBER_ID
              end
            end
          end
        rescue StandardError => e
          # A failed heartbeat is retried next tick; the session timeout is
          # the real deadline.
          @heartbeat_error = e
        end
      end
    end

    # --- coordinator -------------------------------------------------------

    def coordinator_partition
      partitions = @consumer.partitions(OFFSETS_TOPIC)
      Protocol.crc32c(@group_id.b) % partitions.size
    end

    # Send to the group's coordinator, following moves and waiting out loads.
    def coordinator_request(api_key, body, timeout_ms: nil)
      COORDINATOR_ATTEMPTS.times do
        partition = coordinator_partition
        connection = @consumer.router.connection_for(OFFSETS_TOPIC, partition)
        begin
          response = connection.request(api_key, body, timeout_ms: timeout_ms)
        rescue ConnectionError
          @consumer.router.refresh(OFFSETS_TOPIC)
          next
        end
        case Protocol.peek_error_code(response)
        when ErrorCode::COORDINATOR_LOAD_IN_PROGRESS
          sleep 0.1
        when ErrorCode::NOT_COORDINATOR, ErrorCode::NOT_LEADER_OR_FOLLOWER
          @consumer.router.refresh(OFFSETS_TOPIC)
        else
          return response
        end
      end
      raise Error, "group coordinator unavailable after #{COORDINATOR_ATTEMPTS} attempts"
    end

    def monotonic = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end
end
