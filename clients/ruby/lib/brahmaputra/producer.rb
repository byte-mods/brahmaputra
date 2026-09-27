# frozen_string_literal: true

module Brahmaputra
  # Where a record landed. offset is -1 under acks=0, where the broker does
  # not say.
  RecordMetadata = Struct.new(:topic, :partition, :offset, :timestamp)

  # The eventual outcome of one Producer#send, like Kafka's Future<RecordMetadata>.
  class DeliveryFuture
    def initialize
      @lock = Mutex.new
      @cond = ConditionVariable.new
      @done = false
      @metadata = nil
      @error = nil
      @callbacks = []
    end

    def done? = @lock.synchronize { @done }

    # The error the send failed with, or nil (also nil while pending).
    def error = @lock.synchronize { @error }

    # Block until delivered and return the RecordMetadata, or raise the
    # error the send failed with. Raises TimeoutError if timeout_ms passes.
    def value(timeout_ms = nil)
      @lock.synchronize do
        deadline = timeout_ms && monotonic + timeout_ms / 1000.0
        until @done
          remaining = deadline && deadline - monotonic
          raise TimeoutError, "delivery not confirmed within #{timeout_ms} ms" if remaining && remaining <= 0

          @cond.wait(@lock, remaining)
        end
        raise @error if @error

        @metadata
      end
    end
    alias get value

    # Run block(metadata, error) once the send completes (immediately if it
    # already has). It runs on the producer's sender thread: keep it short.
    def on_complete(&block)
      run_now = @lock.synchronize do
        @callbacks << block unless @done
        @done
      end
      block.call(@metadata, @error) if run_now
      self
    end

    def complete(metadata, error)
      callbacks = @lock.synchronize do
        return if @done

        @metadata = metadata
        @error = error
        @done = true
        @cond.broadcast
        @callbacks.dup
      end
      callbacks.each do |callback|
        callback.call(metadata, error)
      rescue StandardError
        nil # a failing callback must not take the sender thread down
      end
    end

    private

    def monotonic = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end

  # A batching, thread-safe producer. Share one across the application
  # rather than creating one per message: the batching is the point.
  #
  # #send only buffers. A background sender thread drains each partition's
  # buffer once it holds batch.size bytes, once its oldest record has waited
  # linger.ms, or when #flush asks.
  class Producer
    DEFAULTS = {
      # 0 fire-and-forget, 1 leader append, -1/"all" every in-sync replica.
      "acks" => 1,
      "batch.size" => 16 * 1024,
      # Kafka defaults to 0; this defaults to 5 because an unbatched
      # producer is slow enough to look broken.
      "linger.ms" => 5,
      "compression.type" => "none",
      # Retries of a send refused with a retriable error (one the broker
      # returns before appending) or that could not reach its leader at all.
      "retries" => 5,
      "retry.backoff.ms" => 100,
      # Caps the whole send: time buffered plus every attempt.
      "delivery.timeout.ms" => 120_000,
      # Caps unsent record bytes held client-side.
      "buffer.memory" => 32 * 1024 * 1024,
      # How long #send may block on a full buffer before raising.
      "max.block.ms" => 60_000
    }.freeze

    Pending = Struct.new(:record, :future, :timestamp)
    Batch = Struct.new(:topic, :partition, :items, :size, :created_at, :force)

    attr_reader :config, :router

    # Producer.new("bootstrap.servers" => "127.0.0.1:9092", "linger.ms" => 0)
    # Producer.new(bootstrap_servers: "127.0.0.1:9092", acks: "all")
    def initialize(config = nil, **overrides)
      @config = Config.build(DEFAULTS, config, overrides)
      @acks = parse_acks(@config["acks"])
      @codec = Protocol::Compression.parse(@config["compression.type"])
      unless Protocol::Compression.registered?(@codec)
        raise ArgumentError, "compression.type=#{@config['compression.type']} needs Brahmaputra.register_codec first"
      end

      @router = Config.router_for(@config)
      @lock = Monitor.new
      @wakeup = @lock.new_cond
      @space = @lock.new_cond
      @slots = {}
      @inflight = []
      @buffered_bytes = 0
      @round_robin = 0
      @closed = false
      # First delivery failure since the last flush, so a batch the sender
      # thread failed in the background still surfaces on flush/close.
      @unreported_error = nil
      @sender = Thread.new { sender_loop }
      @sender.name = "brahmaputra-sender" if @sender.respond_to?(:name=)
      @sender.report_on_exception = false
    end

    # Buffer one record and return a DeliveryFuture.
    #
    # value nil is a tombstone, distinct from an empty value. key picks the
    # partition via murmur2 unless partition: is given; with neither the
    # producer round-robins. headers is a Hash or an Array of RecordHeader
    # (a nil header value stays nil). timestamp is unix ms, default now.
    #
    # Raises BufferFullError when buffer.memory stays full for max.block.ms.
    def send(topic, value, key: nil, partition: nil, headers: nil, timestamp: nil)
      raise Error, "producer is closed" if @closed
      raise ArgumentError, "value must be a String or nil" unless value.nil? || value.is_a?(String)

      headers = RecordHeader.coerce(headers)
      partition = choose_partition(topic, key) if partition.nil?
      record = Protocol::BatchRecord.new(key&.b, value&.b, 0, headers)
      size = record_size(record)
      reserve(size)

      future = DeliveryFuture.new
      pending = Pending.new(record, future, (timestamp || now_ms).to_i)
      @lock.synchronize do
        if @closed
          @buffered_bytes -= size
          raise Error, "producer is closed"
        end
        batches = (@slots[[topic, partition]] ||= [])
        batch = batches.last
        if batch.nil? || (batch.size.positive? && batch.size + size > @config["batch.size"])
          batch = Batch.new(topic, partition, [], 0, monotonic, false)
          batches << batch
        end
        batch.items << pending
        batch.size += size
        @wakeup.signal
      end
      future
    end
    alias produce send

    # Send one record now, bypassing linger, and wait for its RecordMetadata.
    # A full round trip per record: correct, and slow.
    def send_sync(topic, value, **options)
      future = send(topic, value, **options)
      @lock.synchronize do
        @slots.each_value { |batches| batches.each { |batch| batch.force = true } }
        @wakeup.signal
      end
      future.value
    end

    # Send everything buffered and wait for it. Raises the first delivery
    # error among the records that were buffered when flush was called.
    def flush(timeout_ms = nil)
      futures = @lock.synchronize do
        queued = @slots.values.flatten
        queued.each { |batch| batch.force = true }
        @wakeup.signal
        (queued + @inflight).flat_map { |batch| batch.items.map(&:future) }
      end
      first_error = nil
      futures.each do |future|
        future.value(timeout_ms)
      rescue TimeoutError => e
        raise e unless future.done?

        first_error ||= e
      rescue Error, StandardError => e
        first_error ||= e
      end
      background = @lock.synchronize do
        error = @unreported_error
        @unreported_error = nil
        error
      end
      raise first_error if first_error
      raise background if background

      nil
    end

    # Flush, stop the sender thread, and close every connection.
    def close(timeout_ms = nil)
      error = nil
      begin
        flush(timeout_ms) unless @closed
      rescue StandardError => e
        error = e
      end
      @lock.synchronize do
        @closed = true
        @wakeup.broadcast
        @space.broadcast
      end
      @sender.join(timeout_ms ? timeout_ms / 1000.0 : 5)
      @router.close
      raise error if error

      nil
    end

    def closed? = @closed

    # Partition ids of a topic (sorted), creating it if the broker auto-creates.
    def partitions_for(topic) = @router.partitions(topic)

    private

    def parse_acks(value)
      acks = value.to_s == "all" ? -1 : Integer(value)
      raise ArgumentError, "acks must be 0, 1, -1 or \"all\"" unless [0, 1, -1].include?(acks)

      acks
    end

    def choose_partition(topic, key)
      partitions = @router.partitions(topic)
      return Protocol.partition_for_key(key, partitions) unless key.nil?

      @lock.synchronize do
        chosen = partitions[@round_robin % partitions.size]
        @round_robin += 1
        chosen
      end
    end

    def record_size(record)
      size = (record.value ? record.value.bytesize : 0) + (record.key ? record.key.bytesize : 0) + 16
      record.headers.each { |header| size += header.key.to_s.bytesize + (header.value ? header.value.bytesize : 0) + 4 }
      size
    end

    # Wait until size more bytes may be buffered. This is what makes
    # buffer.memory real: a producer faster than its broker is slowed down
    # here rather than allowed to grow without limit.
    def reserve(size)
      limit = @config["buffer.memory"]
      @lock.synchronize do
        # A record larger than the whole budget is admitted rather than
        # waiting forever on a condition that can never hold.
        if limit <= 0 || size >= limit
          @buffered_bytes += size
          return
        end
        deadline = monotonic + @config["max.block.ms"] / 1000.0
        while @buffered_bytes + size > limit
          raise Error, "producer is closed" if @closed

          remaining = deadline - monotonic
          if remaining <= 0
            raise BufferFullError,
                  "producer buffer full: #{@buffered_bytes} of #{limit} bytes unsent " \
                  "after max.block.ms=#{@config['max.block.ms']}"
          end
          @space.wait(remaining)
        end
        @buffered_bytes += size
      end
    end

    def release(size)
      @lock.synchronize do
        @buffered_bytes = [@buffered_bytes - size, 0].max
        @space.broadcast
      end
    end

    # --- sender thread -----------------------------------------------------

    def sender_loop
      loop do
        batches = next_ready_batches
        break if batches.nil?

        batches.each { |batch| deliver(batch) }
      end
    rescue StandardError => e
      # Should be unreachable (deliver rescues), but a dead sender would hang
      # every future, so fail what is left loudly rather than silently.
      @lock.synchronize do
        @slots.values.flatten.each { |batch| batch.items.each { |item| item.future.complete(nil, e) } }
        @slots.clear
      end
    end

    # Block until at least one batch is ready; nil once closed and drained.
    # At most one batch per partition is taken per round, which keeps
    # per-partition order with a single sender.
    def next_ready_batches
      linger = @config["linger.ms"] / 1000.0
      @lock.synchronize do
        loop do
          @slots.delete_if { |_, batches| batches.empty? }
          return nil if @closed && @slots.empty?

          now = monotonic
          ready = []
          wait = nil
          @slots.each_value do |batches|
            batch = batches.first
            age = now - batch.created_at
            if @closed || batch.force || batches.size > 1 || batch.size >= @config["batch.size"] || age >= linger
              ready << batches.shift
            else
              remaining = linger - age
              wait = remaining if wait.nil? || remaining < wait
            end
          end
          unless ready.empty?
            @inflight.concat(ready)
            return ready
          end
          @wakeup.wait(wait)
        end
      end
    end

    def deliver(batch)
      deadline = batch.created_at + @config["delivery.timeout.ms"] / 1000.0
      if monotonic >= deadline
        raise TimeoutError, "#{batch.items.size} record(s) for #{batch.topic}-#{batch.partition} " \
                            "expired after delivery.timeout.ms=#{@config['delivery.timeout.ms']}"
      end

      base_offset = produce_batch(batch, deadline)
      batch.items.each_with_index do |item, index|
        offset = base_offset.negative? ? -1 : base_offset + index
        item.future.complete(RecordMetadata.new(batch.topic, batch.partition, offset, item.timestamp), nil)
      end
    rescue StandardError => e
      @lock.synchronize { @unreported_error ||= e }
      batch.items.each { |item| item.future.complete(nil, e) }
    ensure
      @lock.synchronize { @inflight.delete(batch) }
      release(batch.size)
    end

    def produce_batch(batch, deadline)
      # The batch stores one base timestamp and a delta per record;
      # max_timestamp is the newest record's time.
      max_timestamp = batch.items.map(&:timestamp).max
      records = batch.items.map do |item|
        record = item.record.dup
        record.timestamp_delta = item.timestamp - max_timestamp
        record
      end
      encoded = Protocol.encode_record_batch(records, max_timestamp, @codec)
      body = Protocol.body_writer
                     .string(batch.topic).int32(batch.partition).int32(@acks)
                     .int32(@config["request.timeout.ms"]).int64(encoded.bytesize)
                     .raw(encoded).bytes

      attempts_left = @config["retries"]
      loop do
        error = catch(:retry) do
          begin
            connection = @router.connection_for(batch.topic, batch.partition)
          rescue ConnectionError => e
            # Routing or dialling failed, so nothing was sent and a retry
            # cannot duplicate. A failure once the request is on the wire is
            # not retried: the broker may already have appended it.
            throw :retry, e
          end
          if @acks.zero?
            connection.send_oneway(Protocol::ApiKey::PRODUCE, body)
            return -1
          end
          remaining_ms = ((deadline - monotonic) * 1000).ceil
          timeout = [[@config["request.timeout.ms"], remaining_ms].min, 1].max + 1000
          reader = Protocol.body_reader(connection.request(Protocol::ApiKey::PRODUCE, body, timeout_ms: timeout))
          reader.skip_string # topic
          reader.skip_int32  # partition
          code = reader.int32
          base_offset = reader.int64
          reader.skip_int64 # log_append_time_ms
          return base_offset if code == ErrorCode::NONE

          server_error = ServerError.new(code, "produce to #{batch.topic}-#{batch.partition}")
          # Only codes the broker returns before appending are retried.
          raise server_error unless server_error.retriable?

          server_error
        end
        raise error if attempts_left <= 0 || monotonic >= deadline

        attempts_left -= 1
        # A stale route is the most common retriable cause; resending to the
        # same broker would just repeat it.
        stale = !error.is_a?(ServerError) || ErrorCode::STALE_ROUTE.include?(error.code)
        begin
          @router.refresh(batch.topic) if stale
        rescue Error
          nil # the next attempt reports it
        end
        sleep(@config["retry.backoff.ms"] / 1000.0)
      end
    end

    def now_ms = Process.clock_gettime(Process::CLOCK_REALTIME, :millisecond)
    def monotonic = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end
end
