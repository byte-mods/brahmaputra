module Brahmaputra
  # A (topic, partition) pair. Ordered by topic, then partition as an
  # integer — never as a string, or partition 10 would sort before 2.
  struct TopicPartition
    include Comparable(TopicPartition)
    getter topic : String
    getter partition : Int32

    def initialize(@topic : String, @partition : Int32)
    end

    def <=>(other : TopicPartition) : Int32
      c = @topic <=> other.topic
      c != 0 ? c : @partition <=> other.partition
    end

    def to_s(io : IO) : Nil
      io << @topic << '-' << @partition
    end

    def inspect(io : IO) : Nil
      to_s(io)
    end
  end

  # Producer settings, named as Kafka names them.
  class ProducerConfig
    property client_id : String = "brahmaputra-crystal"
    # `acks`: 0 fire-and-forget, 1 leader append, -1 (all) every in-sync replica.
    property acks : Int32 = 1
    # `batch.size`: flush a partition's buffer once it holds this many bytes.
    property batch_size : Int32 = 16 * 1024
    # `linger.ms`: flush every non-empty buffer at least this often. 0 sends
    # each record immediately.
    property linger_ms : Int32 = 5
    # `compression.type`: none, gzip built in; lz4, zstd, snappy once registered.
    property compression_type : String = "none"
    # `request.timeout.ms`: the broker-side wait for acknowledgements.
    property request_timeout_ms : Int32 = 30_000
    # `retries` of a send refused with a retriable error (one the broker
    # returns before appending, so a retry cannot duplicate).
    property retries : Int32 = 5
    # `retry.backoff.ms`
    property retry_backoff_ms : Int32 = 100
    # `delivery.timeout.ms`: caps a send, first attempt through last retry.
    property delivery_timeout_ms : Int32 = 120_000
    # `buffer.memory`: cap on unflushed record bytes held client-side.
    property buffer_memory : Int64 = 32_i64 * 1024 * 1024
    # `max.block.ms`: how long a send may block on a full buffer.
    property max_block_ms : Int32 = 60_000
    # `socket.connection.setup.timeout.ms`
    property connect_timeout_ms : Int32 = 30_000
    # Client-side bound on one request round trip; nil or 0 disables it.
    property socket_timeout_ms : Int32 = 120_000

    def initialize
    end

    # Configure in a block: `ProducerConfig.new { |c| c.acks = -1 }`.
    def initialize(&)
      yield self
    end

    def buffer_memory=(value : Int) : Nil
      @buffer_memory = value.to_i64
    end
  end

  # Batches records per partition and sends each batch as one Produce
  # request. Share one across fibers: the batching is the point.
  class Producer
    getter router : Router
    getter config : ProducerConfig

    private struct Pending
      getter record : Record
      getter created_ms : Int64

      def initialize(@record, @created_ms)
      end
    end

    def initialize(bootstrap : String, @config : ProducerConfig = ProducerConfig.new)
      @codec = Compression.from_name(@config.compression_type)
      @router = Router.new(bootstrap, @config.client_id,
        @config.connect_timeout_ms.milliseconds, socket_timeout(@config.socket_timeout_ms))
      @mutex = Mutex.new
      @buffers = {} of TopicPartition => Array(Pending)
      @sizes = {} of TopicPartition => Int64
      @buffered_bytes = 0_i64
      @round_robin = 0
      @closed = false
      # One lock per partition, held across the round trip and retries: a
      # partition has at most one batch in flight, so a linger flush and a
      # batch-full flush cannot overtake each other.
      @send_locks = {} of TopicPartition => Mutex
      # First failure of a linger-driven flush; reported by the next
      # flush/close, because those records already left the buffer.
      @background_error = nil.as(Exception?)
      @stop = Channel(Nil).new
      @done = Channel(Nil).new(1)
      if @config.linger_ms > 0
        spawn(name: "brahmaputra-linger") { linger_loop }
      else
        @done.send(nil)
      end
    end

    private def socket_timeout(ms : Int32) : Time::Span?
      ms > 0 ? ms.milliseconds : nil
    end

    # Buffers one record, partitioned by murmur2(key), or round-robin when
    # the key is nil. Call `flush` to await delivery.
    def send(topic : String, value : Bytes | String | Nil, key : Bytes | String | Nil = nil,
             headers : Array(Header) = [] of Header) : Nil
      key_bytes = Brahmaputra.to_bytes(key)
      send_to(topic, choose_partition(topic, key_bytes), value, key_bytes, headers)
    end

    # Buffers one record on an explicit partition. A nil value is a
    # tombstone, distinct from an empty value.
    def send_to(topic : String, partition : Int32, value : Bytes | String | Nil,
                key : Bytes | String | Nil = nil, headers : Array(Header) = [] of Header) : Nil
      raise Error.new("producer is closed") if @closed
      record = Record.new(Brahmaputra.to_bytes(key), Brahmaputra.to_bytes(value), 0_i64, headers)
      size = record_size(record)
      reserve(size)
      slot = TopicPartition.new(topic, partition)
      full = @mutex.synchronize do
        (@buffers[slot] ||= [] of Pending) << Pending.new(record, Brahmaputra.now_ms)
        @sizes[slot] = (@sizes[slot]? || 0_i64) + size
        @sizes[slot] >= @config.batch_size
      end
      flush_partition(slot) if @config.linger_ms == 0 || full
    end

    # Sends one record on its own and returns its offset. A full round trip
    # per record — correct, and slow.
    def send_sync(topic : String, value : Bytes | String | Nil, key : Bytes | String | Nil = nil,
                  headers : Array(Header) = [] of Header) : Int64
      key_bytes = Brahmaputra.to_bytes(key)
      partition = choose_partition(topic, key_bytes)
      record = Record.new(key_bytes, Brahmaputra.to_bytes(value), 0_i64, headers)
      slot = TopicPartition.new(topic, partition)
      send_lock(slot).synchronize do
        produce(topic, partition, [Pending.new(record, Brahmaputra.now_ms)])
      end
    end

    # Sends every buffered record and waits for acknowledgement. Also
    # raises the failure of any background (linger) flush since the last
    # call, because those records are gone and nothing else would say so.
    def flush : Nil
      error = nil
      begin
        flush_all
      rescue ex
        error = ex
      end
      background = @mutex.synchronize do
        b = @background_error
        @background_error = nil
        b
      end
      if e = error || background
        raise e
      end
    end

    # Flushes, stops the linger fiber and releases connections. Resources
    # are released even when the final flush fails; that failure is still
    # raised.
    def close : Nil
      return if @closed
      error = nil
      begin
        flush
      rescue ex
        error = ex
      end
      @closed = true
      @stop.close
      select
      when @done.receive?
      when timeout(2.seconds)
      end
      @router.close
      if e = error
        raise e
      end
    end

    def closed? : Bool
      @closed
    end

    private def record_size(record : Record) : Int64
      size = 16_i64 + (record.key.try(&.size) || 0) + (record.value.try(&.size) || 0)
      record.headers.each { |h| size += h.key.bytesize + (h.value.try(&.size) || 0) + 4 }
      size
    end

    private def choose_partition(topic : String, key : Bytes?) : Int32
      partitions = @router.partitions(topic)
      return Protocol.partition_for_key(key, partitions) if key
      index = @mutex.synchronize do
        i = @round_robin % partitions.size
        @round_robin = @round_robin &+ 1
        i
      end
      partitions[index]
    end

    # Blocks until `size` more bytes may be buffered: what makes
    # buffer.memory real. A producer faster than its broker is slowed down
    # here instead of growing without limit.
    private def reserve(size : Int64) : Nil
      limit = @config.buffer_memory
      if limit <= 0 || size >= limit
        # Larger than the whole budget: admitted rather than waiting on a
        # condition that can never hold. Refusing it is the broker's job.
        @mutex.synchronize { @buffered_bytes += size }
        return
      end
      deadline = Time.monotonic + @config.max_block_ms.milliseconds
      loop do
        admitted = @mutex.synchronize do
          if @buffered_bytes + size <= limit
            @buffered_bytes += size
            true
          else
            false
          end
        end
        return if admitted
        if Time.monotonic >= deadline
          raise BufferFullError.new("producer buffer full: #{@buffered_bytes} of #{limit} bytes unflushed after max.block.ms=#{@config.max_block_ms}")
        end
        sleep 5.milliseconds
      end
    end

    private def release(size : Int64) : Nil
      @mutex.synchronize do
        @buffered_bytes -= size
        @buffered_bytes = 0_i64 if @buffered_bytes < 0
      end
    end

    private def linger_loop : Nil
      interval = @config.linger_ms.milliseconds
      loop do
        select
        when @stop.receive?
          break
        when timeout(interval)
        end
        break if @closed
        begin
          flush_all
        rescue ex
          # A failed background flush must not kill the fiber; the next
          # flush/close raises it to a caller who can act on it.
          @mutex.synchronize { @background_error ||= ex }
        end
      end
    ensure
      @done.send(nil) rescue nil
    end

    private def flush_all : Nil
      slots = @mutex.synchronize { @buffers.select { |_, v| !v.empty? }.keys }
      slots.each { |slot| flush_partition(slot) }
    end

    private def send_lock(slot : TopicPartition) : Mutex
      @mutex.synchronize { @send_locks[slot] ||= Mutex.new }
    end

    private def flush_partition(slot : TopicPartition) : Nil
      send_lock(slot).synchronize do
        batch, size = @mutex.synchronize do
          b = @buffers.delete(slot) || [] of Pending
          {b, @sizes.delete(slot) || 0_i64}
        end
        next if batch.empty?
        release(size)
        produce(slot.topic, slot.partition, batch)
      end
    end

    private def produce(topic : String, partition : Int32, batch : Array(Pending)) : Int64
      return -1_i64 if batch.empty?
      # One base timestamp per batch plus a delta per record; the base is
      # the newest record's time.
      max_ts = batch.max_of(&.created_ms)
      records = batch.map do |p|
        r = p.record
        r.timestamp_delta = p.created_ms - max_ts
        r
      end
      encoded = RecordBatch.encode(records, max_ts, @codec)
      body = Protocol::Writer.new
        .string(topic).int32(partition).int32(@config.acks).int32(@config.request_timeout_ms)
        .int64(encoded.size.to_i64).raw(encoded).to_slice

      if @config.acks == 0
        @router.connection_for(topic, partition).send_oneway(Protocol::PRODUCE, body)
        return -1_i64
      end

      deadline = Time.monotonic + @config.delivery_timeout_ms.milliseconds
      attempts_left = @config.retries
      loop do
        conn = @router.connection_for(topic, partition)
        r = Protocol::Reader.new(conn.request(Protocol::PRODUCE, body))
        r.string # topic
        r.int32  # partition
        code = r.int32
        base_offset = r.int64
        r.int64 # log_append_time_ms
        return base_offset if code == ErrorCode::NONE
        if !ErrorCode.retriable?(code) || attempts_left <= 0 || Time.monotonic >= deadline
          raise ServerError.new(code, "produce to #{topic}-#{partition}")
        end
        attempts_left -= 1
        if code.in?(ErrorCode::NOT_LEADER_OR_FOLLOWER, ErrorCode::FENCED_LEADER_EPOCH, ErrorCode::UNKNOWN_LEADER_EPOCH)
          @router.refresh(topic) rescue nil
        end
        sleep @config.retry_backoff_ms.milliseconds
      end
    end
  end
end
