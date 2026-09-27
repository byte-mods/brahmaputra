module Brahmaputra
  # One record delivered to the application.
  struct ConsumedRecord
    getter topic : String
    getter partition : Int32
    getter offset : Int64
    # nil is a null key; an empty slice is an empty key.
    getter key : Bytes?
    # nil is a tombstone; an empty slice is an empty value.
    getter value : Bytes?
    # Absolute unix milliseconds.
    getter timestamp : Int64
    getter headers : Array(Header)

    def initialize(@topic, @partition, @offset, @key, @value, @timestamp, @headers)
    end

    # The first value stored under `key`, or nil.
    def header(key : String) : Bytes?
      @headers.find { |h| h.key == key }.try(&.value)
    end

    def value_string : String?
      @value.try { |v| String.new(v) }
    end

    def key_string : String?
      @key.try { |k| String.new(k) }
    end
  end

  # Consumer settings, named as Kafka names them.
  class ConsumerConfig
    property client_id : String = "brahmaputra-crystal"
    # `fetch.max.bytes`: cap on one fetch response.
    property fetch_max_bytes : Int32 = 8 * 1024 * 1024
    # `fetch.min.bytes`: the broker answers early once this many bytes are ready.
    property fetch_min_bytes : Int32 = 1
    # `fetch.max.wait.ms`: the long-poll ceiling when caught up.
    property fetch_max_wait_ms : Int32 = 500
    # `max.poll.records`
    property max_poll_records : Int32 = 500
    # `isolation.level`: Protocol::READ_UNCOMMITTED or READ_COMMITTED.
    property isolation_level : Int32 = Protocol::READ_UNCOMMITTED
    # `client.rack`
    property client_rack : String = ""
    property connect_timeout_ms : Int32 = 30_000
    # Client-side bound on one request round trip; 0 disables it.
    property socket_timeout_ms : Int32 = 120_000

    def initialize
    end

    def initialize(&)
      yield self
    end
  end

  # Reads partitions directly, with no group coordination.
  class Consumer
    getter router : Router
    getter config : ConsumerConfig

    def initialize(bootstrap : String, @config : ConsumerConfig = ConsumerConfig.new)
      timeout = @config.socket_timeout_ms > 0 ? @config.socket_timeout_ms.milliseconds : nil
      @router = Router.new(bootstrap, @config.client_id, @config.connect_timeout_ms.milliseconds, timeout)
    end

    def close : Nil
      @router.close
    end

    def partitions(topic : String) : Array(Int32)
      @router.partitions(topic)
    end

    # Resolves EARLIEST, LATEST or a unix-ms timestamp to an offset.
    def list_offsets(topic : String, partition : Int32, timestamp : Int64) : Int64
      body = Protocol::Writer.new.string(topic).int32(partition).int64(timestamp).to_slice
      r = Protocol::Reader.new(@router.connection_for(topic, partition).request(Protocol::LIST_OFFSETS, body))
      r.string # topic
      r.int32  # partition
      code = r.int32
      offset = r.int64
      r.int64 # timestamp
      raise ServerError.new(code, "list_offsets #{topic}-#{partition}") unless code == ErrorCode::NONE
      offset
    end

    # Reads one partition from `offset`.
    def fetch(topic : String, partition : Int32, offset : Int64, max_wait_ms : Int32 = @config.fetch_max_wait_ms) : Array(ConsumedRecord)
      fetch_verbose(topic, partition, offset, max_wait_ms)[0]
    end

    # Also returns the partition's high watermark.
    def fetch_verbose(topic : String, partition : Int32, offset : Int64,
                      max_wait_ms : Int32 = @config.fetch_max_wait_ms) : {Array(ConsumedRecord), Int64}
      max_wait_ms = @config.fetch_max_wait_ms if max_wait_ms > @config.fetch_max_wait_ms
      max_wait_ms = 0 if max_wait_ms < 0
      body = Protocol::Writer.new
        .string(topic).int32(partition).int64(offset)
        .int32(@config.fetch_max_bytes).int32(max_wait_ms).int32(@config.fetch_min_bytes)
        .int32(@config.isolation_level).string(@config.client_rack).to_slice

      code, high_watermark, batches = fetch_once(@router.connection_for(topic, partition), body)
      if code == ErrorCode::NOT_LEADER_OR_FOLLOWER
        @router.refresh(topic)
        code, high_watermark, batches = fetch_once(@router.connection_for(topic, partition), body)
      end
      raise ServerError.new(code, "fetch #{topic}-#{partition}") unless code == ErrorCode::NONE

      out = [] of ConsumedRecord
      batches.each do |batch|
        batch.records.each_with_index do |record, index|
          record_offset = batch.base_offset + index
          # A batch can start before the requested offset.
          next if record_offset < offset
          out << ConsumedRecord.new(topic, partition, record_offset, record.key, record.value,
            batch.max_timestamp + record.timestamp_delta, record.headers)
        end
      end
      {out, high_watermark}
    end

    private def fetch_once(conn : Connection, body : Bytes) : {Int32, Int64, Array(DecodedBatch)}
      r = Protocol::Reader.new(conn.request(Protocol::FETCH, body))
      r.string # topic
      r.int32  # partition
      code = r.int32
      high_watermark = r.int64
      r.int64 # last_stable_offset
      batches_length = r.int64
      # Read even though unused: the batches trail the whole struct.
      r.int32 # preferred_read_replica
      trailing = r.rest
      if batches_length < 0 || batches_length > trailing.size
        raise DecodeError.new("fetch response batch length #{batches_length} is outside the #{trailing.size} bytes it carries")
      end
      raw = trailing[0, batches_length.to_i32]
      batches = [] of DecodedBatch
      pos = 0
      while pos < raw.size
        batch, pos = RecordBatch.decode(raw, pos)
        batches << batch
      end
      {code, high_watermark, batches}
    end
  end
end
