# frozen_string_literal: true

module Brahmaputra
  EARLIEST = -2
  LATEST = -1

  # One record delivered to the application. timestamp is absolute unix ms,
  # already resolved against its batch. value is nil for a tombstone.
  ConsumedRecord = Struct.new(:topic, :partition, :offset, :key, :value, :timestamp, :headers) do
    # The first value of the named header, or nil.
    def header(name)
      found = headers.find { |entry| entry.key == name }
      found&.value
    end

    def tombstone? = value.nil?
  end

  FetchResult = Struct.new(:records, :high_watermark, :last_stable_offset)

  # Reads individual partitions, with no group coordination.
  class Consumer
    DEFAULTS = {
      "fetch.max.bytes" => 8 * 1024 * 1024,
      "fetch.min.bytes" => 1,
      "fetch.max.wait.ms" => 500,
      "max.poll.records" => 500,
      # "read_uncommitted" (default) or "read_committed".
      "isolation.level" => "read_uncommitted",
      # This consumer's failure domain; empty when it has none.
      "client.rack" => ""
    }.freeze

    attr_reader :config, :router

    def initialize(config = nil, **overrides)
      @config = Config.build(DEFAULTS, config, overrides)
      @isolation = case @config["isolation.level"].to_s
                   when "read_uncommitted", "0" then Protocol::READ_UNCOMMITTED
                   when "read_committed", "1" then Protocol::READ_COMMITTED
                   else raise ArgumentError, "isolation.level must be read_uncommitted or read_committed"
                   end
      @router = Config.router_for(@config)
    end

    def close = @router.close

    def partitions(topic) = @router.partitions(topic)

    # Resolve :earliest, :latest (or EARLIEST/LATEST) or a unix-ms timestamp
    # to an offset. A timestamp resolves to the first offset at or after it.
    def list_offsets(topic, partition, timestamp)
      timestamp = case timestamp
                  when :earliest then EARLIEST
                  when :latest then LATEST
                  else Integer(timestamp)
                  end
      body = Protocol.body_writer.string(topic).int32(partition).int64(timestamp).bytes
      reader = Protocol.body_reader(with_leader_retry(topic, partition) do |connection|
        connection.request(Protocol::ApiKey::LIST_OFFSETS, body)
      end)
      reader.skip_string
      reader.skip_int32
      code = reader.int32
      offset = reader.int64
      reader.skip_int64
      raise ServerError.new(code, "list_offsets #{topic}-#{partition}") unless code == ErrorCode::NONE

      offset
    end

    def earliest_offset(topic, partition) = list_offsets(topic, partition, EARLIEST)
    def latest_offset(topic, partition) = list_offsets(topic, partition, LATEST)

    # The partition's high watermark: the offset the next record will get.
    def high_watermark(topic, partition) = fetch_verbose(topic, partition, latest_offset(topic, partition), 0).high_watermark

    # Records from offset on, at most max.poll.records of them.
    def fetch(topic, partition, offset, max_wait_ms = nil)
      fetch_verbose(topic, partition, offset, max_wait_ms).records
    end

    # Like #fetch, also returning the high watermark and last stable offset.
    def fetch_verbose(topic, partition, offset, max_wait_ms = nil)
      wait = [max_wait_ms || @config["fetch.max.wait.ms"], @config["fetch.max.wait.ms"]].min
      wait = 0 if wait.negative?
      body = Protocol.body_writer
                     .string(topic).int32(partition).int64(offset)
                     .int32(@config["fetch.max.bytes"]).int32(wait).int32(@config["fetch.min.bytes"])
                     .int32(@isolation).string(@config["client.rack"].to_s).bytes

      code, high_watermark, last_stable, batches = nil
      2.times do
        response = with_leader_retry(topic, partition) do |connection|
          connection.request(Protocol::ApiKey::FETCH, body,
                             timeout_ms: @config["request.timeout.ms"] + wait)
        end
        code, high_watermark, last_stable, batches = decode_fetch(response)
        break unless code == ErrorCode::NOT_LEADER_OR_FOLLOWER

        @router.refresh(topic)
      end
      raise ServerError.new(code, "fetch #{topic}-#{partition}") unless code == ErrorCode::NONE

      limit = @config["max.poll.records"]
      records = []
      batches.each do |batch|
        batch.records.each_with_index do |record, index|
          record_offset = batch.base_offset + index
          # A batch can start before the requested offset.
          next if record_offset < offset
          break if records.size >= limit

          records << ConsumedRecord.new(topic, partition, record_offset, record.key, record.value,
                                        batch.max_timestamp + record.timestamp_delta, record.headers)
        end
      end
      FetchResult.new(records, high_watermark, last_stable)
    end

    private

    # Run the block against the leader's connection; a broken connection is
    # redialled and retried once.
    def with_leader_retry(topic, partition)
      attempt = 0
      begin
        yield @router.connection_for(topic, partition)
      rescue ConnectionError
        attempt += 1
        raise if attempt > 1

        @router.refresh(topic)
        retry
      end
    end

    def decode_fetch(body)
      reader = Protocol.body_reader(body)
      reader.skip_string # topic
      reader.skip_int32  # partition
      code = reader.int32
      high_watermark = reader.int64
      last_stable = reader.int64
      batches_length = reader.int64
      # Read though unused: the batches trail the whole struct, so skipping
      # a field would take them from the wrong offset.
      reader.skip_int32 # preferred_read_replica
      trailing = reader.rest
      raise ProtocolError, "fetch response claims more batch bytes than it carries" if batches_length > trailing.bytesize

      raw = trailing.byteslice(0, batches_length)
      batches = []
      pos = 0
      while pos < raw.bytesize
        batch, pos = Protocol.decode_record_batch(raw, pos)
        batches << batch
      end
      [code, high_watermark, last_stable, batches]
    end
  end
end
