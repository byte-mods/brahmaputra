# frozen_string_literal: true

require "zlib"
require "stringio"

module Brahmaputra
  # Brahmaputra wire protocol: framing, BitPacker bodies, record batches.
  #
  # Three encodings share one connection and do not agree with each other:
  #
  # * The frame header is fixed big-endian: an int32 length prefix, then
  #   api key / api version / correlation id and an int16-prefixed client id.
  # * A request/response body is BitPacker: every integer is a zigzag varint,
  #   every string and array a varint count then its contents, and the whole
  #   body starts with the schema version string.
  # * A record batch is neither: fixed big-endian header fields and *plain*
  #   (non-zigzag) varints inside each record, because the broker stamps
  #   offsets into it in place and validates its CRC without decoding it.
  module Protocol
    SCHEMA_VERSION = "1.0.0"
    # Wire version this client speaks. The broker requires an exact match.
    API_VERSION = 4

    READ_UNCOMMITTED = 0
    READ_COMMITTED = 1

    BATCH_HEADER_LEN = 12
    MIN_BATCH_LENGTH = 4 + 1 + 4 + 2 + 4 + 8
    PRODUCER_EXTENSION_LEN = 8 + 2 + 4
    MAGIC_V1 = 1
    MAGIC_V2 = 2
    COMPRESSION_MASK = 0x0007
    HEADERS_BIT = 0x0008
    # Some record in the batch has a null value (a tombstone). Set only when
    # one is present, so a batch without one encodes exactly as it always did.
    NULL_VALUE_BIT = 0x0040
    MAX_DECOMPRESSED_BYTES = 256 * 1024 * 1024

    MASK32 = 0xFFFF_FFFF
    MASK64 = 0xFFFF_FFFF_FFFF_FFFF

    module ApiKey
      PRODUCE = 0
      FETCH = 1
      LIST_OFFSETS = 2
      METADATA = 3
      REPLICA_FETCH = 4
      OFFSETS_FOR_LEADER_EPOCH = 5
      INIT_PRODUCER_ID = 6
      JOIN_GROUP = 7
      SYNC_GROUP = 8
      HEARTBEAT = 9
      OFFSET_COMMIT = 10
      OFFSET_FETCH = 11
      LIST_GROUPS = 12
      DESCRIBE_GROUP = 13
      API_VERSIONS = 14
      PRODUCE_MULTI = 15
      FETCH_MULTI = 16
      AUTHENTICATE = 17
      LEAVE_GROUP = 18
    end

    def self.binary(size = 0) = String.new(capacity: size, encoding: Encoding::BINARY)

    # -----------------------------------------------------------------------
    # Varints
    # -----------------------------------------------------------------------

    def self.put_uvarint(buf, value)
      raise ArgumentError, "negative uvarint #{value}" if value.negative?

      while value >= 0x80
        buf << ((value & 0x7F) | 0x80).chr
        value >>= 7
      end
      buf << value.chr
    end

    # Returns [value, next_pos].
    def self.get_uvarint(data, pos)
      result = 0
      shift = 0
      loop do
        byte = data.getbyte(pos)
        raise ProtocolError, "truncated varint" if byte.nil?

        pos += 1
        result |= (byte & 0x7F) << shift
        return [result, pos] if byte & 0x80 == 0

        shift += 7
        raise ProtocolError, "varint overflows 64 bits" if shift > 63
      end
    end

    def self.zigzag64(value) = ((value << 1) ^ (value >> 63)) & MASK64
    def self.zigzag32(value) = ((value << 1) ^ (value >> 31)) & MASK32
    def self.unzigzag(raw) = (raw >> 1) ^ -(raw & 1)

    # -----------------------------------------------------------------------
    # BitPacker bodies
    # -----------------------------------------------------------------------

    # Builds a BitPacker body. Every integer is zigzag-varint encoded, which
    # is why this cannot share code with the record-batch encoder.
    class Writer
      def initialize
        @buf = Protocol.binary(64)
      end

      def raw(bytes)
        @buf << bytes.b
        self
      end

      def int32(value)
        value = value.to_i
        raise ArgumentError, "int32 out of range: #{value}" unless value.between?(-2**31, 2**31 - 1)

        Protocol.put_uvarint(@buf, Protocol.zigzag32(value))
        self
      end

      def int64(value)
        value = value.to_i
        raise ArgumentError, "int64 out of range: #{value}" unless value.between?(-2**63, 2**63 - 1)

        Protocol.put_uvarint(@buf, Protocol.zigzag64(value))
        self
      end

      def bool(value)
        @buf << (value ? "\x01" : "\x00")
        self
      end

      def string(value)
        bytes = value.to_s.b
        int32(bytes.bytesize)
        @buf << bytes
        self
      end

      def string_array(values)
        int32(values.size)
        values.each { |value| string(value) }
        self
      end

      def bytes = @buf
    end

    # Reads a BitPacker body.
    class Reader
      attr_reader :pos

      def initialize(data)
        @data = data.b
        @pos = 0
      end

      def remaining = @data.bytesize - @pos

      def uvarint
        value, @pos = Protocol.get_uvarint(@data, @pos)
        value
      end

      def int32 = Protocol.unzigzag(uvarint)
      def int64 = Protocol.unzigzag(uvarint)

      def bool
        byte = @data.getbyte(@pos)
        raise ProtocolError, "truncated bool" if byte.nil?

        @pos += 1
        byte != 0
      end

      def string
        length = int32
        raise ProtocolError, "truncated string" if length.negative? || @pos + length > @data.bytesize

        value = @data.byteslice(@pos, length).force_encoding(Encoding::UTF_8)
        @pos += length
        value
      end

      def string_array
        Array.new(int32) { string }
      end

      def rest
        value = @data.byteslice(@pos, @data.bytesize - @pos)
        @pos = @data.bytesize
        value
      end

      # The encoding is positional: a skipped field must still be read.
      alias skip_string string
      alias skip_int32 int32
      alias skip_int64 int64
    end

    # A writer already carrying the schema version every body starts with.
    def self.body_writer = Writer.new.string(SCHEMA_VERSION)

    # A reader positioned past the (verified) schema version.
    def self.body_reader(data)
      reader = Reader.new(data)
      version = reader.string
      unless version == SCHEMA_VERSION
        raise ProtocolError,
              "schema version mismatch: broker speaks #{version}, this client speaks #{SCHEMA_VERSION}"
      end
      reader
    end

    # Read a response's leading error code without consuming it. Every group
    # response starts with one.
    def self.peek_error_code(body)
      body_reader(body).int32
    rescue ProtocolError
      ErrorCode::NONE
    end

    # -----------------------------------------------------------------------
    # Frames
    # -----------------------------------------------------------------------

    def self.encode_frame(api_key, correlation_id, client_id, body)
      client = client_id&.b
      header = [api_key, API_VERSION, correlation_id, client ? client.bytesize : -1].pack("s>s>l>s>")
      header << client if client
      length = header.bytesize + body.bytesize
      frame = binary(4 + length)
      frame << [length].pack("l>") << header << body
    end

    # Returns [correlation_id, body].
    def self.decode_frame_payload(payload)
      raise ProtocolError, "frame payload shorter than its header" if payload.bytesize < 10

      correlation_id, client_len = payload.unpack("@4l>s>")
      offset = 10
      offset += client_len if client_len >= 0
      raise ProtocolError, "frame client id runs past the payload" if offset > payload.bytesize

      [correlation_id, payload.byteslice(offset, payload.bytesize - offset)]
    end

    # -----------------------------------------------------------------------
    # CRC32C (Castagnoli). Zlib.crc32 is the wrong polynomial.
    # -----------------------------------------------------------------------

    CRC32C_TABLE = Array.new(256) do |index|
      crc = index
      8.times { crc = crc.odd? ? (crc >> 1) ^ 0x82F63B78 : crc >> 1 }
      crc
    end.freeze

    def self.crc32c(data, from = 0)
      crc = MASK32
      table = CRC32C_TABLE
      data = data.byteslice(from, data.bytesize - from) if from.positive?
      data.each_byte { |byte| crc = table[(crc ^ byte) & 0xFF] ^ (crc >> 8) }
      crc ^ MASK32
    end

    # -----------------------------------------------------------------------
    # Compression
    # -----------------------------------------------------------------------

    module Compression
      NONE = 0
      LZ4 = 1
      ZSTD = 2
      SNAPPY = 3
      GZIP = 4

      NAMES = { "none" => NONE, "lz4" => LZ4, "zstd" => ZSTD, "snappy" => SNAPPY, "gzip" => GZIP }.freeze

      @external = {}
      @lock = Mutex.new

      class << self
        def parse(name)
          return name if name.is_a?(Integer) && NAMES.value?(name)

          NAMES.fetch(name.to_s.downcase) do
            raise ArgumentError, "unknown compression.type #{name.inspect} (#{NAMES.keys.join(', ')})"
          end
        end

        def name_of(codec) = NAMES.key(codec) || "unknown(#{codec})"

        # Register a codec the standard library does not provide. Both
        # callables take and return a binary String.
        #
        # The lz4 payload the broker expects is a little-endian uint32 of the
        # uncompressed length followed by a raw LZ4 block, not the frame format.
        def register(codec, compress:, decompress:)
          codec = parse(codec)
          @lock.synchronize { @external[codec] = [compress, decompress] }
        end

        def registered?(codec)
          codec = parse(codec)
          [NONE, GZIP].include?(codec) || @lock.synchronize { @external.key?(codec) }
        end

        def compress(codec, payload)
          case codec
          when NONE then payload
          when GZIP
            io = StringIO.new(Protocol.binary)
            gz = Zlib::GzipWriter.new(io)
            gz.write(payload)
            gz.close
            io.string
          else external(codec, "compression")[0].call(payload).b
          end
        end

        def decompress(codec, payload)
          case codec
          when NONE then payload
          when GZIP then gunzip(payload)
          else external(codec, "decompression")[1].call(payload).b
          end
        end

        private

        def external(codec, what)
          found = @lock.synchronize { @external[codec] }
          return found if found

          raise Error, "#{name_of(codec)} #{what} is not available; " \
                       "register it with Brahmaputra.register_codec or use none/gzip"
        end

        # Capped so a corrupt or hostile batch cannot name gigabytes of
        # output before it can be rejected.
        def gunzip(payload)
          out = Protocol.binary(payload.bytesize * 4)
          inflater = Zlib::Inflate.new(Zlib::MAX_WBITS + 32)
          begin
            inflater.inflate(payload) do |chunk|
              out << chunk
              raise ProtocolError, "decompressed batch exceeds #{MAX_DECOMPRESSED_BYTES} bytes" if out.bytesize > MAX_DECOMPRESSED_BYTES
            end
            inflater.finish
          rescue Zlib::Error => e
            raise ProtocolError, "gzip: #{e.message}"
          ensure
            inflater.close
          end
          out
        end
      end
    end

    # -----------------------------------------------------------------------
    # Record batches
    # -----------------------------------------------------------------------

    # One record as it travels inside a batch.
    BatchRecord = Struct.new(:key, :value, :timestamp_delta, :headers)

    # Encode one record batch exactly as the broker expects it. The broker
    # never re-encodes this; these bytes go to disk.
    def self.encode_record_batch(records, max_timestamp, codec = Compression::NONE)
      has_headers = records.any? { |record| record.headers && !record.headers.empty? }
      # A nil value is a tombstone and needs the widened length encoding; an
      # empty string is an ordinary record and must not trigger it.
      has_nulls = records.any? { |record| record.value.nil? }

      payload = binary(256)
      records.each do |record|
        rec = binary(64)
        if record.key.nil?
          put_uvarint(rec, 0)
        else
          key = record.key.b
          put_uvarint(rec, key.bytesize + 1)
          rec << key
        end
        if has_nulls
          if record.value.nil?
            put_uvarint(rec, 0)
          else
            value = record.value.b
            put_uvarint(rec, value.bytesize + 1)
            rec << value
          end
        else
          value = record.value.b
          put_uvarint(rec, value.bytesize)
          rec << value
        end
        put_uvarint(rec, zigzag64(record.timestamp_delta.to_i))
        if has_headers
          headers = record.headers || []
          put_uvarint(rec, headers.size)
          headers.each do |header|
            hkey = header.key.to_s.b
            put_uvarint(rec, hkey.bytesize)
            rec << hkey
            if header.value.nil?
              put_uvarint(rec, 0)
            else
              hvalue = header.value.b
              put_uvarint(rec, hvalue.bytesize + 1)
              rec << hvalue
            end
          end
        end
        put_uvarint(payload, rec.bytesize)
        payload << rec
      end

      compressed = Compression.compress(codec, payload)
      attributes = codec & COMPRESSION_MASK
      attributes |= HEADERS_BIT if has_headers
      attributes |= NULL_VALUE_BIT if has_nulls

      batch_length = MIN_BATCH_LENGTH + compressed.bytesize
      out = binary(BATCH_HEADER_LEN + batch_length)
      # base_offset and leader_epoch are stamped by the broker.
      out << [0, batch_length, 0, MAGIC_V1, 0, attributes, [records.size - 1, 0].max, max_timestamp]
             .pack("q>l>l>CNnl>q>")
      out << compressed
      crc = crc32c(out, 21)
      out[17, 4] = [crc].pack("N")
      out
    end

    # One decoded batch.
    DecodedBatch = Struct.new(:base_offset, :max_timestamp, :records)

    # Decode one batch starting at offset; returns [batch, next_offset].
    def self.decode_record_batch(data, offset)
      raise ProtocolError, "truncated batch header" if data.bytesize - offset < BATCH_HEADER_LEN

      base_offset, batch_length = data.unpack("@#{offset}q>l>")
      raise ProtocolError, "batch_length too small" if batch_length < MIN_BATCH_LENGTH

      body_at = offset + BATCH_HEADER_LEN
      finish = body_at + batch_length
      raise ProtocolError, "truncated batch body" if finish > data.bytesize

      magic, stored, attributes, _last_delta, max_timestamp = data.unpack("@#{body_at + 4}CNnl>q>")
      raise ProtocolError, "unsupported magic #{magic}" unless [MAGIC_V1, MAGIC_V2].include?(magic)

      crc_body = data.byteslice(body_at + 9, finish - body_at - 9)
      computed = crc32c(crc_body)
      if stored != computed
        raise ProtocolError, format("crc mismatch: stored 0x%08x, computed 0x%08x", stored, computed)
      end

      cursor = body_at + 9 + 14
      cursor += PRODUCER_EXTENSION_LEN if magic == MAGIC_V2
      payload = Compression.decompress(attributes & COMPRESSION_MASK, data.byteslice(cursor, finish - cursor))
      records = decode_records(payload, attributes & HEADERS_BIT != 0, attributes & NULL_VALUE_BIT != 0)
      [DecodedBatch.new(base_offset, max_timestamp, records), finish]
    end

    def self.decode_records(payload, has_headers, has_nulls)
      records = []
      pos = 0
      size = payload.bytesize
      take = lambda do |length|
        raise ProtocolError, "truncated record" if length.negative? || pos + length > size

        bytes = payload.byteslice(pos, length)
        pos += length
        bytes
      end

      while pos < size
        length, pos = get_uvarint(payload, pos)
        raise ProtocolError, "truncated record" if pos + length > size

        finish = pos + length
        key_plus_one, pos = get_uvarint(payload, pos)
        key = key_plus_one.zero? ? nil : take.call(key_plus_one - 1)

        raw_value_len, pos = get_uvarint(payload, pos)
        # A tombstone decodes to nil, distinct from an empty value.
        value = if has_nulls && raw_value_len.zero?
                  nil
                else
                  take.call(has_nulls ? raw_value_len - 1 : raw_value_len)
                end

        raw_delta, pos = get_uvarint(payload, pos)
        delta = unzigzag(raw_delta)

        headers = []
        if has_headers
          count, pos = get_uvarint(payload, pos)
          # A count larger than the bytes left is corrupt; allocating on it
          # would let a two-byte record ask for gigabytes.
          raise ProtocolError, "record header count exceeds record" if count > finish - pos

          count.times do
            key_len, pos = get_uvarint(payload, pos)
            hkey = take.call(key_len).force_encoding(Encoding::UTF_8)
            value_plus_one, pos = get_uvarint(payload, pos)
            hvalue = value_plus_one.zero? ? nil : take.call(value_plus_one - 1)
            headers << RecordHeader.new(hkey, hvalue)
          end
        end
        raise ProtocolError, "trailing bytes in record" unless pos == finish

        records << BatchRecord.new(key, value, delta, headers)
      end
      records
    end

    # -----------------------------------------------------------------------
    # Partitioning
    # -----------------------------------------------------------------------

    # Kafka's 32-bit murmur2, transcribed so a Ruby producer and a Rust or
    # Java producer writing the same key land on the same partition.
    def self.murmur2(data)
      data = (data || "").b
      seed = 0x9747B28C
      m = 0x5BD1E995
      length = data.bytesize
      h = (seed ^ length) & MASK32
      chunks = length / 4
      words = data.unpack("V#{chunks}")
      words.each do |k|
        k = (k * m) & MASK32
        k ^= k >> 24
        k = (k * m) & MASK32
        h = (h * m) & MASK32
        h ^= k
      end
      tail = chunks * 4
      rest = length - tail
      if rest >= 3
        h ^= data.getbyte(tail + 2) << 16
      end
      if rest >= 2
        h ^= data.getbyte(tail + 1) << 8
      end
      if rest >= 1
        h ^= data.getbyte(tail)
        h = (h * m) & MASK32
      end
      h ^= h >> 13
      h = (h * m) & MASK32
      h ^= h >> 15
      h
    end

    # murmur2(key) % partitions, matching Kafka's default partitioner.
    def self.partition_for_key(key, partitions)
      partitions[(murmur2(key) & 0x7FFFFFFF) % partitions.size]
    end
  end

  # An ordered, possibly repeating annotation on a record. A nil value is
  # preserved as nil, distinct from an empty one.
  RecordHeader = Struct.new(:key, :value) do
    def self.coerce(headers)
      case headers
      when nil then []
      when Hash then headers.map { |key, value| new(key.to_s, value) }
      when Array
        headers.map do |header|
          header.is_a?(RecordHeader) ? header : new(header[0].to_s, header[1])
        end
      else raise ArgumentError, "headers must be a Hash or an Array of RecordHeader"
      end
    end
  end
end
