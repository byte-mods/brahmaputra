require "compress/gzip"

module Brahmaputra
  # Three encodings share one connection and they do not agree with each
  # other, so each encoder here is explicit about which one it is:
  #
  # * The frame header is fixed big-endian.
  # * A request/response body is BitPacker: every integer is a zigzag
  #   varint, every string and array a varint count then its contents, and
  #   the body starts with the schema version string.
  # * A record batch is neither: big-endian header fields and plain
  #   (non-zigzag) varints inside each record.
  module Protocol
    SCHEMA_VERSION = "1.0.0"
    API_VERSION    = 4_i16

    PRODUCE          =  0_i16
    FETCH            =  1_i16
    LIST_OFFSETS     =  2_i16
    METADATA         =  3_i16
    JOIN_GROUP       =  7_i16
    SYNC_GROUP       =  8_i16
    HEARTBEAT        =  9_i16
    OFFSET_COMMIT    = 10_i16
    OFFSET_FETCH     = 11_i16
    LIST_GROUPS      = 12_i16
    DESCRIBE_GROUP   = 13_i16
    API_VERSIONS     = 14_i16
    PRODUCE_MULTI    = 15_i16
    FETCH_MULTI      = 16_i16
    AUTHENTICATE     = 17_i16
    LEAVE_GROUP      = 18_i16
    READ_UNCOMMITTED =  0_i32
    READ_COMMITTED   =  1_i32

    # ---------------------------------------------------------------------
    # BitPacker
    # ---------------------------------------------------------------------

    # Builds a BitPacker body. Starts with the schema version.
    class Writer
      def initialize
        @io = IO::Memory.new(256)
        string(SCHEMA_VERSION)
      end

      def uvarint(value : UInt64) : self
        while value >= 0x80_u64
          @io.write_byte((value & 0x7f_u64).to_u8! | 0x80_u8)
          value >>= 7
        end
        @io.write_byte(value.to_u8!)
        self
      end

      def int32(value : Int32) : self
        uvarint(((value << 1) ^ (value >> 31)).to_u32!.to_u64)
      end

      def int64(value : Int64) : self
        uvarint(((value << 1) ^ (value >> 63)).to_u64!)
      end

      def bool(value : Bool) : self
        @io.write_byte(value ? 1_u8 : 0_u8)
        self
      end

      def string(value : String) : self
        int32(value.bytesize)
        @io.write(value.to_slice)
        self
      end

      def string_array(values : Array(String)) : self
        int32(values.size)
        values.each { |value| string(value) }
        self
      end

      def raw(data : Bytes) : self
        @io.write(data)
        self
      end

      def to_slice : Bytes
        @io.to_slice
      end
    end

    # Reads a BitPacker body, verifying the schema version first. Every
    # length is bounds-checked; anything malformed raises DecodeError.
    class Reader
      getter pos : Int32 = 0

      def initialize(@data : Bytes)
        version = string
        unless version == SCHEMA_VERSION
          raise DecodeError.new("schema version mismatch: broker speaks #{version.inspect}, this client speaks #{SCHEMA_VERSION.inspect}")
        end
      end

      def uvarint : UInt64
        result = 0_u64
        shift = 0
        loop do
          raise DecodeError.new("truncated varint") if @pos >= @data.size
          byte = @data[@pos]
          @pos += 1
          result |= (byte & 0x7f).to_u64 << shift
          return result if byte & 0x80 == 0
          shift += 7
          raise DecodeError.new("varint overflows 64 bits") if shift > 63
        end
      end

      def int32 : Int32
        v = uvarint
        (v >> 1).to_i32! ^ -((v & 1).to_i32!)
      end

      def int64 : Int64
        v = uvarint
        (v >> 1).to_i64! ^ -((v & 1).to_i64!)
      end

      def bool : Bool
        raise DecodeError.new("truncated bool") if @pos >= @data.size
        v = @data[@pos]
        @pos += 1
        v != 0
      end

      def string : String
        length = int32
        if length < 0 || length > @data.size - @pos
          raise DecodeError.new("truncated string")
        end
        value = String.new(@data[@pos, length])
        @pos += length
        value
      end

      # An array count, rejected if negative or larger than the bytes left
      # (every element takes at least one byte).
      def count : Int32
        n = int32
        if n < 0 || n > @data.size - @pos
          raise DecodeError.new("array count #{n} exceeds the response")
        end
        n
      end

      def string_array : Array(String)
        Array(String).new(count) { string }
      end

      def rest : Bytes
        value = @data[@pos..]
        @pos = @data.size
        value
      end

      # Reads a response's leading error code without consuming a reader
      # the caller holds. Every group response starts with one.
      def self.peek_error_code(body : Bytes) : Int32
        new(body).int32
      rescue DecodeError
        0
      end
    end

    # ---------------------------------------------------------------------
    # Frames
    # ---------------------------------------------------------------------

    def self.encode_frame(api_key : Int16, correlation_id : Int32, client_id : String, body : Bytes) : Bytes
      io = IO::Memory.new(14 + client_id.bytesize + body.size)
      payload_len = 8 + 2 + client_id.bytesize + body.size
      io.write_bytes(payload_len.to_i32, IO::ByteFormat::BigEndian)
      io.write_bytes(api_key, IO::ByteFormat::BigEndian)
      io.write_bytes(API_VERSION, IO::ByteFormat::BigEndian)
      io.write_bytes(correlation_id, IO::ByteFormat::BigEndian)
      io.write_bytes(client_id.bytesize.to_i16, IO::ByteFormat::BigEndian)
      io.write(client_id.to_slice)
      io.write(body)
      io.to_slice
    end

    # Splits a frame payload into correlation id and body.
    def self.decode_frame_payload(payload : Bytes) : {Int32, Bytes}
      raise DecodeError.new("frame payload shorter than its header") if payload.size < 10
      correlation_id = IO::ByteFormat::BigEndian.decode(Int32, payload[4, 4])
      client_len = IO::ByteFormat::BigEndian.decode(Int16, payload[8, 2])
      offset = 10
      offset += client_len.to_i32 if client_len >= 0
      raise DecodeError.new("frame client id runs past the payload") if offset > payload.size
      {correlation_id, payload[offset..]}
    end

    # ---------------------------------------------------------------------
    # CRC32C (Castagnoli) — not the zlib CRC32 in Digest::CRC32.
    # ---------------------------------------------------------------------

    CRC32C_TABLE = begin
      table = StaticArray(UInt32, 256).new(0_u32)
      256.times do |i|
        crc = i.to_u32
        8.times do
          crc = (crc & 1) != 0 ? (crc >> 1) ^ 0x82F63B78_u32 : crc >> 1
        end
        table[i] = crc
      end
      table
    end

    def self.crc32c(data : Bytes) : UInt32
      crc = 0xFFFFFFFF_u32
      data.each do |byte|
        crc = CRC32C_TABLE[((crc ^ byte) & 0xff).to_i] ^ (crc >> 8)
      end
      crc ^ 0xFFFFFFFF_u32
    end

    # ---------------------------------------------------------------------
    # Partitioning
    # ---------------------------------------------------------------------

    # Kafka's murmur2, transcribed so a key lands on the same partition as
    # it would with any other Brahmaputra or Kafka client.
    def self.murmur2(data : Bytes) : UInt32
      seed = 0x9747b28c_u32
      m = 0x5bd1e995_u32
      length = data.size
      h = seed ^ length.to_u32!
      chunks = length // 4
      chunks.times do |i|
        o = i * 4
        k = data[o].to_u32 | (data[o + 1].to_u32 << 8) | (data[o + 2].to_u32 << 16) | (data[o + 3].to_u32 << 24)
        k = k &* m
        k ^= k >> 24
        k = k &* m
        h = h &* m
        h ^= k
      end
      tail = chunks * 4
      case length - tail
      when 3
        h ^= data[tail + 2].to_u32 << 16
        h ^= data[tail + 1].to_u32 << 8
        h ^= data[tail].to_u32
        h = h &* m
      when 2
        h ^= data[tail + 1].to_u32 << 8
        h ^= data[tail].to_u32
        h = h &* m
      when 1
        h ^= data[tail].to_u32
        h = h &* m
      end
      h ^= h >> 13
      h = h &* m
      h ^= h >> 15
      h
    end

    # murmur2(key) % partitions, Kafka's default partitioner.
    def self.partition_for_key(key : Bytes, partitions : Array(Int32)) : Int32
      partitions[((murmur2(key) & 0x7fffffff_u32) % partitions.size.to_u32).to_i32]
    end
  end

  # Kafka's murmur2. `Brahmaputra.murmur2("") == 275646681`.
  def self.murmur2(data : Bytes | String) : UInt32
    Protocol.murmur2(data.is_a?(String) ? data.to_slice : data)
  end

  def self.partition_for_key(key : Bytes | String, partitions : Array(Int32)) : Int32
    Protocol.partition_for_key(key.is_a?(String) ? key.to_slice : key, partitions)
  end

  # Normalises the value types callers may pass for keys, values and
  # header values. nil stays nil (null); an empty string stays empty.
  def self.to_bytes(value : Bytes | String | Nil) : Bytes?
    case value
    when String then value.to_slice
    else             value
    end
  end
end
