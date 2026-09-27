module Brahmaputra
  # Compression codecs, numbered as the broker's attribute bits.
  enum Compression
    None   = 0
    Lz4    = 1
    Zstd   = 2
    Snappy = 3
    Gzip   = 4

    # Kafka's `compression.type` spelling.
    def self.from_name(name : String) : Compression
      case name.downcase
      when "none"   then None
      when "lz4"    then Lz4
      when "zstd"   then Zstd
      when "snappy" then Snappy
      when "gzip"   then Gzip
      else
        raise ConfigError.new("unknown compression #{name.inspect} (none, lz4, zstd, snappy, gzip)")
      end
    end

    def wire_name : String
      to_s.downcase
    end
  end

  alias Codec = Proc(Bytes, Bytes)

  @@compressors = {} of Compression => Codec
  @@decompressors = {} of Compression => Codec

  # Plugs in a codec this driver does not carry itself (lz4, zstd, snappy),
  # so an application that wants one pays for that dependency and one that
  # does not, does not. Registering gzip or none overrides the built-in.
  #
  # The broker's lz4 payload is a little-endian u32 of the uncompressed
  # length followed by a raw LZ4 *block* — not the LZ4 frame format.
  def self.register_codec(codec : Compression, compress : Codec, decompress : Codec) : Nil
    @@compressors[codec] = compress
    @@decompressors[codec] = decompress
  end

  # Caps decompression so a corrupt batch cannot make this process
  # allocate gigabytes before it can reject it.
  MAX_DECOMPRESSED_BYTES = 256_i64 * 1024 * 1024

  def self.compress(codec : Compression, payload : Bytes) : Bytes
    if fn = @@compressors[codec]?
      return fn.call(payload)
    end
    case codec
    when .none?
      payload
    when .gzip?
      io = IO::Memory.new
      Compress::Gzip::Writer.open(io) { |gz| gz.write(payload) }
      io.to_slice
    else
      raise ConfigError.new("#{codec.wire_name} compression is not registered; call Brahmaputra.register_codec or use none/gzip")
    end
  end

  def self.decompress(codec : Compression, payload : Bytes) : Bytes
    if fn = @@decompressors[codec]?
      return fn.call(payload)
    end
    case codec
    when .none?
      payload
    when .gzip?
      buf = IO::Memory.new
      begin
        Compress::Gzip::Reader.open(IO::Memory.new(payload)) do |gz|
          copied = IO.copy(gz, buf, MAX_DECOMPRESSED_BYTES)
          if copied == MAX_DECOMPRESSED_BYTES && gz.read_byte
            raise DecodeError.new("gzip batch exceeds #{MAX_DECOMPRESSED_BYTES} bytes")
          end
        end
      rescue ex : Compress::Deflate::Error | Compress::Gzip::Error | IO::Error
        raise DecodeError.new("gzip: #{ex.message}")
      end
      buf.to_slice
    else
      raise ConfigError.new("#{codec.wire_name} decompression is not registered; call Brahmaputra.register_codec")
    end
  end

  # An ordered, possibly repeating annotation on a record. A nil value is
  # null, which is distinct from an empty one.
  struct Header
    getter key : String
    getter value : Bytes?

    def initialize(@key : String, value : Bytes | String | Nil)
      @value = Brahmaputra.to_bytes(value)
    end
  end

  # One record inside a batch, as encoded on the wire.
  struct Record
    getter key : Bytes?
    getter value : Bytes?
    property timestamp_delta : Int64
    getter headers : Array(Header)

    def initialize(@key : Bytes?, @value : Bytes?, @timestamp_delta : Int64 = 0_i64, @headers : Array(Header) = [] of Header)
    end
  end

  # One batch read back off the wire.
  struct DecodedBatch
    getter base_offset : Int64
    getter max_timestamp : Int64
    getter records : Array(Record)

    def initialize(@base_offset, @max_timestamp, @records)
    end
  end

  module RecordBatch
    HEADER_LEN       = 12
    MIN_BATCH_LENGTH = 4 + 1 + 4 + 2 + 4 + 8
    PRODUCER_EXT_LEN = 8 + 2 + 4
    MAGIC_V1         =       1_u8
    MAGIC_V2         =       2_u8
    COMPRESSION_MASK = 0x0007_u16
    HEADERS_BIT      = 0x0008_u16
    NULL_VALUE_BIT   = 0x0040_u16
    BE               = IO::ByteFormat::BigEndian

    private def self.put_uvarint(io : IO, value : UInt64) : Nil
      while value >= 0x80_u64
        io.write_byte((value & 0x7f_u64).to_u8! | 0x80_u8)
        value >>= 7
      end
      io.write_byte(value.to_u8!)
    end

    # Encodes one batch exactly as the broker stores it. The broker never
    # re-encodes: it stamps base_offset and leader_epoch in place (both sit
    # before the CRC) and writes these bytes to disk.
    def self.encode(records : Array(Record), max_timestamp : Int64, codec : Compression) : Bytes
      has_headers = records.any? { |r| !r.headers.empty? }
      # A nil value is a tombstone and needs the widened length encoding;
      # an empty value is an ordinary record and must not trigger it.
      has_nulls = records.any? { |r| r.value.nil? }

      payload = IO::Memory.new
      rec = IO::Memory.new
      records.each do |record|
        rec.clear
        if key = record.key
          put_uvarint(rec, key.size.to_u64 &+ 1)
          rec.write(key)
        else
          put_uvarint(rec, 0_u64)
        end
        value = record.value
        if has_nulls
          if value
            put_uvarint(rec, value.size.to_u64 &+ 1)
            rec.write(value)
          else
            put_uvarint(rec, 0_u64)
          end
        else
          v = value.not_nil!
          put_uvarint(rec, v.size.to_u64)
          rec.write(v)
        end
        delta = record.timestamp_delta
        put_uvarint(rec, ((delta << 1) ^ (delta >> 63)).to_u64!)
        if has_headers
          put_uvarint(rec, record.headers.size.to_u64)
          record.headers.each do |header|
            put_uvarint(rec, header.key.bytesize.to_u64)
            rec.write(header.key.to_slice)
            if hv = header.value
              put_uvarint(rec, hv.size.to_u64 &+ 1)
              rec.write(hv)
            else
              put_uvarint(rec, 0_u64)
            end
          end
        end
        put_uvarint(payload, rec.size.to_u64)
        payload.write(rec.to_slice)
      end

      compressed = Brahmaputra.compress(codec, payload.to_slice)
      attributes = codec.value.to_u16 & COMPRESSION_MASK
      attributes |= HEADERS_BIT if has_headers
      attributes |= NULL_VALUE_BIT if has_nulls
      batch_length = MIN_BATCH_LENGTH + compressed.size

      buf = IO::Memory.new(HEADER_LEN + batch_length)
      buf.write_bytes(0_i64, BE) # base_offset, stamped by the broker
      buf.write_bytes(batch_length.to_i32, BE)
      buf.write_bytes(0_i32, BE) # leader_epoch, likewise
      buf.write_byte(MAGIC_V1)
      crc_at = buf.pos
      buf.write_bytes(0_u32, BE)
      buf.write_bytes(attributes, BE)
      last_delta = records.empty? ? 0 : records.size - 1
      buf.write_bytes(last_delta.to_i32, BE)
      buf.write_bytes(max_timestamp, BE)
      buf.write(compressed)
      bytes = buf.to_slice
      crc = Protocol.crc32c(bytes[crc_at + 4..])
      BE.encode(crc, bytes[crc_at, 4])
      bytes
    end

    # Decodes the batch starting at `offset`, returning it and the offset
    # just past it.
    def self.decode(data : Bytes, offset : Int32) : {DecodedBatch, Int32}
      raise DecodeError.new("truncated batch header") if data.size - offset < HEADER_LEN
      base_offset = BE.decode(Int64, data[offset, 8])
      batch_length = BE.decode(Int32, data[offset + 8, 4])
      if batch_length < MIN_BATCH_LENGTH
        raise DecodeError.new("batch_length #{batch_length} too small")
      end
      body_at = offset + HEADER_LEN
      if batch_length > data.size - body_at
        raise DecodeError.new("truncated batch body")
      end
      finish = body_at + batch_length
      magic = data[body_at + 4]
      unless magic == MAGIC_V1 || magic == MAGIC_V2
        raise DecodeError.new("unsupported magic #{magic}")
      end
      crc_at = body_at + 5
      stored = BE.decode(UInt32, data[crc_at, 4])
      computed = Protocol.crc32c(data[(crc_at + 4)...finish])
      if stored != computed
        raise DecodeError.new("crc mismatch: stored 0x#{stored.to_s(16)}, computed 0x#{computed.to_s(16)}")
      end
      cursor = crc_at + 4
      attributes = BE.decode(UInt16, data[cursor, 2])
      max_timestamp = BE.decode(Int64, data[cursor + 6, 8])
      cursor += 14
      if magic == MAGIC_V2
        cursor += PRODUCER_EXT_LEN
        raise DecodeError.new("truncated producer extension") if cursor > finish
      end
      codec = Compression.from_value?((attributes & COMPRESSION_MASK).to_i32)
      raise DecodeError.new("unknown compression #{attributes & COMPRESSION_MASK}") unless codec
      payload = Brahmaputra.decompress(codec, data[cursor...finish])
      records = decode_records(payload, attributes & HEADERS_BIT != 0, attributes & NULL_VALUE_BIT != 0)
      {DecodedBatch.new(base_offset, max_timestamp, records), finish}
    end

    private def self.get_uvarint(data : Bytes, pos : Int32) : {UInt64, Int32}
      result = 0_u64
      shift = 0
      loop do
        raise DecodeError.new("truncated varint in record") if pos >= data.size
        byte = data[pos]
        pos += 1
        result |= (byte & 0x7f).to_u64 << shift
        return {result, pos} if byte & 0x80 == 0
        shift += 7
        raise DecodeError.new("varint overflows 64 bits") if shift > 63
      end
    end

    private def self.take(data : Bytes, pos : Int32, length : UInt64, finish : Int32, what : String) : Bytes
      raise DecodeError.new("truncated record #{what}") if length > (finish - pos).to_u64
      # Copied so records do not pin the whole response buffer.
      data[pos, length.to_i32].dup
    end

    def self.decode_records(payload : Bytes, has_headers : Bool, has_nulls : Bool) : Array(Record)
      records = [] of Record
      pos = 0
      while pos < payload.size
        length, pos = get_uvarint(payload, pos)
        raise DecodeError.new("truncated record") if length > (payload.size - pos).to_u64
        finish = pos + length.to_i32

        key_plus_one, pos = get_uvarint(payload, pos)
        key = nil
        if key_plus_one > 0
          # Non-nil even when empty: an empty key is not a null key.
          key = take(payload, pos, key_plus_one - 1, finish, "key")
          pos += key.size
        end

        raw_value_len, pos = get_uvarint(payload, pos)
        value = nil
        if has_nulls && raw_value_len == 0
          value = nil # a tombstone
        else
          raw_value_len -= 1 if has_nulls
          value = take(payload, pos, raw_value_len, finish, "value")
          pos += value.size
        end

        raw_delta, pos = get_uvarint(payload, pos)
        delta = (raw_delta >> 1).to_i64! ^ -((raw_delta & 1).to_i64!)

        headers = [] of Header
        if has_headers
          count, pos = get_uvarint(payload, pos)
          if count > (finish - pos).to_u64
            raise DecodeError.new("record header count exceeds record")
          end
          count.times do
            key_len, pos = get_uvarint(payload, pos)
            hkey = take(payload, pos, key_len, finish, "header key")
            pos += hkey.size
            value_plus_one, pos = get_uvarint(payload, pos)
            hvalue = nil
            if value_plus_one > 0
              hvalue = take(payload, pos, value_plus_one - 1, finish, "header value")
              pos += hvalue.size
            end
            headers << Header.new(String.new(hkey), hvalue)
          end
        end
        raise DecodeError.new("trailing bytes in record") if pos != finish
        records << Record.new(key, value, delta, headers)
      end
      records
    end
  end
end
