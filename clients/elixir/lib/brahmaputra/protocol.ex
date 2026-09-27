defmodule Brahmaputra.Protocol do
  @moduledoc """
  Wire encodings for Brahmaputra.

  Three encodings share one connection and they do not agree with each
  other, so keeping them straight is most of the work:

    * The frame header is fixed big-endian: an int32 length prefix, then
      api key / api version / correlation id and an int16-prefixed client id.
    * A request body is BitPacker: every integer is a zigzag varint, every
      string and array is a varint count followed by its contents, and the
      whole body is prefixed with the schema version string.
    * A record batch is neither. Fixed big-endian header fields and plain
      (non-zigzag) varints inside each record, because the broker stamps
      offsets into it in place and validates its CRC without decoding it.
  """

  import Bitwise

  alias Brahmaputra.{Error, ServerError}

  @schema_version "1.0.0"
  @api_version 4

  @doc "The BitPacker schema version every body carries first."
  def schema_version, do: @schema_version

  @doc "The wire version this client speaks; the broker requires an exact match."
  def api_version, do: @api_version

  # API keys
  def api(:produce), do: 0
  def api(:fetch), do: 1
  def api(:list_offsets), do: 2
  def api(:metadata), do: 3
  def api(:join_group), do: 7
  def api(:sync_group), do: 8
  def api(:heartbeat), do: 9
  def api(:offset_commit), do: 10
  def api(:offset_fetch), do: 11
  def api(:list_groups), do: 12
  def api(:describe_group), do: 13
  def api(:api_versions), do: 14
  def api(:authenticate), do: 17
  def api(:leave_group), do: 18

  # Error codes
  @error_names %{
    0 => "NONE",
    1 => "UNKNOWN_TOPIC_OR_PARTITION",
    2 => "OFFSET_OUT_OF_RANGE",
    3 => "INVALID_REQUEST",
    4 => "UNSUPPORTED_VERSION",
    5 => "INTERNAL",
    6 => "NOT_LEADER_OR_FOLLOWER",
    7 => "FENCED_BROKER_EPOCH",
    8 => "FENCED_LEADER_EPOCH",
    9 => "UNKNOWN_LEADER_EPOCH",
    10 => "NOT_ENOUGH_REPLICAS",
    11 => "FENCED_PRODUCER_EPOCH",
    12 => "OUT_OF_ORDER_SEQUENCE",
    13 => "UNKNOWN_MEMBER_ID",
    14 => "REBALANCE_IN_PROGRESS",
    15 => "NOT_COORDINATOR",
    16 => "ILLEGAL_GENERATION",
    17 => "COORDINATOR_LOAD_IN_PROGRESS",
    18 => "SASL_AUTHENTICATION_FAILED",
    19 => "AUTHORIZATION_FAILED"
  }

  @doc "Name of a broker error code."
  def error_name(code), do: Map.get(@error_names, code, "UNKNOWN")

  def err(:none), do: 0
  def err(:unknown_topic_or_partition), do: 1
  def err(:offset_out_of_range), do: 2
  def err(:internal), do: 5
  def err(:not_leader_or_follower), do: 6
  def err(:fenced_leader_epoch), do: 8
  def err(:unknown_leader_epoch), do: 9
  def err(:not_enough_replicas), do: 10
  def err(:unknown_member_id), do: 13
  def err(:rebalance_in_progress), do: 14
  def err(:not_coordinator), do: 15
  def err(:illegal_generation), do: 16
  def err(:coordinator_load_in_progress), do: 17

  @doc """
  Whether a code means "this send did not happen": every one of these is
  returned strictly before the broker appends, so a retry cannot duplicate.
  """
  def retriable?(code), do: code in [6, 8, 9, 10, 17, 5]

  @doc false
  def server_error(code, context), do: %ServerError{code: code, context: context}

  # ---------------------------------------------------------------------------
  # BitPacker writer (iodata builders)
  # ---------------------------------------------------------------------------

  @mask32 0xFFFFFFFF
  @mask64 0xFFFFFFFFFFFFFFFF

  @doc "Unsigned LEB128 varint."
  def uvarint(value) when value >= 0 and value < 0x80, do: <<value>>

  def uvarint(value) when value >= 0x80,
    do: <<(value &&& 0x7F) ||| 0x80>> <> uvarint(value >>> 7)

  def zigzag32(value), do: bxor(value <<< 1, value >>> 31) &&& @mask32
  def zigzag64(value), do: bxor(value <<< 1, value >>> 63) &&& @mask64
  def unzigzag(value), do: bxor(value >>> 1, -(value &&& 1))

  def w_int32(value), do: uvarint(zigzag32(value))
  def w_int64(value), do: uvarint(zigzag64(value))
  def w_bool(true), do: <<1>>
  def w_bool(false), do: <<0>>
  def w_string(nil), do: w_string("")
  def w_string(value) when is_binary(value), do: [w_int32(byte_size(value)), value]
  def w_string_array(values), do: [w_int32(length(values)) | Enum.map(values, &w_string/1)]

  @doc "Builds a request body: the schema version followed by `fields`."
  def body(fields), do: IO.iodata_to_binary([w_string(@schema_version) | fields])

  # ---------------------------------------------------------------------------
  # BitPacker reader. Each function takes a binary and returns {value, rest};
  # malformed input raises Brahmaputra.Error, caught at the API boundary.
  # ---------------------------------------------------------------------------

  @doc "Opens a response body, verifying its schema version."
  def open_body(data) do
    {version, rest} = r_string(data)

    if version != @schema_version do
      raise Error,
        message:
          "schema version mismatch: broker speaks #{inspect(version)}, " <>
            "this client speaks #{inspect(@schema_version)}"
    end

    rest
  end

  def r_uvarint(data), do: r_uvarint(data, 0, 0)

  defp r_uvarint(<<byte, rest::binary>>, shift, acc) do
    acc = acc ||| (byte &&& 0x7F) <<< shift

    cond do
      (byte &&& 0x80) == 0 -> {acc, rest}
      shift + 7 > 63 -> raise Error, message: "varint overflows 64 bits"
      true -> r_uvarint(rest, shift + 7, acc)
    end
  end

  defp r_uvarint(<<>>, _shift, _acc), do: raise(Error, message: "truncated varint")

  def r_int32(data) do
    {raw, rest} = r_uvarint(data)
    {unzigzag(raw &&& @mask32) |> to_int32(), rest}
  end

  def r_int64(data) do
    {raw, rest} = r_uvarint(data)
    {unzigzag(raw), rest}
  end

  defp to_int32(v) when v >= 0x80000000, do: v - 0x100000000
  defp to_int32(v), do: v

  def r_bool(<<b, rest::binary>>), do: {b != 0, rest}
  def r_bool(<<>>), do: raise(Error, message: "truncated bool")

  def r_string(data) do
    {len, rest} = r_int32(data)

    case rest do
      <<value::binary-size(len), rest::binary>> when len >= 0 -> {value, rest}
      _ -> raise Error, message: "truncated string"
    end
  end

  def r_array(data, fun) do
    {count, rest} = r_int32(data)
    r_array(rest, fun, max(count, 0), [])
  end

  defp r_array(rest, _fun, 0, acc), do: {Enum.reverse(acc), rest}

  defp r_array(data, fun, n, acc) do
    {value, rest} = fun.(data)
    r_array(rest, fun, n - 1, [value | acc])
  end

  def r_string_array(data), do: r_array(data, &r_string/1)

  @doc "Reads a response's leading error code without consuming anything."
  def peek_error_code(body) do
    try do
      {code, _} = body |> open_body() |> r_int32()
      code
    rescue
      _ -> 0
    end
  end

  # ---------------------------------------------------------------------------
  # Frames
  # ---------------------------------------------------------------------------

  @doc """
  Frame payload (without the 4-byte length prefix, which :gen_tcp's
  `packet: 4` mode adds).
  """
  def encode_frame_payload(api_key, correlation_id, client_id, body) do
    [
      <<api_key::big-signed-16, @api_version::big-signed-16, correlation_id::big-signed-32,
        byte_size(client_id)::big-signed-16>>,
      client_id,
      body
    ]
  end

  @doc "Splits a response frame payload into its correlation id and body."
  def decode_frame_payload(<<_::binary-size(4), corr::big-signed-32, clen::big-signed-16,
                             rest::binary>>) do
    skip = max(clen, 0)

    case rest do
      <<_::binary-size(skip), body::binary>> -> {:ok, corr, body}
      _ -> {:error, %Error{message: "frame client id runs past the payload"}}
    end
  end

  def decode_frame_payload(_),
    do: {:error, %Error{message: "frame payload shorter than its header"}}

  # ---------------------------------------------------------------------------
  # CRC32C (Castagnoli) — not the zlib CRC32 that :erlang.crc32 computes.
  # ---------------------------------------------------------------------------

  @crc_table (for n <- 0..255 do
                Enum.reduce(1..8, n, fn _, c ->
                  if (c &&& 1) == 1, do: bxor(c >>> 1, 0x82F63B78), else: c >>> 1
                end)
              end)
             |> List.to_tuple()

  @doc "CRC32C, the Castagnoli CRC record batches carry."
  def crc32c(data) when is_binary(data), do: bxor(crc_loop(data, 0xFFFFFFFF), 0xFFFFFFFF)

  defp crc_loop(<<byte, rest::binary>>, crc),
    do: crc_loop(rest, bxor(elem(@crc_table, bxor(crc, byte) &&& 0xFF), crc >>> 8))

  defp crc_loop(<<>>, crc), do: crc

  # ---------------------------------------------------------------------------
  # Compression
  # ---------------------------------------------------------------------------

  @codecs %{none: 0, lz4: 1, zstd: 2, snappy: 3, gzip: 4}
  @max_decompressed 256 * 1024 * 1024

  @doc "Maps Kafka's compression.type spelling onto a codec id."
  def parse_compression(name) when is_binary(name) do
    case Enum.find(@codecs, fn {atom, _} -> Atom.to_string(atom) == name end) do
      {atom, _} -> {:ok, atom}
      nil -> {:error, %Error{message: "unknown compression #{inspect(name)} (none, lz4, zstd, snappy, gzip)"}}
    end
  end

  def parse_compression(name) when is_atom(name) do
    if Map.has_key?(@codecs, name),
      do: {:ok, name},
      else: {:error, %Error{message: "unknown compression #{inspect(name)}"}}
  end

  def codec_id(codec), do: Map.fetch!(@codecs, codec)

  def codec_name(id) do
    case Enum.find(@codecs, fn {_, v} -> v == id end) do
      {name, _} -> name
      nil -> {:unknown, id}
    end
  end

  @doc """
  Plugs in a codec this driver does not carry itself (lz4, zstd, snappy).
  Both functions take and return a binary.

  The lz4 payload the broker expects is a little-endian uint32 of the
  uncompressed length followed by a raw LZ4 block — not the LZ4 frame format.
  """
  def register_codec(codec, compress_fun, decompress_fun)
      when codec in [:lz4, :zstd, :snappy] and is_function(compress_fun, 1) and
             is_function(decompress_fun, 1) do
    :persistent_term.put({__MODULE__, :codec, codec}, {compress_fun, decompress_fun})
    :ok
  end

  def compress(:none, payload), do: payload
  def compress(:gzip, payload), do: :zlib.gzip(payload)

  def compress(codec, payload) do
    case :persistent_term.get({__MODULE__, :codec, codec}, nil) do
      {fun, _} -> fun.(payload)
      nil -> raise Error, message: "#{codec} compression is not registered; call Brahmaputra.register_codec/3 or use none/gzip"
    end
  end

  def decompress(:none, payload), do: payload
  def decompress(:gzip, payload), do: gunzip_capped(payload)

  def decompress(codec, payload) do
    case :persistent_term.get({__MODULE__, :codec, codec}, nil) do
      {_, fun} -> fun.(payload)
      nil -> raise Error, message: "#{inspect(codec)} decompression is not registered"
    end
  end

  # Capped so a corrupt or hostile batch cannot name gigabytes of output
  # that this process allocates before it can reject it.
  defp gunzip_capped(payload) do
    z = :zlib.open()

    try do
      :ok = :zlib.inflateInit(z, 31)
      out = inflate_loop(z, :zlib.safeInflate(z, payload), [], 0)
      :zlib.inflateEnd(z)
      out
    after
      :zlib.close(z)
    end
  end

  defp inflate_loop(z, {status, chunk}, acc, size) do
    size = size + IO.iodata_length(chunk)
    if size > @max_decompressed, do: raise(Error, message: "gzip batch exceeds decompression cap")
    acc = [acc | chunk]

    case status do
      :finished -> IO.iodata_to_binary(acc)
      :continue -> inflate_loop(z, :zlib.safeInflate(z, []), acc, size)
    end
  end

  # ---------------------------------------------------------------------------
  # Record batches
  # ---------------------------------------------------------------------------

  @min_batch_length 4 + 1 + 4 + 2 + 4 + 8
  @producer_extension_len 8 + 2 + 4
  @compression_mask 0x0007
  @headers_bit 0x0008
  @null_value_bit 0x0040

  @doc """
  Encodes one batch exactly as the broker stores it.

  `records` is a list of maps with `:key`, `:value` (nil = tombstone),
  `:timestamp_delta` and `:headers` (list of `{key, value | nil}`).
  """
  def encode_record_batch(records, max_timestamp, codec) do
    has_headers = Enum.any?(records, fn r -> (r[:headers] || []) != [] end)
    # A nil value is a tombstone and needs the widened length encoding; an
    # empty value is an ordinary record and must not trigger it.
    has_nulls = Enum.any?(records, fn r -> r.value == nil end)

    payload =
      records
      |> Enum.map(fn record ->
        rec = IO.iodata_to_binary(encode_record(record, has_headers, has_nulls))
        [uvarint(byte_size(rec)), rec]
      end)
      |> IO.iodata_to_binary()

    compressed = compress(codec, payload)

    attributes =
      (codec_id(codec) &&& @compression_mask) |||
        if(has_headers, do: @headers_bit, else: 0) |||
        if(has_nulls, do: @null_value_bit, else: 0)

    last_delta = max(length(records) - 1, 0)
    batch_length = @min_batch_length + byte_size(compressed)

    crc_body =
      <<attributes::big-16, last_delta::big-signed-32, max_timestamp::big-signed-64,
        compressed::binary>>

    # base_offset and leader_epoch are zero: the broker stamps both in place.
    <<0::big-64, batch_length::big-32, 0::big-32, 1, crc32c(crc_body)::big-32,
      crc_body::binary>>
  end

  defp encode_record(record, has_headers, has_nulls) do
    key =
      case record[:key] do
        nil -> uvarint(0)
        k -> [uvarint(byte_size(k) + 1), k]
      end

    value =
      cond do
        has_nulls and record.value == nil -> uvarint(0)
        has_nulls -> [uvarint(byte_size(record.value) + 1), record.value]
        true -> [uvarint(byte_size(record.value)), record.value]
      end

    delta = uvarint(zigzag64(record[:timestamp_delta] || 0))

    headers =
      if has_headers do
        hs = record[:headers] || []

        [
          uvarint(length(hs))
          | Enum.map(hs, fn {hk, hv} ->
              hv_enc = if hv == nil, do: uvarint(0), else: [uvarint(byte_size(hv) + 1), hv]
              [uvarint(byte_size(hk)), hk, hv_enc]
            end)
        ]
      else
        []
      end

    [key, value, delta, headers]
  end

  @doc """
  Decodes every batch in `data`, returning a list of
  `%{base_offset, max_timestamp, records}`.
  """
  def decode_record_batches(data), do: decode_batches(data, [])

  defp decode_batches(<<>>, acc), do: Enum.reverse(acc)

  defp decode_batches(data, acc) do
    {batch, rest} = decode_record_batch(data)
    decode_batches(rest, [batch | acc])
  end

  def decode_record_batch(
        <<base_offset::big-signed-64, batch_length::big-signed-32, rest::binary>>
      ) do
    if batch_length < @min_batch_length, do: raise(Error, message: "batch_length too small")

    case rest do
      <<body::binary-size(batch_length), after_batch::binary>> ->
        <<_leader_epoch::32, magic, stored_crc::big-32, crc_region::binary>> = body

        unless magic in [1, 2], do: raise(Error, message: "unsupported magic #{magic}")
        computed = crc32c(crc_region)

        if stored_crc != computed do
          raise Error,
            message: "crc mismatch: stored #{stored_crc}, computed #{computed}"
        end

        <<attributes::big-16, _last_delta::big-32, max_timestamp::big-signed-64,
          records_region::binary>> = crc_region

        records_region =
          if magic == 2 do
            <<_::binary-size(@producer_extension_len), r::binary>> = records_region
            r
          else
            records_region
          end

        payload = decompress(codec_name(attributes &&& @compression_mask), records_region)

        records =
          decode_records(
            payload,
            (attributes &&& @headers_bit) != 0,
            (attributes &&& @null_value_bit) != 0,
            []
          )

        {%{base_offset: base_offset, max_timestamp: max_timestamp, records: records},
         after_batch}

      _ ->
        raise Error, message: "truncated batch body"
    end
  end

  def decode_record_batch(_), do: raise(Error, message: "truncated batch header")

  defp decode_records(<<>>, _h, _n, acc), do: Enum.reverse(acc)

  defp decode_records(data, has_headers, has_nulls, acc) do
    {len, rest} = r_uvarint(data)

    case rest do
      <<rec::binary-size(len), rest::binary>> ->
        decode_records(rest, has_headers, has_nulls, [
          decode_record(rec, has_headers, has_nulls) | acc
        ])

      _ ->
        raise Error, message: "truncated record"
    end
  end

  defp decode_record(rec, has_headers, has_nulls) do
    {key, rest} = take_nullable(rec)
    {raw_value_len, rest} = r_uvarint(rest)

    {value, rest} =
      cond do
        # A tombstone: nil, which is what distinguishes it from an empty value.
        has_nulls and raw_value_len == 0 -> {nil, rest}
        true -> take(rest, if(has_nulls, do: raw_value_len - 1, else: raw_value_len))
      end

    {raw_delta, rest} = r_uvarint(rest)

    {headers, rest} =
      if has_headers do
        {count, rest} = r_uvarint(rest)

        # A count is a promise about bytes that follow; one exceeding what is
        # left is corrupt, and allocating on it would be a hazard.
        if count > byte_size(rest), do: raise(Error, message: "record header count exceeds record")

        {hs, rest} =
          Enum.reduce(List.duplicate(nil, count), {[], rest}, fn _, {hs, rest} ->
            {klen, rest} = r_uvarint(rest)
            {hk, rest} = take(rest, klen)
            {hv, rest} = take_nullable(rest)
            {[{hk, hv} | hs], rest}
          end)

        {Enum.reverse(hs), rest}
      else
        {[], rest}
      end

    if rest != <<>>, do: raise(Error, message: "trailing bytes in record")
    %{key: key, value: value, timestamp_delta: unzigzag(raw_delta), headers: headers}
  end

  defp take_nullable(data) do
    {len_plus_one, rest} = r_uvarint(data)
    if len_plus_one == 0, do: {nil, rest}, else: take(rest, len_plus_one - 1)
  end

  defp take(data, len) do
    case data do
      <<v::binary-size(len), rest::binary>> -> {v, rest}
      _ -> raise Error, message: "truncated record field"
    end
  end

  # ---------------------------------------------------------------------------
  # Partitioning
  # ---------------------------------------------------------------------------

  @doc """
  Kafka's 32-bit murmur2, so a key lands on the same partition here as it
  would with any other Brahmaputra or Kafka client. `murmur2("") == 275646681`.
  """
  def murmur2(nil), do: murmur2("")

  def murmur2(data) when is_binary(data) do
    m = 0x5BD1E995
    h = bxor(0x9747B28C, byte_size(data)) &&& @mask32
    {h, tail} = murmur_chunks(data, h, m)

    h =
      case tail do
        <<a, b, c>> -> bxor(h, c <<< 16) |> bxor(b <<< 8) |> bxor(a) |> Kernel.*(m) |> band(@mask32)
        <<a, b>> -> bxor(h, b <<< 8) |> bxor(a) |> Kernel.*(m) |> band(@mask32)
        <<a>> -> bxor(h, a) |> Kernel.*(m) |> band(@mask32)
        <<>> -> h
      end

    h = bxor(h, h >>> 13)
    h = h * m &&& @mask32
    bxor(h, h >>> 15)
  end

  defp murmur_chunks(<<k::little-32, rest::binary>>, h, m) do
    k = k * m &&& @mask32
    k = bxor(k, k >>> 24)
    k = k * m &&& @mask32
    h = h * m &&& @mask32
    murmur_chunks(rest, bxor(h, k), m)
  end

  defp murmur_chunks(tail, h, _m), do: {h, tail}

  @doc "`murmur2(key) % partitions`, Kafka's default partitioner."
  def partition_for_key(key, partitions) when is_list(partitions) and partitions != [] do
    Enum.at(partitions, rem(murmur2(key) &&& 0x7FFFFFFF, length(partitions)))
  end
end
