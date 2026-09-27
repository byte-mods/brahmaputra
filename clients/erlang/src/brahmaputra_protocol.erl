%% @doc Wire encodings for the Brahmaputra protocol.
%%
%% Three encodings share one connection and they do not agree with each
%% other, so keeping them straight is most of the work:
%%
%%   * The frame header is fixed big-endian: api key, api version,
%%     correlation id and an int16-prefixed client id. The int32 length
%%     prefix is added by the socket itself (`{packet, 4}').
%%   * A request/response body is BitPacker: every integer is a zigzag
%%     varint, every string and array a varint count then its contents, and
%%     the whole body is prefixed with the schema version string.
%%   * A record batch is neither: fixed big-endian header fields and plain
%%     (non-zigzag) varints inside each record, because the broker stamps
%%     offsets into it in place and validates its CRC without decoding it.
-module(brahmaputra_protocol).

-include("brahmaputra.hrl").

%% BitPacker
-export([body/1, decode_body/2, error_name/1, server_error/2, retriable/1,
         peek_error_code/1]).
-export([enc_uvarint/1, enc_int32/1, enc_int64/1, enc_bool/1, enc_string/1,
         enc_string_array/1]).
-export([dec_uvarint/1, dec_int32/1, dec_int64/1, dec_bool/1, dec_string/1,
         dec_string_array/1, dec_array/2]).
%% Frames
-export([encode_frame/4, decode_frame/1]).
%% Record batches
-export([encode_record_batch/3, decode_record_batch/1, decode_record_batches/1]).
%% Checksums, hashing, compression
-export([crc32c/1, murmur2/1, partition_for_key/2]).
-export([parse_compression/1, compression_name/1, compress/2, decompress/2,
         register_codec/3, unregister_codec/1]).

-export_type([record/0, header/0, batch/0, codec/0]).

-type header() :: {Key :: binary(), Value :: binary() | undefined}.
-type record() :: #{key := binary() | undefined,
                    value := binary() | undefined,
                    timestamp_delta := integer(),
                    headers := [header()]}.
-type batch() :: #{base_offset := integer(),
                   max_timestamp := integer(),
                   records := [record()]}.
-type codec() :: none | gzip | lz4 | zstd | snappy.

-define(BATCH_HEADER_LEN, 12).
-define(MIN_BATCH_LENGTH, (4 + 1 + 4 + 2 + 4 + 8)).
-define(PRODUCER_EXTENSION_LEN, (8 + 2 + 4)).
-define(MAGIC_V1, 1).
-define(MAGIC_V2, 2).
-define(COMPRESSION_MASK, 16#0007).
-define(HEADERS_BIT, 16#0008).
%% Some record in the batch has a null value: a tombstone.
-define(NULL_VALUE_BIT, 16#0040).
-define(MAX_DECOMPRESSED_BYTES, (256 * 1024 * 1024)).
-define(U32, 16#FFFFFFFF).
-define(U64, 16#FFFFFFFFFFFFFFFF).

%% ===========================================================================
%% BitPacker
%% ===========================================================================

%% @doc Build a request body from already-encoded fields, prefixed with the
%% schema version every body starts with.
-spec body(iodata()) -> binary().
body(Fields) ->
    iolist_to_binary([enc_string(?SCHEMA_VERSION), Fields]).

%% @doc Verify the schema version and run a positional decoder over the
%% rest. Truncation or a version mismatch becomes `{error, _}' rather than
%% garbage decoded into plausible-looking fields.
-spec decode_body(binary(), fun((binary()) -> term())) -> {ok, term()} | {error, term()}.
decode_body(Body, Decoder) ->
    try
        case dec_string(Body) of
            {?SCHEMA_VERSION, Rest} ->
                {ok, Decoder(Rest)};
            {Other, _} ->
                {error, {schema_version_mismatch, Other, ?SCHEMA_VERSION}}
        end
    catch
        throw:{decode_error, Reason} -> {error, {decode_error, Reason}};
        throw:{server_error, _, _, _} = E -> {error, E}
    end.

%% @doc Read a response's leading error code without consuming it. Every
%% group response starts with one, which is what makes a generic
%% coordinator-retry wrapper possible.
-spec peek_error_code(binary()) -> integer().
peek_error_code(Body) ->
    case decode_body(Body, fun(R) -> element(1, dec_int32(R)) end) of
        {ok, Code} -> Code;
        {error, _} -> ?ERR_NONE
    end.

enc_uvarint(V) when V >= 16#80 ->
    [(V band 16#7F) bor 16#80 | enc_uvarint(V bsr 7)];
enc_uvarint(V) when V >= 0 ->
    [V].

enc_int32(V) -> enc_uvarint(((V bsl 1) bxor (V bsr 31)) band ?U32).
enc_int64(V) -> enc_uvarint(((V bsl 1) bxor (V bsr 63)) band ?U64).

enc_bool(true) -> [1];
enc_bool(false) -> [0].

enc_string(S) ->
    Bin = iolist_to_binary(S),
    [enc_int32(byte_size(Bin)), Bin].

enc_string_array(List) ->
    [enc_int32(length(List)) | [enc_string(S) || S <- List]].

dec_uvarint(Bin) -> dec_uvarint(Bin, 0, 0).

dec_uvarint(_, _, Shift) when Shift > 63 ->
    throw({decode_error, varint_overflow});
dec_uvarint(<<1:1, B:7, Rest/binary>>, Acc, Shift) ->
    dec_uvarint(Rest, Acc bor (B bsl Shift), Shift + 7);
dec_uvarint(<<0:1, B:7, Rest/binary>>, Acc, Shift) ->
    {Acc bor (B bsl Shift), Rest};
dec_uvarint(<<>>, _, _) ->
    throw({decode_error, truncated_varint}).

unzigzag(U) -> (U bsr 1) bxor -(U band 1).

dec_int32(Bin) ->
    {U, Rest} = dec_uvarint(Bin),
    {unzigzag(U band ?U32), Rest}.

dec_int64(Bin) ->
    {U, Rest} = dec_uvarint(Bin),
    {unzigzag(U), Rest}.

dec_bool(<<B, Rest/binary>>) -> {B =/= 0, Rest};
dec_bool(<<>>) -> throw({decode_error, truncated_bool}).

dec_string(Bin) ->
    {Len, Rest} = dec_int32(Bin),
    case Rest of
        <<S:Len/binary, Rest2/binary>> when Len >= 0 -> {S, Rest2};
        _ -> throw({decode_error, truncated_string})
    end.

dec_string_array(Bin) -> dec_array(Bin, fun dec_string/1).

%% @doc Decode a count-prefixed array with an element decoder.
dec_array(Bin, Elem) ->
    {Count, Rest} = dec_int32(Bin),
    dec_array(Count, Rest, Elem, []).

dec_array(N, Bin, _Elem, Acc) when N =< 0 -> {lists:reverse(Acc), Bin};
dec_array(N, Bin, Elem, Acc) ->
    {V, Rest} = Elem(Bin),
    dec_array(N - 1, Rest, Elem, [V | Acc]).

%% ===========================================================================
%% Errors
%% ===========================================================================

error_name(0) -> 'NONE';
error_name(1) -> 'UNKNOWN_TOPIC_OR_PARTITION';
error_name(2) -> 'OFFSET_OUT_OF_RANGE';
error_name(3) -> 'INVALID_REQUEST';
error_name(4) -> 'UNSUPPORTED_VERSION';
error_name(5) -> 'INTERNAL';
error_name(6) -> 'NOT_LEADER_OR_FOLLOWER';
error_name(7) -> 'FENCED_BROKER_EPOCH';
error_name(8) -> 'FENCED_LEADER_EPOCH';
error_name(9) -> 'UNKNOWN_LEADER_EPOCH';
error_name(10) -> 'NOT_ENOUGH_REPLICAS';
error_name(11) -> 'FENCED_PRODUCER_EPOCH';
error_name(12) -> 'OUT_OF_ORDER_SEQUENCE';
error_name(13) -> 'UNKNOWN_MEMBER_ID';
error_name(14) -> 'REBALANCE_IN_PROGRESS';
error_name(15) -> 'NOT_COORDINATOR';
error_name(16) -> 'ILLEGAL_GENERATION';
error_name(17) -> 'COORDINATOR_LOAD_IN_PROGRESS';
error_name(18) -> 'SASL_AUTHENTICATION_FAILED';
error_name(19) -> 'AUTHORIZATION_FAILED';
error_name(_) -> 'UNKNOWN'.

%% @doc The error term every API returns for a non-zero broker code.
server_error(Code, Context) ->
    {server_error, Code, error_name(Code), Context}.

%% @doc Whether a code means "this send did not happen" — every code here
%% is one the broker returns strictly before it appends, so a retry cannot
%% duplicate a record.
retriable(?ERR_NOT_LEADER_OR_FOLLOWER) -> true;
retriable(?ERR_FENCED_LEADER_EPOCH) -> true;
retriable(?ERR_UNKNOWN_LEADER_EPOCH) -> true;
retriable(?ERR_NOT_ENOUGH_REPLICAS) -> true;
retriable(?ERR_COORDINATOR_LOAD_IN_PROGRESS) -> true;
retriable(?ERR_INTERNAL) -> true;
retriable(_) -> false.

%% ===========================================================================
%% Frames
%% ===========================================================================

%% @doc One frame, without the int32 length prefix (the socket adds it).
%% The header is fixed big-endian because the broker must read it before
%% it knows which body decoder to use.
-spec encode_frame(integer(), integer(), binary(), iodata()) -> iodata().
encode_frame(ApiKey, CorrelationId, ClientId, Body) ->
    [<<ApiKey:16/signed, ?API_VERSION:16/signed, CorrelationId:32/signed,
       (byte_size(ClientId)):16/signed>>, ClientId, Body].

%% @doc Split a response frame payload into its correlation id and body.
-spec decode_frame(binary()) -> {ok, integer(), binary()} | {error, term()}.
decode_frame(<<_:32, Corr:32/signed, ClientLen:16/signed, Rest/binary>>) ->
    Skip = max(ClientLen, 0),
    case Rest of
        <<_:Skip/binary, Body/binary>> -> {ok, Corr, Body};
        _ -> {error, frame_client_id_overrun}
    end;
decode_frame(_) ->
    {error, frame_too_short}.

%% ===========================================================================
%% CRC32C (Castagnoli) — not the zlib CRC32 erlang:crc32/1 computes
%% ===========================================================================

-spec crc32c(iodata()) -> non_neg_integer().
crc32c(Data) ->
    Table = crc32c_table(),
    crc32c(iolist_to_binary(Data), Table, ?U32) bxor ?U32.

crc32c(<<B, Rest/binary>>, Table, Crc) ->
    Index = (Crc bxor B) band 16#FF,
    crc32c(Rest, Table, element(Index + 1, Table) bxor (Crc bsr 8));
crc32c(<<>>, _, Crc) ->
    Crc.

crc32c_table() ->
    case persistent_term:get({?MODULE, crc32c_table}, undefined) of
        undefined ->
            Table = list_to_tuple([crc32c_entry(N, 8) || N <- lists:seq(0, 255)]),
            persistent_term:put({?MODULE, crc32c_table}, Table),
            Table;
        Table ->
            Table
    end.

crc32c_entry(C, 0) -> C;
crc32c_entry(C, K) when C band 1 =:= 1 -> crc32c_entry((C bsr 1) bxor 16#82F63B78, K - 1);
crc32c_entry(C, K) -> crc32c_entry(C bsr 1, K - 1).

%% ===========================================================================
%% Partitioning
%% ===========================================================================

%% @doc Kafka's 32-bit murmur2, transcribed so a key lands on the same
%% partition here as from every other client. murmur2(<<>>) =:= 275646681.
-spec murmur2(binary() | undefined) -> non_neg_integer().
murmur2(undefined) -> murmur2(<<>>);
murmur2(Data) when is_binary(Data) ->
    M = 16#5bd1e995,
    H0 = 16#9747b28c bxor byte_size(Data),
    {H1, Tail} = murmur2_chunks(Data, H0, M),
    H2 = case Tail of
             <<A, B, C>> -> mul32(H1 bxor (C bsl 16) bxor (B bsl 8) bxor A, M);
             <<A, B>> -> mul32(H1 bxor (B bsl 8) bxor A, M);
             <<A>> -> mul32(H1 bxor A, M);
             <<>> -> H1
         end,
    H3 = mul32(H2 bxor (H2 bsr 13), M),
    H3 bxor (H3 bsr 15).

murmur2_chunks(<<K0:32/little-unsigned, Rest/binary>>, H, M) ->
    K1 = mul32(K0, M),
    K2 = mul32(K1 bxor (K1 bsr 24), M),
    murmur2_chunks(Rest, mul32(H, M) bxor K2, M);
murmur2_chunks(Tail, H, _M) ->
    {H, Tail}.

mul32(A, B) -> (A * B) band ?U32.

%% @doc murmur2(key) mod partitions — Kafka's default partitioner.
-spec partition_for_key(binary(), [integer()]) -> integer().
partition_for_key(Key, Partitions) ->
    Index = (murmur2(Key) band 16#7fffffff) rem length(Partitions),
    lists:nth(Index + 1, Partitions).

%% ===========================================================================
%% Compression
%% ===========================================================================

codec_id(none) -> ?COMPRESSION_NONE;
codec_id(lz4) -> ?COMPRESSION_LZ4;
codec_id(zstd) -> ?COMPRESSION_ZSTD;
codec_id(snappy) -> ?COMPRESSION_SNAPPY;
codec_id(gzip) -> ?COMPRESSION_GZIP.

codec_of(?COMPRESSION_NONE) -> {ok, none};
codec_of(?COMPRESSION_LZ4) -> {ok, lz4};
codec_of(?COMPRESSION_ZSTD) -> {ok, zstd};
codec_of(?COMPRESSION_SNAPPY) -> {ok, snappy};
codec_of(?COMPRESSION_GZIP) -> {ok, gzip};
codec_of(Other) -> {error, {unsupported_compression, Other}}.

%% @doc Map Kafka's `compression.type' spelling (atom, string or binary)
%% onto a codec.
-spec parse_compression(atom() | string() | binary()) -> {ok, codec()} | {error, term()}.
parse_compression(Name) when is_atom(Name) ->
    case lists:member(Name, [none, gzip, lz4, zstd, snappy]) of
        true -> {ok, Name};
        false -> {error, {unknown_compression, Name}}
    end;
parse_compression(Name) when is_list(Name) ->
    parse_compression(list_to_binary(Name));
parse_compression(Name) when is_binary(Name) ->
    case Name of
        <<"none">> -> {ok, none};
        <<"gzip">> -> {ok, gzip};
        <<"lz4">> -> {ok, lz4};
        <<"zstd">> -> {ok, zstd};
        <<"snappy">> -> {ok, snappy};
        _ -> {error, {unknown_compression, Name}}
    end.

compression_name(Codec) -> atom_to_binary(Codec, utf8).

%% @doc Plug in a codec this library does not carry, so an application
%% that wants lz4 or zstd pays for that dependency and one that does not,
%% does not. The broker's lz4 is a little-endian u32 of the uncompressed
%% length followed by a raw LZ4 block, not the LZ4 frame format.
-spec register_codec(lz4 | zstd | snappy,
                     fun((binary()) -> binary()),
                     fun((binary()) -> binary())) -> ok.
register_codec(Codec, CompressFun, DecompressFun)
  when Codec =:= lz4; Codec =:= zstd; Codec =:= snappy ->
    persistent_term:put({?MODULE, codec, Codec}, {CompressFun, DecompressFun}).

unregister_codec(Codec) ->
    _ = persistent_term:erase({?MODULE, codec, Codec}),
    ok.

-spec compress(codec(), iodata()) -> {ok, iodata()} | {error, term()}.
compress(none, Payload) -> {ok, Payload};
compress(gzip, Payload) -> {ok, zlib:gzip(Payload)};
compress(Codec, Payload) ->
    case persistent_term:get({?MODULE, codec, Codec}, undefined) of
        {Fun, _} -> {ok, Fun(iolist_to_binary(Payload))};
        undefined -> {error, {codec_not_registered, Codec}}
    end.

-spec decompress(codec(), binary()) -> {ok, binary()} | {error, term()}.
decompress(none, Payload) -> {ok, Payload};
decompress(gzip, Payload) -> gunzip_capped(Payload);
decompress(Codec, Payload) ->
    case persistent_term:get({?MODULE, codec, Codec}, undefined) of
        {_, Fun} -> {ok, Fun(Payload)};
        undefined -> {error, {codec_not_registered, Codec}}
    end.

%% Capped so a corrupt or hostile batch cannot name gigabytes of output
%% that this process allocates before it can reject it.
gunzip_capped(Payload) ->
    Z = zlib:open(),
    try
        ok = zlib:inflateInit(Z, 31),
        Result = inflate_loop(Z, zlib:safeInflate(Z, Payload), [], 0),
        Result
    catch
        error:Reason -> {error, {gzip, Reason}}
    after
        zlib:close(Z)
    end.

inflate_loop(_Z, _, _Acc, Size) when Size > ?MAX_DECOMPRESSED_BYTES ->
    {error, decompressed_too_large};
inflate_loop(Z, {continue, Out}, Acc, Size) ->
    inflate_loop(Z, zlib:safeInflate(Z, []), [Out | Acc], Size + iolist_size(Out));
inflate_loop(_Z, {finished, Out}, Acc, _Size) ->
    {ok, iolist_to_binary(lists:reverse([Out | Acc]))};
inflate_loop(_Z, {need_dictionary, _, _}, _Acc, _Size) ->
    {error, {gzip, need_dictionary}}.

%% ===========================================================================
%% Record batches
%% ===========================================================================

%% @doc Encode one batch exactly as the broker stores it. The broker never
%% re-encodes: it stamps base_offset and leader_epoch in place (both sit
%% before the CRC) and writes these bytes to disk.
%%
%% Records are maps with `key', `value' (binary or `undefined' for a
%% tombstone), `timestamp_delta' and `headers'.
-spec encode_record_batch([record()], integer(), codec()) -> {ok, binary()} | {error, term()}.
encode_record_batch(Records, MaxTimestamp, Codec) ->
    HasHeaders = lists:any(fun(#{headers := H}) -> H =/= [] end, Records),
    %% A missing value is a tombstone and needs the widened length
    %% encoding; an empty binary is an ordinary record and must not.
    HasNull = lists:any(fun(#{value := V}) -> V =:= undefined end, Records),
    Payload = [encode_record(R, HasHeaders, HasNull) || R <- Records],
    case compress(Codec, Payload) of
        {error, _} = E ->
            E;
        {ok, Compressed0} ->
            Compressed = iolist_to_binary(Compressed0),
            Attributes = (codec_id(Codec) band ?COMPRESSION_MASK)
                bor flag(HasHeaders, ?HEADERS_BIT)
                bor flag(HasNull, ?NULL_VALUE_BIT),
            LastDelta = max(length(Records) - 1, 0),
            AfterCrc = <<Attributes:16, LastDelta:32/signed,
                         MaxTimestamp:64/signed, Compressed/binary>>,
            BatchLength = ?MIN_BATCH_LENGTH + byte_size(Compressed),
            Crc = crc32c(AfterCrc),
            {ok, <<0:64, BatchLength:32/signed, 0:32, ?MAGIC_V1, Crc:32,
                   AfterCrc/binary>>}
    end.

flag(true, Bit) -> Bit;
flag(false, _) -> 0.

encode_record(Record, HasHeaders, HasNull) ->
    #{key := Key, value := Value, timestamp_delta := Delta, headers := Headers} = Record,
    KeyPart = nullable(Key),
    ValuePart = case HasNull of
                    true -> nullable(Value);
                    false -> [enc_uvarint(byte_size(Value)), Value]
                end,
    DeltaPart = enc_uvarint(((Delta bsl 1) bxor (Delta bsr 63)) band ?U64),
    HeaderPart = case HasHeaders of
                     true ->
                         [enc_uvarint(length(Headers)) |
                          [[enc_uvarint(byte_size(HK)), HK, nullable(HV)]
                           || {HK, HV} <- Headers]];
                     false ->
                         []
                 end,
    Rec = iolist_to_binary([KeyPart, ValuePart, DeltaPart, HeaderPart]),
    [enc_uvarint(byte_size(Rec)), Rec].

nullable(undefined) -> [0];
nullable(Bin) -> [enc_uvarint(byte_size(Bin) + 1), Bin].

%% @doc Decode every batch in a fetch response's batch bytes.
-spec decode_record_batches(binary()) -> {ok, [batch()]} | {error, term()}.
decode_record_batches(Bin) -> decode_record_batches(Bin, []).

decode_record_batches(<<>>, Acc) ->
    {ok, lists:reverse(Acc)};
decode_record_batches(Bin, Acc) ->
    case decode_record_batch(Bin) of
        {ok, Batch, Rest} -> decode_record_batches(Rest, [Batch | Acc]);
        {error, _} = E -> E
    end.

%% @doc Decode the batch at the front of Bin, returning it and what follows.
-spec decode_record_batch(binary()) -> {ok, batch(), binary()} | {error, term()}.
decode_record_batch(<<BaseOffset:64/signed, BatchLength:32/signed, Rest/binary>>) ->
    if
        BatchLength < ?MIN_BATCH_LENGTH ->
            {error, batch_length_too_small};
        byte_size(Rest) < BatchLength ->
            {error, truncated_batch_body};
        true ->
            <<Body:BatchLength/binary, After/binary>> = Rest,
            <<_LeaderEpoch:32, Magic, StoredCrc:32, Checked/binary>> = Body,
            decode_batch_body(Magic, StoredCrc, Checked, BaseOffset, After)
    end;
decode_record_batch(_) ->
    {error, truncated_batch_header}.

decode_batch_body(Magic, _, _, _, _) when Magic =/= ?MAGIC_V1, Magic =/= ?MAGIC_V2 ->
    {error, {unsupported_magic, Magic}};
decode_batch_body(Magic, StoredCrc, Checked, BaseOffset, After) ->
    case crc32c(Checked) of
        StoredCrc ->
            <<Attributes:16, _LastDelta:32, MaxTimestamp:64/signed, Rest0/binary>> = Checked,
            Rest = case Magic of
                       ?MAGIC_V2 -> binary:part(Rest0, ?PRODUCER_EXTENSION_LEN,
                                                byte_size(Rest0) - ?PRODUCER_EXTENSION_LEN);
                       ?MAGIC_V1 -> Rest0
                   end,
            maybe_decode_records(Attributes, Rest, BaseOffset, MaxTimestamp, After);
        Computed ->
            {error, {crc_mismatch, StoredCrc, Computed}}
    end.

maybe_decode_records(Attributes, Compressed, BaseOffset, MaxTimestamp, After) ->
    Result =
        case codec_of(Attributes band ?COMPRESSION_MASK) of
            {ok, Codec} ->
                case decompress(Codec, Compressed) of
                    {ok, Payload} ->
                        try
                            {ok, decode_records(Payload,
                                                Attributes band ?HEADERS_BIT =/= 0,
                                                Attributes band ?NULL_VALUE_BIT =/= 0,
                                                [])}
                        catch
                            throw:{decode_error, Reason} -> {error, Reason};
                            error:{badmatch, _} -> {error, truncated_record}
                        end;
                    {error, _} = E -> E
                end;
            {error, _} = E -> E
        end,
    case Result of
        {ok, Records} ->
            {ok, #{base_offset => BaseOffset, max_timestamp => MaxTimestamp,
                   records => Records}, After};
        {error, _} = Err ->
            Err
    end.

decode_records(<<>>, _, _, Acc) ->
    lists:reverse(Acc);
decode_records(Payload, HasHeaders, HasNull, Acc) ->
    {Len, Rest0} = dec_uvarint(Payload),
    case Rest0 of
        <<Rec:Len/binary, Rest/binary>> ->
            decode_records(Rest, HasHeaders, HasNull,
                           [decode_record(Rec, HasHeaders, HasNull) | Acc]);
        _ ->
            throw({decode_error, truncated_record})
    end.

decode_record(Rec, HasHeaders, HasNull) ->
    {Key, R1} = dec_nullable(Rec),
    {Value, R2} = case HasNull of
                      %% A tombstone decodes to `undefined', which is what
                      %% distinguishes it from a merely empty value.
                      true -> dec_nullable(R1);
                      false ->
                          {VLen, R1b} = dec_uvarint(R1),
                          <<V:VLen/binary, R1c/binary>> = R1b,
                          {V, R1c}
                  end,
    {RawDelta, R3} = dec_uvarint(R2),
    {Headers, R4} = case HasHeaders of
                        true -> dec_headers(R3);
                        false -> {[], R3}
                    end,
    case R4 of
        <<>> -> ok;
        _ -> throw({decode_error, trailing_bytes_in_record})
    end,
    #{key => Key, value => Value, timestamp_delta => unzigzag(RawDelta),
      headers => Headers}.

dec_nullable(Bin) ->
    case dec_uvarint(Bin) of
        {0, Rest} -> {undefined, Rest};
        {N, Rest} ->
            Size = N - 1,
            case Rest of
                <<V:Size/binary, Rest2/binary>> -> {V, Rest2};
                _ -> throw({decode_error, truncated_record_field})
            end
    end.

dec_headers(Bin) ->
    {Count, Rest} = dec_uvarint(Bin),
    %% A count is a promise about bytes that follow; one exceeding what is
    %% left is corrupt, and allocating on it would let two bytes ask for
    %% gigabytes.
    case Count > byte_size(Rest) of
        true -> throw({decode_error, header_count_exceeds_record});
        false -> dec_headers(Count, Rest, [])
    end.

dec_headers(0, Bin, Acc) -> {lists:reverse(Acc), Bin};
dec_headers(N, Bin, Acc) ->
    {KLen, R1} = dec_uvarint(Bin),
    case R1 of
        <<K:KLen/binary, R2/binary>> ->
            {V, R3} = dec_nullable(R2),
            dec_headers(N - 1, R3, [{K, V} | Acc]);
        _ ->
            throw({decode_error, truncated_header})
    end.
