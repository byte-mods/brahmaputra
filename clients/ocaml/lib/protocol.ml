(* Wire encodings for Brahmaputra.

   Three encodings share one connection and they do not agree with each
   other, so keeping them straight is most of the work:

   - The frame header is fixed big-endian: an int32 length prefix, then
     api key / api version / correlation id and an int16-prefixed client id.
   - A request or response body is BitPacker: every integer is a zigzag
     varint, every string and array is a varint count followed by its
     contents, and the whole body is prefixed with the schema version.
   - A record batch is neither: fixed big-endian header fields and plain
     (non-zigzag) varints inside each record, because the broker stamps
     offsets into it in place and validates its CRC without decoding it.

   OCaml's native [int] is 63 bits wide, so every value that crosses the
   wire is an [Int32.t] or [Int64.t]; zigzag, CRC32C and murmur2 are all
   computed in those types so that they wrap exactly where the broker's do. *)

let schema_version = "1.0.0"

(* The wire version this client speaks. The broker requires an exact match.
   Version 4 added tombstones (a null record value) and [client.rack]. *)
let api_version = 4

let read_uncommitted = 0l
let read_committed = 1l

(* Offset sentinels for ListOffsets. *)
let earliest = -2L
let latest = -1L

(* API keys, in wire order. They are int16 on the wire; [int] holds them. *)
let api_produce = 0
let api_fetch = 1
let api_list_offsets = 2
let api_metadata = 3
let api_join_group = 7
let api_sync_group = 8
let api_heartbeat = 9
let api_offset_commit = 10
let api_offset_fetch = 11
let api_api_versions = 14
let api_leave_group = 18

(* Error codes the broker returns in a response's error_code field. *)
let err_none = 0l
let err_unknown_topic_or_partition = 1l
let err_offset_out_of_range = 2l
let err_invalid_request = 3l
let err_unsupported_version = 4l
let err_internal = 5l
let err_not_leader_or_follower = 6l
let err_fenced_broker_epoch = 7l
let err_fenced_leader_epoch = 8l
let err_unknown_leader_epoch = 9l
let err_not_enough_replicas = 10l
let err_fenced_producer_epoch = 11l
let err_out_of_order_sequence = 12l
let err_unknown_member_id = 13l
let err_rebalance_in_progress = 14l
let err_not_coordinator = 15l
let err_illegal_generation = 16l
let err_coordinator_load_in_progress = 17l
let err_sasl_authentication_failed = 18l
let err_authorization_failed = 19l

let error_name code =
  match Int32.to_int code with
  | 0 -> "NONE"
  | 1 -> "UNKNOWN_TOPIC_OR_PARTITION"
  | 2 -> "OFFSET_OUT_OF_RANGE"
  | 3 -> "INVALID_REQUEST"
  | 4 -> "UNSUPPORTED_VERSION"
  | 5 -> "INTERNAL"
  | 6 -> "NOT_LEADER_OR_FOLLOWER"
  | 7 -> "FENCED_BROKER_EPOCH"
  | 8 -> "FENCED_LEADER_EPOCH"
  | 9 -> "UNKNOWN_LEADER_EPOCH"
  | 10 -> "NOT_ENOUGH_REPLICAS"
  | 11 -> "FENCED_PRODUCER_EPOCH"
  | 12 -> "OUT_OF_ORDER_SEQUENCE"
  | 13 -> "UNKNOWN_MEMBER_ID"
  | 14 -> "REBALANCE_IN_PROGRESS"
  | 15 -> "NOT_COORDINATOR"
  | 16 -> "ILLEGAL_GENERATION"
  | 17 -> "COORDINATOR_LOAD_IN_PROGRESS"
  | 18 -> "SASL_AUTHENTICATION_FAILED"
  | 19 -> "AUTHORIZATION_FAILED"
  | _ -> "UNKNOWN"

(* A non-zero error code from the broker. *)
exception Server_error of { code : int32; context : string }

(* A response (or batch) that does not decode: truncated, a length that
   runs past its buffer, a bad CRC, a schema mismatch. *)
exception Decode_error of string

(* An I/O failure, a timeout or a desynchronised stream. The connection
   that raised it is closed and marked broken; the router redials. *)
exception Connection_error of string

(* A producer's buffer.memory stayed full for max.block.ms. *)
exception Buffer_full of string

(* auto.offset.reset = none and there is no position to resume from. *)
exception No_offset_for_partition of { topic : string; partition : int32 }

let () =
  Printexc.register_printer (function
    | Server_error { code; context } ->
        Some
          (Printf.sprintf "broker returned %s[%ld]%s" (error_name code) code
             (if context = "" then "" else " (" ^ context ^ ")"))
    | Decode_error msg -> Some ("decode error: " ^ msg)
    | Connection_error msg -> Some ("connection error: " ^ msg)
    | Buffer_full msg -> Some msg
    | No_offset_for_partition { topic; partition } ->
        Some
          (Printf.sprintf
             "no committed offset for partition %s-%ld and auto.offset.reset=none"
             topic partition)
    | _ -> None)

let server_error code context = Server_error { code; context }
let decode_error fmt = Printf.ksprintf (fun msg -> raise (Decode_error msg)) fmt

(* Whether a code means "this send did not happen": every code here is one
   the broker returns strictly before it appends, so a retry cannot
   duplicate a record. *)
let retriable code =
  code = err_not_leader_or_follower
  || code = err_fenced_leader_epoch
  || code = err_unknown_leader_epoch
  || code = err_not_enough_replicas
  || code = err_coordinator_load_in_progress
  || code = err_internal

let now_ms () = Int64.of_float (Unix.gettimeofday () *. 1000.)

(* ------------------------------------------------------------------------ *)
(* Varints shared by both encodings                                          *)
(* ------------------------------------------------------------------------ *)

(* Unsigned LEB128 of the 64 bits of [v]. *)
let add_uvarint buf v =
  let rec go v =
    if Int64.logand v (Int64.lognot 0x7fL) = 0L then
      Buffer.add_char buf (Char.unsafe_chr (Int64.to_int v))
    else begin
      Buffer.add_char buf
        (Char.unsafe_chr (Int64.to_int (Int64.logor (Int64.logand v 0x7fL) 0x80L)));
      go (Int64.shift_right_logical v 7)
    end
  in
  go v

(* Reads an unsigned varint at [pos] of [data], bounded by [limit]; returns
   the value and the position after it. *)
let get_uvarint data pos limit =
  let rec go pos result shift =
    if pos >= limit then decode_error "truncated varint"
    else
      let b = Char.code (String.unsafe_get data pos) in
      let result =
        Int64.logor result (Int64.shift_left (Int64.of_int (b land 0x7f)) shift)
      in
      if b land 0x80 = 0 then (result, pos + 1)
      else if shift + 7 > 63 then decode_error "varint overflows 64 bits"
      else go (pos + 1) result (shift + 7)
  in
  go pos 0L 0

let zigzag32 v = Int32.logxor (Int32.shift_left v 1) (Int32.shift_right v 31)
let zigzag64 v = Int64.logxor (Int64.shift_left v 1) (Int64.shift_right v 63)

let unzigzag64 v =
  Int64.logxor (Int64.shift_right_logical v 1) (Int64.neg (Int64.logand v 1L))

let unzigzag32 v =
  Int32.logxor
    (Int64.to_int32 (Int64.shift_right_logical v 1))
    (Int32.neg (Int64.to_int32 (Int64.logand v 1L)))

(* Checks an unsigned 64-bit length against the bytes that remain, so a
   length near 2^64 cannot wrap negative and slip past. *)
let checked_length v remaining what =
  if Int64.unsigned_compare v (Int64.of_int remaining) > 0 then
    decode_error "truncated %s" what
  else Int64.to_int v

(* ------------------------------------------------------------------------ *)
(* BitPacker bodies                                                          *)
(* ------------------------------------------------------------------------ *)

module Writer = struct
  type t = Buffer.t

  let uvarint = add_uvarint
  let int32 w v = add_uvarint w (Int64.logand (Int64.of_int32 (zigzag32 v)) 0xFFFFFFFFL)
  let int64 w v = add_uvarint w (zigzag64 v)
  let bool w v = Buffer.add_char w (if v then '\001' else '\000')

  let string w s =
    int32 w (Int32.of_int (String.length s));
    Buffer.add_string w s

  let string_array w values =
    int32 w (Int32.of_int (List.length values));
    List.iter (string w) values

  let raw w s = Buffer.add_string w s
  let contents w = Buffer.contents w

  (* A writer already carrying the schema version every body starts with. *)
  let body () =
    let w = Buffer.create 256 in
    string w schema_version;
    w
end

module Reader = struct
  type t = { data : string; mutable pos : int }

  let uvarint r =
    let v, next = get_uvarint r.data r.pos (String.length r.data) in
    r.pos <- next;
    v

  let int32 r = unzigzag32 (uvarint r)
  let int64 r = unzigzag64 (uvarint r)

  let bool r =
    if r.pos >= String.length r.data then decode_error "truncated bool";
    let v = r.data.[r.pos] in
    r.pos <- r.pos + 1;
    v <> '\000'

  let remaining r = String.length r.data - r.pos

  let string r =
    let length = Int32.to_int (int32 r) in
    if length < 0 || length > remaining r then decode_error "truncated string";
    let s = String.sub r.data r.pos length in
    r.pos <- r.pos + length;
    s

  (* An array count. Every element takes at least one byte, so a count
     larger than what is left is corrupt, and allocating on it would let a
     two-byte body ask for gigabytes. *)
  let count r =
    let n = Int32.to_int (int32 r) in
    if n < 0 || n > remaining r then decode_error "array count %d out of range" n;
    n

  let list r f = List.init (count r) (fun _ -> f r)
  let string_array r = list r string

  let rest r =
    let s = String.sub r.data r.pos (remaining r) in
    r.pos <- String.length r.data;
    s

  (* A reader positioned past the schema version, which it verifies: a
     mismatch means broker and client disagree about the message shapes
     themselves, so failing loudly beats decoding garbage. *)
  let body data =
    let r = { data; pos = 0 } in
    let version = string r in
    if version <> schema_version then
      decode_error "schema version mismatch: broker speaks %S, this client speaks %S"
        version schema_version;
    r

  (* A group response's leading error code, without consuming the body. *)
  let peek_error_code data =
    try int32 (body data) with Decode_error _ -> err_none
end

(* ------------------------------------------------------------------------ *)
(* Frames                                                                    *)
(* ------------------------------------------------------------------------ *)

(* One complete frame, length prefix included. The header is big-endian
   while the body is BitPacker: the broker reads the header before it knows
   which body decoder to use. *)
let encode_frame api_key correlation_id client_id body =
  let payload_len = 8 + 2 + String.length client_id + String.length body in
  let b = Buffer.create (4 + payload_len) in
  Buffer.add_int32_be b (Int32.of_int payload_len);
  Buffer.add_int16_be b api_key;
  Buffer.add_int16_be b api_version;
  Buffer.add_int32_be b correlation_id;
  Buffer.add_int16_be b (String.length client_id);
  Buffer.add_string b client_id;
  Buffer.add_string b body;
  Buffer.contents b

(* Splits a response frame payload into its correlation id and body. *)
let decode_frame_payload payload =
  let len = String.length payload in
  if len < 10 then decode_error "frame payload shorter than its header";
  let correlation_id = String.get_int32_be payload 4 in
  let client_len = String.get_int16_be payload 8 in
  let offset = 10 + max 0 client_len in
  if offset > len then decode_error "frame client id runs past the payload";
  (correlation_id, String.sub payload offset (len - offset))

(* ------------------------------------------------------------------------ *)
(* CRC32C (Castagnoli), not the zlib CRC32                                   *)
(* ------------------------------------------------------------------------ *)

let crc_table =
  let open Bigarray in
  let t = Array1.create int32 c_layout 256 in
  for n = 0 to 255 do
    let c = ref (Int32.of_int n) in
    for _ = 0 to 7 do
      if Int32.logand !c 1l <> 0l then
        c := Int32.logxor 0x82F63B78l (Int32.shift_right_logical !c 1)
      else c := Int32.shift_right_logical !c 1
    done;
    t.{n} <- !c
  done;
  t

let crc32c_sub s off len =
  let c = ref 0xFFFFFFFFl in
  for i = off to off + len - 1 do
    let index =
      Int32.to_int
        (Int32.logand (Int32.logxor !c (Int32.of_int (Char.code (String.unsafe_get s i)))) 0xFFl)
    in
    c := Int32.logxor (Bigarray.Array1.unsafe_get crc_table index) (Int32.shift_right_logical !c 8)
  done;
  Int32.lognot !c

let crc32c s = crc32c_sub s 0 (String.length s)

(* ------------------------------------------------------------------------ *)
(* Compression                                                               *)
(* ------------------------------------------------------------------------ *)

(* Codecs, matching the broker's attribute values. *)
type compression = [ `None | `Lz4 | `Zstd | `Snappy | `Gzip ]

let compression_id : compression -> int = function
  | `None -> 0
  | `Lz4 -> 1
  | `Zstd -> 2
  | `Snappy -> 3
  | `Gzip -> 4

let compression_of_id = function
  | 0 -> Some `None
  | 1 -> Some `Lz4
  | 2 -> Some `Zstd
  | 3 -> Some `Snappy
  | 4 -> Some `Gzip
  | _ -> None

let compression_name : compression -> string = function
  | `None -> "none"
  | `Lz4 -> "lz4"
  | `Zstd -> "zstd"
  | `Snappy -> "snappy"
  | `Gzip -> "gzip"

(* Kafka's compression.type spelling. *)
let parse_compression = function
  | "none" -> `None
  | "lz4" -> `Lz4
  | "zstd" -> `Zstd
  | "snappy" -> `Snappy
  | "gzip" -> `Gzip
  | other ->
      invalid_arg
        (Printf.sprintf "unknown compression %S (none, lz4, zstd, snappy, gzip)" other)

(* Capped so a corrupt or hostile batch cannot name gigabytes of output
   that this process allocates before it can reject it. *)
let max_decompressed_bytes = 256 * 1024 * 1024

let wrap_zlib f x =
  try f x with Zlib.Error (func, msg) -> decode_error "zlib %s: %s" func msg

(* gzip framing around camlzip's raw deflate stream (RFC 1952). *)
let gzip_compress payload =
  let len = String.length payload in
  let out = Buffer.create ((len / 2) + 64) in
  Buffer.add_string out "\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff";
  let z = Zlib.deflate_init 6 false in
  let chunk = Bytes.create 65536 in
  let rec loop pos =
    let finished, used_in, used_out =
      Zlib.deflate_string z payload pos (len - pos) chunk 0 (Bytes.length chunk) Zlib.Z_FINISH
    in
    Buffer.add_subbytes out chunk 0 used_out;
    if not finished then loop (pos + used_in)
  in
  Fun.protect ~finally:(fun () -> Zlib.deflate_end z) (fun () -> loop 0);
  Buffer.add_int32_le out (Zlib.update_crc_string 0l payload 0 len);
  Buffer.add_int32_le out (Int32.of_int (len land 0xFFFFFFFF));
  Buffer.contents out

let gzip_decompress data =
  let n = String.length data in
  let out = Buffer.create (max 64 (n * 3)) in
  let chunk = Bytes.create 65536 in
  let rec skip_zero_terminated p =
    if p >= n then decode_error "truncated gzip header"
    else if data.[p] = '\000' then p + 1
    else skip_zero_terminated (p + 1)
  in
  let rec member pos =
    if pos + 10 > n then decode_error "truncated gzip header";
    if data.[pos] <> '\x1f' || data.[pos + 1] <> '\x8b' || data.[pos + 2] <> '\x08' then
      decode_error "not a gzip stream";
    let flags = Char.code data.[pos + 3] in
    let p = pos + 10 in
    let p =
      if flags land 4 <> 0 then begin
        if p + 2 > n then decode_error "truncated gzip header";
        p + 2 + String.get_uint16_le data p
      end
      else p
    in
    let p = if flags land 8 <> 0 then skip_zero_terminated p else p in
    let p = if flags land 16 <> 0 then skip_zero_terminated p else p in
    let p = if flags land 2 <> 0 then p + 2 else p in
    if p > n then decode_error "truncated gzip header";
    let start = Buffer.length out in
    let z = Zlib.inflate_init false in
    let rec inflate pos =
      let finished, used_in, used_out =
        Zlib.inflate_string z data pos (n - pos) chunk 0 (Bytes.length chunk) Zlib.Z_SYNC_FLUSH
      in
      Buffer.add_subbytes out chunk 0 used_out;
      if Buffer.length out > max_decompressed_bytes then
        decode_error "gzip payload exceeds %d bytes" max_decompressed_bytes;
      if finished then pos + used_in
      else if used_in = 0 && used_out = 0 then decode_error "truncated gzip stream"
      else inflate (pos + used_in)
    in
    let stream_end =
      Fun.protect ~finally:(fun () -> Zlib.inflate_end z) (fun () -> inflate p)
    in
    if stream_end + 8 > n then decode_error "truncated gzip trailer";
    let size = Buffer.length out - start in
    let expected_crc = String.get_int32_le data stream_end in
    let member_bytes = Buffer.sub out start size in
    if Zlib.update_crc_string 0l member_bytes 0 size <> expected_crc then
      decode_error "gzip crc mismatch";
    let next = stream_end + 8 in
    (* Concatenated members are one stream, as gzip readers treat them. *)
    if next < n then member next
  in
  if n = 0 then decode_error "empty gzip payload";
  member 0;
  Buffer.contents out

let codecs : (int, (string -> string) * (string -> string)) Hashtbl.t = Hashtbl.create 8
let codecs_mu = Mutex.create ()

(* Plugs in a codec this library does not carry itself (lz4, zstd,
   snappy), so an application that wants one pays for that dependency and
   one that does not, does not. Can also replace gzip.

   The lz4 payload the broker expects is a little-endian uint32 of the
   uncompressed length followed by a raw LZ4 block, not the LZ4 frame
   format. *)
let register_codec (codec : compression) ~compress ~decompress =
  Mutex.lock codecs_mu;
  Hashtbl.replace codecs (compression_id codec) (compress, decompress);
  Mutex.unlock codecs_mu

let () =
  register_codec `None ~compress:Fun.id ~decompress:Fun.id;
  register_codec `Gzip ~compress:gzip_compress ~decompress:gzip_decompress

let find_codec id =
  Mutex.lock codecs_mu;
  let found = Hashtbl.find_opt codecs id in
  Mutex.unlock codecs_mu;
  found

let compress (codec : compression) payload =
  match find_codec (compression_id codec) with
  | Some (fn, _) -> wrap_zlib fn payload
  | None ->
      invalid_arg
        (Printf.sprintf
           "%s compression is not registered; call Protocol.register_codec or use none/gzip"
           (compression_name codec))

let decompress id payload =
  match find_codec id with
  | Some (_, fn) -> wrap_zlib fn payload
  | None -> (
      match compression_of_id id with
      | Some codec ->
          decode_error "%s decompression is not registered; call Protocol.register_codec"
            (compression_name codec)
      | None -> decode_error "unsupported compression %d" id)

(* ------------------------------------------------------------------------ *)
(* Record batches                                                            *)
(* ------------------------------------------------------------------------ *)

(* An ordered, possibly repeating annotation on a record. [value = None] is
   a null header value, distinct from [Some ""]. *)
module Header = struct
  type t = { key : string; value : string option }

  let make key value = { key; value = Some value }
  let null key = { key; value = None }
end

(* One record inside a batch. [key = None] is a null key; [value = None] is
   a tombstone. Both are distinct from the empty string. *)
module Record = struct
  type t = {
    key : string option;
    value : string option;
    (* Milliseconds relative to the batch's max timestamp, so normally zero
       or negative. *)
    timestamp_delta : int64;
    headers : Header.t list;
  }
end

let batch_header_len = 12
let min_batch_length = 4 + 1 + 4 + 2 + 4 + 8
let producer_extension_len = 8 + 2 + 4
let magic_v1 = 1
let magic_v2 = 2
let compression_mask = 0x0007
let headers_bit = 0x0008

(* Some record in the batch has a null value: a tombstone. Set only when
   one is present, so a batch without one encodes exactly as before. *)
let null_value_bit = 0x0040

let add_bytes_plus_one buf = function
  | None -> add_uvarint buf 0L
  | Some s ->
      add_uvarint buf (Int64.of_int (String.length s + 1));
      Buffer.add_string buf s

(* Encodes one batch exactly as the broker stores it. The broker never
   re-encodes it: it stamps base_offset and leader_epoch in place (both sit
   before the CRC) and writes these bytes to disk, so getting this wrong
   corrupts the log rather than merely failing a request. *)
let encode_record_batch (records : Record.t list) ~max_timestamp (codec : compression) =
  let has_headers = List.exists (fun (r : Record.t) -> r.Record.headers <> []) records in
  let has_null_values = List.exists (fun (r : Record.t) -> r.Record.value = None) records in
  let payload = Buffer.create 256 in
  let rec_buf = Buffer.create 256 in
  List.iter
    (fun (record : Record.t) ->
      Buffer.clear rec_buf;
      add_bytes_plus_one rec_buf record.Record.key;
      (if has_null_values then add_bytes_plus_one rec_buf record.Record.value
       else
         let v = Option.value record.Record.value ~default:"" in
         add_uvarint rec_buf (Int64.of_int (String.length v));
         Buffer.add_string rec_buf v);
      add_uvarint rec_buf (zigzag64 record.Record.timestamp_delta);
      if has_headers then begin
        add_uvarint rec_buf (Int64.of_int (List.length record.Record.headers));
        List.iter
          (fun (h : Header.t) ->
            add_uvarint rec_buf (Int64.of_int (String.length h.Header.key));
            Buffer.add_string rec_buf h.Header.key;
            add_bytes_plus_one rec_buf h.Header.value)
          record.Record.headers
      end;
      add_uvarint payload (Int64.of_int (Buffer.length rec_buf));
      Buffer.add_buffer payload rec_buf)
    records;
  let compressed = compress codec (Buffer.contents payload) in
  let attributes =
    compression_id codec land compression_mask
    lor (if has_headers then headers_bit else 0)
    lor if has_null_values then null_value_bit else 0
  in
  let batch_length = min_batch_length + String.length compressed in
  let out = Buffer.create (batch_header_len + batch_length) in
  Buffer.add_int64_be out 0L (* base_offset, stamped by the broker *);
  Buffer.add_int32_be out (Int32.of_int batch_length);
  Buffer.add_int32_be out 0l (* leader_epoch, likewise *);
  Buffer.add_uint8 out magic_v1;
  let crc_at = Buffer.length out in
  Buffer.add_int32_be out 0l;
  Buffer.add_uint16_be out attributes;
  Buffer.add_int32_be out (Int32.of_int (max 0 (List.length records - 1)));
  Buffer.add_int64_be out max_timestamp;
  Buffer.add_string out compressed;
  let bytes = Buffer.to_bytes out in
  let crc =
    crc32c_sub (Bytes.unsafe_to_string bytes) (crc_at + 4) (Bytes.length bytes - crc_at - 4)
  in
  Bytes.set_int32_be bytes crc_at crc;
  Bytes.unsafe_to_string bytes

type decoded_batch = { base_offset : int64; max_timestamp : int64; records : Record.t list }

let decode_records payload ~has_headers ~has_null_values =
  let limit = String.length payload in
  let records = ref [] in
  let pos = ref 0 in
  let uvarint end_ =
    let v, next = get_uvarint payload !pos end_ in
    pos := next;
    v
  in
  let take n =
    let s = String.sub payload !pos n in
    pos := !pos + n;
    s
  in
  while !pos < limit do
    let length = checked_length (uvarint limit) (limit - !pos) "record" in
    let end_ = !pos + length in
    let key_plus_one = uvarint end_ in
    let key =
      if key_plus_one = 0L then None
      else
        (* Some "" when empty: an empty key is not a null key. *)
        Some (take (checked_length (Int64.pred key_plus_one) (end_ - !pos) "record key"))
    in
    let raw_value_len = uvarint end_ in
    let value =
      if has_null_values && raw_value_len = 0L then None
      else
        let n = if has_null_values then Int64.pred raw_value_len else raw_value_len in
        Some (take (checked_length n (end_ - !pos) "record value"))
    in
    let timestamp_delta = unzigzag64 (uvarint end_) in
    let headers =
      if not has_headers then []
      else begin
        let count = uvarint end_ in
        if Int64.unsigned_compare count (Int64.of_int (end_ - !pos)) > 0 then
          decode_error "record header count exceeds record";
        List.init (Int64.to_int count) (fun _ ->
            let key_len = checked_length (uvarint end_) (end_ - !pos) "record header key" in
            let key = take key_len in
            let value_plus_one = uvarint end_ in
            let value =
              if value_plus_one = 0L then None
              else
                Some
                  (take
                     (checked_length (Int64.pred value_plus_one) (end_ - !pos)
                        "record header value"))
            in
            { Header.key; value })
      end
    in
    if !pos <> end_ then decode_error "trailing bytes in record";
    records := { Record.key; value; timestamp_delta; headers } :: !records
  done;
  List.rev !records

(* Decodes one batch starting at [offset] of [data]; returns it and the
   offset just past it. *)
let decode_record_batch data offset =
  let len = String.length data in
  if len - offset < batch_header_len then decode_error "truncated batch header";
  let base_offset = String.get_int64_be data offset in
  let batch_length = Int32.to_int (String.get_int32_be data (offset + 8)) in
  (* Covers a negative length too. *)
  if batch_length < min_batch_length then decode_error "batch_length %d too small" batch_length;
  let body_at = offset + batch_header_len in
  let end_ = body_at + batch_length in
  if end_ > len then decode_error "truncated batch body";
  let magic = Char.code data.[body_at + 4] in
  if magic <> magic_v1 && magic <> magic_v2 then decode_error "unsupported magic %d" magic;
  let crc_at = body_at + 5 in
  let stored = String.get_int32_be data crc_at in
  let computed = crc32c_sub data (crc_at + 4) (end_ - crc_at - 4) in
  if stored <> computed then
    decode_error "crc mismatch: stored %08lx, computed %08lx" stored computed;
  let cursor = crc_at + 4 in
  let attributes = String.get_uint16_be data cursor in
  let max_timestamp = String.get_int64_be data (cursor + 6) in
  let cursor = cursor + 14 + if magic = magic_v2 then producer_extension_len else 0 in
  if cursor > end_ then decode_error "truncated batch producer extension";
  let payload =
    decompress (attributes land compression_mask) (String.sub data cursor (end_ - cursor))
  in
  let records =
    decode_records payload
      ~has_headers:(attributes land headers_bit <> 0)
      ~has_null_values:(attributes land null_value_bit <> 0)
  in
  ({ base_offset; max_timestamp; records }, end_)

(* ------------------------------------------------------------------------ *)
(* Partitioning                                                              *)
(* ------------------------------------------------------------------------ *)

(* Kafka's 32-bit murmur2, transcribed rather than imported: a producer in
   any language writing the same key has to land on the same partition. *)
let murmur2 data =
  let seed = 0x9747b28cl and m = 0x5bd1e995l and r = 24 in
  let length = String.length data in
  let h = ref (Int32.logxor seed (Int32.of_int length)) in
  let byte i = Int32.of_int (Char.code data.[i]) in
  let chunks = length / 4 in
  for i = 0 to chunks - 1 do
    let o = i * 4 in
    let k =
      Int32.logor (byte o)
        (Int32.logor
           (Int32.shift_left (byte (o + 1)) 8)
           (Int32.logor (Int32.shift_left (byte (o + 2)) 16) (Int32.shift_left (byte (o + 3)) 24)))
    in
    let k = Int32.mul k m in
    let k = Int32.logxor k (Int32.shift_right_logical k r) in
    let k = Int32.mul k m in
    h := Int32.logxor (Int32.mul !h m) k
  done;
  let tail = chunks * 4 in
  let rest = length - tail in
  if rest = 3 then h := Int32.logxor !h (Int32.shift_left (byte (tail + 2)) 16);
  if rest >= 2 then h := Int32.logxor !h (Int32.shift_left (byte (tail + 1)) 8);
  if rest >= 1 then begin
    h := Int32.logxor !h (byte tail);
    h := Int32.mul !h m
  end;
  h := Int32.logxor !h (Int32.shift_right_logical !h 13);
  h := Int32.mul !h m;
  h := Int32.logxor !h (Int32.shift_right_logical !h 15);
  !h

(* murmur2(key) % partitions, Kafka's default partitioner. *)
let partition_for_key key (partitions : int32 list) =
  let n = List.length partitions in
  if n = 0 then invalid_arg "partition_for_key: no partitions";
  let positive = Int32.logand (murmur2 key) 0x7fffffffl in
  List.nth partitions (Int32.to_int (Int32.rem positive (Int32.of_int n)))
