(* A consumer reads one partition at a time, with no group coordination.
   See [Group] for consumer groups. *)

open Protocol

(* One record delivered to the application. [key]/[value] = [None] are a
   null key and a tombstone, distinct from [Some ""]. *)
type record = {
  topic : string;
  partition : int32;
  offset : int64;
  key : string option;
  value : string option;
  (* Absolute unix milliseconds, already resolved against the batch base. *)
  timestamp : int64;
  headers : Header.t list;
}

(* The first value stored under [key]: [None] when absent, [Some None] for a
   null header value. *)
let header record key =
  List.find_map
    (fun (h : Header.t) -> if h.Header.key = key then Some h.Header.value else None)
    record.headers

(* Named as Kafka names its consumer settings. *)
type config = {
  client_id : string;
  (* fetch.max.bytes caps a response. *)
  fetch_max_bytes : int32;
  (* fetch.min.bytes returns early once this many bytes are ready. *)
  fetch_min_bytes : int32;
  (* fetch.max.wait.ms is the long-poll ceiling when caught up. *)
  fetch_max_wait_ms : int32;
  (* client.rack, empty for none. *)
  rack : string;
  (* [Protocol.read_uncommitted] (default) or [Protocol.read_committed]. *)
  isolation_level : int32;
  (* max.poll.records: how many records a group poll returns. *)
  max_poll_records : int;
  dial_timeout_ms : int;
  socket_timeout_ms : int;
}

let default_config =
  {
    client_id = Connection.default_client_id;
    fetch_max_bytes = 8_388_608l;
    fetch_min_bytes = 1l;
    fetch_max_wait_ms = 500l;
    rack = "";
    isolation_level = read_uncommitted;
    max_poll_records = 500;
    dial_timeout_ms = Connection.default_dial_timeout_ms;
    socket_timeout_ms = Connection.default_request_timeout_ms;
  }

type t = { config : config; router : Router.t }

let create ?(config = default_config) address =
  let router =
    Router.create ~client_id:config.client_id ~dial_timeout_ms:config.dial_timeout_ms
      ~request_timeout_ms:config.socket_timeout_ms address
  in
  { config; router }

let close t = Router.close t.router
let router t = t.router
let config t = t.config
let partitions t topic = Router.partitions t.router topic

(* Resolves [Protocol.earliest], [Protocol.latest] or a unix-ms timestamp
   to an offset. *)
let list_offsets t topic partition timestamp =
  let w = Writer.body () in
  Writer.string w topic;
  Writer.int32 w partition;
  Writer.int64 w timestamp;
  let conn = Router.conn_for t.router topic partition in
  let r = Reader.body (Connection.request conn api_list_offsets (Writer.contents w)) in
  ignore (Reader.string r : string) (* topic *);
  ignore (Reader.int32 r : int32) (* partition *);
  let code = Reader.int32 r in
  let offset = Reader.int64 r in
  ignore (Reader.int64 r : int64) (* timestamp *);
  if code <> err_none then
    raise (server_error code (Printf.sprintf "list_offsets %s-%ld" topic partition));
  offset

let fetch_once conn body =
  let r = Reader.body (Connection.request conn api_fetch body) in
  ignore (Reader.string r : string) (* topic *);
  ignore (Reader.int32 r : int32) (* partition *);
  let code = Reader.int32 r in
  let high_watermark = Reader.int64 r in
  ignore (Reader.int64 r : int64) (* last_stable_offset *);
  let batches_length = Reader.int64 r in
  (* Read even though unused: the batches trail the whole struct, so
     skipping a field would take them from the wrong offset. *)
  ignore (Reader.int32 r : int32) (* preferred_read_replica *);
  let trailing = Reader.rest r in
  if Int64.compare batches_length 0L < 0
     || Int64.compare batches_length (Int64.of_int (String.length trailing)) > 0
  then decode_error "fetch response claims %Ld batch bytes but carries %d" batches_length
      (String.length trailing);
  let raw = String.sub trailing 0 (Int64.to_int batches_length) in
  let rec batches pos acc =
    if pos >= String.length raw then List.rev acc
    else
      let batch, next = decode_record_batch raw pos in
      batches next (batch :: acc)
  in
  (code, high_watermark, batches 0 [])

(* Reads one partition from [offset]; also returns the high watermark.
   [max_wait_ms] is capped at fetch.max.wait.ms. *)
let fetch_verbose t ?max_wait_ms topic partition offset =
  let max_wait =
    match max_wait_ms with
    | Some ms when Int32.compare ms t.config.fetch_max_wait_ms < 0 -> ms
    | _ -> t.config.fetch_max_wait_ms
  in
  let w = Writer.body () in
  Writer.string w topic;
  Writer.int32 w partition;
  Writer.int64 w offset;
  Writer.int32 w t.config.fetch_max_bytes;
  Writer.int32 w max_wait;
  Writer.int32 w t.config.fetch_min_bytes;
  Writer.int32 w t.config.isolation_level;
  Writer.string w t.config.rack;
  let body = Writer.contents w in
  let code, high_watermark, batches =
    let ((code, _, _) as first) = fetch_once (Router.conn_for t.router topic partition) body in
    if code = err_not_leader_or_follower then begin
      ignore (Router.refresh t.router topic : Router.metadata);
      fetch_once (Router.conn_for t.router topic partition) body
    end
    else first
  in
  if code <> err_none then
    raise (server_error code (Printf.sprintf "fetch %s-%ld" topic partition));
  let records =
    List.concat_map
      (fun (batch : decoded_batch) ->
        List.mapi
          (fun index (r : Record.t) ->
            {
              topic;
              partition;
              offset = Int64.add batch.base_offset (Int64.of_int index);
              key = r.Record.key;
              value = r.Record.value;
              timestamp = Int64.add batch.max_timestamp r.Record.timestamp_delta;
              headers = r.Record.headers;
            })
          batch.records
        (* A batch can start before the requested offset; drop what the
           caller has already seen. *)
        |> List.filter (fun r -> Int64.compare r.offset offset >= 0))
      batches
  in
  (records, high_watermark)

let fetch t ?max_wait_ms topic partition offset =
  fst (fetch_verbose t ?max_wait_ms topic partition offset)
