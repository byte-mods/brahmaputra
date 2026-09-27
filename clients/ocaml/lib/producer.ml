(* A producer batches records per partition and sends each batch as one
   Produce request. Share one across threads rather than creating one per
   message: the batching is the point.

   A background linger thread flushes every non-empty buffer each
   [linger_ms]; a buffer that reaches [batch_size] is flushed by the send
   that filled it. A partition has at most one batch in flight, and batches
   leave in the order they were taken, so neither path can reorder a
   partition. *)

open Protocol

(* Named as Kafka names its producer settings. *)
type config = {
  client_id : string;
  (* 0 fire-and-forget, 1 leader append, -1 ("all") every in-sync replica. *)
  acks : int32;
  (* batch.size: flush a partition buffer once it holds this many bytes. *)
  batch_size : int;
  (* linger.ms: flush every non-empty buffer at least this often; 0 sends
     each record immediately. Kafka defaults to 0; this defaults to 5. *)
  linger_ms : int;
  (* compression.type: none, gzip (built in), lz4, zstd, snappy (register
     with [Protocol.register_codec] first). *)
  compression_type : string;
  (* request.timeout.ms: the broker-side wait for acknowledgements. *)
  request_timeout_ms : int32;
  (* retries of a send refused with a retriable error, one the broker
     returns before appending, so a retry cannot duplicate. *)
  retries : int;
  retry_backoff_ms : int;
  (* delivery.timeout.ms caps the whole send, first attempt to last retry. *)
  delivery_timeout_ms : int;
  (* buffer.memory caps unflushed record bytes held client-side. *)
  buffer_memory : int;
  (* max.block.ms: how long a send may block on a full buffer. *)
  max_block_ms : int;
  dial_timeout_ms : int;
  (* Client-side bound on one request/response round trip. *)
  socket_timeout_ms : int;
}

let default_config =
  {
    client_id = Connection.default_client_id;
    acks = 1l;
    batch_size = 16 * 1024;
    linger_ms = 5;
    compression_type = "none";
    request_timeout_ms = 30_000l;
    retries = 5;
    retry_backoff_ms = 100;
    delivery_timeout_ms = 120_000;
    buffer_memory = 32 * 1024 * 1024;
    max_block_ms = 60_000;
    dial_timeout_ms = Connection.default_dial_timeout_ms;
    socket_timeout_ms = Connection.default_request_timeout_ms;
  }

type pending = { record : Record.t; created_ms : int64 }
type slot = string * int32
type buffer = { mutable items : pending list (* newest first *); mutable size : int }

type t = {
  config : config;
  codec : compression;
  router : Router.t;
  mu : Mutex.t;
  buffers : (slot, buffer) Hashtbl.t;
  mutable buffered_bytes : int;
  mutable round_robin : int;
  mutable closed : bool;
  linger_done : bool Atomic.t;
  (* Serialises sends per partition. Without it the linger thread and a
     send that fills a batch could each take a batch for the same
     partition and race to the connection, and the log would end up in a
     different order from the one the application sent. *)
  send_locks : (slot, Mutex.t) Hashtbl.t;
  (* The first failure of a linger-driven flush. Those records already left
     the buffer, so the error is the only trace of them; the next flush or
     close raises it rather than reporting a success that did not happen. *)
  mutable background_error : exn option;
}

let with_lock = Connection.with_lock

let router t = t.router

let rec flush_partition t slot =
  let lock =
    with_lock t.mu (fun () ->
        match Hashtbl.find_opt t.send_locks slot with
        | Some l -> l
        | None ->
            let l = Mutex.create () in
            Hashtbl.replace t.send_locks slot l;
            l)
  in
  (* Held across the round trip and any retries. *)
  with_lock lock (fun () ->
      let taken =
        with_lock t.mu (fun () ->
            match Hashtbl.find_opt t.buffers slot with
            | Some b when b.items <> [] ->
                let items = List.rev b.items and size = b.size in
                b.items <- [];
                b.size <- 0;
                t.buffered_bytes <- max 0 (t.buffered_bytes - size);
                Some items
            | _ -> None)
      in
      match taken with
      | None -> -1L
      | Some batch -> produce t (fst slot) (snd slot) batch)

and produce t topic partition batch =
  match batch with
  | [] -> -1L
  | first :: _ ->
      (* The batch stores one base timestamp and a delta per record;
         max_timestamp is the newest record's time. *)
      let max_timestamp =
        List.fold_left (fun m p -> if Int64.compare p.created_ms m > 0 then p.created_ms else m)
          first.created_ms batch
      in
      let records =
        List.map
          (fun p -> { p.record with Record.timestamp_delta = Int64.sub p.created_ms max_timestamp })
          batch
      in
      let encoded = encode_record_batch records ~max_timestamp t.codec in
      let w = Writer.body () in
      Writer.string w topic;
      Writer.int32 w partition;
      Writer.int32 w t.config.acks;
      Writer.int32 w t.config.request_timeout_ms;
      Writer.int64 w (Int64.of_int (String.length encoded));
      Writer.raw w encoded;
      let body = Writer.contents w in
      if t.config.acks = 0l then begin
        Connection.send_oneway (Router.conn_for t.router topic partition) api_produce body;
        -1L
      end
      else
        let deadline =
          Unix.gettimeofday () +. (float_of_int t.config.delivery_timeout_ms /. 1000.)
        in
        let rec attempt attempts_left =
          let conn = Router.conn_for t.router topic partition in
          let r = Reader.body (Connection.request conn api_produce body) in
          ignore (Reader.string r : string) (* topic *);
          ignore (Reader.int32 r : int32) (* partition *);
          let code = Reader.int32 r in
          let base_offset = Reader.int64 r in
          ignore (Reader.int64 r : int64) (* log_append_time_ms *);
          if code = err_none then base_offset
          else if (not (retriable code)) || attempts_left <= 0 || Unix.gettimeofday () > deadline
          then raise (server_error code (Printf.sprintf "produce to %s-%ld" topic partition))
          else begin
            if code = err_not_leader_or_follower || code = err_fenced_leader_epoch
               || code = err_unknown_leader_epoch
            then (try ignore (Router.refresh t.router topic : Router.metadata) with _ -> ());
            Thread.delay (float_of_int t.config.retry_backoff_ms /. 1000.);
            attempt (attempts_left - 1)
          end
        in
        attempt t.config.retries

let flush_all t =
  let slots =
    with_lock t.mu (fun () ->
        Hashtbl.fold (fun slot b acc -> if b.items <> [] then slot :: acc else acc) t.buffers [])
  in
  List.iter (fun slot -> ignore (flush_partition t slot : int64)) slots

let linger_loop t =
  let interval = float_of_int t.config.linger_ms /. 1000. in
  let rec loop () =
    Thread.delay interval;
    if not (with_lock t.mu (fun () -> t.closed)) then begin
      (* A failed background flush must not kill the thread; the next
         explicit flush surfaces it to a caller who can act on it. *)
      (try flush_all t
       with e ->
         with_lock t.mu (fun () ->
             if t.background_error = None then t.background_error <- Some e));
      loop ()
    end
  in
  Fun.protect ~finally:(fun () -> Atomic.set t.linger_done true) loop

let create ?(config = default_config) address =
  let codec = parse_compression config.compression_type in
  let router =
    Router.create ~client_id:config.client_id ~dial_timeout_ms:config.dial_timeout_ms
      ~request_timeout_ms:config.socket_timeout_ms address
  in
  let t =
    {
      config;
      codec;
      router;
      mu = Mutex.create ();
      buffers = Hashtbl.create 16;
      buffered_bytes = 0;
      round_robin = 0;
      closed = false;
      linger_done = Atomic.make (config.linger_ms <= 0);
      send_locks = Hashtbl.create 16;
      background_error = None;
    }
  in
  if config.linger_ms > 0 then ignore (Thread.create linger_loop t : Thread.t);
  t

let choose_partition t topic key =
  let partitions = Router.partitions t.router topic in
  match key with
  | Some k -> partition_for_key k partitions
  | None ->
      let index =
        with_lock t.mu (fun () ->
            let i = t.round_robin mod List.length partitions in
            t.round_robin <- t.round_robin + 1;
            i)
      in
      List.nth partitions index

(* Blocks until [size] more bytes may be buffered. This is what makes
   buffer.memory real: a producer faster than its broker is slowed down
   here rather than allowed to grow without limit. *)
let reserve t size =
  let limit = t.config.buffer_memory in
  Mutex.lock t.mu;
  if limit <= 0 || size >= limit then begin
    (* A record larger than the whole budget is admitted rather than
       waiting forever on a condition that can never hold. *)
    t.buffered_bytes <- t.buffered_bytes + size;
    Mutex.unlock t.mu
  end
  else begin
    let deadline = Unix.gettimeofday () +. (float_of_int t.config.max_block_ms /. 1000.) in
    let rec wait () =
      if t.buffered_bytes + size > limit then
        if Unix.gettimeofday () > deadline then begin
          let held = t.buffered_bytes in
          Mutex.unlock t.mu;
          raise
            (Buffer_full
               (Printf.sprintf
                  "producer buffer full: %d of %d bytes unflushed after max.block.ms=%d" held limit
                  t.config.max_block_ms))
        end
        else begin
          Mutex.unlock t.mu;
          Thread.delay 0.01;
          Mutex.lock t.mu;
          wait ()
        end
    in
    wait ();
    t.buffered_bytes <- t.buffered_bytes + size;
    Mutex.unlock t.mu
  end

let record_size key value headers =
  let len = function None -> 0 | Some s -> String.length s in
  List.fold_left
    (fun n (h : Header.t) -> n + String.length h.Header.key + len h.Header.value + 4)
    (len value + len key + 16) headers

(* Buffers one record; [flush] awaits delivery. [value = None] is a
   tombstone. Without [~partition], a keyed record goes to
   murmur2(key) mod partitions and an unkeyed one round-robins.
   [~timestamp] is unix milliseconds (default: now). *)
let send t ?key ?(headers = []) ?partition ?timestamp topic value =
  let partition =
    match partition with Some p -> p | None -> choose_partition t topic key
  in
  let size = record_size key value headers in
  reserve t size;
  let created_ms = match timestamp with Some ts -> ts | None -> now_ms () in
  let slot = (topic, partition) in
  let full =
    with_lock t.mu (fun () ->
        let b =
          match Hashtbl.find_opt t.buffers slot with
          | Some b -> b
          | None ->
              let b = { items = []; size = 0 } in
              Hashtbl.replace t.buffers slot b;
              b
        in
        b.items <- { record = { Record.key; value; timestamp_delta = 0L; headers }; created_ms } :: b.items;
        b.size <- b.size + size;
        b.size >= t.config.batch_size)
  in
  if t.config.linger_ms <= 0 || full then ignore (flush_partition t slot : int64)

(* Sends one record on its own and returns its offset (-1 with acks=0). A
   full round trip per record: correct, and slow. *)
let send_sync t ?key ?(headers = []) ?partition ?timestamp topic value =
  let partition =
    match partition with Some p -> p | None -> choose_partition t topic key
  in
  let created_ms = match timestamp with Some ts -> ts | None -> now_ms () in
  produce t topic partition
    [ { record = { Record.key; value; timestamp_delta = 0L; headers }; created_ms } ]

(* Sends every buffered record and waits for acknowledgement. Also raises
   the failure of any background (linger) flush since the last call,
   because those records are gone and nothing else would say so. *)
let flush t =
  let result = try Ok (flush_all t) with e -> Error e in
  let background =
    with_lock t.mu (fun () ->
        let e = t.background_error in
        t.background_error <- None;
        e)
  in
  match (result, background) with
  | Error e, _ -> raise e
  | Ok (), Some e -> raise e
  | Ok (), None -> ()

(* Flushes, stops the linger thread and releases connections. The thread
   and connections are released even when the final flush fails; that
   failure is still raised. *)
let close t =
  let result = try Ok (flush t) with e -> Error e in
  with_lock t.mu (fun () -> t.closed <- true);
  let deadline = Unix.gettimeofday () +. 2.0 in
  while (not (Atomic.get t.linger_done)) && Unix.gettimeofday () < deadline do
    Thread.delay 0.005
  done;
  Router.close t.router;
  match result with Ok () -> () | Error e -> raise e
