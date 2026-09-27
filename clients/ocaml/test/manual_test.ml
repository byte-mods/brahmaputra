(* End-to-end suite for the OCaml driver against a live broker; a port of
   the Go suite (clients/go/cmd/manualtest) with the same sections and
   checks.

     brahmaputra-server --data-dir ./data --default-partitions 4
     manual_test.exe [HOST [PORT]]

   Every check asserts a property of the system, not that a function ran:
   records come back byte-identical, keys pin partitions, headers survive,
   offsets are contiguous. *)

open Brahmaputra
module P = Protocol

let passed = ref 0
let failed = ref 0

let check name ok detail =
  if ok then begin
    incr passed;
    Printf.printf "  ok   %s\n%!" name
  end
  else begin
    incr failed;
    if detail <> "" then Printf.printf "  FAIL %s: %s\n%!" name detail
    else Printf.printf "  FAIL %s\n%!" name
  end

let section title = Printf.printf "\n%s\n%!" title

let counter = ref 0

let unique prefix =
  incr counter;
  let ns = Int64.of_float (Unix.gettimeofday () *. 1e9) in
  Printf.sprintf "%s-%Ld%d" prefix (Int64.rem ns 1_000_000_000L) !counter

let must f =
  try f ()
  with e ->
    Printf.printf "  FATAL %s\n%!" (Printexc.to_string e);
    exit 2

let now_ms () = Int64.of_float (Unix.gettimeofday () *. 1000.)
let sleep_ms ms = Thread.delay (float_of_int ms /. 1000.)
let sp = Printf.sprintf
let err_string = function None -> "<nil>" | Some e -> Printexc.to_string e
let show_opt = function None -> "None" | Some s -> sp "Some %S" s
let contains s sub =
  let n = String.length s and m = String.length sub in
  let rec go i = i + m <= n && (String.sub s i m = sub || go (i + 1)) in
  go 0

let producer_config = { Producer.default_config with linger_ms = 0 }
let group_config = { Group.default_config with auto_commit_interval_ms = 0 }

(* Fetches forward from offset 0 until [want] records arrive or a fetch
   comes back empty. *)
let fetch_all consumer topic partition want =
  let rec go offset acc n =
    if n >= want then List.concat (List.rev acc)
    else
      match Consumer.fetch consumer topic partition offset with
      | [] -> List.concat (List.rev acc)
      | batch ->
          let last = List.nth batch (List.length batch - 1) in
          go (Int64.succ last.Consumer.offset) (batch :: acc) (n + List.length batch)
      | exception _ -> List.concat (List.rev acc)
  in
  go 0L [] 0

(* Polls until [want] records arrive or [seconds] pass. *)
let poll_until group ~want ~seconds ~timeout_ms =
  let deadline = Unix.gettimeofday () +. seconds in
  let rec go acc n =
    if n >= want || Unix.gettimeofday () > deadline then List.concat (List.rev acc)
    else
      let records = must (fun () -> Group.poll group ~timeout_ms) in
      go (records :: acc) (n + List.length records)
  in
  go [] 0

(* Polls for [seconds], ignoring errors, and returns everything seen. *)
let poll_for group ~seconds ~timeout_ms =
  let deadline = Unix.gettimeofday () +. seconds in
  let rec go acc =
    if Unix.gettimeofday () > deadline then List.concat (List.rev acc)
    else
      let records = try Group.poll group ~timeout_ms with _ -> [] in
      go (records :: acc)
  in
  go []

(* ------------------------------------------------------------------------ *)
(* A TCP proxy that can sever every live connection, which is how a broker  *)
(* restart or an idle timeout looks to a client.                            *)
(* ------------------------------------------------------------------------ *)

type proxy = {
  address : string;
  listener : Unix.file_descr;
  mu : Mutex.t;
  mutable live : Unix.file_descr list;
}

let listen_local () =
  let fd = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Unix.setsockopt fd Unix.SO_REUSEADDR true;
  Unix.bind fd (Unix.ADDR_INET (Unix.inet_addr_loopback, 0));
  Unix.listen fd 64;
  let port = match Unix.getsockname fd with Unix.ADDR_INET (_, p) -> p | _ -> 0 in
  (fd, sp "127.0.0.1:%d" port)

let shutdown_quietly fd = try Unix.shutdown fd Unix.SHUTDOWN_ALL with Unix.Unix_error _ -> ()

let new_proxy target =
  let listener, address = listen_local () in
  let p = { address; listener; mu = Mutex.create (); live = [] } in
  let host, port =
    let i = String.rindex target ':' in
    (String.sub target 0 i, int_of_string (String.sub target (i + 1) (String.length target - i - 1)))
  in
  let target_addr = Unix.ADDR_INET ((Unix.gethostbyname host).Unix.h_addr_list.(0), port) in
  let pump src dst finished =
    let buf = Bytes.create 65536 in
    let rec go () =
      match Unix.read src buf 0 (Bytes.length buf) with
      | 0 -> ()
      | n ->
          let rec write off =
            if off < n then write (off + Unix.write dst buf off (n - off))
          in
          write 0;
          go ()
    in
    (try go () with Unix.Unix_error _ -> ());
    shutdown_quietly src;
    shutdown_quietly dst;
    finished ()
  in
  let rec accept_loop () =
    match Unix.accept listener with
    | client, _ -> (
        let upstream = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
        match Unix.connect upstream target_addr with
        | () ->
            Mutex.lock p.mu;
            p.live <- client :: upstream :: p.live;
            Mutex.unlock p.mu;
            (* The second pump to finish closes both sockets. *)
            let remaining = ref 2 and m = Mutex.create () in
            let finished () =
              Mutex.lock m;
              decr remaining;
              let last = !remaining = 0 in
              Mutex.unlock m;
              if last then begin
                Mutex.lock p.mu;
                p.live <- List.filter (fun fd -> fd != client && fd != upstream) p.live;
                Mutex.unlock p.mu;
                (try Unix.close client with Unix.Unix_error _ -> ());
                try Unix.close upstream with Unix.Unix_error _ -> ()
              end
            in
            ignore (Thread.create (fun () -> pump client upstream finished) ());
            ignore (Thread.create (fun () -> pump upstream client finished) ());
            accept_loop ()
        | exception Unix.Unix_error _ ->
            Unix.close client;
            Unix.close upstream;
            accept_loop ())
    | exception Unix.Unix_error _ -> ()
  in
  ignore (Thread.create accept_loop ());
  p

let drop_all p =
  Mutex.lock p.mu;
  List.iter shutdown_quietly p.live;
  Mutex.unlock p.mu;
  sleep_ms 50

let close_proxy p =
  shutdown_quietly p.listener;
  drop_all p

(* ------------------------------------------------------------------------ *)
(* Coverage: one check per client feature the Go suite's sections do not   *)
(* already exercise.                                                        *)
(* ------------------------------------------------------------------------ *)

(* The broker's lz4 payload: a little-endian uncompressed length, then a raw
   LZ4 block. This encoder writes literals only (valid, if uncompressed). *)
let lz4_compress s =
  let n = String.length s in
  let b = Buffer.create (n + 16) in
  Buffer.add_int32_le b (Int32.of_int n);
  if n >= 15 then begin
    Buffer.add_char b '\xf0';
    let rec ext k =
      if k >= 255 then (Buffer.add_char b '\xff'; ext (k - 255)) else Buffer.add_char b (Char.chr k)
    in
    ext (n - 15)
  end
  else Buffer.add_char b (Char.chr (n lsl 4));
  Buffer.add_string b s;
  Buffer.contents b

let lz4_decompress s =
  let out = Buffer.create (String.length s) in
  let pos = ref 4 in
  let byte () = let c = Char.code s.[!pos] in incr pos; c in
  let length n = if n < 15 then n else
      let rec more acc = let b = byte () in if b = 255 then more (acc + 255) else acc + b in
      more 15
  in
  let rec block () =
    if !pos < String.length s then begin
      let token = byte () in
      let lits = length (token lsr 4) in
      Buffer.add_string out (String.sub s !pos lits);
      pos := !pos + lits;
      if !pos < String.length s then begin
        let lo = byte () in
        let offset = lo lor (byte () lsl 8) in
        let mlen = length (token land 15) + 4 in
        for _ = 1 to mlen do
          Buffer.add_char out (Buffer.nth out (Buffer.length out - offset))
        done;
        block ()
      end
    end
  in
  block ();
  Buffer.contents out

(* A broker that answers Metadata with itself as the only broker and refuses
   every produce: topic "fatal" with a non-retriable code, anything else
   with NOT_ENOUGH_REPLICAS (retriable). Records (topic, acks, timeout) for
   each produce it sees. *)
type fake = { fake_address : string; fake_listener : Unix.file_descr; fake_mu : Mutex.t;
              mutable produces : (string * int32 * int32) list }

let read_exact fd n =
  let buf = Bytes.create n in
  let rec go off = if off < n then
      let got = Unix.read fd buf off (n - off) in
      if got = 0 then raise End_of_file else go (off + got)
  in
  go 0;
  Bytes.to_string buf

let fake_broker () =
  let listener, address = listen_local () in
  let port = match Unix.getsockname listener with Unix.ADDR_INET (_, p) -> p | _ -> 0 in
  let f = { fake_address = address; fake_listener = listener; fake_mu = Mutex.create (); produces = [] } in
  let answer api req =
    let w = P.Writer.body () in
    (match api with
    | 3 ->
        let r = P.Reader.body req in
        let topics = P.Reader.string_array r in
        List.iter (P.Writer.int32 w) [ 0l; 1l; 0l ];
        P.Writer.string w "127.0.0.1";
        P.Writer.int32 w (Int32.of_int port);
        P.Writer.string w "";
        P.Writer.int32 w 0l;
        P.Writer.int32 w (Int32.of_int (List.length topics));
        List.iter
          (fun t ->
            P.Writer.string w t;
            List.iter (P.Writer.int32 w) [ 0l; 1l; 0l; 0l; 1l; 0l; 1l; 0l; 0l ])
          topics
    | 0 ->
        let r = P.Reader.body req in
        let topic = P.Reader.string r in
        let partition = P.Reader.int32 r in
        let acks = P.Reader.int32 r in
        let timeout = P.Reader.int32 r in
        Mutex.lock f.fake_mu;
        f.produces <- f.produces @ [ (topic, acks, timeout) ];
        Mutex.unlock f.fake_mu;
        P.Writer.string w topic;
        P.Writer.int32 w partition;
        P.Writer.int32 w (if topic = "fatal" then 87l else 10l);
        P.Writer.int64 w (-1L);
        P.Writer.int64 w (-1L)
    | _ -> P.Writer.int32 w 35l);
    P.Writer.contents w
  in
  let serve fd =
    (try
       while true do
         let n = Int32.to_int (String.get_int32_be (read_exact fd 4) 0) in
         let payload = read_exact fd n in
         let api = String.get_int16_be payload 0 in
         let clen = String.get_int16_be payload 8 in
         let header = String.sub payload 0 (10 + clen) in
         let resp = header ^ answer api (String.sub payload (10 + clen) (n - 10 - clen)) in
         let b = Buffer.create (String.length resp + 4) in
         Buffer.add_int32_be b (Int32.of_int (String.length resp));
         Buffer.add_string b resp;
         let out = Buffer.contents b in
         ignore (Unix.write_substring fd out 0 (String.length out) : int)
       done
     with _ -> ());
    try Unix.close fd with Unix.Unix_error _ -> ()
  in
  ignore
    (Thread.create
       (fun () ->
         let rec loop () =
           match Unix.accept listener with
           | fd, _ -> ignore (Thread.create serve fd : Thread.t); loop ()
           | exception _ -> ()
         in
         loop ())
       ()
      : Thread.t);
  f

let fake_produces f = Mutex.lock f.fake_mu; let p = f.produces in Mutex.unlock f.fake_mu; p
let fake_reset f = Mutex.lock f.fake_mu; f.produces <- []; Mutex.unlock f.fake_mu

let fails f = match f () with _ -> false | exception _ -> true
let elapsed_ms started = int_of_float ((Unix.gettimeofday () -. started) *. 1000.)

(* Sizes of successive non-empty polls until [want] records or [seconds]. *)
let poll_sizes group ~want ~seconds =
  let deadline = Unix.gettimeofday () +. seconds in
  let rec go acc =
    if List.fold_left ( + ) 0 acc >= want || Unix.gettimeofday () > deadline then List.rev acc
    else
      match Group.poll group ~timeout_ms:300 with
      | [] -> go acc
      | records -> go (List.length records :: acc)
      | exception _ -> go acc
  in
  go []

(* Polls every group in parallel (a join blocks until every member has
   rejoined) until [is_done ()] or [seconds] pass. *)
let settle groups is_done ~seconds =
  let deadline = Unix.gettimeofday () +. seconds in
  let rec go () =
    let threads =
      List.map (fun g -> Thread.create (fun g -> try ignore (Group.poll g ~timeout_ms:200) with _ -> ()) g) groups
    in
    List.iter Thread.join threads;
    if is_done () then true else if Unix.gettimeofday () > deadline then false else go ()
  in
  go ()

let quietly f = try f () with _ -> ()

let coverage address =
  section "producer settings";
  let c = must (fun () -> Consumer.create address) in
  (let topic = unique "ml-batchsize" in
   let p = must (fun () ->
     Producer.create ~config:{ Producer.default_config with linger_ms = 60_000; batch_size = 64 } address) in
   for i = 0 to 2 do
     must (fun () -> Producer.send p ~partition:0l topic (Some (String.make 100 'b' ^ string_of_int i)))
   done;
   let got = must (fun () -> Consumer.fetch c ~max_wait_ms:1000l topic 0l 0L) in
   check "batch.size sends a full batch without waiting for linger" (List.length got = 3)
     (sp "got %d" (List.length got));
   quietly (fun () -> Producer.close p));
  (let topic = unique "ml-linger" in
   let p = must (fun () ->
     Producer.create ~config:{ Producer.default_config with linger_ms = 50; batch_size = 1_048_576 } address) in
   must (fun () -> Producer.send p ~partition:0l topic (Some "lingering"));
   sleep_ms 500;
   let got = must (fun () -> Consumer.fetch c ~max_wait_ms:1000l topic 0l 0L) in
   check "linger.ms flushes a partial batch on its own" (List.length got = 1) (sp "got %d" (List.length got));
   quietly (fun () -> Producer.close p));
  (let topic = unique "ml-sync" in
   let p = must (fun () -> Producer.create ~config:producer_config address) in
   let stamp = 1_600_000_000_000L in
   let first = must (fun () -> Producer.send_sync p ~partition:2l ~timestamp:stamp topic (Some "one")) in
   let second =
     must (fun () -> Producer.send_sync p ~partition:2l ~timestamp:(Int64.add stamp 1000L) topic (Some "two"))
   in
   check "send_sync returns consecutive offsets" (first = 0L && second = 1L) (sp "%Ld, %Ld" first second);
   let got = must (fun () -> Consumer.fetch c ~max_wait_ms:1000l topic 2l 0L) in
   check "an explicit partition is honoured" (List.length got = 2) (sp "partition 2 holds %d" (List.length got));
   let stamps = List.map (fun r -> r.Consumer.timestamp) got in
   check "an explicit timestamp is stored exactly" (stamps = [ stamp; Int64.add stamp 1000L ])
     (String.concat "," (List.map Int64.to_string stamps));
   let rr_topic = unique "ml-roundrobin" in
   let parts = must (fun () -> Router.partitions (Producer.router p) rr_topic) in
   for i = 1 to 2 * List.length parts do
     must (fun () -> Producer.send p rr_topic (Some (sp "rr%d" i)))
   done;
   must (fun () -> Producer.flush p);
   let counts = List.map (fun part -> List.length (must (fun () -> Consumer.fetch c ~max_wait_ms:300l rr_topic part 0L))) parts in
   check "keyless records are spread round-robin" (List.for_all (( = ) 2) counts)
     (String.concat "," (List.map string_of_int counts));
   must (fun () -> Producer.close p));
  (* A codec the driver does not carry, registered by the application: a
     valid LZ4 block of literals only, which the broker accepts as-is. *)
  (P.register_codec `Lz4 ~compress:lz4_compress ~decompress:lz4_decompress;
   let topic = unique "ml-lz4" in
   let body i = String.concat "" (List.init 20 (fun _ -> "registered codec payload ")) ^ string_of_int i in
   let p = must (fun () -> Producer.create ~config:{ producer_config with compression_type = "lz4" } address) in
   for i = 0 to 4 do
     must (fun () -> Producer.send p ~partition:0l topic (Some (body i)))
   done;
   must (fun () -> Producer.close p);
   let got = must (fun () -> Consumer.fetch c ~max_wait_ms:1000l topic 0l 0L) in
   check "a registered codec round-trips through the broker"
     (List.map (fun r -> r.Consumer.value) got = List.init 5 (fun i -> Some (body i)))
     (sp "got %d" (List.length got)));
  Consumer.close c;

  section "retries against a broker that refuses";
  (let f = fake_broker () in
   let p = must (fun () ->
     Producer.create
       ~config:{ producer_config with acks = -1l; request_timeout_ms = 1234l; retries = 2; retry_backoff_ms = 150 }
       f.fake_address) in
   let started = Unix.gettimeofday () in
   let failed_send = fails (fun () -> Producer.send_sync p ~partition:0l "retriable" (Some "x")) in
   let took = elapsed_ms started in
   let attempts = fake_produces f in
   check "request.timeout.ms and acks reach the broker"
     (attempts <> [] && List.for_all (fun (_, acks, t) -> acks = -1l && t = 1234l) attempts)
     (sp "%d attempts" (List.length attempts));
   check "a retriable error is retried `retries` times" (failed_send && List.length attempts = 3)
     (sp "%d attempts" (List.length attempts));
   check "retry.backoff.ms spaces the retries" (took >= 300) (sp "%d ms" took);
   fake_reset f;
   let fatal = fails (fun () -> Producer.send_sync p ~partition:0l "fatal" (Some "x")) in
   let n = List.length (fake_produces f) in
   check "a non-retriable error is not retried" (fatal && n = 1) (sp "%d attempts" n);
   quietly (fun () -> Producer.close p);
   fake_reset f;
   let p = must (fun () ->
     Producer.create
       ~config:{ producer_config with retries = 1_000_000; retry_backoff_ms = 50; delivery_timeout_ms = 400 }
       f.fake_address) in
   let started = Unix.gettimeofday () in
   let capped = fails (fun () -> Producer.send_sync p ~partition:0l "retriable" (Some "x")) in
   let took = elapsed_ms started in
   check "delivery.timeout.ms caps the retries" (capped && took < 3000)
     (sp "%d ms, %d attempts" took (List.length (fake_produces f)));
   quietly (fun () -> Producer.close p);
   shutdown_quietly f.fake_listener;
   (try Unix.close f.fake_listener with Unix.Unix_error _ -> ()));

  section "consumer settings";
  (let topic = unique "ml-fetchcfg" in
   let p = must (fun () -> Producer.create ~config:producer_config address) in
   for i = 0 to 19 do
     must (fun () -> Producer.send p ~partition:0l topic (Some (String.make 1000 'f' ^ string_of_int i)))
   done;
   must (fun () -> Producer.close p);
   let c = must (fun () -> Consumer.create address) in
   let records, hw = must (fun () -> Consumer.fetch_verbose c ~max_wait_ms:500l topic 0l 0L) in
   check "fetch reports the high watermark" (hw = 20L) (Int64.to_string hw);
   check "a default fetch returns every record" (List.length records = 20) (sp "got %d" (List.length records));
   let meta = must (fun () -> Router.refresh (Consumer.router c) topic) in
   let brokers = List.map (fun b -> b.Router.node_id) meta.Router.brokers in
   let infos =
     match List.find_opt (fun t -> t.Router.name = topic) meta.Router.topics with
     | Some t -> t.Router.partitions
     | None -> []
   in
   check "metadata names a live leader for every partition"
     (infos <> [] && List.for_all (fun i -> List.mem i.Router.leader brokers) infos)
     (sp "%d partitions" (List.length infos));
   Consumer.close c;
   let small = must (fun () ->
     Consumer.create ~config:{ Consumer.default_config with fetch_max_bytes = 2500l } address) in
   let got = must (fun () -> Consumer.fetch small ~max_wait_ms:500l topic 0l 0L) in
   check "fetch.max.bytes caps a response" (got <> [] && List.length got < 20) (sp "got %d" (List.length got));
   Consumer.close small;
   let patient = must (fun () ->
     Consumer.create
       ~config:{ Consumer.default_config with fetch_min_bytes = 10_000_000l; fetch_max_wait_ms = 400l }
       address) in
   let started = Unix.gettimeofday () in
   let got = must (fun () -> Consumer.fetch patient ~max_wait_ms:400l topic 0l 19L) in
   let waited = elapsed_ms started in
   check "fetch.min.bytes holds a fetch for up to fetch.max.wait.ms"
     (List.length got = 1 && waited >= 300 && waited < 5000)
     (sp "%d ms, %d records" waited (List.length got));
   Consumer.close patient);
  (let topic = unique "ml-bytime" in
   let p = must (fun () -> Producer.create ~config:producer_config address) in
   let base = 1_700_000_000_000L in
   for i = 0 to 2 do
     must (fun () ->
       Producer.send p ~partition:0l ~timestamp:(Int64.add base (Int64.of_int (i * 10_000))) topic
         (Some (sp "t%d" i)))
   done;
   must (fun () -> Producer.close p);
   let c = must (fun () -> Consumer.create address) in
   let at = must (fun () -> Consumer.list_offsets c topic 0l (Int64.add base 5000L)) in
   check "list offsets by timestamp finds the first record at or after it" (at = 1L) (Int64.to_string at);
   Consumer.close c);
  (let topic = unique "ml-maxpoll" in
   let p = must (fun () -> Producer.create ~config:producer_config address) in
   for i = 0 to 9 do
     must (fun () -> Producer.send p ~partition:0l topic (Some (sp "m%d" i)))
   done;
   must (fun () -> Producer.close p);
   let g = must (fun () ->
     Group.create ~config:{ group_config with max_poll_records = 3 } address (unique "ml-maxpoll-grp")) in
   Group.subscribe g [ topic ];
   let sizes = poll_sizes g ~want:10 ~seconds:15. in
   check "max.poll.records caps a poll"
     (List.fold_left ( + ) 0 sizes = 10 && List.for_all (fun n -> n <= 3) sizes)
     (String.concat "," (List.map string_of_int sizes));
   quietly (fun () -> Group.close g));

  section "consumer group settings";
  let p = must (fun () -> Producer.create ~config:producer_config address) in
  (let t1 = unique "ml-multi-a" and t2 = unique "ml-multi-b" in
   for i = 0 to 4 do
     must (fun () -> Producer.send p t1 (Some (sp "a%d" i)));
     must (fun () -> Producer.send p t2 (Some (sp "b%d" i)))
   done;
   let g = must (fun () -> Group.create ~config:group_config address (unique "ml-multi-grp")) in
   Group.subscribe g [ t1; t2 ];
   let got = poll_until g ~want:10 ~seconds:15. ~timeout_ms:300 in
   let count t = List.length (List.filter (fun r -> r.Consumer.topic = t) got) in
   check "a group consumes every subscribed topic" (count t1 = 5 && count t2 = 5)
     (sp "%d + %d" (count t1) (count t2));
   quietly (fun () -> Group.close g));
  (let topic = unique "ml-autocommit" in
   for i = 0 to 5 do
     must (fun () -> Producer.send p ~partition:0l topic (Some (sp "c%d" i)))
   done;
   let committed_after config =
     let g = must (fun () -> Group.create ~config address (unique "ml-auto-grp")) in
     Group.subscribe g [ topic ];
     ignore (poll_until g ~want:6 ~seconds:15. ~timeout_ms:300 : Consumer.record list);
     sleep_ms 200;
     quietly (fun () -> ignore (Group.poll g ~timeout_ms:300 : Consumer.record list));
     let committed = must (fun () -> Group.committed g [ (topic, 0l) ]) in
     quietly (fun () -> Group.close g);
     List.assoc_opt (topic, 0l) committed
   in
   let auto = committed_after { Group.default_config with auto_commit_interval_ms = 100 } in
   check "auto-commit records positions without an explicit commit" (auto = Some 6L)
     (match auto with Some o -> Int64.to_string o | None -> "none");
   let manual = committed_after group_config in
   check "disabled auto-commit commits nothing"
     (match manual with None -> true | Some o -> Int64.compare o 0L < 0)
     (match manual with Some o -> Int64.to_string o | None -> "none"));
  (* Static membership: a second instance presenting the same
     group.instance.id takes over the first one's partitions at once,
     without a rebalance, while the first is still heartbeating. *)
  (let topic = unique "ml-static" in
   ignore (must (fun () -> Router.partitions (Producer.router p) topic) : int32 list);
   let group_id = unique "ml-static-grp" in
   let config = { group_config with group_instance_id = "instance-1" } in
   let first = must (fun () -> Group.create ~config address group_id) in
   Group.subscribe first [ topic ];
   quietly (fun () -> ignore (Group.poll first ~timeout_ms:2000 : Consumer.record list));
   let first_assignment = Group.assignment first in
   let second = must (fun () -> Group.create ~config address group_id) in
   Group.subscribe second [ topic ];
   let started = Unix.gettimeofday () in
   quietly (fun () -> ignore (Group.poll second ~timeout_ms:200 : Consumer.record list));
   let took = elapsed_ms started in
   let second_assignment = Group.assignment second in
   check "a static member reclaims its partitions without a rebalance"
     (List.length first_assignment = 4
      && List.sort compare second_assignment = List.sort compare first_assignment
      && took < 2000)
     (sp "first=%d second=%d %d ms" (List.length first_assignment) (List.length second_assignment) took);
   quietly (fun () -> Group.close second);
   quietly (fun () -> Group.close first));
  (* LeaveGroup on close: with a 30 s session and a 200 ms heartbeat, the
     survivor takes over within a heartbeat, not a session. *)
  (let topic = unique "ml-leave" in
   ignore (must (fun () -> Router.partitions (Producer.router p) topic) : int32 list);
   let group_id = unique "ml-leave-grp" in
   let config = { group_config with session_timeout_ms = 30_000l; heartbeat_interval_ms = 200 } in
   let a = must (fun () -> Group.create ~config address group_id) in
   let b = must (fun () -> Group.create ~config address group_id) in
   Group.subscribe a [ topic ];
   Group.subscribe b [ topic ];
   let split =
     settle [ a; b ] (fun () -> List.map (fun g -> List.length (Group.assignment g)) [ a; b ] = [ 2; 2 ]) ~seconds:20.
   in
   must (fun () -> Group.close a);
   let started = Unix.gettimeofday () in
   let took_over = settle [ b ] (fun () -> List.length (Group.assignment b) = 4) ~seconds:15. in
   let took = elapsed_ms started in
   check "closing a member hands its partitions over within a heartbeat" (split && took_over && took < 5000)
     (sp "split=%b took_over=%b %d ms" split took_over took);
   quietly (fun () -> Group.close b));
  (* session.timeout.ms: a member that goes silent without leaving (its only
     route to the broker is a proxy that is shut) is evicted once its
     session lapses, and the survivor takes over. *)
  (let topic = unique "ml-session" in
   ignore (must (fun () -> Router.partitions (Producer.router p) topic) : int32 list);
   let group_id = unique "ml-session-grp" in
   let config = { group_config with session_timeout_ms = 2_000l; heartbeat_interval_ms = 200 } in
   let proxy = new_proxy address in
   let a = must (fun () -> Group.create ~config proxy.address group_id) in
   let b = must (fun () -> Group.create ~config address group_id) in
   Group.subscribe a [ topic ];
   Group.subscribe b [ topic ];
   let split =
     settle [ a; b ] (fun () -> List.map (fun g -> List.length (Group.assignment g)) [ a; b ] = [ 2; 2 ]) ~seconds:20.
   in
   close_proxy proxy;
   let started = Unix.gettimeofday () in
   let took_over = settle [ b ] (fun () -> List.length (Group.assignment b) = 4) ~seconds:20. in
   let took = elapsed_ms started in
   check "a silent member is evicted after session.timeout.ms"
     (split && took_over && took >= 1000 && took < 12_000)
     (sp "split=%b took_over=%b %d ms" split took_over took);
   quietly (fun () -> Group.close b);
   quietly (fun () -> Group.close a));
  (* Generation fencing: a member whose generation moved on cannot commit. *)
  (let topic = unique "ml-fence" in
   for i = 0 to 3 do
     must (fun () -> Producer.send p topic (Some (sp "f%d" i)))
   done;
   let group_id = unique "ml-fence-grp" in
   let a = must (fun () -> Group.create ~config:group_config address group_id) in
   Group.subscribe a [ topic ];
   ignore (poll_until a ~want:4 ~seconds:10. ~timeout_ms:300 : Consumer.record list);
   let b = must (fun () -> Group.create ~config:group_config address group_id) in
   Group.subscribe b [ topic ];
   quietly (fun () -> ignore (Group.poll b ~timeout_ms:500 : Consumer.record list));
   check "a commit from a stale generation is refused" (fails (fun () -> Group.commit a)) "commit succeeded";
   quietly (fun () -> Group.close b);
   quietly (fun () -> Group.close a));
  must (fun () -> Producer.close p);

  section "assignors (unit)";
  (let members = [ { Assignor.id = "a"; topics = [ "t" ] }; { Assignor.id = "b"; topics = [ "t" ] } ] in
   let slots l = List.map (fun i -> ("t", Int32.of_int i)) l in
   let range a b = List.init (b - a + 1) (fun i -> a + i) in
   let sticky =
     Assignor.sticky members [ ("t", List.map Int32.of_int (range 0 11)) ] [ ("a", slots (range 0 11)); ("b", []) ]
   in
   check "sticky keeps partitions in numeric order"
     (sticky = [ ("a", slots (range 0 5)); ("b", slots (range 6 11)) ]) "";
   let held = [ ("a", slots [ 1; 3 ]); ("b", slots [ 0; 2 ]) ] in
   let kept = Assignor.sticky members [ ("t", [ 0l; 1l; 2l; 3l ]) ] held in
   check "sticky keeps what members already hold" (kept = held) "");

  section "decoder bounds";
  (let body f = let w = P.Writer.body () in f w; P.Writer.contents w in
   let negative = body (fun w -> P.Writer.int32 w (-5l)) in
   check "a negative length is an error"
     (fails (fun () -> P.Reader.string (P.Reader.body negative))) "decoded";
   let oversized = body (fun w -> P.Writer.int32 w 1_000_000l; P.Writer.raw w "short") in
   check "a length past the end of the data is an error"
     (fails (fun () -> P.Reader.string (P.Reader.body oversized))) "decoded";
   let batch = "\000\000\000\000\000\000\000\000\x7f\xff\xff\xff\000\000\000\000" in
   check "a batch longer than its bytes is an error"
     (fails (fun () -> P.decode_record_batch batch 0)) "decoded")

let () =
  Sys.set_signal Sys.sigpipe Sys.Signal_ignore;
  let host = if Array.length Sys.argv > 1 then Sys.argv.(1) else "127.0.0.1" in
  let port = if Array.length Sys.argv > 2 then Sys.argv.(2) else "9092" in
  let address = if contains host ":" && Array.length Sys.argv <= 2 then host else host ^ ":" ^ port in

  section "connection and metadata";
  (let consumer = must (fun () -> Consumer.create address) in
   let versions =
     try Ok (Connection.api_versions (Router.seed (Consumer.router consumer))) with e -> Error e
   in
   (match versions with
   | Ok (ranges, broker_version) ->
       check "ApiVersions answers" (ranges <> []) "";
       check "broker reports a version" (broker_version <> "") broker_version
   | Error e ->
       check "ApiVersions answers" false (Printexc.to_string e);
       check "broker reports a version" false "");
   let metadata = must (fun () -> Router.metadata ~refresh:true (Consumer.router consumer) []) in
   check "metadata lists brokers"
     (List.length metadata.Router.brokers >= 1)
     (sp "%d brokers" (List.length metadata.Router.brokers));
   Consumer.close consumer);

  section "produce and consume round trip";
  let topic = unique "ocaml-roundtrip" in
  let payloads = List.init 50 (fun i -> sp "record-%d" i) in
  (let producer = must (fun () -> Producer.create ~config:producer_config address) in
   List.iter (fun payload -> must (fun () -> Producer.send producer ~partition:0l topic (Some payload))) payloads;
   must (fun () -> Producer.flush producer);
   must (fun () -> Producer.close producer));
  (let consumer = must (fun () -> Consumer.create address) in
   let got = must (fun () -> Consumer.fetch consumer ~max_wait_ms:500l topic 0l 0L) in
   check "every record comes back" (List.length got = 50) (sp "got %d" (List.length got));
   let identical =
     List.length got = 50
     && List.for_all2
          (fun (r : Consumer.record) (i, payload) -> r.value = Some payload && r.offset = Int64.of_int i)
          got (List.mapi (fun i p -> (i, p)) payloads)
   in
   check "values byte-identical and offsets contiguous" identical "";
   Consumer.close consumer);

  section "compression codecs";
  (* Only none and gzip ship in the driver; lz4/zstd/snappy are opt-in via
     Protocol.register_codec. *)
  List.iter
    (fun codec ->
      let codec_topic = unique ("ocaml-" ^ codec) in
      let body = String.concat "" (List.init 40 (fun _ -> "the same line over and over. ")) in
      let config = { producer_config with compression_type = codec } in
      let producer = must (fun () -> Producer.create ~config address) in
      for i = 0 to 19 do
        must (fun () ->
            Producer.send producer ~partition:0l codec_topic
              (Some (body ^ String.make 1 (Char.chr (Char.code '0' + (i mod 10))))))
      done;
      must (fun () -> Producer.flush producer);
      must (fun () -> Producer.close producer);
      let consumer = must (fun () -> Consumer.create address) in
      let got = must (fun () -> Consumer.fetch consumer ~max_wait_ms:500l codec_topic 0l 0L) in
      let prefixed =
        match got with
        | { Consumer.value = Some v; _ } :: _ ->
            String.length v >= String.length body && String.sub v 0 (String.length body) = body
        | _ -> false
      in
      check (codec ^ ": round trips") (List.length got = 20 && prefixed)
        (sp "got %d records" (List.length got));
      Consumer.close consumer)
    [ "none"; "gzip" ];

  section "keys, partitioning and ordering";
  (let key_topic = unique "ocaml-keys" in
   let producer = must (fun () -> Producer.create ~config:producer_config address) in
   let partitions = must (fun () -> Router.partitions (Producer.router producer) key_topic) in
   for i = 0 to 29 do
     must (fun () -> Producer.send producer ~key:"user-7" key_topic (Some (sp "v%d" i)))
   done;
   must (fun () -> Producer.flush producer);
   must (fun () -> Producer.close producer);
   let target = P.partition_for_key "user-7" partitions in
   let consumer = must (fun () -> Consumer.create address) in
   let on_target = must (fun () -> Consumer.fetch consumer ~max_wait_ms:500l key_topic target 0L) in
   check "a key pins every record to one partition"
     (List.length on_target = 30)
     (sp "partition %ld holds %d of 30" target (List.length on_target));
   let ordered =
     List.length on_target = 30
     && List.for_all2 (fun (r : Consumer.record) i -> r.value = Some (sp "v%d" i)) on_target
          (List.init 30 Fun.id)
   in
   check "per-key order is preserved" ordered "";
   let strays =
     List.fold_left
       (fun n partition ->
         if partition = target then n
         else
           n + List.length (must (fun () -> Consumer.fetch consumer ~max_wait_ms:200l key_topic partition 0L)))
       0 partitions
   in
   check "no keyed record landed elsewhere" (strays = 0) (sp "%d strays" strays);
   Consumer.close consumer);

  section "murmur2 agrees with the broker's partitioner";
  check "murmur2(\"\") is stable" (P.murmur2 "" = 275646681l) (Int32.to_string (P.murmur2 ""));
  check "murmur2 is deterministic" (P.murmur2 "user-7" = P.murmur2 "user-7") "";
  check "different keys hash differently" (P.murmur2 "user-7" <> P.murmur2 "user-8") "";

  section "record headers and timestamps";
  (let header_topic = unique "ocaml-headers" in
   let before = Int64.sub (now_ms ()) 1000L in
   let producer = must (fun () -> Producer.create ~config:producer_config address) in
   must (fun () ->
       Producer.send producer ~partition:0l
         ~headers:
           [
             P.Header.make "trace-id" "abc-123";
             P.Header.make "content-type" "application/json";
             P.Header.null "tombstone-reason";
           ]
         header_topic (Some "annotated"));
   must (fun () -> Producer.send producer ~partition:0l header_topic (Some "plain"));
   must (fun () -> Producer.flush producer);
   must (fun () -> Producer.close producer);
   let after = Int64.add (now_ms ()) 1000L in
   let consumer = must (fun () -> Consumer.create address) in
   let got = must (fun () -> Consumer.fetch consumer ~max_wait_ms:500l header_topic 0l 0L) in
   check "both records arrive" (List.length got = 2) (sp "got %d" (List.length got));
   (match got with
   | [ annotated; plain ] ->
       let n = List.length annotated.headers in
       check "headers survive the round trip" (n = 3) (sp "%d headers" n);
       check "header values are exact" (Consumer.header annotated "trace-id" = Some (Some "abc-123")) "";
       check "a null header value stays null"
         (n = 3 && (List.nth annotated.headers 2).P.Header.value = None)
         "";
       check "a record with no headers gains none from its batch" (plain.headers = [])
         (sp "%d headers" (List.length plain.headers));
       let in_window =
         List.for_all
           (fun (r : Consumer.record) ->
             Int64.compare r.timestamp before >= 0 && Int64.compare r.timestamp after <= 0)
           got
       in
       check "timestamps are real wall-clock values" in_window
         (sp "%Ld,%Ld outside %Ld..%Ld" annotated.timestamp plain.timestamp before after)
   | _ -> ());
   Consumer.close consumer);

  section "tombstones";
  (let tomb_topic = unique "ocaml-tombstones" in
   let producer = must (fun () -> Producer.create ~config:producer_config address) in
   must (fun () -> Producer.send producer ~partition:0l ~key:"k1" tomb_topic (Some "set"));
   must (fun () -> Producer.send producer ~partition:0l ~key:"k2" tomb_topic (Some ""));
   (* A None value is a deletion, and must stay distinguishable from the
      empty value above all the way through the round trip. *)
   must (fun () -> Producer.send producer ~partition:0l ~key:"k3" tomb_topic None);
   must (fun () -> Producer.flush producer);
   must (fun () -> Producer.close producer);
   let consumer = must (fun () -> Consumer.create address) in
   let got = must (fun () -> Consumer.fetch consumer ~max_wait_ms:500l tomb_topic 0l 0L) in
   check "all three records arrive" (List.length got = 3) (sp "got %d" (List.length got));
   (match got with
   | [ a; b; c ] ->
       check "an ordinary value round-trips" (a.value = Some "set") "";
       check "an empty value is empty, not null" (b.value = Some "") (show_opt b.value);
       check "a tombstone arrives as a null value" (c.value = None) (show_opt c.value)
   | _ -> ());
   Consumer.close consumer);

  section "offsets";
  (let consumer = must (fun () -> Consumer.create address) in
   let earliest = must (fun () -> Consumer.list_offsets consumer topic 0l P.earliest) in
   let latest = must (fun () -> Consumer.list_offsets consumer topic 0l P.latest) in
   check "earliest is 0 on a fresh topic" (earliest = 0L) (Int64.to_string earliest);
   check "latest equals the record count" (latest = 50L) (Int64.to_string latest);
   Consumer.close consumer);

  section "acks";
  List.iter
    (fun acks ->
      let acks_topic = unique (sp "ocaml-acks%ld" acks) in
      let producer = must (fun () -> Producer.create ~config:{ producer_config with acks } address) in
      must (fun () -> Producer.send producer ~partition:0l acks_topic (Some "durable"));
      must (fun () -> Producer.flush producer);
      must (fun () -> Producer.close producer);
      sleep_ms 400;
      let consumer = must (fun () -> Consumer.create address) in
      let got = must (fun () -> Consumer.fetch consumer ~max_wait_ms:500l acks_topic 0l 0L) in
      check (sp "acks=%ld stores the record" acks) (List.length got = 1) (sp "got %d" (List.length got));
      Consumer.close consumer)
    [ 0l; 1l; -1l ];

  section "consumer group: assignment, commit, resume";
  (let group_topic = unique "ocaml-group" in
   let group_id = unique "ocaml-billing" in
   let producer = must (fun () -> Producer.create ~config:producer_config address) in
   for i = 0 to 39 do
     must (fun () -> Producer.send producer group_topic (Some (sp "g%d" i)))
   done;
   must (fun () -> Producer.flush producer);
   must (fun () -> Producer.close producer);
   let consumer = must (fun () -> Group.create ~config:group_config address group_id) in
   Group.subscribe consumer [ group_topic ];
   let seen = poll_until consumer ~want:40 ~seconds:30. ~timeout_ms:500 in
   check "the group consumes every record" (List.length seen = 40) (sp "got %d" (List.length seen));
   let distinct =
     List.sort_uniq compare (List.map (fun (r : Consumer.record) -> (r.partition, r.offset)) seen)
   in
   check "no record is delivered twice" (List.length distinct = List.length seen) "";
   must (fun () -> Group.commit consumer);
   let committed = must (fun () -> Group.committed consumer []) in
   let total = List.fold_left (fun n (_, offset) -> Int64.add n offset) 0L committed in
   check "commit records a position" (total = 40L) (Int64.to_string total);
   must (fun () -> Group.close consumer);
   (* A second consumer in the same group must resume, not replay. *)
   let rejoined = must (fun () -> Group.create ~config:group_config address group_id) in
   Group.subscribe rejoined [ group_topic ];
   let replayed = poll_for rejoined ~seconds:5. ~timeout_ms:300 in
   check "a rejoining group resumes from its commit" (replayed = [])
     (sp "replayed %d records it had already committed" (List.length replayed));
   must (fun () -> Group.close rejoined));

  section "auto.offset.reset";
  (let reset_topic = unique "ocaml-reset" in
   let producer = must (fun () -> Producer.create ~config:producer_config address) in
   for i = 0 to 9 do
     must (fun () -> Producer.send producer reset_topic (Some (sp "r%d" i)))
   done;
   must (fun () -> Producer.flush producer);
   must (fun () -> Producer.close producer);
   let consumer =
     must (fun () ->
         Group.create ~config:{ group_config with auto_offset_reset = `Latest } address
           (unique "ocaml-latest"))
   in
   Group.subscribe consumer [ reset_topic ];
   let skipped = poll_for consumer ~seconds:4. ~timeout_ms:300 in
   check "latest skips records produced before the group existed" (skipped = [])
     (sp "saw %d" (List.length skipped));
   must (fun () -> Group.close consumer);
   let strict =
     must (fun () ->
         Group.create ~config:{ group_config with auto_offset_reset = `None } address
           (unique "ocaml-none"))
   in
   Group.subscribe strict [ reset_topic ];
   let deadline = Unix.gettimeofday () +. 5. in
   let rec wait_raise () =
     if Unix.gettimeofday () > deadline then false
     else
       match Group.poll strict ~timeout_ms:300 with
       | _ -> wait_raise ()
       | exception P.No_offset_for_partition _ -> true
       | exception e -> contains (Printexc.to_string e) "no committed offset" || wait_raise ()
   in
   check "none refuses to guess a position" (wait_raise ()) "";
   must (fun () -> Group.close strict));

  section "assignors";
  List.iter
    (fun (assignor : Assignor.strategy) ->
      let name = Assignor.strategy_name assignor in
      let assignor_topic = unique ("ocaml-" ^ name) in
      let producer = must (fun () -> Producer.create ~config:producer_config address) in
      for i = 0 to 19 do
        must (fun () -> Producer.send producer assignor_topic (Some (sp "a%d" i)))
      done;
      must (fun () -> Producer.flush producer);
      must (fun () -> Producer.close producer);
      let consumer =
        must (fun () ->
            Group.create ~config:{ group_config with assignor } address (unique ("ocaml-grp-" ^ name)))
      in
      Group.subscribe consumer [ assignor_topic ];
      let deadline = Unix.gettimeofday () +. 20. in
      let rec collect acc n =
        if n >= 20 || Unix.gettimeofday () > deadline then n
        else
          let records = try Group.poll consumer ~timeout_ms:500 with _ -> [] in
          collect (records :: acc) (n + List.length records)
      in
      let n = collect [] 0 in
      check (name ^ ": consumes every record") (n = 20) (sp "got %d" n);
      must (fun () -> Group.close consumer))
    [ `Range; `Roundrobin; `Sticky ];

  section "bounded client buffer";
  (let buffer_topic = unique "ocaml-buffer" in
   let config =
     { Producer.default_config with linger_ms = 10_000; buffer_memory = 2048; max_block_ms = 300 }
   in
   let producer = must (fun () -> Producer.create ~config address) in
   let rec fill i =
     i < 500
     &&
     match Producer.send producer ~partition:0l buffer_topic (Some (String.make 256 'x')) with
     | () -> fill (i + 1)
     | exception P.Buffer_full msg -> contains msg "buffer full"
     | exception _ -> fill (i + 1)
   in
   check "a full buffer blocks and then reports" (fill 0) "";
   (try Producer.close producer with _ -> ()));

  section "wire edge cases";
  (let edge_topic = unique "ocaml-edge" in
   let producer = must (fun () -> Producer.create ~config:producer_config address) in
   let large = String.init (1 lsl 20) (fun i -> Char.chr ((i * 7) land 0xff)) in
   let unicode_key = "ключ-✓-🔑" and unicode_value = "значение — 数据 — 🚀" in
   must (fun () -> Producer.send producer ~partition:0l edge_topic (Some large));
   must (fun () ->
       Producer.send producer ~partition:0l ~key:unicode_key
         ~headers:[ P.Header.make "ünïcødé-🏷" "✓" ]
         edge_topic (Some unicode_value));
   (* An empty key and an empty header value are values, not nulls. *)
   must (fun () ->
       Producer.send producer ~partition:0l ~key:""
         ~headers:[ P.Header.make "empty" ""; P.Header.null "null" ]
         edge_topic (Some "empty-key"));
   must (fun () -> Producer.send producer ~partition:0l edge_topic (Some "null-key"));
   must (fun () -> Producer.close producer);
   let consumer = must (fun () -> Consumer.create address) in
   let got = fetch_all consumer edge_topic 0l 4 in
   check "edge records all arrive" (List.length got = 4) (sp "got %d" (List.length got));
   (match got with
   | [ r0; r1; r2; r3 ] ->
       check "a 1 MiB value round-trips byte-identical" (r0.value = Some large)
         (sp "%d bytes" (String.length (Option.value r0.value ~default:"")));
       check "unicode key, value and header key round-trip"
         (r1.key = Some unicode_key && r1.value = Some unicode_value
         && List.map (fun (h : P.Header.t) -> h.key) r1.headers = [ "ünïcødé-🏷" ])
         "";
       check "an empty key stays empty, not null" (r2.key = Some "") (show_opt r2.key);
       check "an empty header value stays empty, not null"
         (List.map (fun (h : P.Header.t) -> h.value) r2.headers = [ Some ""; None ])
         (String.concat ", "
            (List.map (fun (h : P.Header.t) -> h.key ^ "=" ^ show_opt h.value) r2.headers));
       check "a null key stays null" (r3.key = None) (show_opt r3.key)
   | _ -> ());
   Consumer.close consumer);

  section "ordering under linger flushes";
  (let order_topic = unique "ocaml-order" in
   let config = { Producer.default_config with linger_ms = 1; batch_size = 256 } in
   let producer = must (fun () -> Producer.create ~config address) in
   let total = 5000 in
   for i = 0 to total - 1 do
     must (fun () -> Producer.send producer ~partition:0l order_topic (Some (string_of_int i)))
   done;
   must (fun () -> Producer.close producer);
   let consumer = must (fun () -> Consumer.create address) in
   let values =
     List.map
       (fun (r : Consumer.record) ->
         Option.value (Option.bind r.value int_of_string_opt) ~default:(-1))
       (fetch_all consumer order_topic 0l total)
   in
   let rec inversions n = function
     | a :: (b :: _ as rest) -> inversions (if b < a then n + 1 else n) rest
     | _ -> n
   in
   let inv = inversions 0 values in
   check "every record of a partition arrives" (List.length values = total)
     (sp "got %d" (List.length values));
   check "a partition's records keep send order" (inv = 0) (sp "%d inversions" inv);
   Consumer.close consumer);

  section "background flush failures are reported";
  (let producer =
     must (fun () -> Producer.create ~config:{ Producer.default_config with linger_ms = 20 } address)
   in
   (* Partition 999 does not exist, so the linger thread's flush fails. *)
   let send_err =
     try
       Producer.send producer ~partition:999l (unique "ocaml-bgfail") (Some "lost");
       None
     with e -> Some e
   in
   sleep_ms 300;
   let flush_err = try Producer.flush producer; None with e -> Some e in
   check "a failed linger flush surfaces on the next Flush"
     (send_err = None && flush_err <> None)
     (sp "send=%s flush=%s" (err_string send_err) (err_string flush_err));
   let closed = Atomic.make false in
   ignore
     (Thread.create
        (fun () ->
          (try Producer.close producer with _ -> ());
          Atomic.set closed true)
        ());
   let deadline = Unix.gettimeofday () +. 5. in
   while (not (Atomic.get closed)) && Unix.gettimeofday () < deadline do
     sleep_ms 10
   done;
   check "Close returns after a failed flush" (Atomic.get closed)
     (if Atomic.get closed then "" else "hung"));

  section "connection failures";
  (* A broker that accepts and never answers must cost an error, not a
     thread blocked forever. *)
  (let silent, silent_address = listen_local () in
   ignore
     (Thread.create
        (fun () ->
          let rec loop () =
            match Unix.accept silent with
            | fd, _ ->
                ignore
                  (Thread.create
                     (fun () ->
                       let buf = Bytes.create 4096 in
                       let rec drain () =
                         match Unix.read fd buf 0 4096 with
                         | 0 -> ()
                         | _ -> drain ()
                         | exception Unix.Unix_error _ -> ()
                       in
                       drain ();
                       try Unix.close fd with Unix.Unix_error _ -> ())
                     ());
                loop ()
            | exception Unix.Unix_error _ -> ()
          in
          loop ())
        ());
   let conn =
     must (fun () -> Connection.dial ~client_id:"ocaml-test" ~dial_timeout_ms:1000 silent_address)
   in
   Connection.set_request_timeout_ms conn 300;
   let started = Unix.gettimeofday () in
   let request_err =
     try
       ignore (Connection.api_versions conn);
       None
     with e -> Some e
   in
   check "a request to an unresponsive broker times out"
     (request_err <> None && Unix.gettimeofday () -. started < 3.)
     (err_string request_err);
   check "a timed-out connection is not reused" (Connection.broken conn) "";
   Connection.close conn;
   shutdown_quietly silent);
  (* A connection the broker drops is redialled, not kept forever. *)
  (let proxy = new_proxy address in
   let drop_topic = unique "ocaml-drop" in
   let producer = must (fun () -> Producer.create ~config:producer_config proxy.address) in
   must (fun () -> Producer.send producer ~partition:0l drop_topic (Some "before"));
   drop_all proxy;
   let rec attempt n last =
     if n >= 3 then last
     else
       match Producer.send producer ~partition:0l drop_topic (Some "after") with
       | () -> None
       | exception e -> attempt (n + 1) (Some e)
   in
   let recovered = attempt 0 (Some (Failure "not attempted")) in
   check "a producer recovers after its connection drops" (recovered = None) (err_string recovered);
   (try Producer.close producer with _ -> ());
   let consumer = must (fun () -> Consumer.create proxy.address) in
   ignore (must (fun () -> Consumer.fetch consumer ~max_wait_ms:100l drop_topic 0l 0L));
   drop_all proxy;
   let rec refetch n last =
     if n >= 3 then Error last
     else
       match Consumer.fetch consumer ~max_wait_ms:100l drop_topic 0l 0L with
       | records -> Ok records
       | exception e -> refetch (n + 1) e
   in
   (match refetch 0 (Failure "not attempted") with
   | Ok fetched ->
       check "a consumer recovers after its connection drops" (List.length fetched >= 1)
         (sp "fetched %d" (List.length fetched))
   | Error e -> check "a consumer recovers after its connection drops" false (Printexc.to_string e));
   Consumer.close consumer;
   close_proxy proxy);

  section "consumer group: max.poll.interval and rejoin";
  (let slow_topic = unique "ocaml-slow" in
   let producer = must (fun () -> Producer.create ~config:producer_config address) in
   for i = 0 to 9 do
     must (fun () -> Producer.send producer slow_topic (Some (sp "s%d" i)))
   done;
   let consumer =
     must (fun () ->
         Group.create ~config:{ group_config with max_poll_interval_ms = 1500 } address
           (unique "ocaml-slow-grp"))
   in
   Group.subscribe consumer [ slow_topic ];
   let poll_n want =
     let deadline = Unix.gettimeofday () +. 15. in
     let rec go n =
       if n >= want || Unix.gettimeofday () > deadline then (n, None)
       else
         match Group.poll consumer ~timeout_ms:300 with
         | records -> go (n + List.length records)
         | exception e -> (n, Some e)
     in
     go 0
   in
   let first, _ = poll_n 10 in
   must (fun () -> Group.commit consumer);
   (* Stall past max.poll.interval.ms: the member leaves the group. *)
   sleep_ms 2500;
   for i = 10 to 19 do
     must (fun () -> Producer.send producer slow_topic (Some (sp "s%d" i)))
   done;
   must (fun () -> Producer.close producer);
   let second, poll_err = poll_n 10 in
   check "a member that stalled rejoins on its next poll"
     (first = 10 && second = 10 && poll_err = None)
     (sp "first=%d second=%d err=%s" first second (err_string poll_err));
   must (fun () -> Group.close consumer));

  section "consumer group: time inside poll does not count against max.poll.interval";
  (let join_topic = unique "ocaml-inpoll" in
   let producer = must (fun () -> Producer.create ~config:producer_config address) in
   ignore (must (fun () -> Router.partitions (Producer.router producer) join_topic));
   (* Far shorter than the first poll below, which spends ~1s joining (the
      broker's initial rebalance delay) and then waits for data. *)
   let consumer =
     must (fun () ->
         Group.create ~config:{ group_config with max_poll_interval_ms = 600 } address
           (unique "ocaml-inpoll-grp"))
   in
   Group.subscribe consumer [ join_topic ];
   let feeder =
     Thread.create
       (fun () ->
         sleep_ms 2000;
         for i = 0 to 9 do
           try Producer.send producer join_topic (Some (sp "j%d" i)) with _ -> ()
         done)
       ()
   in
   (* One long poll: it joins, then waits for the records above. *)
   let got = try Ok (Group.poll consumer ~timeout_ms:4000) with e -> Error e in
   (* Committed straight away, before another poll could quietly rejoin:
      this fails if the member left the group mid-poll. *)
   let commit_err = try Group.commit consumer; None with e -> Some e in
   (match got with
   | Ok records ->
       check "a member is still in its group after a long poll"
         (records <> [] && commit_err = None)
         (sp "got=%d commit=%s" (List.length records) (err_string commit_err))
   | Error e ->
       check "a member is still in its group after a long poll" false
         (sp "poll=%s commit=%s" (Printexc.to_string e) (err_string commit_err)));
   Thread.join feeder;
   must (fun () -> Group.close consumer);
   must (fun () -> Producer.close producer));

  coverage address;
  Printf.printf "\n%d passed, %d failed\n%!" !passed !failed;
  exit (if !failed > 0 then 1 else 0)
