(* A group consumer shares its subscribed topics' partitions with the rest
   of its group: join/sync/heartbeat with generation fencing, committed
   offsets, auto.offset.reset, auto commit, static membership and an
   explicit LeaveGroup on close.

   Single-threaded by design, as Kafka's consumer is: use one per thread.
   A background heartbeat thread keeps the membership alive and enforces
   max.poll.interval.ms. *)

open Protocol

(* The internal topic whose partition leaders coordinate groups. *)
let offsets_topic = "__consumer_offsets"

let coordinator_attempts = 4
let join_attempts = 4

(* Where to start when a partition has no valid position. [`None] refuses
   to guess and raises [Protocol.No_offset_for_partition]. *)
type auto_offset_reset = [ `Earliest | `Latest | `None ]

type config = {
  client_id : string;
  (* session.timeout.ms: the coordinator evicts a member that stops
     heartbeating for this long. *)
  session_timeout_ms : int32;
  (* heartbeat.interval.ms: how often this member heartbeats, and so how
     soon it notices a rebalance; 0 means a third of the session timeout. *)
  heartbeat_interval_ms : int;
  (* How long the coordinator waits for members to rejoin. *)
  rebalance_timeout_ms : int32;
  (* max.poll.interval.ms: the longest gap between polls before this member
     is presumed stuck and leaves. Time spent inside [poll] never counts. *)
  max_poll_interval_ms : int;
  (* auto.commit.interval.ms; 0 disables auto commit. *)
  auto_commit_interval_ms : int;
  auto_offset_reset : auto_offset_reset;
  (* partition.assignment.strategy *)
  assignor : Assignor.strategy;
  (* group.instance.id: a stable identity across restarts (static
     membership); empty for a dynamic member. *)
  group_instance_id : string;
  max_poll_records : int;
  fetch_max_bytes : int32;
  dial_timeout_ms : int;
  socket_timeout_ms : int;
}

let default_config =
  {
    client_id = Connection.default_client_id;
    session_timeout_ms = 10_000l;
    heartbeat_interval_ms = 0;
    rebalance_timeout_ms = 3_000l;
    max_poll_interval_ms = 300_000;
    auto_commit_interval_ms = 5_000;
    auto_offset_reset = `Earliest;
    assignor = `Range;
    group_instance_id = "";
    max_poll_records = 500;
    fetch_max_bytes = 8_388_608l;
    dial_timeout_ms = Connection.default_dial_timeout_ms;
    socket_timeout_ms = Connection.default_request_timeout_ms;
  }

type slot = Assignor.slot

type t = {
  group_id : string;
  config : config;
  consumer : Consumer.t;
  mutable subscribed : string list;
  mutable assignment : slot list;
  (* The next offset to deliver: what gets committed. It only advances over
     records handed to the caller. *)
  positions : (slot, int64) Hashtbl.t;
  (* The next offset to fetch; runs ahead of [positions] by exactly the
     records sitting in [buffered]. *)
  mutable fetch_positions : (slot, int64) Hashtbl.t;
  mutable buffered : Consumer.record list;
  mutable last_commit_ms : int64;
  (* Shared with the heartbeat thread; every access goes through [mu]. *)
  mu : Mutex.t;
  mutable member_id : string;
  mutable generation : int32;
  mutable joined : bool;
  mutable last_poll_ms : int64;
  (* True while [poll] runs: max.poll.interval.ms bounds the gap between
     polls, so a poll that is itself busy (joining a slow rebalance,
     waiting for data) must not count against it. *)
  mutable in_poll : bool;
  mutable closed : bool;
  heartbeat_done : bool Atomic.t;
}

let with_lock = Connection.with_lock

let membership t = with_lock t.mu (fun () -> (t.member_id, t.generation, t.joined))
let set_joined t joined = with_lock t.mu (fun () -> t.joined <- joined)
let clear_member_id t = with_lock t.mu (fun () -> t.member_id <- "")
let consumer t = t.consumer
let assignment t = t.assignment

(* ------------------------------------------------------------------------ *)
(* Coordinator routing                                                       *)
(* ------------------------------------------------------------------------ *)

let coordinator_partition t =
  let n = List.length (Consumer.partitions t.consumer offsets_topic) in
  let crc = Int64.logand (Int64.of_int32 (crc32c t.group_id)) 0xFFFFFFFFL in
  Int64.to_int32 (Int64.rem crc (Int64.of_int n))

(* Sends to the group's coordinator, following moves and waiting out
   loads. Every group response starts with an error code, which is what
   makes this generic wrapper possible. *)
let coordinator_request t api_key body =
  let router = Consumer.router t.consumer in
  let rec attempt n =
    if n >= coordinator_attempts then
      raise
        (Connection_error
           (Printf.sprintf "group coordinator unavailable after %d attempts" coordinator_attempts))
    else
      let conn = Router.conn_for router offsets_topic (coordinator_partition t) in
      let response = Connection.request conn api_key body in
      let code = Reader.peek_error_code response in
      if code = err_coordinator_load_in_progress then begin
        Thread.delay 0.1;
        attempt (n + 1)
      end
      else if code = err_not_coordinator || code = err_not_leader_or_follower then begin
        ignore (Router.refresh router offsets_topic : Router.metadata);
        attempt (n + 1)
      end
      else Reader.body response
  in
  attempt 0

let expect_ok r context =
  let code = Reader.int32 r in
  if code <> err_none then raise (server_error code context)

(* ------------------------------------------------------------------------ *)
(* Offsets                                                                   *)
(* ------------------------------------------------------------------------ *)

(* Records the delivered positions. At-least-once: call it after
   processing, not before. *)
let commit t =
  let slots =
    Hashtbl.fold (fun slot offset acc -> (slot, offset) :: acc) t.positions []
    |> List.sort (fun (a, _) (b, _) -> Assignor.compare_slot a b)
  in
  if slots <> [] then begin
    let member_id, generation, _ = membership t in
    let w = Writer.body () in
    Writer.string w t.group_id;
    Writer.int32 w generation;
    Writer.string w member_id;
    Writer.int32 w (Int32.of_int (List.length slots));
    List.iter
      (fun ((topic, partition), offset) ->
        Writer.string w topic;
        Writer.int32 w partition;
        Writer.int64 w offset)
      slots;
    expect_ok (coordinator_request t api_offset_commit (Writer.contents w)) "offset_commit";
    t.last_commit_ms <- now_ms ()
  end

(* The group's committed offsets. An empty list asks for every partition
   the group holds. *)
let committed t (partitions : slot list) =
  let w = Writer.body () in
  Writer.string w t.group_id;
  Writer.int32 w (Int32.of_int (List.length partitions));
  List.iter
    (fun (topic, partition) ->
      Writer.string w topic;
      Writer.int32 w partition)
    partitions;
  let r = coordinator_request t api_offset_fetch (Writer.contents w) in
  expect_ok r "offset_fetch";
  Reader.list r (fun r ->
      let topic = Reader.string r in
      let partition = Reader.int32 r in
      let offset = Reader.int64 r in
      ((topic, partition), offset))

let reset_offset t topic partition =
  match t.config.auto_offset_reset with
  | `Earliest -> Consumer.list_offsets t.consumer topic partition earliest
  | `Latest -> Consumer.list_offsets t.consumer topic partition latest
  | `None -> raise (No_offset_for_partition { topic; partition })

let maybe_auto_commit t =
  let interval = t.config.auto_commit_interval_ms in
  if interval > 0 && Hashtbl.length t.positions > 0
     && Int64.compare (Int64.sub (now_ms ()) t.last_commit_ms) (Int64.of_int interval) >= 0
  then
    (* A failed auto-commit is retried next poll; an explicit [commit] is
       what a caller relies on. *)
    try commit t with Server_error _ | Connection_error _ | Decode_error _ -> ()

(* ------------------------------------------------------------------------ *)
(* Membership                                                                *)
(* ------------------------------------------------------------------------ *)

let apply_assignment t assignment =
  t.assignment <- assignment;
  let owned slot = List.exists (fun s -> Assignor.compare_slot s slot = 0) assignment in
  let stale = Hashtbl.fold (fun slot _ acc -> if owned slot then acc else slot :: acc) t.positions [] in
  List.iter (Hashtbl.remove t.positions) stale;
  (* Buffered records sit ahead of the consumed position and were never
     delivered, so a new assignment simply drops them. *)
  t.buffered <- [];
  let needed = List.filter (fun slot -> not (Hashtbl.mem t.positions slot)) assignment in
  if needed <> [] then begin
    let found = committed t needed in
    List.iter
      (fun ((topic, partition) as slot) ->
        let offset =
          match List.assoc_opt slot found with
          | Some o when Int64.compare o 0L >= 0 -> o
          | _ -> reset_offset t topic partition
        in
        Hashtbl.replace t.positions slot offset)
      needed
  end;
  t.fetch_positions <- Hashtbl.copy t.positions

let sync t assignments =
  let member_id, generation, _ = membership t in
  let w = Writer.body () in
  Writer.string w t.group_id;
  Writer.int32 w generation;
  Writer.string w member_id;
  Writer.int32 w (Int32.of_int (List.length assignments));
  List.iter
    (fun (id, slots) ->
      Writer.string w id;
      Writer.int32 w (Int32.of_int (List.length slots));
      List.iter
        (fun (topic, partition) ->
          Writer.string w topic;
          Writer.int32 w partition)
        slots)
    assignments;
  let r = coordinator_request t api_sync_group (Writer.contents w) in
  let code = Reader.int32 r in
  if code = err_rebalance_in_progress || code = err_illegal_generation then false
  else if code = err_unknown_member_id then begin
    (* Evicted while syncing: rejoin as a new member. *)
    clear_member_id t;
    false
  end
  else if code <> err_none then raise (server_error code "sync_group")
  else begin
    let assignment =
      Reader.list r (fun r ->
          let topic = Reader.string r in
          let partition = Reader.int32 r in
          (topic, partition))
    in
    apply_assignment t assignment;
    true
  end

let join t =
  let rec attempt n =
    if n >= join_attempts then
      failwith
        (Printf.sprintf "consumer group failed to stabilise after %d join attempts" join_attempts)
    else begin
      let current_member_id, _, _ = membership t in
      let w = Writer.body () in
      Writer.string w t.group_id;
      Writer.int32 w t.config.session_timeout_ms;
      Writer.int32 w t.config.rebalance_timeout_ms;
      Writer.string w current_member_id;
      Writer.string_array w t.subscribed;
      Writer.string w t.config.group_instance_id;
      let r = coordinator_request t api_join_group (Writer.contents w) in
      let code = Reader.int32 r in
      if code = err_rebalance_in_progress then begin
        Thread.delay 0.1;
        attempt (n + 1)
      end
      else if code = err_unknown_member_id then begin
        (* The coordinator dropped this member (session expiry, or removed
           while it waited): join again as a new one. *)
        clear_member_id t;
        attempt (n + 1)
      end
      else if code <> err_none then raise (server_error code "join_group")
      else begin
        let generation = Reader.int32 r in
        let member_id = Reader.string r in
        let leader_id = Reader.string r in
        let members =
          Reader.list r (fun r ->
              let id = Reader.string r in
              let topics = Reader.string_array r in
              let held =
                Reader.list r (fun r ->
                    let topic = Reader.string r in
                    let partition = Reader.int32 r in
                    (topic, partition))
              in
              ({ Assignor.id; topics }, held))
        in
        with_lock t.mu (fun () ->
            t.member_id <- member_id;
            t.generation <- generation);
        let assignments =
          if member_id <> leader_id then []
          else begin
            let topics =
              List.concat_map (fun ((m : Assignor.member), _) -> m.Assignor.topics) members
              |> List.sort_uniq String.compare
            in
            let topic_partitions =
              List.map (fun topic -> (topic, Consumer.partitions t.consumer topic)) topics
            in
            let previous = List.map (fun ((m : Assignor.member), held) -> (m.Assignor.id, held)) members in
            Assignor.assign t.config.assignor (List.map fst members) topic_partitions previous
          end
        in
        if sync t assignments then set_joined t true else attempt (n + 1)
      end
    end
  in
  attempt 0

let leave t =
  let member_id, _, _ = membership t in
  let w = Writer.body () in
  Writer.string w t.group_id;
  Writer.string w member_id;
  expect_ok (coordinator_request t api_leave_group (Writer.contents w)) "leave_group";
  set_joined t false

let heartbeat_loop t =
  (* Two independent deadlines, so wake often enough for the shorter. *)
  let heartbeat_every =
    if t.config.heartbeat_interval_ms > 0 then t.config.heartbeat_interval_ms
    else max 1 (Int32.to_int t.config.session_timeout_ms / 3)
  in
  let poll_check_every = max 1 (t.config.max_poll_interval_ms / 3) in
  let interval = float_of_int (min heartbeat_every poll_check_every) /. 1000. in
  let is_closed () = with_lock t.mu (fun () -> t.closed) in
  let rec sleep remaining =
    if remaining > 0. && not (is_closed ()) then begin
      let step = Float.min remaining 0.05 in
      Thread.delay step;
      sleep (remaining -. step)
    end
  in
  let rec loop left_for_slow_poll =
    sleep interval;
    let closed, idle_ms, in_poll =
      with_lock t.mu (fun () -> (t.closed, Int64.sub (now_ms ()) t.last_poll_ms, t.in_poll))
    in
    if not closed then begin
      let member_id, generation, joined = membership t in
      if (not joined) || member_id = "" then loop left_for_slow_poll
      else if (not in_poll)
              && Int64.compare idle_ms (Int64.of_int t.config.max_poll_interval_ms) >= 0
      then begin
        (* The application stopped consuming although the process is
           alive. Heartbeating on would hold its partitions away from a
           consumer that could make progress. *)
        if not left_for_slow_poll then begin
          (try leave t with _ -> ());
          set_joined t false
        end;
        loop true
      end
      else begin
        let w = Writer.body () in
        Writer.string w t.group_id;
        Writer.int32 w generation;
        Writer.string w member_id;
        (match coordinator_request t api_heartbeat (Writer.contents w) with
        | r ->
            let code = try Reader.int32 r with Decode_error _ -> err_none in
            if code = err_rebalance_in_progress || code = err_unknown_member_id
               || code = err_illegal_generation
            then
              (* Only if nothing changed since the snapshot: an answer for
                 an old generation arriving after the member already
                 rejoined must not send it round again. *)
              with_lock t.mu (fun () ->
                  if t.generation = generation && t.member_id = member_id then t.joined <- false)
        | exception _ -> () (* transient: retry next tick *));
        loop false
      end
    end
  in
  Fun.protect ~finally:(fun () -> Atomic.set t.heartbeat_done true) (fun () -> loop false)

(* ------------------------------------------------------------------------ *)
(* Public API                                                                *)
(* ------------------------------------------------------------------------ *)

(* Connects and starts heartbeating. *)
let create ?(config = default_config) address group_id =
  let consumer =
    Consumer.create
      ~config:
        {
          Consumer.default_config with
          Consumer.client_id = config.client_id;
          fetch_max_bytes = config.fetch_max_bytes;
          max_poll_records = config.max_poll_records;
          dial_timeout_ms = config.dial_timeout_ms;
          socket_timeout_ms = config.socket_timeout_ms;
        }
      address
  in
  let t =
    {
      group_id;
      config;
      consumer;
      subscribed = [];
      assignment = [];
      positions = Hashtbl.create 16;
      fetch_positions = Hashtbl.create 16;
      buffered = [];
      last_commit_ms = now_ms ();
      mu = Mutex.create ();
      member_id = "";
      generation = -1l;
      joined = false;
      last_poll_ms = now_ms ();
      in_poll = false;
      closed = false;
      heartbeat_done = Atomic.make false;
    }
  in
  ignore (Thread.create heartbeat_loop t : Thread.t);
  t

(* Sets the topics this member wants a share of; takes effect (with a
   rebalance) on the next poll. *)
let subscribe t topics =
  t.subscribed <- topics;
  set_joined t false

let take_buffered t =
  let limit = if t.config.max_poll_records <= 0 then max_int else t.config.max_poll_records in
  let rec split n acc rest =
    match rest with
    | r :: tail when n < limit -> split (n + 1) (r :: acc) tail
    | _ -> (List.rev acc, rest)
  in
  let delivered, rest = split 0 [] t.buffered in
  t.buffered <- rest;
  (* The consumed position advances only over records actually handed to
     the caller; committing what was merely fetched would skip records
     nobody processed. *)
  List.iter
    (fun (r : Consumer.record) ->
      Hashtbl.replace t.positions (r.Consumer.topic, r.Consumer.partition) (Int64.succ r.Consumer.offset))
    delivered;
  delivered

(* Returns up to max.poll.records records, joining the group if needed and
   waiting up to [timeout_ms] for data. *)
let poll t ~timeout_ms =
  if t.subscribed = [] then invalid_arg "subscribe to at least one topic before polling";
  (* Stamped on entry and again on return, and never enforced in between. *)
  let stamp in_poll =
    with_lock t.mu (fun () ->
        t.last_poll_ms <- now_ms ();
        t.in_poll <- in_poll)
  in
  stamp true;
  Fun.protect ~finally:(fun () -> stamp false) (fun () ->
      let deadline = Unix.gettimeofday () +. (float_of_int timeout_ms /. 1000.) in
      let rec sweep () =
        (* Checked every sweep: a rebalance the heartbeat learns of mid-poll
           must stop this member fetching partitions it may no longer own. *)
        let _, _, joined = membership t in
        if not joined then join t;
        if t.buffered <> [] then take_buffered t
        else if t.assignment = [] then
          if Unix.gettimeofday () > deadline then []
          else begin
            Thread.delay 0.05;
            sweep ()
          end
        else begin
          let got_any = ref false in
          List.iter
            (fun ((topic, partition) as slot) ->
              let remaining = Float.max 0. (deadline -. Unix.gettimeofday ()) in
              let wait_ms = Int32.of_int (min 500 (int_of_float (remaining *. 1000.))) in
              let offset =
                Option.value (Hashtbl.find_opt t.fetch_positions slot) ~default:0L
              in
              match Consumer.fetch t.consumer ~max_wait_ms:wait_ms topic partition offset with
              | records ->
                  if records <> [] then begin
                    got_any := true;
                    let last = List.nth records (List.length records - 1) in
                    Hashtbl.replace t.fetch_positions slot (Int64.succ last.Consumer.offset);
                    t.buffered <- t.buffered @ records
                  end
              | exception Server_error { code; _ } when code = err_offset_out_of_range ->
                  (* The position fell off the log; restart where the policy
                     says. *)
                  let reset = reset_offset t topic partition in
                  Hashtbl.replace t.fetch_positions slot reset;
                  Hashtbl.replace t.positions slot reset
              | exception Server_error { code; _ } when code = err_not_leader_or_follower ->
                  ignore (Router.refresh (Consumer.router t.consumer) topic : Router.metadata))
            t.assignment;
          maybe_auto_commit t;
          if t.buffered <> [] then take_buffered t
          else if (not !got_any) && Unix.gettimeofday () > deadline then []
          else sweep ()
        end
      in
      sweep ())

(* Commits, leaves the group, then stops. Leaving is what separates a clean
   shutdown from a crash: without it the coordinator waits out the session
   timeout before reassigning. *)
let close t =
  with_lock t.mu (fun () -> t.closed <- true);
  let member_id, _, joined = membership t in
  (* Best effort, as the leave below: a commit refused mid-rebalance must
     not keep the consumer from shutting down. *)
  if joined then (try commit t with _ -> ());
  (* Best effort: failing here costs only the session timeout. *)
  if member_id <> "" then (try leave t with _ -> ());
  let deadline = Unix.gettimeofday () +. 2.0 in
  while (not (Atomic.get t.heartbeat_done)) && Unix.gettimeofday () < deadline do
    Thread.delay 0.005
  done;
  Consumer.close t.consumer
