(* Metadata and leader routing.

   The router keeps one connection per broker and routes each request to
   its partition's leader. Metadata is cached and refreshed only when a
   request says the route was stale, because refreshing per request would
   put the control plane on the data path.

   A connection that failed is replaced on its next use rather than kept:
   without that, one dropped socket (a broker restart, an idle timeout on a
   load balancer) would fail every later request for the life of the
   client. That includes the seed connection. *)

open Protocol

type broker = {
  node_id : int32;
  host : string;
  port : int32;
  (* Failure domain, empty when the broker was started without --rack. *)
  rack : string;
}

type partition_info = {
  partition : int32;
  leader : int32;
  replicas : int32 list;
  isr : int32 list;
  leader_epoch : int32;
}

type topic_info = { name : string; partitions : partition_info list }
type metadata = { brokers : broker list; topics : topic_info list }

(* A topic's partition ids, ascending. *)
let partitions_of (m : metadata) topic =
  match List.find_opt (fun t -> t.name = topic) m.topics with
  | None -> []
  | Some t -> List.sort Int32.compare (List.map (fun p -> p.partition) t.partitions)

(* The broker id leading a partition, or -1. *)
let leader_of (m : metadata) topic partition =
  match List.find_opt (fun t -> t.name = topic) m.topics with
  | None -> -1l
  | Some t -> (
      match List.find_opt (fun p -> p.partition = partition) t.partitions with
      | None -> -1l
      | Some p -> p.leader)

(* Field order is exactly the schema's: error_code, brokers,
   controller_id, topics. The leading code is request-level (an
   authorization denial, say), distinct from the per-topic one. *)
let decode_metadata r =
  let code = Reader.int32 r in
  if code <> err_none then raise (server_error code "metadata");
  let brokers =
    Reader.list r (fun r ->
        let node_id = Reader.int32 r in
        let host = Reader.string r in
        let port = Reader.int32 r in
        let rack = Reader.string r in
        { node_id; host; port; rack })
  in
  ignore (Reader.int32 r : int32) (* controller_id *);
  let topics =
    Reader.list r (fun r ->
        let name = Reader.string r in
        let topic_error = Reader.int32 r in
        let partitions =
          Reader.list r (fun r ->
              let partition = Reader.int32 r in
              let leader = Reader.int32 r in
              let replicas = Reader.list r Reader.int32 in
              let isr = Reader.list r Reader.int32 in
              let leader_epoch = Reader.int32 r in
              { partition; leader; replicas; isr; leader_epoch })
        in
        if topic_error <> err_none && topic_error <> err_unknown_topic_or_partition then
          raise (server_error topic_error ("metadata for " ^ name));
        { name; partitions })
  in
  { brokers; topics }

type t = {
  client_id : string;
  dial_timeout_ms : int;
  request_timeout_ms : int;
  seed_address : string;
  mu : Mutex.t;
  mutable seed : Connection.t;
  conns : (int32, Connection.t) Hashtbl.t;
  mutable cached : metadata option;
}

let with_lock = Connection.with_lock

let create ?(client_id = Connection.default_client_id)
    ?(dial_timeout_ms = Connection.default_dial_timeout_ms)
    ?(request_timeout_ms = Connection.default_request_timeout_ms) address =
  let dial () = Connection.dial ~client_id ~dial_timeout_ms ~request_timeout_ms address in
  {
    client_id;
    dial_timeout_ms;
    request_timeout_ms;
    seed_address = address;
    mu = Mutex.create ();
    seed = dial ();
    conns = Hashtbl.create 8;
    cached = None;
  }

let dial t address =
  Connection.dial ~client_id:t.client_id ~dial_timeout_ms:t.dial_timeout_ms
    ~request_timeout_ms:t.request_timeout_ms address

let close t =
  with_lock t.mu (fun () ->
      Hashtbl.iter (fun _ c -> if c != t.seed then Connection.close c) t.conns;
      Hashtbl.reset t.conns;
      Connection.close t.seed)

(* The seed connection, redialled if it broke. Called with [t.mu] held. *)
let live_seed_locked t =
  if not (Connection.broken t.seed) then t.seed
  else begin
    let conn = dial t t.seed_address in
    let old = t.seed in
    t.seed <- conn;
    let stale = Hashtbl.fold (fun id c acc -> if c == old then id :: acc else acc) t.conns [] in
    List.iter (fun id -> Hashtbl.replace t.conns id conn) stale;
    conn
  end

(* The connection this router was opened with, redialled if it failed. *)
let seed t = with_lock t.mu (fun () -> try live_seed_locked t with Connection_error _ -> t.seed)

(* Cluster metadata for [topics] (all topics when empty). Served from the
   cache unless [refresh]. *)
let metadata ?(refresh = false) t topics =
  with_lock t.mu (fun () ->
      match t.cached with
      | Some m when not refresh -> m
      | _ ->
          let seed = live_seed_locked t in
          let w = Writer.body () in
          Writer.string_array w topics;
          let m = decode_metadata (Reader.body (Connection.request seed api_metadata (Writer.contents w))) in
          t.cached <- Some m;
          m)

let refresh t topic = metadata ~refresh:true t [ topic ]

(* A topic's partitions. A topic auto-created on first reference is not in
   the cached image yet; one refresh distinguishes "new" from "absent". *)
let partitions t topic =
  let ps = partitions_of (metadata t [ topic ]) topic in
  let ps = if ps = [] then partitions_of (refresh t topic) topic else ps in
  if ps = [] then raise
      (server_error err_unknown_topic_or_partition (Printf.sprintf "topic %S has no partitions" topic));
  ps

(* The connection to a partition's leader. *)
let conn_for t topic partition =
  let m = metadata t [ topic ] in
  let m, leader =
    let leader = leader_of m topic partition in
    if leader >= 0l then (m, leader)
    else
      let m = refresh t topic in
      (m, leader_of m topic partition)
  in
  if leader < 0l then
    raise (Server_error { code = err_unknown_topic_or_partition;
                          context = Printf.sprintf "no leader for %s-%ld" topic partition });
  with_lock t.mu (fun () ->
      match Hashtbl.find_opt t.conns leader with
      | Some c when not (Connection.broken c) -> c
      | existing -> (
          (match existing with
          | Some c ->
              Hashtbl.remove t.conns leader;
              if c != t.seed then Connection.close c
          | None -> ());
          match List.find_opt (fun b -> b.node_id = leader) m.brokers with
          | None -> invalid_arg (Printf.sprintf "broker %ld is not in the metadata" leader)
          | Some b ->
              (* A single-broker cluster advertises the address it was
                 configured with, which may not be the one we dialled; reuse
                 the seed rather than opening a second connection. *)
              let c =
                if List.length m.brokers = 1 then live_seed_locked t
                else dial t (Printf.sprintf "%s:%ld" b.host b.port)
              in
              Hashtbl.replace t.conns leader c;
              c))
