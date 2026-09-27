(* Partition assignment strategies, run by the group leader.

   Pure functions over plain data so they can be tested on their own.
   Partitions are compared as (topic, int32) pairs, never as strings:
   "t-10" sorts before "t-2" as a string, and members that disagree about
   an order compute different assignments. *)

type strategy = [ `Range | `Roundrobin | `Sticky ]

let strategy_name : strategy -> string = function
  | `Range -> "range"
  | `Roundrobin -> "roundrobin"
  | `Sticky -> "sticky"

type slot = string * int32

let compare_slot ((ta, pa) : slot) ((tb, pb) : slot) =
  let c = String.compare ta tb in
  if c <> 0 then c else Int32.compare pa pb

let sort_slots = List.sort compare_slot

(* One group member: its id and the topics it subscribes to. *)
type member = { id : string; topics : string list }

let subscribes m topic = List.mem topic m.topics

(* Mutable per-member lists, kept in insertion order. *)
module Acc = struct
  type t = (string, slot list) Hashtbl.t (* reversed *)

  let create members : t =
    let h = Hashtbl.create 8 in
    List.iter (fun m -> Hashtbl.replace h m.id []) members;
    h

  let add (h : t) id slot =
    Hashtbl.replace h id (slot :: Option.value (Hashtbl.find_opt h id) ~default:[])

  let length (h : t) id = List.length (Option.value (Hashtbl.find_opt h id) ~default:[])

  (* Sorted by member id, each list in insertion order. *)
  let result (h : t) =
    Hashtbl.fold (fun id slots acc -> (id, List.rev slots) :: acc) h []
    |> List.sort (fun (a, _) (b, _) -> String.compare a b)
end

let sorted_topics topic_partitions =
  List.sort (fun (a, _) (b, _) -> String.compare a b) topic_partitions

(* Each subscribed member takes a contiguous range per topic; the first
   (partitions mod members) members take one extra. *)
let range members topic_partitions =
  let acc = Acc.create members in
  List.iter
    (fun (topic, partitions) ->
      let subscribers =
        List.filter (fun m -> subscribes m topic) members
        |> List.map (fun m -> m.id)
        |> List.sort String.compare
      in
      let n = List.length subscribers in
      if n > 0 then begin
        let partitions = Array.of_list partitions in
        let base = Array.length partitions / n and extra = Array.length partitions mod n in
        let cursor = ref 0 in
        List.iteri
          (fun index id ->
            let count = base + if index < extra then 1 else 0 in
            for i = !cursor to !cursor + count - 1 do
              Acc.add acc id (topic, partitions.(i))
            done;
            cursor := !cursor + count)
          subscribers
      end)
    (sorted_topics topic_partitions);
  Acc.result acc

(* Deals every partition around the circle of members sorted by id,
   skipping members not subscribed to the partition's topic. *)
let round_robin members topic_partitions =
  let acc = Acc.create members in
  let circle = Array.of_list (List.sort (fun a b -> String.compare a.id b.id) members) in
  let n = Array.length circle in
  if n > 0 then begin
    let cursor = ref 0 in
    List.iter
      (fun (topic, partitions) ->
        List.iter
          (fun partition ->
            let start = !cursor in
            let rec deal () =
              let m = circle.(!cursor mod n) in
              incr cursor;
              if subscribes m topic then Acc.add acc m.id (topic, partition)
              else if !cursor - start < n then deal ()
            in
            deal ())
          partitions)
      (sorted_topics topic_partitions)
  end;
  Acc.result acc

(* Keeps members on what they already hold and moves only what balance
   requires. Mirrors the Rust (and Go) implementation exactly, because a
   leader running a different algorithm from its predecessor would
   reshuffle the whole group. [previous] maps member id to the partitions
   it held before this rebalance. *)
let sticky members topic_partitions previous =
  let acc = Acc.create members in
  let find_member id = List.find_opt (fun m -> m.id = id) members in
  let member_subscribes id topic =
    match find_member id with Some m -> subscribes m topic | None -> false
  in
  let previous = List.sort (fun (a, _) (b, _) -> String.compare a b) previous in
  let unassigned = ref [] in
  let claimed = ref [] in
  List.iter
    (fun (topic, partitions) ->
      List.iter
        (fun partition ->
          let slot = (topic, partition) in
          let holder =
            List.find_opt
              (fun (id, held) ->
                List.exists (fun s -> compare_slot s slot = 0) held && member_subscribes id topic)
              previous
          in
          match holder with
          | None -> unassigned := slot :: !unassigned
          | Some (id, _) -> claimed := (slot, id) :: !claimed)
        partitions)
    (sorted_topics topic_partitions);
  let eligible =
    List.filter
      (fun m -> List.exists (fun t -> List.mem_assoc t topic_partitions) m.topics)
      members
    |> List.map (fun m -> m.id)
    |> List.sort String.compare
  in
  if eligible <> [] && members <> [] then begin
    let total = List.fold_left (fun n (_, ps) -> n + List.length ps) 0 topic_partitions in
    let n = List.length eligible in
    let base = total / n and extra = total mod n in
    let quota = Hashtbl.create 8 in
    List.iteri (fun i id -> Hashtbl.replace quota id (base + if i < extra then 1 else 0)) eligible;
    let quota_of id = Option.value (Hashtbl.find_opt quota id) ~default:0 in
    let claimed = List.sort (fun (a, _) (b, _) -> compare_slot a b) !claimed in
    List.iter
      (fun (slot, id) ->
        if Hashtbl.mem acc id && Acc.length acc id < quota_of id then Acc.add acc id slot
        else unassigned := slot :: !unassigned)
      claimed;
    List.iter
      (fun ((topic, _) as slot) ->
        let taker =
          match
            List.find_opt (fun id -> member_subscribes id topic && Acc.length acc id < quota_of id) eligible
          with
          | Some id -> Some id
          | None ->
              (* Quotas exhausted (possible with uneven subscriptions): an
                 unassigned partition is a stalled partition, so fall back to
                 any subscribed member rather than dropping it. *)
              List.find_opt (fun id -> member_subscribes id topic) eligible
        in
        Option.iter (fun id -> Acc.add acc id slot) taker)
      (sort_slots !unassigned)
  end;
  List.map (fun (id, slots) -> (id, sort_slots slots)) (Acc.result acc)

let assign (strategy : strategy) members topic_partitions previous =
  match strategy with
  | `Range -> range members topic_partitions
  | `Roundrobin -> round_robin members topic_partitions
  | `Sticky -> sticky members topic_partitions previous
