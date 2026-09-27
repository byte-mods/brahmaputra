(* Cross-language conformance test for the BitPacker OCaml target.
   Run through run.sh, which generates the modules and compiles this file. *)

let pass = ref 0
let fail = ref 0

let check name ok detail =
  if ok then (incr pass; Printf.printf "  ok   %s\n" name)
  else (incr fail; Printf.printf "  FAIL %s (%s)\n" name detail)

let eq name got want show = check name (got = want) (Printf.sprintf "got %s, want %s" (show got) (show want))
let eqp name got want = check name (got = want) "values differ"

let read_file path =
  let ic = open_in_bin path in
  let n = in_channel_length ic in
  let s = really_input_string ic n in
  close_in ic;
  s

let write_file path s =
  let oc = open_out_bin path in
  output_string oc s;
  close_out oc

let is_error = function Error _ -> true | Ok _ -> false

let rec uv n = if n < 128 then String.make 1 (Char.chr n)
  else String.make 1 (Char.chr (n land 127 lor 128)) ^ uv (n lsr 7)

module B = Bench_complex
module E = Edge

let world : B.world_state =
  let sword = { B.Item.id = 1l; name = "Excalibur"; value = 9999l; weight = 15l; rarity = "Legendary" } in
  let potion = { B.Item.id = 2l; name = "HealthPotion"; value = 50l; weight = 1l; rarity = "Common" } in
  let hero = {
    B.Character.name = "TestHero"; level = 99l; hp = 1000l; mp = 500l; is_alive = true;
    position = { B.Vec3.x = 10l; y = -20l; z = 30l };
    skills = [ 1l; 2l; 3l; 100l ]; inventory = [ sword ] } in
  let guild = { B.Guild.name = "TestGuild"; description = "A test guild for cross-language"; members = [ hero ] } in
  { B.WorldState.world_id = 42l; seed = "cross_lang_test"; guilds = [ guild ]; loot_table = [ potion ] }

let canonical : E.edge = {
  E.Edge.i_min = Int32.min_int; i_max = Int32.max_int; i_zero = 0l; i_neg = -1l;
  l_min = Int64.min_int; l_max = Int64.max_int; l_neg = -300L;
  f = -1.25; d = 1234.5625; d_neg = -0.5;
  yes = true; no = false;
  empty = ""; unicode = "h\u{e9}llo w\u{f6}rld \u{2713} \u{65e5}\u{672c} \u{1f680}";
  ints = [ 0l; -1l; 1l; -64l; 64l; Int32.min_int; Int32.max_int ];
  longs = [ 0L; -1L; Int64.max_int; Int64.min_int; 4294967296L ];
  floats = [ 0.0; 0.5; -2.25 ];
  doubles = [ 0.0; 3.5; -1000000.25 ];
  bools = [ true; false; true ];
  strings = [ ""; "a"; "\u{65e5}\u{672c}\u{8a9e}" ];
  no_ints = [];
  inner = { E.Inner.big = 1099511627776L; label = "inner" };
  inners = [ { E.Inner.big = -1L; label = "" }; { E.Inner.big = 0L; label = "x" } ];
  no_inners = [];
}

let () =
  let here = Sys.argv.(1) in
  let parent = Filename.dirname here in
  (try
    (* ---- bench ---- *)
    let ref_ = read_file (Filename.concat parent "test_data.bin") in
    let enc = B.WorldState.encode world in
    write_file (Filename.concat parent "test_data_ocaml.bin") enc;
    check "bench: encode == test_data.bin" (enc = ref_)
      (Printf.sprintf "%d vs %d bytes" (String.length enc) (String.length ref_));
    (match B.WorldState.decode ref_ with
     | Error e -> check "bench: decode test_data.bin" false e
     | Ok w ->
       check "bench: decode test_data.bin" true "";
       let i32 = Int32.to_string and str s = Printf.sprintf "%S" s and int = string_of_int in
       eq "bench: world_id" w.world_id 42l i32;
       eq "bench: seed" w.seed "cross_lang_test" str;
       eq "bench: guilds length" (List.length w.guilds) 1 int;
       let g = List.hd w.guilds in
       eq "bench: guild name" g.name "TestGuild" str;
       eq "bench: guild description" g.description "A test guild for cross-language" str;
       eq "bench: members length" (List.length g.members) 1 int;
       let h = List.hd g.members in
       eq "bench: hero name" h.name "TestHero" str;
       eq "bench: hero level" h.level 99l i32;
       eq "bench: hero hp" h.hp 1000l i32;
       eq "bench: hero mp" h.mp 500l i32;
       eq "bench: hero is_alive" h.is_alive true string_of_bool;
       eqp "bench: position" h.position { B.Vec3.x = 10l; y = -20l; z = 30l };
       eqp "bench: skills" h.skills [ 1l; 2l; 3l; 100l ];
       eq "bench: inventory length" (List.length h.inventory) 1 int;
       let s = List.hd h.inventory in
       eq "bench: sword name" s.name "Excalibur" str;
       eq "bench: sword value" s.value 9999l i32;
       eq "bench: sword rarity" s.rarity "Legendary" str;
       eq "bench: loot length" (List.length w.loot_table) 1 int;
       let p = List.hd w.loot_table in
       eq "bench: potion name" p.name "HealthPotion" str;
       eq "bench: potion rarity" p.rarity "Common" str;
       check "bench: re-encode decoded == test_data.bin" (B.WorldState.encode w = ref_) "");
    check "bench: round-trip" (B.WorldState.decode enc = Ok world) "";

    (* ---- edge ---- *)
    let eref = read_file (Filename.concat (Filename.concat parent "edge") "edge_ref.bin") in
    let eenc = E.Edge.encode canonical in
    check "edge: encode == edge_ref.bin" (eenc = eref)
      (Printf.sprintf "%d vs %d bytes" (String.length eenc) (String.length eref));
    (match E.Edge.decode eref with
     | Error e -> check "edge: decode edge_ref.bin" false e
     | Ok d ->
       check "edge: decode edge_ref.bin" true "";
       let c = canonical in
       let f name ok = check ("edge: field " ^ name) ok "differs" in
       f "i_min" (d.i_min = c.i_min); f "i_max" (d.i_max = c.i_max);
       f "i_zero" (d.i_zero = c.i_zero); f "i_neg" (d.i_neg = c.i_neg);
       f "l_min" (d.l_min = c.l_min); f "l_max" (d.l_max = c.l_max); f "l_neg" (d.l_neg = c.l_neg);
       f "f" (d.f = c.f); f "d" (d.d = c.d); f "d_neg" (d.d_neg = c.d_neg);
       f "yes" (d.yes = c.yes); f "no" (d.no = c.no);
       f "empty" (d.empty = c.empty); f "unicode" (d.unicode = c.unicode);
       f "ints" (d.ints = c.ints); f "longs" (d.longs = c.longs);
       f "floats" (d.floats = c.floats); f "doubles" (d.doubles = c.doubles);
       f "bools" (d.bools = c.bools); f "strings" (d.strings = c.strings);
       f "no_ints" (d.no_ints = c.no_ints); f "inner" (d.inner = c.inner);
       f "inners" (d.inners = c.inners); f "no_inners" (d.no_inners = c.no_inners);
       check "edge: decoded == canonical" (d = c) "";
       check "edge: re-encode decoded == edge_ref.bin" (E.Edge.encode d = eref) "");
    let bad = Bytes.of_string eref in
    Bytes.set bad 5 (Char.chr (Char.code (Bytes.get bad 5) lxor 1));
    (match E.Edge.decode (Bytes.to_string bad) with
     | Error e -> check "edge: wrong version rejected"
                    (String.length e >= 16 && String.sub e 0 16 = "version mismatch") e
     | Ok _ -> check "edge: wrong version rejected" false "decoded");
    let accepted = ref [] in
    for l = String.length eref - 1 downto 0 do
      let r = try is_error (E.Edge.decode (String.sub eref 0 l)) with _ -> false in
      if not r then accepted := l :: !accepted
    done;
    check (Printf.sprintf "edge: all %d truncations rejected" (String.length eref)) (!accepted = [])
      (String.concat "," (List.map string_of_int !accepted));

    (* ---- extras ---- *)
    check "extra: garbage input is an error" (is_error (E.Edge.decode "\255\255\255")) "";
    let huge = "\n1.0.0" ^ "\084\000" ^ uv 4000000000 in
    check "extra: huge array count is an error" (is_error (B.WorldState.decode huge)) "";
    check "extra: out-of-range float raises Invalid_argument"
      (match E.Edge.encode { canonical with f = 1e30 } with
       | _ -> false
       | exception Invalid_argument _ -> true) "";
    check "extra: decode_exn raises Decode_error"
      (match E.Edge.decode_exn "" with _ -> false | exception E.Decode_error _ -> true) "";
    (* float32(0.29) * 10000 is 2899.99991... in double but rounds to 2900 in
       single precision, which is what the float32 targets put on the wire *)
    let one = E.Edge.encode { canonical with f = 0.29 } in
    check "extra: float field x10000 in single precision (0.29)"
      (match E.Edge.decode one with
       | Ok d -> d.f = Int32.float_of_bits (Int32.bits_of_float 0.29)
       | Error _ -> false) ""
  with e -> check "no crash" false (Printexc.to_string e));
  Printf.printf "ocaml: %d passed, %d failed\n" !pass !fail;
  exit (if !fail = 0 then 0 else 1)
