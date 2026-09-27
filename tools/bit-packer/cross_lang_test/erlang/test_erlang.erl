%% Cross-language conformance test for the BitPacker Erlang target.
%% Run through run.sh, which generates the code and compiles this module.
-module(test_erlang).
-export([main/1]).

-include("bench_complex.hrl").
-include("edge.hrl").

main([Dir]) ->
    put(pass, 0),
    put(fail, 0),
    Parent = filename:dirname(Dir),
    try
        bench(Parent),
        edge(Parent),
        extra()
    catch C:R:St ->
        check("no crash", false, io_lib:format("~p:~p ~p", [C, R, St]))
    end,
    P = get(pass), F = get(fail),
    io:format("erlang: ~b passed, ~b failed~n", [P, F]),
    erlang:halt(case F of 0 -> 0; _ -> 1 end).

check(Name, true, _) ->
    put(pass, get(pass) + 1),
    io:format("  ok   ~s~n", [Name]);
check(Name, _, Detail) ->
    put(fail, get(fail) + 1),
    io:format("  FAIL ~s (~s)~n", [Name, Detail]).

check(Name, Cond) -> check(Name, Cond, "").

eq(Name, Got, Want) ->
    check(Name, Got =:= Want, io_lib:format("got ~p, want ~p", [Got, Want])).

%% ---------------------------------------------------------------- bench

world() ->
    Sword = #item{id = 1, name = <<"Excalibur">>, value = 9999, weight = 15, rarity = <<"Legendary">>},
    Hero = #character{name = <<"TestHero">>, level = 99, hp = 1000, mp = 500, is_alive = true,
                      position = #vec3{x = 10, y = -20, z = 30}, skills = [1, 2, 3, 100],
                      inventory = [Sword]},
    Guild = #guild{name = <<"TestGuild">>, description = <<"A test guild for cross-language">>,
                   members = [Hero]},
    Potion = #item{id = 2, name = <<"HealthPotion">>, value = 50, weight = 1, rarity = <<"Common">>},
    #world_state{world_id = 42, seed = <<"cross_lang_test">>, guilds = [Guild], loot_table = [Potion]}.

verify_world(L, W) ->
    eq(L ++ " world_id", W#world_state.world_id, 42),
    eq(L ++ " seed", W#world_state.seed, <<"cross_lang_test">>),
    eq(L ++ " guilds length", length(W#world_state.guilds), 1),
    [G] = W#world_state.guilds,
    eq(L ++ " guild name", G#guild.name, <<"TestGuild">>),
    eq(L ++ " guild description", G#guild.description, <<"A test guild for cross-language">>),
    eq(L ++ " members length", length(G#guild.members), 1),
    [H] = G#guild.members,
    eq(L ++ " hero name", H#character.name, <<"TestHero">>),
    eq(L ++ " hero level", H#character.level, 99),
    eq(L ++ " hero hp", H#character.hp, 1000),
    eq(L ++ " hero mp", H#character.mp, 500),
    eq(L ++ " hero is_alive", H#character.is_alive, true),
    eq(L ++ " position", H#character.position, #vec3{x = 10, y = -20, z = 30}),
    eq(L ++ " skills", H#character.skills, [1, 2, 3, 100]),
    eq(L ++ " inventory length", length(H#character.inventory), 1),
    [S] = H#character.inventory,
    eq(L ++ " sword name", S#item.name, <<"Excalibur">>),
    eq(L ++ " sword value", S#item.value, 9999),
    eq(L ++ " sword rarity", S#item.rarity, <<"Legendary">>),
    eq(L ++ " loot length", length(W#world_state.loot_table), 1),
    [P] = W#world_state.loot_table,
    eq(L ++ " potion name", P#item.name, <<"HealthPotion">>),
    eq(L ++ " potion rarity", P#item.rarity, <<"Common">>).

bench(Parent) ->
    {ok, Ref} = file:read_file(filename:join(Parent, "test_data.bin")),
    Enc = bench_complex:encode_world_state(world()),
    ok = file:write_file(filename:join(Parent, "test_data_erlang.bin"), Enc),
    check("bench: encode == test_data.bin", Enc =:= Ref,
          io_lib:format("~b vs ~b bytes", [byte_size(Enc), byte_size(Ref)])),
    case bench_complex:decode_world_state(Ref) of
        {ok, W} ->
            check("bench: decode test_data.bin", true),
            verify_world("bench:", W),
            eq("bench: re-encode decoded == test_data.bin", bench_complex:encode_world_state(W), Ref);
        Err ->
            check("bench: decode test_data.bin", false, io_lib:format("~p", [Err]))
    end,
    case bench_complex:decode_world_state(Enc) of
        {ok, W2} -> eq("bench: round-trip", W2, world());
        Err2 -> check("bench: round-trip", false, io_lib:format("~p", [Err2]))
    end.

%% ---------------------------------------------------------------- edge

canonical() ->
    #edge{i_min = -2147483648, i_max = 2147483647, i_zero = 0, i_neg = -1,
          l_min = -9223372036854775808, l_max = 9223372036854775807, l_neg = -300,
          f = -1.25, d = 1234.5625, d_neg = -0.5,
          yes = true, no = false,
          empty = <<>>,
          unicode = unicode:characters_to_binary([$h, 16#e9, $l, $l, $o, $\s, $w, 16#f6, $r, $l, $d,
                                                  $\s, 16#2713, $\s, 16#65e5, 16#672c, $\s, 16#1f680]),
          ints = [0, -1, 1, -64, 64, -2147483648, 2147483647],
          longs = [0, -1, 9223372036854775807, -9223372036854775808, 4294967296],
          floats = [0.0, 0.5, -2.25],
          doubles = [0.0, 3.5, -1000000.25],
          bools = [true, false, true],
          strings = [<<>>, <<"a">>, unicode:characters_to_binary([16#65e5, 16#672c, 16#8a9e])],
          no_ints = [],
          inner = #inner{big = 1099511627776, label = <<"inner">>},
          inners = [#inner{big = -1, label = <<>>}, #inner{big = 0, label = <<"x">>}],
          no_inners = []}.

edge(Parent) ->
    {ok, Ref} = file:read_file(filename:join([Parent, "edge", "edge_ref.bin"])),
    C = canonical(),
    Enc = edge:encode_edge(C),
    check("edge: encode == edge_ref.bin", Enc =:= Ref,
          io_lib:format("~b vs ~b bytes", [byte_size(Enc), byte_size(Ref)])),
    case edge:decode_edge(Ref) of
        {ok, D} ->
            check("edge: decode edge_ref.bin", true),
            Fields = record_info(fields, edge),
            lists:foreach(
              fun({I, Name}) ->
                      eq("edge: field " ++ atom_to_list(Name), element(I + 1, D), element(I + 1, C))
              end, lists:zip(lists:seq(1, length(Fields)), Fields)),
            eq("edge: decoded == canonical", D, C),
            eq("edge: re-encode decoded == edge_ref.bin", edge:encode_edge(D), Ref);
        Err ->
            check("edge: decode edge_ref.bin", false, io_lib:format("~p", [Err]))
    end,
    <<Pre:5/binary, Last, Rest/binary>> = Ref,
    Bad = <<Pre/binary, (Last bxor 1), Rest/binary>>,
    case edge:decode_edge(Bad) of
        {error, {version_mismatch, _, _}} -> check("edge: wrong version rejected", true);
        Other -> check("edge: wrong version rejected", false, io_lib:format("~p", [Other]))
    end,
    Bads = [L || L <- lists:seq(0, byte_size(Ref) - 1),
                 not is_error(catch edge:decode_edge(binary:part(Ref, 0, L)))],
    check(io_lib:format("edge: all ~b truncations rejected", [byte_size(Ref)]), Bads =:= [],
          io_lib:format("accepted prefixes ~p", [Bads])).

uv(N) when N < 128 -> <<N>>;
uv(N) -> <<((N band 127) bor 128), (uv(N bsr 7))/binary>>.

is_error({error, _}) -> true;
is_error(_) -> false.

%% ---------------------------------------------------------------- extras

extra() ->
    %% bignum inputs wrap like the fixed-width targets' casts
    eq("extra: int wraps to 32 bits",
       edge:encode_edge((canonical())#edge{i_zero = 1 bsl 32}), edge:encode_edge(canonical())),
    eq("extra: long wraps to 64 bits",
       edge:encode_inner(#inner{big = (1 bsl 64) - 1}), edge:encode_inner(#inner{big = -1})),
    check("extra: garbage input is an error", is_error(edge:decode_edge(<<255, 255, 255>>))),
    Huge = <<10, "1.0.0", 84, 0, (uv(4000000000))/binary>>,
    check("extra: huge array count is an error", is_error(bench_complex:decode_world_state(Huge))),
    check("extra: non-binary input is an error", is_error(edge:decode_edge(not_a_binary))),
    %% float32(0.29) * 10000 is 2899.9999... in double but 2900 in single
    %% precision, which is what the float32 targets put on the wire.
    {ok, D29} = edge:decode_edge(edge:encode_edge((canonical())#edge{f = 0.29})),
    <<F29:32/float>> = <<(0.29):32/float>>,
    eq("extra: float field x10000 in single precision (0.29)", D29#edge.f, F29),
    check("extra: out-of-float32-range value raises",
          case catch edge:encode_edge((canonical())#edge{f = 1.0e300}) of
              {'EXIT', {{bitpacker, _}, _}} -> true;
              _ -> false
          end).
