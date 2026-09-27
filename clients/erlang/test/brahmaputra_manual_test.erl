%% @doc End-to-end suite: exercises the Erlang driver against a live broker.
%%
%%   brahmaputra-server --data-dir ./data --default-partitions 4
%%   erl -noshell -pa ebin -pa test \
%%       -eval 'brahmaputra_manual_test:main(["127.0.0.1", "9092"])'
%%
%% Every check asserts a property of the system, not that a function ran:
%% records come back byte-identical, keys pin partitions, headers survive,
%% offsets are contiguous. Halts non-zero on any failure.
-module(brahmaputra_manual_test).

-export([main/1]).

-define(P, brahmaputra_producer).
-define(C, brahmaputra_consumer).
-define(G, brahmaputra_group).

main(Args) ->
    {Host, Port} = case Args of
                       [H, Pt | _] -> {H, list_to_integer(Pt)};
                       [H] -> {H, 9092};
                       [] -> {"127.0.0.1", 9092}
                   end,
    put(passed, 0),
    put(failed, 0),
    Code = try
               run({Host, Port}),
               Passed = get(passed),
               Failed = get(failed),
               io:format("~n~b passed, ~b failed~n", [Passed, Failed]),
               case Failed of 0 -> 0; _ -> 1 end
           catch
               throw:{fatal, What} ->
                   io:format("  FATAL ~p~n", [What]),
                   2;
               Class:Reason:Stack ->
                   io:format("  FATAL ~p:~p~n~p~n", [Class, Reason, Stack]),
                   2
           end,
    erlang:halt(Code).

check(Name, Ok, Detail) ->
    case Ok of
        true ->
            put(passed, get(passed) + 1),
            io:format("  ok   ~s~n", [Name]);
        false ->
            put(failed, get(failed) + 1),
            case Detail of
                "" -> io:format("  FAIL ~s~n", [Name]);
                _ -> io:format("  FAIL ~s: ~s~n", [Name, Detail])
            end
    end.

section(Title) -> io:format("~n~s~n", [Title]).

fmt(Format, Args) -> lists:flatten(io_lib:format(Format, Args)).

unique(Prefix) ->
    list_to_binary(fmt("~s-~b", [Prefix, erlang:system_time(nanosecond) rem 1000000000])).

must(ok) -> ok;
must({ok, V}) -> V;
must({ok, V, W}) -> {V, W};
must({error, Reason}) -> throw({fatal, Reason}).

now_ms() -> erlang:system_time(millisecond).

producer(Address, Extra) ->
    must(?P:start_link(Address, maps:merge(#{linger_ms => 0}, Extra))).

consumer(Address) -> must(?C:new(Address, #{})).

%% Poll until Want records arrive or Ms passes (Want = infinity: always
%% poll for the whole window).
poll_for(Group, Want, Ms, PollMs) ->
    poll_for(Group, Want, now_ms() + Ms, PollMs, []).

poll_for(Group, Want, Deadline, PollMs, Acc) ->
    case (Want =:= infinity orelse length(Acc) < Want) andalso now_ms() < Deadline of
        false ->
            Acc;
        true ->
            case ?G:poll(Group, PollMs) of
                {ok, Records} -> poll_for(Group, Want, Deadline, PollMs, Acc ++ Records);
                {error, Reason} -> throw({fatal, {poll, Reason}})
            end
    end.

run(Address) ->
    section("connection and metadata"),
    (fun() ->
        C = consumer(Address),
        {ok, Seed} = brahmaputra_router:seed(?C:router(C)),
        Versions = brahmaputra_conn:api_versions(Seed),
        case Versions of
            {ok, Ranges, BrokerVersion} ->
                check("ApiVersions answers", length(Ranges) > 0, ""),
                check("broker reports a version", BrokerVersion =/= <<>>,
                      binary_to_list(BrokerVersion));
            {error, R} ->
                check("ApiVersions answers", false, fmt("~p", [R])),
                check("broker reports a version", false, "")
        end,
        #{brokers := Brokers} = must(brahmaputra_router:metadata(?C:router(C), [], true)),
        check("metadata lists brokers", length(Brokers) >= 1,
              fmt("~b brokers", [length(Brokers)])),
        ?C:close(C)
    end)(),

    section("produce and consume round trip"),
    Topic = unique("erl-roundtrip"),
    Payloads = [list_to_binary(fmt("record-~b", [I])) || I <- lists:seq(0, 49)],
    (fun() ->
        Producer = producer(Address, #{}),
        [must(?P:send_to(Producer, Topic, 0, V)) || V <- Payloads],
        must(?P:flush(Producer)),
        must(?P:close(Producer)),
        C = consumer(Address),
        Got = must(?C:fetch(C, Topic, 0, 0, 500)),
        check("every record comes back", length(Got) =:= length(Payloads),
              fmt("got ~b", [length(Got)])),
        Identical = length(Got) =:= length(Payloads) andalso
            lists:all(fun({I, #{value := V, offset := O}, Want}) ->
                              V =:= Want andalso O =:= I
                      end, lists:zip3(lists:seq(0, length(Got) - 1), Got,
                                      lists:sublist(Payloads, length(Got)))),
        check("values byte-identical and offsets contiguous", Identical, ""),
        ?C:close(C)
    end)(),

    section("compression codecs"),
    %% Only none and gzip ship in the driver; lz4/zstd/snappy are opt-in
    %% via brahmaputra_protocol:register_codec/3.
    lists:foreach(
      fun(Codec) ->
              CodecTopic = unique("erl-" ++ atom_to_list(Codec)),
              Body = binary:copy(<<"the same line over and over. ">>, 40),
              Producer = producer(Address, #{compression => Codec}),
              [must(?P:send_to(Producer, CodecTopic, 0, <<Body/binary, ($0 + I rem 10)>>))
               || I <- lists:seq(0, 19)],
              must(?P:flush(Producer)),
              must(?P:close(Producer)),
              C = consumer(Address),
              Got = must(?C:fetch(C, CodecTopic, 0, 0, 500)),
              Prefixed = case Got of
                             [#{value := V0} | _] -> binary:longest_common_prefix([V0, Body])
                                                         =:= byte_size(Body);
                             [] -> false
                         end,
              check(atom_to_list(Codec) ++ ": round trips",
                    length(Got) =:= 20 andalso Prefixed,
                    fmt("got ~b records", [length(Got)])),
              ?C:close(C)
      end, [none, gzip]),

    section("keys, partitioning and ordering"),
    (fun() ->
        KeyTopic = unique("erl-keys"),
        Producer = producer(Address, #{}),
        Partitions = must(brahmaputra_router:partitions(?P:router(Producer), KeyTopic)),
        [must(?P:send(Producer, KeyTopic, list_to_binary(fmt("v~b", [I])),
                      #{key => <<"user-7">>})) || I <- lists:seq(0, 29)],
        must(?P:flush(Producer)),
        must(?P:close(Producer)),
        Target = brahmaputra_protocol:partition_for_key(<<"user-7">>, Partitions),
        C = consumer(Address),
        OnTarget = must(?C:fetch(C, KeyTopic, Target, 0, 500)),
        check("a key pins every record to one partition", length(OnTarget) =:= 30,
              fmt("partition ~b holds ~b of 30", [Target, length(OnTarget)])),
        Ordered = length(OnTarget) =:= 30 andalso
            [V || #{value := V} <- OnTarget]
                =:= [list_to_binary(fmt("v~b", [I])) || I <- lists:seq(0, 29)],
        check("per-key order is preserved", Ordered, ""),
        Strays = lists:sum([length(must(?C:fetch(C, KeyTopic, Pt, 0, 200)))
                            || Pt <- Partitions, Pt =/= Target]),
        check("no keyed record landed elsewhere", Strays =:= 0, fmt("~b strays", [Strays])),
        ?C:close(C)
    end)(),

    section("murmur2 agrees with the broker's partitioner"),
    M = brahmaputra_protocol:murmur2(<<>>),
    check("murmur2(\"\") is stable", M =:= 275646681, integer_to_list(M)),
    check("murmur2 is deterministic",
          brahmaputra_protocol:murmur2(<<"user-7">>) =:= brahmaputra_protocol:murmur2(<<"user-7">>),
          ""),
    check("different keys hash differently",
          brahmaputra_protocol:murmur2(<<"user-7">>) =/= brahmaputra_protocol:murmur2(<<"user-8">>),
          ""),

    section("record headers and timestamps"),
    (fun() ->
        HeaderTopic = unique("erl-headers"),
        Before = now_ms() - 1000,
        Producer = producer(Address, #{}),
        must(?P:send_to(Producer, HeaderTopic, 0, <<"annotated">>,
                        #{headers => [{<<"trace-id">>, <<"abc-123">>},
                                      {<<"content-type">>, <<"application/json">>},
                                      {<<"tombstone-reason">>, undefined}]})),
        must(?P:send_to(Producer, HeaderTopic, 0, <<"plain">>)),
        must(?P:flush(Producer)),
        must(?P:close(Producer)),
        After = now_ms() + 1000,
        C = consumer(Address),
        Got = must(?C:fetch(C, HeaderTopic, 0, 0, 500)),
        check("both records arrive", length(Got) =:= 2, fmt("got ~b", [length(Got)])),
        case Got of
            [#{headers := AH} = Annotated, #{headers := PH} = Plain] ->
                check("headers survive the round trip", length(AH) =:= 3,
                      fmt("~b headers", [length(AH)])),
                check("header values are exact",
                      proplists:get_value(<<"trace-id">>, AH) =:= <<"abc-123">>, ""),
                check("a null header value stays null",
                      length(AH) =:= 3 andalso element(2, lists:nth(3, AH)) =:= undefined, ""),
                check("a record with no headers gains none from its batch", PH =:= [],
                      fmt("~b headers", [length(PH)])),
                Ts = [maps:get(timestamp, R) || R <- [Annotated, Plain]],
                check("timestamps are real wall-clock values",
                      lists:all(fun(T) -> T >= Before andalso T =< After end, Ts),
                      fmt("~w outside ~b..~b", [Ts, Before, After]));
            _ ->
                ok
        end,
        ?C:close(C)
    end)(),

    section("tombstones"),
    (fun() ->
        TombTopic = unique("erl-tombstones"),
        Producer = producer(Address, #{}),
        must(?P:send_to(Producer, TombTopic, 0, <<"set">>, #{key => <<"k1">>})),
        must(?P:send_to(Producer, TombTopic, 0, <<>>, #{key => <<"k2">>})),
        %% `undefined' is a deletion, and must stay distinguishable from
        %% the empty value above all the way through the round trip.
        must(?P:send_to(Producer, TombTopic, 0, undefined, #{key => <<"k3">>})),
        must(?P:flush(Producer)),
        must(?P:close(Producer)),
        C = consumer(Address),
        Got = must(?C:fetch(C, TombTopic, 0, 0, 500)),
        check("all three records arrive", length(Got) =:= 3, fmt("got ~b", [length(Got)])),
        case Got of
            [#{value := V1}, #{value := V2}, #{value := V3}] ->
                check("an ordinary value round-trips", V1 =:= <<"set">>, ""),
                check("an empty value is empty, not null", V2 =:= <<>>, fmt("~p", [V2])),
                check("a tombstone arrives as a null value", V3 =:= undefined, fmt("~p", [V3]));
            _ ->
                ok
        end,
        ?C:close(C)
    end)(),

    section("offsets"),
    (fun() ->
        C = consumer(Address),
        Earliest = must(?C:list_offsets(C, Topic, 0, earliest)),
        Latest = must(?C:list_offsets(C, Topic, 0, latest)),
        check("earliest is 0 on a fresh topic", Earliest =:= 0, integer_to_list(Earliest)),
        check("latest equals the record count", Latest =:= 50, integer_to_list(Latest)),
        ?C:close(C)
    end)(),

    section("acks"),
    lists:foreach(
      fun(Acks) ->
              AcksTopic = unique(fmt("erl-acks~b", [Acks])),
              Producer = producer(Address, #{acks => Acks}),
              must(?P:send_to(Producer, AcksTopic, 0, <<"durable">>)),
              must(?P:flush(Producer)),
              must(?P:close(Producer)),
              timer:sleep(400),
              C = consumer(Address),
              Got = must(?C:fetch(C, AcksTopic, 0, 0, 500)),
              check(fmt("acks=~b stores the record", [Acks]), length(Got) =:= 1,
                    fmt("got ~b", [length(Got)])),
              ?C:close(C)
      end, [0, 1, -1]),

    section("consumer group: assignment, commit, resume"),
    (fun() ->
        GroupTopic = unique("erl-group"),
        GroupId = unique("erl-billing"),
        Producer = producer(Address, #{}),
        [must(?P:send(Producer, GroupTopic, list_to_binary(fmt("g~b", [I]))))
         || I <- lists:seq(0, 39)],
        must(?P:flush(Producer)),
        must(?P:close(Producer)),

        GroupConfig = #{auto_commit_interval_ms => 0},
        Group = must(?G:start_link(Address, GroupId, GroupConfig)),
        ok = ?G:subscribe(Group, [GroupTopic]),
        Seen = poll_for(Group, 40, 30000, 500),
        check("the group consumes every record", length(Seen) =:= 40,
              fmt("got ~b", [length(Seen)])),
        Distinct = lists:usort([{Pt, O} || #{partition := Pt, offset := O} <- Seen]),
        check("no record is delivered twice", length(Distinct) =:= length(Seen), ""),
        must(?G:commit(Group)),
        Committed = must(?G:committed(Group)),
        Total = lists:sum(maps:values(Committed)),
        check("commit records a position", Total =:= 40, integer_to_list(Total)),
        must(?G:close(Group)),

        %% A second member of the same group must resume, not replay.
        Rejoined = must(?G:start_link(Address, GroupId, GroupConfig)),
        ok = ?G:subscribe(Rejoined, [GroupTopic]),
        Replayed = poll_for(Rejoined, infinity, 5000, 300),
        check("a rejoining group resumes from its commit", Replayed =:= [],
              fmt("replayed ~b records it had already committed", [length(Replayed)])),
        must(?G:close(Rejoined))
    end)(),

    section("auto.offset.reset"),
    (fun() ->
        ResetTopic = unique("erl-reset"),
        Producer = producer(Address, #{}),
        [must(?P:send(Producer, ResetTopic, list_to_binary(fmt("r~b", [I]))))
         || I <- lists:seq(0, 9)],
        must(?P:flush(Producer)),
        must(?P:close(Producer)),

        Latest = must(?G:start_link(Address, unique("erl-latest"),
                                    #{auto_commit_interval_ms => 0,
                                      auto_offset_reset => latest})),
        ok = ?G:subscribe(Latest, [ResetTopic]),
        Skipped = poll_for(Latest, infinity, 4000, 300),
        check("latest skips records produced before the group existed", Skipped =:= [],
              fmt("saw ~b", [length(Skipped)])),
        must(?G:close(Latest)),

        Strict = must(?G:start_link(Address, unique("erl-none"),
                                    #{auto_commit_interval_ms => 0,
                                      auto_offset_reset => none})),
        ok = ?G:subscribe(Strict, [ResetTopic]),
        Raised = wait_for_no_offset(Strict, now_ms() + 5000),
        check("none refuses to guess a position", Raised, ""),
        must(?G:close(Strict))
    end)(),

    section("assignors"),
    lists:foreach(
      fun(Assignor) ->
              AssignorTopic = unique("erl-" ++ atom_to_list(Assignor)),
              Producer = producer(Address, #{}),
              [must(?P:send(Producer, AssignorTopic, list_to_binary(fmt("a~b", [I]))))
               || I <- lists:seq(0, 19)],
              must(?P:flush(Producer)),
              must(?P:close(Producer)),
              Group = must(?G:start_link(Address, unique("erl-grp-" ++ atom_to_list(Assignor)),
                                         #{auto_commit_interval_ms => 0,
                                           assignor => Assignor})),
              ok = ?G:subscribe(Group, [AssignorTopic]),
              Collected = poll_for(Group, 20, 20000, 500),
              check(atom_to_list(Assignor) ++ ": consumes every record",
                    length(Collected) =:= 20, fmt("got ~b", [length(Collected)])),
              must(?G:close(Group))
      end, [range, roundrobin, sticky]),

    section("bounded client buffer"),
    (fun() ->
        BufferTopic = unique("erl-buffer"),
        Producer = must(?P:start_link(Address, #{linger_ms => 10000, % never flush on time here
                                                 buffer_memory => 2048,
                                                 max_block_ms => 300})),
        Blocked = fill_until_blocked(Producer, BufferTopic, 500),
        check("a full buffer blocks and then reports", Blocked, ""),
        ?P:stop(Producer)
    end)(),
    ok.

wait_for_no_offset(Group, Deadline) ->
    case now_ms() < Deadline of
        false -> false;
        true ->
            case ?G:poll(Group, 300) of
                {error, {no_offset_for_partition, _, _}} -> true;
                _ -> wait_for_no_offset(Group, Deadline)
            end
    end.

fill_until_blocked(_Producer, _Topic, 0) ->
    false;
fill_until_blocked(Producer, Topic, N) ->
    case ?P:send_to(Producer, Topic, 0, binary:copy(<<"x">>, 256)) of
        {error, {buffer_full, _}} -> true;
        _ -> fill_until_blocked(Producer, Topic, N - 1)
    end.
