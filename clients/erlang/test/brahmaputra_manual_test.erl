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

    wire_edge_cases(Address),
    ordering_under_linger(Address),
    background_flush_failures(Address),
    connection_failures(Address),
    max_poll_interval_rejoin(Address),
    time_inside_poll(Address),
    ok.

%% Fetch from offset 0 until Want records are in hand or a fetch comes
%% back empty.
fetch_all(C, Topic, Partition, Want) -> fetch_all(C, Topic, Partition, Want, 0, []).

fetch_all(C, Topic, Partition, Want, Offset, Acc) when length(Acc) < Want ->
    case ?C:fetch(C, Topic, Partition, Offset, 500) of
        {ok, [_ | _] = Batch} ->
            #{offset := Last} = lists:last(Batch),
            fetch_all(C, Topic, Partition, Want, Last + 1, Acc ++ Batch);
        _ ->
            Acc
    end;
fetch_all(_C, _Topic, _Partition, _Want, _Offset, Acc) ->
    Acc.

wire_edge_cases(Address) ->
    section("wire edge cases"),
    EdgeTopic = unique("erl-edge"),
    Producer = producer(Address, #{}),
    Large = << <<((I * 7) band 16#FF)>> || I <- lists:seq(0, (1 bsl 20) - 1) >>,
    UnicodeKey = <<"ключ-✓-🔑"/utf8>>,
    UnicodeValue = <<"значение — 数据 — 🚀"/utf8>>,
    HeaderKey = <<"ünïcødé-🏷"/utf8>>,
    must(?P:send_to(Producer, EdgeTopic, 0, Large)),
    must(?P:send_to(Producer, EdgeTopic, 0, UnicodeValue,
                    #{key => UnicodeKey, headers => [{HeaderKey, <<"✓"/utf8>>}]})),
    %% An empty key and an empty header value are values, not nulls.
    must(?P:send_to(Producer, EdgeTopic, 0, <<"empty-key">>,
                    #{key => <<>>, headers => [{<<"empty">>, <<>>}, {<<"null">>, undefined}]})),
    must(?P:send_to(Producer, EdgeTopic, 0, <<"null-key">>)),
    must(?P:close(Producer)),
    C = consumer(Address),
    Got = fetch_all(C, EdgeTopic, 0, 4),
    check("edge records all arrive", length(Got) =:= 4, fmt("got ~b", [length(Got)])),
    case Got of
        [R0, R1, R2, R3] ->
            V0 = maps:get(value, R0),
            check("a 1 MiB value round-trips byte-identical", V0 =:= Large,
                  fmt("~b bytes", [byte_size(V0)])),
            check("unicode key, value and header key round-trip",
                  maps:get(key, R1) =:= UnicodeKey andalso maps:get(value, R1) =:= UnicodeValue
                  andalso [K || {K, _} <- maps:get(headers, R1)] =:= [HeaderKey], ""),
            check("an empty key stays empty, not null", maps:get(key, R2) =:= <<>>,
                  fmt("~p", [maps:get(key, R2)])),
            check("an empty header value stays empty, not null",
                  maps:get(headers, R2) =:= [{<<"empty">>, <<>>}, {<<"null">>, undefined}],
                  fmt("~p", [maps:get(headers, R2)])),
            check("a null key stays null", maps:get(key, R3) =:= undefined,
                  fmt("~p", [maps:get(key, R3)]));
        _ ->
            ok
    end,
    ?C:close(C).

ordering_under_linger(Address) ->
    section("ordering under linger flushes"),
    OrderTopic = unique("erl-order"),
    Producer = must(?P:start_link(Address, #{linger_ms => 1, batch_size => 256})),
    Total = 5000,
    [must(?P:send_to(Producer, OrderTopic, 0, integer_to_binary(I)))
     || I <- lists:seq(0, Total - 1)],
    must(?P:close(Producer)),
    C = consumer(Address),
    Values = [binary_to_integer(V) || #{value := V} <- fetch_all(C, OrderTopic, 0, Total)],
    Inversions = length([x || {A, B} <- lists:zip(lists:droplast([0 | Values]), Values),
                              B < A]),
    check("every record of a partition arrives", length(Values) =:= Total,
          fmt("got ~b", [length(Values)])),
    check("a partition's records keep send order", Inversions =:= 0,
          fmt("~b inversions", [Inversions])),
    ?C:close(C).

background_flush_failures(Address) ->
    section("background flush failures are reported"),
    Producer = must(?P:start_link(Address, #{linger_ms => 20})),
    %% Partition 999 does not exist, so the linger timer's flush fails.
    SendResult = ?P:send_to(Producer, unique("erl-bgfail"), 999, <<"lost">>),
    timer:sleep(300),
    FlushResult = ?P:flush(Producer),
    check("a failed linger flush surfaces on the next Flush",
          SendResult =:= ok andalso FlushResult =/= ok,
          fmt("send=~p flush=~p", [SendResult, FlushResult])),
    Self = self(),
    Closer = spawn(fun() -> Self ! {closed, self(), ?P:close(Producer)} end),
    receive
        {closed, Closer, _} -> check("Close returns after a failed flush", true, "")
    after 5000 ->
        check("Close returns after a failed flush", false, "hung")
    end.

connection_failures({Host, Port} = Address) ->
    section("connection failures"),
    %% A broker that accepts and never answers must cost an error, not a
    %% caller blocked forever.
    {ok, Silent} = gen_tcp:listen(0, [binary, {active, true}, {reuseaddr, true}]),
    {ok, SilentPort} = inet:port(Silent),
    SilentAcceptor = spawn(fun() -> silent_accept(Silent) end),
    ok = gen_tcp:controlling_process(Silent, SilentAcceptor),
    Conn = must(brahmaputra_conn:start("127.0.0.1", SilentPort, <<"erl-test">>, 1000)),
    ok = brahmaputra_conn:set_request_timeout(Conn, 300),
    Started = now_ms(),
    RequestResult = brahmaputra_conn:api_versions(Conn),
    check("a request to an unresponsive broker times out",
          element(1, RequestResult) =:= error andalso now_ms() - Started < 3000,
          fmt("~p", [RequestResult])),
    check("a timed-out connection is not reused", brahmaputra_conn:broken(Conn), ""),
    brahmaputra_conn:stop(Conn),
    exit(SilentAcceptor, kill),

    %% A connection the broker drops is redialled, not kept forever.
    Proxy = start_proxy(Host, Port),
    ProxyAddress = {"127.0.0.1", proxy_port(Proxy)},
    DropTopic = unique("erl-drop"),
    Producer = producer(ProxyAddress, #{}),
    must(?P:send_to(Producer, DropTopic, 0, <<"before">>)),
    drop_all(Proxy),
    Recovered = retry(3, fun() -> ?P:send_to(Producer, DropTopic, 0, <<"after">>) end),
    check("a producer recovers after its connection drops", Recovered =:= ok,
          fmt("~p", [Recovered])),
    _ = ?P:close(Producer),
    C = consumer(ProxyAddress),
    _ = must(?C:fetch(C, DropTopic, 0, 0, 100)),
    drop_all(Proxy),
    Fetched = retry(3, fun() -> ?C:fetch(C, DropTopic, 0, 0, 100) end),
    check("a consumer recovers after its connection drops",
          case Fetched of {ok, [_ | _]} -> true; _ -> false end, fmt("~p", [Fetched])),
    ?C:close(C),
    stop_proxy(Proxy),
    _ = Address,
    ok.

retry(1, Fun) -> Fun();
retry(N, Fun) ->
    case Fun() of
        {error, _} -> retry(N - 1, Fun);
        Other -> Other
    end.

silent_accept(Listen) ->
    case gen_tcp:accept(Listen) of
        {ok, _Socket} -> silent_accept(Listen); % active: bytes arrive and are ignored
        {error, _} -> ok
    end.

%% A TCP proxy to the broker that can sever every live connection, which is
%% how a broker restart or a load balancer's idle timeout looks to a client.
start_proxy(Host, Port) ->
    Self = self(),
    Manager = spawn(fun() -> proxy_manager(Self, Host, Port) end),
    receive {proxy_port, Manager, P} -> {Manager, P} end.

proxy_port({_, P}) -> P.

proxy_manager(Owner, Host, Port) ->
    {ok, Listen} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
    {ok, P} = inet:port(Listen),
    Manager = self(),
    spawn_link(fun() -> proxy_accept(Manager, Listen, Host, Port) end),
    Owner ! {proxy_port, self(), P},
    proxy_loop(Listen, []).

proxy_loop(Listen, Pairs) ->
    receive
        {pair, Pid} ->
            proxy_loop(Listen, [Pid | Pairs]);
        {drop, From} ->
            [exit(Pid, kill) || Pid <- Pairs],
            From ! dropped,
            proxy_loop(Listen, []);
        {stop, From} ->
            [exit(Pid, kill) || Pid <- Pairs],
            gen_tcp:close(Listen),
            From ! stopped
    end.

proxy_accept(Manager, Listen, Host, Port) ->
    case gen_tcp:accept(Listen) of
        {ok, Client} ->
            Pair = spawn(fun() -> proxy_pair(Host, Port) end),
            ok = gen_tcp:controlling_process(Client, Pair),
            Pair ! {client, Client},
            Manager ! {pair, Pair},
            proxy_accept(Manager, Listen, Host, Port);
        {error, _} ->
            ok
    end.

%% Owns both sockets, so killing this process closes both.
proxy_pair(Host, Port) ->
    receive {client, Client} -> ok end,
    {ok, Upstream} = gen_tcp:connect(Host, Port, [binary, {active, true}]),
    ok = inet:setopts(Client, [{active, true}]),
    proxy_pump(Client, Upstream).

proxy_pump(Client, Upstream) ->
    receive
        {tcp, Client, Data} -> ok = gen_tcp:send(Upstream, Data), proxy_pump(Client, Upstream);
        {tcp, Upstream, Data} -> ok = gen_tcp:send(Client, Data), proxy_pump(Client, Upstream);
        {tcp_closed, _} -> gen_tcp:close(Client), gen_tcp:close(Upstream);
        {tcp_error, _, _} -> gen_tcp:close(Client), gen_tcp:close(Upstream)
    end.

drop_all({Manager, _}) ->
    Manager ! {drop, self()},
    receive dropped -> ok end,
    timer:sleep(50).

stop_proxy({Manager, _}) ->
    Manager ! {stop, self()},
    receive stopped -> ok end.

max_poll_interval_rejoin(Address) ->
    section("consumer group: max.poll.interval and rejoin"),
    SlowTopic = unique("erl-slow"),
    Producer = producer(Address, #{}),
    [must(?P:send(Producer, SlowTopic, list_to_binary(fmt("s~b", [I])))) || I <- lists:seq(0, 9)],
    Group = must(?G:start_link(Address, unique("erl-slow-grp"),
                               #{auto_commit_interval_ms => 0, max_poll_interval_ms => 1500})),
    ok = ?G:subscribe(Group, [SlowTopic]),
    {First, _} = poll_until(Group, 10, 15000, 300),
    must(?G:commit(Group)),
    %% Stall past max.poll.interval.ms: the member leaves the group.
    timer:sleep(2500),
    [must(?P:send(Producer, SlowTopic, list_to_binary(fmt("s~b", [I])))) || I <- lists:seq(10, 19)],
    must(?P:close(Producer)),
    {Second, PollErr} = poll_until(Group, 10, 15000, 300),
    check("a member that stalled rejoins on its next poll",
          length(First) =:= 10 andalso length(Second) =:= 10 andalso PollErr =:= ok,
          fmt("first=~b second=~b err=~p", [length(First), length(Second), PollErr])),
    must(?G:close(Group)).

%% Like poll_for, but a poll error ends the loop and is returned.
poll_until(Group, Want, Ms, PollMs) -> poll_until(Group, Want, now_ms() + Ms, PollMs, []).

poll_until(Group, Want, Deadline, PollMs, Acc) ->
    case length(Acc) < Want andalso now_ms() < Deadline of
        false -> {Acc, ok};
        true ->
            case ?G:poll(Group, PollMs) of
                {ok, Records} -> poll_until(Group, Want, Deadline, PollMs, Acc ++ Records);
                {error, _} = E -> {Acc, E}
            end
    end.

time_inside_poll(Address) ->
    section("consumer group: time inside poll does not count against max.poll.interval"),
    JoinTopic = unique("erl-inpoll"),
    Producer = producer(Address, #{}),
    _ = must(brahmaputra_router:partitions(?P:router(Producer), JoinTopic)),
    %% Far shorter than the poll below, which spends ~1s joining (the
    %% broker's initial rebalance delay) and then waits for data.
    Group = must(?G:start_link(Address, unique("erl-inpoll-grp"),
                               #{auto_commit_interval_ms => 0, max_poll_interval_ms => 600})),
    ok = ?G:subscribe(Group, [JoinTopic]),
    spawn(fun() ->
                  timer:sleep(2000),
                  [?P:send(Producer, JoinTopic, list_to_binary(fmt("j~b", [I])))
                   || I <- lists:seq(0, 9)]
          end),
    %% One long poll: it joins, then waits for the records above.
    PollResult = ?G:poll(Group, 4000),
    %% Committed straight away, before another poll could quietly rejoin:
    %% this fails if the member left the group mid-poll.
    CommitResult = ?G:commit(Group),
    Got = case PollResult of {ok, Rs} -> length(Rs); _ -> 0 end,
    check("a member is still in its group after a long poll",
          Got > 0 andalso CommitResult =:= ok,
          fmt("got=~b poll=~p commit=~p", [Got, element(1, PollResult), CommitResult])),
    must(?G:close(Group)),
    must(?P:close(Producer)).

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
