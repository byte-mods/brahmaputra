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
    extra(Address),
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

%% ===========================================================================
%% Checks beyond the Go suite's 54: one per feature of the client contract
%% that those do not already exercise.
%% ===========================================================================

extra(Address) ->
    explicit_partition_timestamp_sync(Address),
    round_robin(Address),
    batch_linger_close(Address),
    retries_and_timeouts(Address),
    codec_registration(Address),
    fetch_limits(Address),
    bounds_checked_decoding(),
    auto_commit_topics_heartbeats(Address),
    fencing_rejoin_leave_static(Address),
    sticky_unit(),
    ok.

bin(Format, Args) -> list_to_binary(fmt(Format, Args)).

count_on(C, Topic, Partition) ->
    length(must(?C:fetch(C, Topic, Partition, 0, 300))).

committed_total(Group) ->
    lists:sum(maps:values(must(?G:committed(Group)))).

explicit_partition_timestamp_sync(Address) ->
    section("producer: explicit partition, timestamp and synchronous send"),
    T = unique("erl-sync"),
    Producer = producer(Address, #{}),
    Offsets = [must(?P:send_sync(Producer, T, bin("sync-~b", [I]), #{partition => 0}))
               || I <- lists:seq(0, 2)],
    check("send_sync returns each record's offset", Offsets =:= [0, 1, 2], fmt("~p", [Offsets])),
    must(?P:send_to(Producer, T, 2, <<"stamped">>, #{timestamp => 1600000000123})),
    must(?P:close(Producer)),
    C = consumer(Address),
    OnTwo = must(?C:fetch(C, T, 2, 0, 500)),
    check("an explicit partition is honoured",
          length(OnTwo) =:= 1 andalso count_on(C, T, 0) =:= 3,
          fmt("partition 2 holds ~b", [length(OnTwo)])),
    check("an explicit timestamp survives the round trip",
          case OnTwo of [#{timestamp := 1600000000123}] -> true; _ -> false end,
          fmt("~p", [[Ts || #{timestamp := Ts} <- OnTwo]])),
    ?C:close(C).

round_robin(Address) ->
    section("producer: round-robin for records without a key"),
    T = unique("erl-rr"),
    Producer = producer(Address, #{}),
    Partitions = must(brahmaputra_router:partitions(?P:router(Producer), T)),
    [must(?P:send(Producer, T, bin("rr~b", [I]))) || I <- lists:seq(0, 7)],
    must(?P:close(Producer)),
    C = consumer(Address),
    Counts = [count_on(C, T, P) || P <- Partitions],
    check("unkeyed records are spread evenly over every partition", Counts =:= [2, 2, 2, 2],
          fmt("~p", [Counts])),
    ?C:close(C).

batch_linger_close(Address) ->
    section("producer: batch.size, linger.ms and close"),
    C = consumer(Address),
    Full = unique("erl-batchfull"),
    Eager = producer(Address, #{linger_ms => 60000, batch_size => 64}),
    must(?P:send_to(Eager, Full, 0, binary:copy(<<"b">>, 100))),
    check("a batch that reaches batch.size is sent without waiting for linger.ms",
          count_on(C, Full, 0) =:= 1, ""),

    Lingering = unique("erl-linger"),
    Lazy = producer(Address, #{linger_ms => 100, batch_size => 1 bsl 20}),
    must(?P:send_to(Lazy, Lingering, 0, <<"waits">>)),
    HeldBack = length(must(?C:fetch(C, Lingering, 0, 0, 0))) =:= 0,
    timer:sleep(800),
    check("linger.ms holds a partial batch, then sends it in the background",
          HeldBack andalso count_on(C, Lingering, 0) =:= 1,
          case HeldBack of true -> "never sent"; false -> "sent before linger.ms" end),

    Closing = unique("erl-close"),
    Closer = producer(Address, #{linger_ms => 60000, batch_size => 1 bsl 20}),
    [must(?P:send_to(Closer, Closing, 0, bin("c~b", [I]))) || I <- lists:seq(0, 4)],
    must(?P:close(Closer)),
    check("close flushes what is still buffered", count_on(C, Closing, 0) =:= 5, ""),
    must(?P:close(Eager)),
    must(?P:close(Lazy)),
    ?C:close(C).

retries_and_timeouts({Host, Port} = Address) ->
    section("producer: retries, request.timeout.ms and delivery.timeout.ms"),
    Proxy = start_fault_proxy(Host, Port),
    ProxyAddress = {"127.0.0.1", fault_proxy_port(Proxy)},
    T = unique("erl-retry"),
    Base = #{linger_ms => 0, acks => all, request_timeout_ms => 4321},
    Producer = must(?P:start_link(ProxyAddress, Base#{retries => 3, retry_backoff_ms => 50})),
    _ = must(brahmaputra_router:partitions(?P:router(Producer), T)),
    fail_produces(Proxy, 2, 6),
    Sent = ?P:send_to(Producer, T, 0, <<"persistent">>),
    {Produces, Acks, Timeout} = fault_stats(Proxy),
    C = consumer(Address),
    check("a retriable error is retried until the send succeeds",
          Sent =:= ok andalso Produces =:= 3 andalso count_on(C, T, 0) =:= 1,
          fmt("attempts=~b ~p", [Produces, Sent])),
    check("request.timeout.ms and acks travel on the produce request",
          Timeout =:= 4321 andalso Acks =:= -1, fmt("~b/~b", [Timeout, Acks])),
    ?C:close(C),

    Bounded = must(?P:start_link(ProxyAddress, Base#{retries => 2, retry_backoff_ms => 150})),
    _ = must(brahmaputra_router:partitions(?P:router(Bounded), T)),
    fail_produces(Proxy, 1000, 6),
    Started = now_ms(),
    Doomed = ?P:send_to(Bounded, T, 0, <<"doomed">>),
    Took = now_ms() - Started,
    {Produces2, _, _} = fault_stats(Proxy),
    check("retries are bounded and spaced by retry.backoff.ms",
          case Doomed of {error, {server_error, 6, _, _}} -> true; _ -> false end
              andalso Produces2 =:= 3 andalso Took >= 300,
          fmt("attempts=~b took ~bms ~p", [Produces2, Took, Doomed])),

    fail_produces(Proxy, 1000, 3),
    _ = ?P:send_to(Bounded, T, 0, <<"malformed">>),
    {Produces3, _, _} = fault_stats(Proxy),
    check("a non-retriable error is not retried", Produces3 =:= 1, fmt("attempts=~b", [Produces3])),
    _ = ?P:close(Bounded),

    Capped = must(?P:start_link(ProxyAddress, Base#{retries => 1000, retry_backoff_ms => 50,
                                                    delivery_timeout_ms => 400})),
    _ = must(brahmaputra_router:partitions(?P:router(Capped), T)),
    fail_produces(Proxy, 100000, 6),
    Started2 = now_ms(),
    Late = ?P:send_to(Capped, T, 0, <<"late">>),
    Took2 = now_ms() - Started2,
    {Produces4, _, _} = fault_stats(Proxy),
    check("delivery.timeout.ms caps the whole retry loop",
          element(1, Late) =:= error andalso Took2 < 3000,
          fmt("took ~bms, attempts=~b", [Took2, Produces4])),
    fail_produces(Proxy, 0, 0),
    _ = ?P:close(Capped),
    must(?P:close(Producer)),
    stop_fault_proxy(Proxy).

codec_registration(Address) ->
    section("compression: registering a codec"),
    Refused = case ?P:start_link(Address, #{compression => snappy}) of
                  {error, _} -> true;
                  {ok, Unexpected} -> ?P:stop(Unexpected), false
              end,
    check("an unregistered codec is refused up front", Refused, ""),
    %% A toy reversible codec: enough to prove the hook is used on both the
    %% produce and the fetch path. The broker stores batches as-is.
    Flip = fun(Data) -> list_to_binary(lists:reverse([B bxor 16#5a || <<B>> <= Data])) end,
    ok = brahmaputra_protocol:register_codec(snappy, Flip, Flip),
    T = unique("erl-codec"),
    Producer = producer(Address, #{compression => snappy}),
    must(?P:send_to(Producer, T, 0, <<"through a registered codec">>,
                    #{key => <<"k">>, headers => [{<<"h">>, <<"v">>}]})),
    must(?P:close(Producer)),
    C = consumer(Address),
    Got = must(?C:fetch(C, T, 0, 0, 300)),
    check("a registered codec compresses on produce and decompresses on fetch",
          case Got of
              [#{value := <<"through a registered codec">>, key := <<"k">>, headers := [_]}] -> true;
              _ -> false
          end, fmt("~p", [Got])),
    ?C:close(C),
    Record = #{key => <<"k">>, value => <<"v">>, headers => [], timestamp_delta => 0},
    {ok, Encoded} = brahmaputra_protocol:encode_record_batch([Record], now_ms(), snappy),
    Decoded = brahmaputra_protocol:decode_record_batch(Encoded),
    check("a batch encoded with it decodes offline",
          case Decoded of {ok, #{records := [#{value := <<"v">>}]}, <<>>} -> true; _ -> false end,
          fmt("~p", [Decoded])).

fetch_limits(Address) ->
    section("consumer: fetch limits, watermark, offsets by time, metadata"),
    T = unique("erl-fetch"),
    Producer = producer(Address, #{}),
    Base = 1700000000000,
    [must(?P:send_to(Producer, T, 0, binary:copy(<<($a + I)>>, 1000), #{timestamp => Base + I * 1000}))
     || I <- lists:seq(0, 19)],
    must(?P:close(Producer)),

    Limited = must(?C:new(Address, #{fetch_max_bytes => 2500})),
    Capped = must(?C:fetch(Limited, T, 0, 0, 300)),
    check("fetch.max.bytes caps a response", length(Capped) > 0 andalso length(Capped) < 20,
          fmt("~b records", [length(Capped)])),
    ?C:close(Limited),

    C = consumer(Address),
    {ok, _, HighWatermark} = ?C:fetch_verbose(C, T, 0, 0, 300),
    check("the high watermark is reported", HighWatermark =:= 20, fmt("~b", [HighWatermark])),

    Waiter = must(?C:new(Address, #{fetch_max_wait_ms => 400, fetch_min_bytes => 1})),
    Started = now_ms(),
    None = must(?C:fetch(Waiter, T, 0, 20, 10000)),
    Took = now_ms() - Started,
    check("fetch.max.wait.ms bounds a long poll at the end of the log",
          None =:= [] andalso Took >= 250 andalso Took < 3000, fmt("~bms", [Took])),
    ?C:close(Waiter),

    ByTime = must(?C:list_offsets(C, T, 0, Base + 5000)),
    Between = must(?C:list_offsets(C, T, 0, Base + 5500)),
    check("list offsets by timestamp finds the first record at or after it",
          ByTime =:= 5 andalso Between =:= 6, fmt("~b,~b", [ByTime, Between])),

    Meta = must(brahmaputra_router:metadata(?C:router(C), [T], true)),
    Partitions = brahmaputra_router:partitions_of(Meta, T),
    Led = [P || P <- Partitions, brahmaputra_router:leader_of(Meta, T, P) >= 0],
    check("metadata lists a topic's partitions and their leaders",
          length(Partitions) =:= 4 andalso Led =:= Partitions,
          fmt("~b partitions", [length(Partitions)])),
    ?C:close(C),

    Group = must(?G:start_link(Address, unique("erl-maxpoll"),
                               #{enable_auto_commit => false, max_poll_records => 3})),
    ok = ?G:subscribe(Group, [T]),
    Sizes = poll_sizes(Group, 20, now_ms() + 20000, []),
    check("max.poll.records caps every poll",
          lists:sum(Sizes) =:= 20 andalso lists:max([0 | Sizes]) =:= 3,
          fmt("~b records, polls ~p", [lists:sum(Sizes), Sizes])),
    must(?G:close(Group)).

poll_sizes(Group, Want, Deadline, Acc) ->
    case lists:sum(Acc) < Want andalso now_ms() < Deadline of
        false -> lists:reverse(Acc);
        true ->
            case ?G:poll(Group, 300) of
                {ok, Records} -> poll_sizes(Group, Want, Deadline, [length(Records) | Acc]);
                {error, _} -> poll_sizes(Group, Want, Deadline, Acc)
            end
    end.

bounds_checked_decoding() ->
    section("decoding is bounds-checked"),
    P = brahmaputra_protocol,
    Negative = P:decode_body(P:body([P:enc_int32(-5)]), fun(R) -> P:dec_string(R) end),
    check("a negative length is an error, not a read",
          element(1, Negative) =:= error, fmt("~p", [Negative])),
    Oversized = P:decode_body(P:body([P:enc_int32(1 bsl 30)]), fun(R) -> P:dec_string(R) end),
    Record = #{key => <<"k">>, value => <<"v">>, headers => [], timestamp_delta => 0},
    {ok, <<Head:8/binary, _:8, Tail/binary>>} = P:encode_record_batch([Record], now_ms(), none),
    Truncated = P:decode_record_batch(<<Head/binary, 16#7f, Tail/binary>>),
    check("an oversized length is an error, not a read",
          element(1, Oversized) =:= error andalso element(1, Truncated) =:= error,
          fmt("~p ~p", [Oversized, Truncated])).

auto_commit_topics_heartbeats(Address) ->
    section("consumer groups: auto commit, several topics, heartbeats"),
    T1 = unique("erl-multi-a"),
    T2 = unique("erl-multi-b"),
    Producer = producer(Address, #{}),
    [begin
         must(?P:send(Producer, T1, bin("a~b", [I]))),
         must(?P:send(Producer, T2, bin("b~b", [I])))
     end || I <- lists:seq(0, 5)],
    must(?P:close(Producer)),

    Group = must(?G:start_link(Address, unique("erl-multi"),
                               #{enable_auto_commit => true, auto_commit_interval_ms => 200})),
    ok = ?G:subscribe(Group, [T1, T2]),
    {Seen, _} = poll_until(Group, 12, 20000, 300),
    Topics = lists:usort([Tp || #{topic := Tp} <- Seen]),
    check("one member subscribed to two topics consumes both",
          length(Seen) =:= 12 andalso length(Topics) =:= 2, fmt("~b records", [length(Seen)])),
    timer:sleep(300),
    _ = ?G:poll(Group, 300),
    Total = committed_total(Group),
    check("enable.auto.commit commits on poll after auto.commit.interval.ms", Total =:= 12,
          fmt("committed ~b", [Total])),
    must(?G:close(Group)),

    Idle = unique("erl-idle"),
    Seeder = producer(Address, #{}),
    must(?P:send(Seeder, Idle, <<"x">>)),
    must(?P:close(Seeder)),
    Quiet = must(?G:start_link(Address, unique("erl-heartbeat"),
                               #{enable_auto_commit => false, session_timeout_ms => 1500,
                                 heartbeat_interval_ms => 300})),
    ok = ?G:subscribe(Quiet, [Idle]),
    _ = poll_until(Quiet, 1, 15000, 300),
    Generation = ?G:generation(Quiet),
    timer:sleep(4000), % no poll: only heartbeats keep it in
    Committed = ?G:commit(Quiet),
    check("heartbeats keep an idle member in its group past session.timeout.ms",
          Committed =:= ok andalso ?G:generation(Quiet) =:= Generation, fmt("~p", [Committed])),
    must(?G:close(Quiet)).

fencing_rejoin_leave_static(Address) ->
    section("consumer groups: fencing, rejoin, leave and static membership"),
    T = unique("erl-fence"),
    Producer = producer(Address, #{}),
    [must(?P:send(Producer, T, bin("f~b", [I]))) || I <- lists:seq(0, 7)],

    GroupId = unique("erl-fence-grp"),
    Config = #{enable_auto_commit => false, max_poll_interval_ms => 60000,
               heartbeat_interval_ms => 200},
    First = must(?G:start_link(Address, GroupId, Config)),
    ok = ?G:subscribe(First, [T]),
    _ = poll_until(First, 8, 15000, 300),

    %% The coordinator forgets this member behind its back, as it does
    %% when a session expires.
    P = brahmaputra_protocol,
    OldMember = ?G:member_id(First),
    OldGeneration = ?G:generation(First),
    Router = must(brahmaputra_router:start_link(Address, #{})),
    {ok, Seed} = brahmaputra_router:seed(Router),
    {ok, _} = brahmaputra_conn:request(Seed, 18, P:body([P:enc_string(GroupId),
                                                          P:enc_string(OldMember)])),
    brahmaputra_router:stop(Router),
    timer:sleep(1000), % a heartbeat learns UNKNOWN_MEMBER_ID
    [must(?P:send(Producer, T, bin("f~b", [I]))) || I <- lists:seq(8, 11)],
    {After, _} = poll_until(First, 4, 15000, 300),
    Recommit = ?G:commit(First),
    check("a member the coordinator forgot rejoins on its next poll",
          length(After) =:= 4 andalso ?G:generation(First) > OldGeneration andalso Recommit =:= ok,
          fmt("~b records, ~s@~b -> ~s@~b ~p", [length(After), OldMember, OldGeneration,
                                                ?G:member_id(First), ?G:generation(First), Recommit])),

    %% A second member joins; the first sits out the rebalance and its
    %% generation goes stale.
    StaleGeneration = ?G:generation(First),
    Second = must(?G:start_link(Address, GroupId, Config)),
    ok = ?G:subscribe(Second, [T]),
    _ = poll_until(Second, 1000, 8000, 300),
    Fenced = ?G:commit(First),
    check("a commit from a stale generation is fenced",
          case Fenced of
              {error, {server_error, Code, _, _}} -> Code =:= 16 orelse Code =:= 13;
              _ -> false
          end, fmt("generation ~b -> ~p", [StaleGeneration, Fenced])),
    must(?G:close(Second)),
    must(?G:close(First)),

    %% Close sends LeaveGroup: the next member gets every partition at once
    %% instead of waiting out a long session.
    Slow = Config#{session_timeout_ms => 30000, rebalance_timeout_ms => 30000},
    LeaveGroup = <<GroupId/binary, "-leave">>,
    Leaver = must(?G:start_link(Address, LeaveGroup, Slow)),
    ok = ?G:subscribe(Leaver, [T]),
    _ = poll_until(Leaver, 12, 15000, 300),
    must(?G:close(Leaver)),
    Successor = must(?G:start_link(Address, LeaveGroup, Slow)),
    ok = ?G:subscribe(Successor, [T]),
    Started = now_ms(),
    [must(?P:send(Producer, T, bin("f~b", [I]))) || I <- lists:seq(12, 13)],
    {HandedOver, _} = poll_until(Successor, 2, 15000, 300),
    Took = now_ms() - Started,
    Held = ?G:assignment(Successor),
    check("close leaves the group so partitions move without a session timeout",
          length(HandedOver) =:= 2 andalso length(Held) =:= 4 andalso Took < 10000,
          fmt("~b records after ~bms", [length(HandedOver), Took])),
    must(?G:close(Successor)),

    StaticGroup = unique("erl-static"),
    Static = Config#{group_instance_id => <<"erl-instance-1">>},
    Original = must(?G:start_link(Address, StaticGroup, Static)),
    ok = ?G:subscribe(Original, [T]),
    _ = poll_until(Original, 14, 15000, 300),
    OriginalMember = ?G:member_id(Original),
    OriginalGeneration = ?G:generation(Original),
    Restarted = must(?G:start_link(Address, StaticGroup, Static)),
    ok = ?G:subscribe(Restarted, [T]),
    _ = poll_until(Restarted, 1000, 3000, 300),
    check("a static member reclaims its member id without a rebalance",
          OriginalMember =/= <<>> andalso ?G:member_id(Restarted) =:= OriginalMember
              andalso ?G:generation(Restarted) =:= OriginalGeneration,
          fmt("~s@~b vs ~s@~b", [OriginalMember, OriginalGeneration,
                                 ?G:member_id(Restarted), ?G:generation(Restarted)])),
    must(?G:close(Restarted)),
    must(?G:close(Original)),
    must(?P:close(Producer)).

sticky_unit() ->
    section("assignors: sticky keeps what members hold"),
    Members = [{<<"m1">>, [<<"t">>]}, {<<"m2">>, [<<"t">>]}],
    Topics = #{<<"t">> => lists:seq(0, 11)},
    Previous = #{<<"m1">> => [{<<"t">>, 2}, {<<"t">>, 10}, {<<"t">>, 11}],
                 <<"m2">> => [{<<"t">>, 0}, {<<"t">>, 1}]},
    {ok, Sticky} = brahmaputra_assignor:assign(sticky, Members, Topics, Previous),
    #{<<"m1">> := M1, <<"m2">> := M2} = Sticky,
    Holds = fun(Slots, Ps) -> lists:all(fun(Pt) -> lists:member({<<"t">>, Pt}, Slots) end, Ps) end,
    check("sticky leaves every held partition where it was",
          Holds(M1, [2, 10, 11]) andalso Holds(M2, [0, 1])
              andalso length(M1) =:= 6 andalso length(M2) =:= 6, fmt("~p", [Sticky])),
    Ids = [Pt || {_, Pt} <- M1],
    check("sticky orders partitions as numbers, not strings",
          length(Ids) > 1 andalso Ids =:= lists:usort(Ids), fmt("~p", [Ids])),
    {ok, Range} = brahmaputra_assignor:assign(range, Members, Topics, #{}),
    {ok, RoundRobin} = brahmaputra_assignor:assign(roundrobin, Members, Topics, #{}),
    check("range and roundrobin split twelve partitions six and six",
          length(maps:get(<<"m1">>, Range)) =:= 6 andalso length(maps:get(<<"m2">>, Range)) =:= 6
              andalso length(maps:get(<<"m1">>, RoundRobin)) =:= 6
              andalso lists:nth(2, maps:get(<<"m1">>, RoundRobin)) =:= {<<"t">>, 2},
          fmt("~p ~p", [Range, RoundRobin])).

%% A proxy that understands frames. It forwards every request to the broker
%% except Produce, which it can answer itself with an error code for the
%% next N requests -- how a leader move or an under-replicated partition
%% looks to a producer -- and it records what each Produce asked for. The
%% connections are pipelined, so replies are matched by correlation id, not
%% by order; the proxy only has to keep each frame whole.
start_fault_proxy(Host, Port) ->
    Self = self(),
    Manager = spawn(fun() -> fault_manager(Self, Host, Port) end),
    receive {fault_proxy_port, Manager, P} -> {Manager, P} end.

fault_proxy_port({_, P}) -> P.

fault_manager(Owner, Host, Port) ->
    {ok, Listen} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}, {packet, 4}]),
    {ok, P} = inet:port(Listen),
    Manager = self(),
    spawn_link(fun() -> fault_accept(Manager, Listen, Host, Port) end),
    Owner ! {fault_proxy_port, self(), P},
    fault_loop(Listen, [], #{failures => 0, code => 0, produces => 0, acks => 0, timeout => 0}).

fault_loop(Listen, Pairs, Stats) ->
    receive
        {pair, Pid} ->
            fault_loop(Listen, [Pid | Pairs], Stats);
        {produce, From, Acks, Timeout} ->
            #{failures := Failures, code := Code, produces := Produces} = Stats,
            Reply = case Failures > 0 of true -> {fail, Code}; false -> forward end,
            From ! {verdict, Reply},
            fault_loop(Listen, Pairs, Stats#{failures := max(0, Failures - 1),
                                             produces := Produces + 1,
                                             acks := Acks, timeout := Timeout});
        {fail, From, Count, Code} ->
            From ! failing,
            fault_loop(Listen, Pairs, Stats#{failures := Count, code := Code, produces := 0});
        {stats, From} ->
            From ! {stats, maps:get(produces, Stats), maps:get(acks, Stats), maps:get(timeout, Stats)},
            fault_loop(Listen, Pairs, Stats);
        {stop, From} ->
            [exit(Pid, kill) || Pid <- Pairs],
            gen_tcp:close(Listen),
            From ! stopped
    end.

fault_accept(Manager, Listen, Host, Port) ->
    case gen_tcp:accept(Listen) of
        {ok, Client} ->
            Pair = spawn(fun() -> fault_pair(Manager, Host, Port) end),
            ok = gen_tcp:controlling_process(Client, Pair),
            Pair ! {client, Client},
            Manager ! {pair, Pair},
            fault_accept(Manager, Listen, Host, Port);
        {error, _} ->
            ok
    end.

fault_pair(Manager, Host, Port) ->
    receive {client, Client} -> ok end,
    {ok, Upstream} = gen_tcp:connect(Host, Port, [binary, {active, true}, {packet, 4}]),
    ok = inet:setopts(Client, [{active, true}]),
    fault_pump(Manager, Client, Upstream).

fault_pump(Manager, Client, Upstream) ->
    P = brahmaputra_protocol,
    receive
        {tcp, Client, <<0:16/signed, _:16, Corr:32/signed, ClientLen:16/signed, Rest/binary>> = Frame} ->
            <<_:ClientLen/binary, Body/binary>> = Rest,
            {ok, {Topic, Partition, Acks, Timeout}} =
                P:decode_body(Body, fun(R0) ->
                    {Tp, R1} = P:dec_string(R0),
                    {Pt, R2} = P:dec_int32(R1),
                    {A, R3} = P:dec_int32(R2),
                    {To, _} = P:dec_int32(R3),
                    {Tp, Pt, A, To}
                end),
            Manager ! {produce, self(), Acks, Timeout},
            receive
                {verdict, forward} ->
                    ok = gen_tcp:send(Upstream, Frame);
                {verdict, {fail, Code}} ->
                    Reply = P:body([P:enc_string(Topic), P:enc_int32(Partition), P:enc_int32(Code),
                                    P:enc_int64(-1), P:enc_int64(-1)]),
                    ok = gen_tcp:send(Client, P:encode_frame(0, Corr, <<>>, Reply))
            end,
            fault_pump(Manager, Client, Upstream);
        {tcp, Client, Data} -> ok = gen_tcp:send(Upstream, Data), fault_pump(Manager, Client, Upstream);
        {tcp, Upstream, Data} -> ok = gen_tcp:send(Client, Data), fault_pump(Manager, Client, Upstream);
        {tcp_closed, _} -> gen_tcp:close(Client), gen_tcp:close(Upstream);
        {tcp_error, _, _} -> gen_tcp:close(Client), gen_tcp:close(Upstream)
    end.

fail_produces({Manager, _}, Count, Code) ->
    Manager ! {fail, self(), Count, Code},
    receive failing -> ok end.

fault_stats({Manager, _}) ->
    Manager ! {stats, self()},
    receive {stats, Produces, Acks, Timeout} -> {Produces, Acks, Timeout} end.

stop_fault_proxy({Manager, _}) ->
    Manager ! {stop, self()},
    receive stopped -> ok end.
