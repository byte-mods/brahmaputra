%% @doc A consumer-group member.
%%
%% A gen_server that shares its subscribed topics' partitions with the rest
%% of its group: join/sync with generation fencing, heartbeats on an
%% `erlang:send_after/3' timer, offset commit/fetch, and an explicit
%% LeaveGroup on close.
%%
%% `poll/2' never blocks the server for its whole timeout. It runs one
%% fetch round at a time and re-schedules itself between rounds, so the
%% heartbeat timer keeps firing while a caller waits for records.
%%
%% Two independent deadlines are enforced: `session_timeout_ms' (the
%% coordinator evicts a member that stops heartbeating) and
%% `max_poll_interval_ms' (this member leaves when the application stops
%% calling poll — heartbeats prove the process is alive, polls prove it is
%% still consuming).
-module(brahmaputra_group).
-behaviour(gen_server).

-include("brahmaputra.hrl").

-export([start_link/3, subscribe/2, poll/2, commit/1, committed/1, committed/2,
         assignment/1, member_id/1, generation/1, close/1, default_config/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(COORDINATOR_ATTEMPTS, 4).
-define(JOIN_ATTEMPTS, 4).

-record(state, {group_id :: binary(),
                config :: map(),
                consumer :: brahmaputra_consumer:consumer(),
                subscribed = [] :: [binary()],
                member_id = <<>> :: binary(),
                generation = -1 :: integer(),
                joined = false :: boolean(),
                assignment = [] :: [{binary(), integer()}],
                %% Next offset to *deliver* — what gets committed. Advances
                %% only over records handed to the caller.
                positions = #{} :: #{{binary(), integer()} => integer()},
                %% Next offset to *fetch*; runs ahead of positions by
                %% exactly the records sitting in buffered.
                fetch_positions = #{} :: #{{binary(), integer()} => integer()},
                buffered = [] :: [map()],
                last_poll_ms :: integer(),
                last_commit_ms :: integer(),
                left_for_slow_poll = false :: boolean(),
                heartbeat_timer :: reference() | undefined,
                pending_poll :: {gen_server:from(), integer()} | undefined}).

%% @doc Defaults, named as Kafka names them.
%%
%% <ul>
%%   <li>`session_timeout_ms' (10000), `rebalance_timeout_ms' (3000),
%%       `max_poll_interval_ms' (300000).</li>
%%   <li>`heartbeat_interval_ms' — how often to heartbeat; 0 means
%%       `session_timeout_ms div 3'. Must be below the session timeout.</li>
%%   <li>`enable_auto_commit' (true) and `auto_commit_interval_ms' (5000;
%%       0 also disables auto-commit).</li>
%%   <li>`auto_offset_reset' — `earliest', `latest' or `none'.</li>
%%   <li>`assignor' — `range', `roundrobin' or `sticky'.</li>
%%   <li>`group_instance_id' — static membership; `<<>>' for dynamic.</li>
%%   <li>plus every {@link brahmaputra_consumer:default_config/0} key.</li>
%% </ul>
default_config() ->
    maps:merge(brahmaputra_consumer:default_config(),
               #{session_timeout_ms => 10000,
                 heartbeat_interval_ms => 0,
                 rebalance_timeout_ms => 3000,
                 max_poll_interval_ms => 300000,
                 enable_auto_commit => true,
                 auto_commit_interval_ms => 5000,
                 auto_offset_reset => earliest,
                 assignor => range,
                 group_instance_id => <<>>}).

-spec start_link(term(), binary(), map()) -> {ok, pid()} | {error, term()}.
start_link(Address, GroupId, Opts) ->
    Config = maps:merge(default_config(), Opts),
    case maps:get(heartbeat_interval_ms, Config) >= maps:get(session_timeout_ms, Config) of
        true -> {error, {heartbeat_interval_ms_not_below_session_timeout_ms,
                         maps:get(heartbeat_interval_ms, Config)}};
        false -> gen_server:start_link(?MODULE, {Address, to_bin(GroupId), Config}, [])
    end.

%% @doc Set the topics this member wants a share of. Takes effect on the
%% next poll, which (re)joins the group.
subscribe(Group, Topics) ->
    gen_server:call(Group, {subscribe, [to_bin(T) || T <- Topics]}, infinity).

%% @doc Up to `max_poll_records' records, joining the group if needed.
%% Returns `{ok, []}' when none arrive within TimeoutMs.
-spec poll(pid(), non_neg_integer()) ->
          {ok, [brahmaputra_consumer:consumed_record()]} | {error, term()}.
poll(Group, TimeoutMs) ->
    gen_server:call(Group, {poll, TimeoutMs}, infinity).

%% @doc Commit the delivered positions. At-least-once: call it after
%% processing, not before.
commit(Group) -> gen_server:call(Group, commit, infinity).

%% @doc The group's committed offsets; with no list, every partition the
%% group holds.
committed(Group) -> committed(Group, []).
committed(Group, Partitions) -> gen_server:call(Group, {committed, Partitions}, infinity).

assignment(Group) -> gen_server:call(Group, assignment, infinity).
member_id(Group) -> gen_server:call(Group, member_id, infinity).
%% @doc The generation this member last joined; -1 before the first join.
generation(Group) -> gen_server:call(Group, generation, infinity).

%% @doc Commit, leave the group, then stop.
%%
%% Leaving is what separates a clean shutdown from a crash: without it the
%% coordinator must wait out the session timeout before reassigning.
close(Group) ->
    try gen_server:call(Group, close, infinity)
    catch exit:{noproc, _} -> ok
    end.

%% ---------------------------------------------------------------------------
%% gen_server
%% ---------------------------------------------------------------------------

init({Address, GroupId, Config}) ->
    process_flag(trap_exit, true),
    case brahmaputra_consumer:new(Address, Config) of
        {ok, Consumer} ->
            Now = now_ms(),
            State = #state{group_id = GroupId, config = Config, consumer = Consumer,
                           last_poll_ms = Now, last_commit_ms = Now},
            {ok, schedule_heartbeat(State)};
        {error, Reason} ->
            {stop, Reason}
    end.

handle_call({subscribe, Topics}, _From, State) ->
    {reply, ok, State#state{subscribed = Topics, joined = false}};
handle_call({poll, _}, _From, State = #state{subscribed = []}) ->
    {reply, {error, not_subscribed}, State};
handle_call({poll, _}, _From, State = #state{pending_poll = {_, _}}) ->
    {reply, {error, poll_in_progress}, State};
handle_call({poll, TimeoutMs}, From, State) ->
    %% Stamped on entry and on return, and not enforced in between: the
    %% interval bounds how long the application goes without asking, and a
    %% poll blocking for its timeout or on a slow rebalance is working.
    Now = now_ms(),
    run_poll(State#state{last_poll_ms = Now, pending_poll = {From, Now + TimeoutMs}});
handle_call(commit, _From, State) ->
    {Reply, S} = do_commit(State),
    {reply, Reply, S};
handle_call({committed, Partitions}, _From, State) ->
    {reply, do_committed(Partitions, State), State};
handle_call(assignment, _From, State) ->
    {reply, State#state.assignment, State};
handle_call(member_id, _From, State) ->
    {reply, State#state.member_id, State};
handle_call(generation, _From, State) ->
    {reply, State#state.generation, State};
handle_call(close, _From, State) ->
    S1 = case State#state.joined of
             true -> element(2, do_commit(State));
             false -> State
         end,
    %% Best effort: failing costs only the session timeout it avoids.
    _ = case S1#state.member_id of
            <<>> -> ok;
            _ -> do_leave(S1)
        end,
    {stop, normal, ok, S1#state{member_id = <<>>, joined = false}}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(poll_step, State = #state{pending_poll = {_, _}}) ->
    run_poll(State);
handle_info(poll_step, State) ->
    {noreply, State};
handle_info(heartbeat, State) ->
    {noreply, schedule_heartbeat(heartbeat_tick(State#state{heartbeat_timer = undefined}))};
handle_info({'EXIT', _Pid, Reason}, State) ->
    {stop, {router_down, Reason}, State};
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    case State#state.pending_poll of
        {From, _} -> gen_server:reply(From, {error, closed});
        undefined -> ok
    end,
    brahmaputra_consumer:close(State#state.consumer).

%% ---------------------------------------------------------------------------
%% poll
%% ---------------------------------------------------------------------------

run_poll(State = #state{pending_poll = {From, Deadline}}) ->
    case poll_step(Deadline, State) of
        {done, Reply, S} ->
            gen_server:reply(From, Reply),
            %% Stamped again on return: the interval measures the gap
            %% between polls, which starts when this one hands back.
            {noreply, S#state{pending_poll = undefined, last_poll_ms = now_ms()}};
        {continue, Delay, S} ->
            _ = erlang:send_after(Delay, self(), poll_step),
            {noreply, S}
    end.

poll_step(Deadline, State0) ->
    case ensure_joined(State0) of
        {error, Reason, S} ->
            {done, {error, Reason}, S};
        {ok, #state{buffered = [_ | _]} = S} ->
            {S1, Records} = take_buffered(S),
            {done, {ok, Records}, S1};
        {ok, #state{assignment = []} = S} ->
            case now_ms() >= Deadline of
                true -> {done, {ok, []}, S};
                false -> {continue, 50, S}
            end;
        {ok, S} ->
            case fetch_round(S#state.assignment, Deadline, false, S) of
                {error, Reason, S1} ->
                    {done, {error, Reason}, S1};
                {ok, GotAny, S1} ->
                    S2 = maybe_auto_commit(S1),
                    case S2#state.buffered of
                        [_ | _] ->
                            {S3, Records} = take_buffered(S2),
                            {done, {ok, Records}, S3};
                        [] ->
                            case not GotAny andalso now_ms() >= Deadline of
                                true -> {done, {ok, []}, S2};
                                false -> {continue, 0, S2}
                            end
                    end
            end
    end.

ensure_joined(State = #state{joined = true}) -> {ok, State};
ensure_joined(State) ->
    case do_join(State, ?JOIN_ATTEMPTS) of
        {ok, S} -> {ok, S};
        {error, Reason, S} -> {error, Reason, S}
    end.

fetch_round([], _Deadline, GotAny, State) ->
    {ok, GotAny, State};
fetch_round([Slot = {Topic, Partition} | Rest], Deadline, GotAny, State) ->
    WaitMs = max(0, min(Deadline - now_ms(), 500)),
    Offset = maps:get(Slot, State#state.fetch_positions),
    case brahmaputra_consumer:fetch(State#state.consumer, Topic, Partition, Offset, WaitMs) of
        {ok, []} ->
            fetch_round(Rest, Deadline, GotAny, State);
        {ok, Records} ->
            #{offset := Last} = lists:last(Records),
            S = State#state{fetch_positions = maps:put(Slot, Last + 1,
                                                       State#state.fetch_positions),
                            buffered = State#state.buffered ++ Records},
            fetch_round(Rest, Deadline, true, S);
        {error, {server_error, ?ERR_OFFSET_OUT_OF_RANGE, _, _}} ->
            %% The committed offset fell off the log; restart where the
            %% policy says.
            case reset_offset(Topic, Partition, State) of
                {ok, Reset} ->
                    S = State#state{fetch_positions = maps:put(Slot, Reset,
                                                               State#state.fetch_positions),
                                    positions = maps:put(Slot, Reset, State#state.positions)},
                    fetch_round(Rest, Deadline, GotAny, S);
                {error, Reason} ->
                    {error, Reason, State}
            end;
        {error, {server_error, ?ERR_NOT_LEADER_OR_FOLLOWER, _, _}} ->
            _ = brahmaputra_router:refresh(brahmaputra_consumer:router(State#state.consumer),
                                           Topic),
            fetch_round(Rest, Deadline, GotAny, State);
        {error, Reason} ->
            {error, Reason, State}
    end.

take_buffered(State = #state{buffered = Buffered, config = Config}) ->
    Limit = case maps:get(max_poll_records, Config) of
                N when N > 0 -> min(N, length(Buffered));
                _ -> length(Buffered)
            end,
    {Delivered, Rest} = lists:split(Limit, Buffered),
    %% The committed position advances only over records actually handed
    %% to the caller; committing what was merely fetched would skip records
    %% nobody processed.
    Positions = lists:foldl(fun(#{topic := T, partition := P, offset := O}, Acc) ->
                                    maps:put({T, P}, O + 1, Acc)
                            end, State#state.positions, Delivered),
    {State#state{buffered = Rest, positions = Positions}, Delivered}.

maybe_auto_commit(State = #state{config = Config}) ->
    Interval = maps:get(auto_commit_interval_ms, Config),
    case maps:get(enable_auto_commit, Config) =:= true andalso Interval > 0
        andalso map_size(State#state.positions) > 0
        andalso now_ms() - State#state.last_commit_ms >= Interval of
        %% A failed auto-commit is retried on the next poll; an explicit
        %% commit is what a caller relies on.
        true -> element(2, do_commit(State));
        false -> State
    end.

reset_offset(Topic, Partition, #state{config = Config, consumer = Consumer}) ->
    case to_atom(maps:get(auto_offset_reset, Config)) of
        earliest -> brahmaputra_consumer:list_offsets(Consumer, Topic, Partition, earliest);
        latest -> brahmaputra_consumer:list_offsets(Consumer, Topic, Partition, latest);
        none -> {error, {no_offset_for_partition, Topic, Partition}};
        Other -> {error, {unknown_auto_offset_reset, Other}}
    end.

%% ---------------------------------------------------------------------------
%% offsets
%% ---------------------------------------------------------------------------

do_commit(State = #state{positions = Positions}) when map_size(Positions) =:= 0 ->
    {ok, State};
do_commit(State) ->
    P = brahmaputra_protocol,
    Slots = lists:sort(maps:to_list(State#state.positions)),
    Body = P:body([P:enc_string(State#state.group_id),
                   P:enc_int32(State#state.generation),
                   P:enc_string(State#state.member_id),
                   P:enc_int32(length(Slots)),
                   [[P:enc_string(T), P:enc_int32(Pt), P:enc_int64(O)]
                    || {{T, Pt}, O} <- Slots]]),
    case coordinator_request(?API_OFFSET_COMMIT, Body, State) of
        {ok, Resp} ->
            case P:peek_error_code(Resp) of
                ?ERR_NONE -> {ok, State#state{last_commit_ms = now_ms()}};
                Code when Code =:= ?ERR_ILLEGAL_GENERATION;
                          Code =:= ?ERR_UNKNOWN_MEMBER_ID;
                          Code =:= ?ERR_REBALANCE_IN_PROGRESS ->
                    %% Generation fencing: the group moved on without this
                    %% member's generation; the next poll rejoins.
                    {{error, P:server_error(Code, offset_commit)}, State#state{joined = false}};
                Code -> {{error, P:server_error(Code, offset_commit)}, State}
            end;
        {error, _} = E ->
            {E, State}
    end.

do_committed(Partitions, State) ->
    P = brahmaputra_protocol,
    Body = P:body([P:enc_string(State#state.group_id),
                   P:enc_int32(length(Partitions)),
                   [[P:enc_string(T), P:enc_int32(Pt)] || {T, Pt} <- Partitions]]),
    case coordinator_request(?API_OFFSET_FETCH, Body, State) of
        {ok, Resp} ->
            Decoded = P:decode_body(Resp, fun(R0) ->
                {Code, R1} = P:dec_int32(R0),
                Code =:= ?ERR_NONE orelse throw(P:server_error(Code, offset_fetch)),
                {Entries, _} = P:dec_array(R1, fun(B0) ->
                    {T, B1} = P:dec_string(B0),
                    {Pt, B2} = P:dec_int32(B1),
                    {O, B3} = P:dec_int64(B2),
                    {{{T, Pt}, O}, B3}
                end),
                maps:from_list(Entries)
            end),
            Decoded;
        {error, _} = E ->
            E
    end.

%% ---------------------------------------------------------------------------
%% membership
%% ---------------------------------------------------------------------------

do_join(State, 0) ->
    {error, {group_unstable, ?JOIN_ATTEMPTS}, State};
do_join(State, AttemptsLeft) ->
    P = brahmaputra_protocol,
    Config = State#state.config,
    Body = P:body([P:enc_string(State#state.group_id),
                   P:enc_int32(maps:get(session_timeout_ms, Config)),
                   P:enc_int32(maps:get(rebalance_timeout_ms, Config)),
                   P:enc_string(State#state.member_id),
                   P:enc_string_array(State#state.subscribed),
                   P:enc_string(to_bin(maps:get(group_instance_id, Config)))]),
    case coordinator_request(?API_JOIN_GROUP, Body, State) of
        {error, Reason} ->
            {error, Reason, State};
        {ok, Resp} ->
            case P:decode_body(Resp, fun decode_join/1) of
                {error, Reason} ->
                    {error, Reason, State};
                {ok, {?ERR_REBALANCE_IN_PROGRESS}} ->
                    timer:sleep(100),
                    do_join(State, AttemptsLeft - 1);
                {ok, {?ERR_UNKNOWN_MEMBER_ID}} ->
                    %% The coordinator dropped this member (session expiry,
                    %% or it left for a slow poll): join again as a new one.
                    do_join(State#state{member_id = <<>>}, AttemptsLeft - 1);
                {ok, {Code}} ->
                    {error, P:server_error(Code, join_group), State};
                {ok, {Generation, MemberId, LeaderId, Members}} ->
                    S1 = State#state{member_id = MemberId, generation = Generation},
                    case leader_assignment(MemberId =:= LeaderId, Members, S1) of
                        {error, Reason} ->
                            {error, Reason, S1};
                        {ok, Assignments} ->
                            case do_sync(Assignments, S1) of
                                {ok, S2} ->
                                    {ok, S2#state{joined = true, left_for_slow_poll = false}};
                                {retry, S2} ->
                                    do_join(S2, AttemptsLeft - 1);
                                {error, Reason, S2} ->
                                    {error, Reason, S2}
                            end
                    end
            end
    end.

decode_join(R0) ->
    P = brahmaputra_protocol,
    case P:dec_int32(R0) of
        {?ERR_NONE, R1} ->
            {Generation, R2} = P:dec_int32(R1),
            {MemberId, R3} = P:dec_string(R2),
            {LeaderId, R4} = P:dec_string(R3),
            {Members, _} = P:dec_array(R4, fun(B0) ->
                {Id, B1} = P:dec_string(B0),
                {Topics, B2} = P:dec_string_array(B1),
                {Held, B3} = P:dec_array(B2, fun(C0) ->
                    {T, C1} = P:dec_string(C0),
                    {Pt, C2} = P:dec_int32(C1),
                    {{T, Pt}, C2}
                end),
                {{Id, Topics, Held}, B3}
            end),
            {Generation, MemberId, LeaderId, Members};
        {Code, _} ->
            {Code}
    end.

leader_assignment(false, _Members, _State) ->
    {ok, []};
leader_assignment(true, Members, State) ->
    Topics = lists:usort(lists:append([Ts || {_, Ts, _} <- Members])),
    case topic_partitions(Topics, State, #{}) of
        {error, _} = E ->
            E;
        {ok, TopicPartitions} ->
            Previous = maps:from_list([{Id, Held} || {Id, _, Held} <- Members]),
            case brahmaputra_assignor:assign(to_atom(maps:get(assignor, State#state.config)),
                                             [{Id, Ts} || {Id, Ts, _} <- Members],
                                             TopicPartitions, Previous) of
                {ok, Assignment} -> {ok, lists:sort(maps:to_list(Assignment))};
                {error, _} = E -> E
            end
    end.

topic_partitions([], _State, Acc) ->
    {ok, Acc};
topic_partitions([Topic | Rest], State, Acc) ->
    case brahmaputra_consumer:partitions(State#state.consumer, Topic) of
        {ok, Ps} -> topic_partitions(Rest, State, maps:put(Topic, Ps, Acc));
        {error, _} = E -> E
    end.

do_sync(Assignments, State) ->
    P = brahmaputra_protocol,
    Body = P:body([P:enc_string(State#state.group_id),
                   P:enc_int32(State#state.generation),
                   P:enc_string(State#state.member_id),
                   P:enc_int32(length(Assignments)),
                   [[P:enc_string(Id), P:enc_int32(length(Slots)),
                     [[P:enc_string(T), P:enc_int32(Pt)] || {T, Pt} <- Slots]]
                    || {Id, Slots} <- Assignments]]),
    case coordinator_request(?API_SYNC_GROUP, Body, State) of
        {error, Reason} ->
            {error, Reason, State};
        {ok, Resp} ->
            Decoded = P:decode_body(Resp, fun(R0) ->
                case P:dec_int32(R0) of
                    {?ERR_NONE, R1} ->
                        {Slots, _} = P:dec_array(R1, fun(B0) ->
                            {T, B1} = P:dec_string(B0),
                            {Pt, B2} = P:dec_int32(B1),
                            {{T, Pt}, B2}
                        end),
                        {ok, Slots};
                    {Code, _} ->
                        {code, Code}
                end
            end),
            case Decoded of
                {ok, {ok, Slots}} ->
                    apply_assignment(Slots, State);
                {ok, {code, Code}} when Code =:= ?ERR_REBALANCE_IN_PROGRESS;
                                        Code =:= ?ERR_ILLEGAL_GENERATION ->
                    {retry, State};
                {ok, {code, ?ERR_UNKNOWN_MEMBER_ID}} ->
                    {retry, State#state{member_id = <<>>}};
                {ok, {code, Code}} ->
                    {error, P:server_error(Code, sync_group), State};
                {error, Reason} ->
                    {error, Reason, State}
            end
    end.

apply_assignment(Slots, State) ->
    Owned = maps:from_list([{S, true} || S <- Slots]),
    Positions0 = maps:filter(fun(S, _) -> maps:is_key(S, Owned) end, State#state.positions),
    %% Buffered records sit ahead of the committed position and were never
    %% delivered, so a new assignment simply drops them.
    S1 = State#state{assignment = Slots, buffered = [], positions = Positions0},
    Needed = [S || S <- Slots, not maps:is_key(S, Positions0)],
    Resolved =
        case Needed of
            [] -> {ok, Positions0};
            _ ->
                case do_committed(Needed, S1) of
                    {ok, Committed} -> resolve_positions(Needed, Committed, S1, Positions0);
                    {error, _} = E -> E
                end
        end,
    case Resolved of
        {ok, Positions} ->
            {ok, S1#state{positions = Positions, fetch_positions = Positions}};
        {error, Reason} ->
            {error, Reason, S1}
    end.

resolve_positions([], _Committed, _State, Acc) ->
    {ok, Acc};
resolve_positions([Slot = {T, Pt} | Rest], Committed, State, Acc) ->
    case maps:get(Slot, Committed, -1) of
        Offset when Offset >= 0 ->
            resolve_positions(Rest, Committed, State, maps:put(Slot, Offset, Acc));
        _ ->
            case reset_offset(T, Pt, State) of
                {ok, Offset} ->
                    resolve_positions(Rest, Committed, State, maps:put(Slot, Offset, Acc));
                {error, _} = E ->
                    E
            end
    end.

do_leave(State) ->
    P = brahmaputra_protocol,
    Body = P:body([P:enc_string(State#state.group_id), P:enc_string(State#state.member_id)]),
    case coordinator_request(?API_LEAVE_GROUP, Body, State) of
        {ok, Resp} ->
            case P:peek_error_code(Resp) of
                ?ERR_NONE -> ok;
                Code -> {error, P:server_error(Code, leave_group)}
            end;
        {error, _} = E ->
            E
    end.

%% ---------------------------------------------------------------------------
%% heartbeat
%% ---------------------------------------------------------------------------

%% The tick has to wake often enough for the shorter of the two deadlines;
%% deriving it from the session timeout alone would leave a long session
%% with a short poll interval unchecked long after it stalled.
schedule_heartbeat(State = #state{config = Config}) ->
    HeartbeatEvery = case maps:get(heartbeat_interval_ms, Config) of
                         N when N > 0 -> N;
                         _ -> maps:get(session_timeout_ms, Config) div 3
                     end,
    Interval = max(1, min(HeartbeatEvery, maps:get(max_poll_interval_ms, Config) div 3)),
    State#state{heartbeat_timer = erlang:send_after(Interval, self(), heartbeat)}.

heartbeat_tick(State = #state{joined = false}) -> State;
heartbeat_tick(State = #state{member_id = <<>>}) -> State;
heartbeat_tick(State = #state{config = Config}) ->
    Idle = now_ms() - State#state.last_poll_ms,
    InPoll = State#state.pending_poll =/= undefined,
    case not InPoll andalso Idle >= maps:get(max_poll_interval_ms, Config) of
        true ->
            %% The application stopped consuming though the process is
            %% alive. Heartbeating on would assert a liveness this member no
            %% longer has, holding partitions from one that could progress.
            case State#state.left_for_slow_poll of
                true -> State;
                false ->
                    _ = do_leave(State),
                    State#state{left_for_slow_poll = true, joined = false}
            end;
        false ->
            P = brahmaputra_protocol,
            Body = P:body([P:enc_string(State#state.group_id),
                           P:enc_int32(State#state.generation),
                           P:enc_string(State#state.member_id)]),
            case coordinator_request(?API_HEARTBEAT, Body, State) of
                {ok, Resp} ->
                    case P:peek_error_code(Resp) of
                        Code when Code =:= ?ERR_REBALANCE_IN_PROGRESS;
                                  Code =:= ?ERR_UNKNOWN_MEMBER_ID;
                                  Code =:= ?ERR_ILLEGAL_GENERATION ->
                            %% Fenced: rejoin on the next poll.
                            State#state{joined = false};
                        _ ->
                            State#state{left_for_slow_poll = false}
                    end;
                {error, _} ->
                    State % transient: retry next tick
            end
    end.

%% ---------------------------------------------------------------------------
%% coordinator routing
%% ---------------------------------------------------------------------------

coordinator_partition(State) ->
    case brahmaputra_consumer:partitions(State#state.consumer, ?OFFSETS_TOPIC) of
        {ok, Ps} ->
            {ok, brahmaputra_protocol:crc32c(State#state.group_id) rem length(Ps)};
        {error, _} = E ->
            E
    end.

%% Send to the group's coordinator, following moves and waiting out loads.
coordinator_request(ApiKey, Body, State) ->
    coordinator_request(ApiKey, Body, State, ?COORDINATOR_ATTEMPTS).

coordinator_request(_ApiKey, _Body, _State, 0) ->
    {error, {coordinator_unavailable, ?COORDINATOR_ATTEMPTS}};
coordinator_request(ApiKey, Body, State, AttemptsLeft) ->
    Router = brahmaputra_consumer:router(State#state.consumer),
    Timeout = maps:get(request_timeout_ms, State#state.config)
        + maps:get(rebalance_timeout_ms, State#state.config)
        + maps:get(session_timeout_ms, State#state.config),
    Result =
        case coordinator_partition(State) of
            {ok, Partition} ->
                case brahmaputra_router:conn_for(Router, ?OFFSETS_TOPIC, Partition) of
                    {ok, Conn} -> brahmaputra_conn:request(Conn, ApiKey, Body, Timeout);
                    {error, _} = E -> E
                end;
            {error, _} = E ->
                E
        end,
    case Result of
        {ok, Resp} ->
            case brahmaputra_protocol:peek_error_code(Resp) of
                ?ERR_COORDINATOR_LOAD_IN_PROGRESS ->
                    timer:sleep(100),
                    coordinator_request(ApiKey, Body, State, AttemptsLeft - 1);
                Code when Code =:= ?ERR_NOT_COORDINATOR;
                          Code =:= ?ERR_NOT_LEADER_OR_FOLLOWER ->
                    _ = brahmaputra_router:refresh(Router, ?OFFSETS_TOPIC),
                    coordinator_request(ApiKey, Body, State, AttemptsLeft - 1);
                _ ->
                    {ok, Resp}
            end;
        {error, _} = E2 ->
            E2
    end.

%% ---------------------------------------------------------------------------

now_ms() -> erlang:system_time(millisecond).

to_bin(B) when is_binary(B) -> B;
to_bin(L) when is_list(L) -> list_to_binary(L);
to_bin(A) when is_atom(A) -> atom_to_binary(A, utf8).

to_atom(A) when is_atom(A) -> A;
to_atom(B) when is_binary(B) -> binary_to_atom(B, utf8);
to_atom(L) when is_list(L) -> list_to_atom(L).
