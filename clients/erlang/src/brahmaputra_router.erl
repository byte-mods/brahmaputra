%% @doc Metadata cache and leader routing.
%%
%% Holds one {@link brahmaputra_conn} per broker and the latest cluster
%% metadata. Metadata is refreshed only when a request reports the route
%% stale, because refreshing per request would put the control plane on the
%% data path. The router hands out connection pids; the requests themselves
%% go straight to the connection, never through this process.
%%
%% Connections are linked and exits are trapped: a broker that drops its
%% socket is simply redialled the next time it is needed.
-module(brahmaputra_router).
-behaviour(gen_server).

-include("brahmaputra.hrl").

-export([start_link/2, stop/1, seed/1, metadata/3, refresh/2, partitions/2,
         conn_for/3, leader_of/3, partitions_of/2, parse_address/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-export_type([metadata/0]).

-type broker() :: #{node_id := integer(), host := binary(), port := integer(),
                    rack := binary()}.
-type partition_info() :: #{partition := integer(), leader := integer(),
                            replicas := [integer()], isr := [integer()],
                            leader_epoch := integer()}.
-type metadata() :: #{brokers := [broker()],
                      topics := #{binary() => [partition_info()]}}.

-record(state, {host :: string(),
                port :: inet:port_number(),
                client_id :: binary(),
                connect_timeout :: timeout(),
                seed :: pid() | undefined,
                conns = #{} :: #{integer() => pid()},
                metadata :: metadata() | undefined}).

%% @doc Address is `"host:port"', `<<"host:port">>' or `{Host, Port}'.
-spec start_link(term(), map()) -> {ok, pid()} | {error, term()}.
start_link(Address, Opts) ->
    {Host, Port} = parse_address(Address),
    ClientId = to_bin(maps:get(client_id, Opts, ?DEFAULT_CLIENT_ID)),
    Timeout = maps:get(connect_timeout_ms, Opts, 30000),
    gen_server:start_link(?MODULE, {Host, Port, ClientId, Timeout}, []).

stop(Router) ->
    try gen_server:stop(Router) catch exit:_ -> ok end.

%% @doc The connection this router was opened with.
-spec seed(pid()) -> {ok, pid()} | {error, term()}.
seed(Router) -> gen_server:call(Router, seed, infinity).

%% @doc Cluster metadata, from cache unless Refresh is true. An empty
%% topic list asks for every topic.
-spec metadata(pid(), [binary()], boolean()) -> {ok, metadata()} | {error, term()}.
metadata(Router, Topics, Refresh) ->
    gen_server:call(Router, {metadata, [to_bin(T) || T <- Topics], Refresh}, infinity).

refresh(Router, Topic) -> metadata(Router, [Topic], true).

%% @doc A topic's partition ids in ascending order, creating it implicitly
%% if the broker auto-creates on first reference.
-spec partitions(pid(), binary()) -> {ok, [integer()]} | {error, term()}.
partitions(Router, Topic) -> gen_server:call(Router, {partitions, to_bin(Topic)}, infinity).

%% @doc The connection to a partition's leader.
-spec conn_for(pid(), binary(), integer()) -> {ok, pid()} | {error, term()}.
conn_for(Router, Topic, Partition) ->
    gen_server:call(Router, {conn_for, to_bin(Topic), Partition}, infinity).

-spec partitions_of(metadata(), binary()) -> [integer()].
partitions_of(#{topics := Topics}, Topic) ->
    lists:sort([P || #{partition := P} <- maps:get(Topic, Topics, [])]).

-spec leader_of(metadata(), binary(), integer()) -> integer().
leader_of(#{topics := Topics}, Topic, Partition) ->
    case [L || #{partition := P, leader := L} <- maps:get(Topic, Topics, []), P =:= Partition] of
        [Leader | _] -> Leader;
        [] -> -1
    end.

parse_address({Host, Port}) -> {to_list(Host), Port};
parse_address(Address) ->
    S = to_list(Address),
    case string:split(S, ":", trailing) of
        [Host, Port] -> {Host, list_to_integer(Port)};
        [Host] -> {Host, 9092}
    end.

%% ---------------------------------------------------------------------------
%% gen_server
%% ---------------------------------------------------------------------------

init({Host, Port, ClientId, Timeout}) ->
    process_flag(trap_exit, true),
    case brahmaputra_conn:start_link(Host, Port, ClientId, Timeout) of
        {ok, Seed} ->
            {ok, #state{host = Host, port = Port, client_id = ClientId,
                        connect_timeout = Timeout, seed = Seed}};
        {error, Reason} ->
            {stop, Reason}
    end.

handle_call(seed, _From, State) ->
    case ensure_seed(State) of
        {ok, S} -> {reply, {ok, S#state.seed}, S};
        {error, _} = E -> {reply, E, State}
    end;
handle_call({metadata, Topics, Refresh}, _From, State) ->
    {Reply, S} = do_metadata(Topics, Refresh, State),
    {reply, Reply, S};
handle_call({partitions, Topic}, _From, State) ->
    {Reply, S} = do_partitions(Topic, State),
    {reply, Reply, S};
handle_call({conn_for, Topic, Partition}, _From, State) ->
    {Reply, S} = do_conn_for(Topic, Partition, State),
    {reply, Reply, S}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, _Reason}, State = #state{seed = Pid}) ->
    Conns = maps:filter(fun(_, C) -> C =/= Pid end, State#state.conns),
    {noreply, State#state{seed = undefined, conns = Conns}};
handle_info({'EXIT', Pid, _Reason}, State) ->
    Conns = maps:filter(fun(_, C) -> C =/= Pid end, State#state.conns),
    {noreply, State#state{conns = Conns}};
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    Pids = lists:usort([P || P <- [State#state.seed | maps:values(State#state.conns)],
                             is_pid(P)]),
    lists:foreach(fun brahmaputra_conn:stop/1, Pids).

%% ---------------------------------------------------------------------------
%% internals
%% ---------------------------------------------------------------------------

ensure_seed(State = #state{seed = undefined}) ->
    case brahmaputra_conn:start_link(State#state.host, State#state.port,
                                     State#state.client_id, State#state.connect_timeout) of
        {ok, Seed} -> {ok, State#state{seed = Seed}};
        {error, _} = E -> E
    end;
ensure_seed(State) ->
    {ok, State}.

do_metadata(_Topics, false, State = #state{metadata = M}) when M =/= undefined ->
    {{ok, M}, State};
do_metadata(Topics, _Refresh, State0) ->
    case ensure_seed(State0) of
        {error, _} = E ->
            {E, State0};
        {ok, State} ->
            P = brahmaputra_protocol,
            Body = P:body(P:enc_string_array(Topics)),
            case brahmaputra_conn:request(State#state.seed, ?API_METADATA, Body) of
                {ok, Resp} ->
                    case P:decode_body(Resp, fun decode_metadata/1) of
                        {ok, M} -> {{ok, merge(State#state.metadata, M)},
                                    State#state{metadata = merge(State#state.metadata, M)}};
                        {error, _} = E -> {E, State}
                    end;
                {error, _} = E ->
                    {E, State}
            end
    end.

%% A refresh for one topic must not forget the others already cached.
merge(undefined, New) -> New;
merge(#{topics := Old}, New = #{topics := Fresh}) ->
    New#{topics := maps:merge(Old, Fresh)}.

do_partitions(Topic, State0) ->
    case do_metadata([Topic], false, State0) of
        {{ok, M0}, State1} ->
            case partitions_of(M0, Topic) of
                [] ->
                    %% A topic auto-created on first reference is not in the
                    %% cached image yet; one refresh tells "new" from "absent".
                    case do_metadata([Topic], true, State1) of
                        {{ok, M1}, State2} ->
                            case partitions_of(M1, Topic) of
                                [] -> {{error, {no_partitions, Topic}}, State2};
                                Ps -> {{ok, Ps}, State2}
                            end;
                        Other ->
                            Other
                    end;
                Ps ->
                    {{ok, Ps}, State1}
            end;
        Other ->
            Other
    end.

do_conn_for(Topic, Partition, State0) ->
    case do_metadata([Topic], false, State0) of
        {{ok, M0}, State1} ->
            case leader_of(M0, Topic, Partition) of
                L when L < 0 ->
                    case do_metadata([Topic], true, State1) of
                        {{ok, M1}, State2} ->
                            connect_leader(leader_of(M1, Topic, Partition), M1,
                                           Topic, Partition, State2);
                        Other ->
                            Other
                    end;
                L ->
                    connect_leader(L, M0, Topic, Partition, State1)
            end;
        Other ->
            Other
    end.

connect_leader(Leader, _M, Topic, Partition, State) when Leader < 0 ->
    {{error, {no_leader, Topic, Partition}}, State};
connect_leader(Leader, #{brokers := Brokers}, _Topic, _Partition, State) ->
    case maps:find(Leader, State#state.conns) of
        {ok, Conn} ->
            {{ok, Conn}, State};
        error ->
            case [B || B = #{node_id := Id} <- Brokers, Id =:= Leader] of
                [] ->
                    {{error, {unknown_broker, Leader}}, State};
                [_] when length(Brokers) =:= 1 ->
                    %% A single-broker cluster advertises the address it was
                    %% configured with, which may not be the one we dialled;
                    %% reuse the seed instead of a second connection.
                    case ensure_seed(State) of
                        {ok, S} ->
                            {{ok, S#state.seed},
                             S#state{conns = maps:put(Leader, S#state.seed, S#state.conns)}};
                        {error, _} = E ->
                            {E, State}
                    end;
                [#{host := Host, port := Port} | _] ->
                    case brahmaputra_conn:start_link(Host, Port, State#state.client_id,
                                                     State#state.connect_timeout) of
                        {ok, Conn} ->
                            {{ok, Conn},
                             State#state{conns = maps:put(Leader, Conn, State#state.conns)}};
                        {error, _} = E ->
                            {E, State}
                    end
            end
    end.

%% Field order is exactly the schema's: error_code, brokers, controller_id,
%% topics. The leading code is request-level (an authorization denial),
%% distinct from the per-topic one.
decode_metadata(R0) ->
    P = brahmaputra_protocol,
    {Code, R1} = P:dec_int32(R0),
    Code =:= ?ERR_NONE orelse throw(P:server_error(Code, metadata)),
    {Brokers, R2} = P:dec_array(R1, fun(B0) ->
        {Id, B1} = P:dec_int32(B0),
        {Host, B2} = P:dec_string(B1),
        {Port, B3} = P:dec_int32(B2),
        {Rack, B4} = P:dec_string(B3),
        {#{node_id => Id, host => Host, port => Port, rack => Rack}, B4}
    end),
    {_Controller, R3} = P:dec_int32(R2),
    {Topics, _} = P:dec_array(R3, fun(T0) ->
        {Name, T1} = P:dec_string(T0),
        {TopicErr, T2} = P:dec_int32(T1),
        {Parts, T3} = P:dec_array(T2, fun(Q0) ->
            {Id, Q1} = P:dec_int32(Q0),
            {Leader, Q2} = P:dec_int32(Q1),
            {Replicas, Q3} = P:dec_array(Q2, fun P:dec_int32/1),
            {Isr, Q4} = P:dec_array(Q3, fun P:dec_int32/1),
            {Epoch, Q5} = P:dec_int32(Q4),
            {#{partition => Id, leader => Leader, replicas => Replicas, isr => Isr,
               leader_epoch => Epoch}, Q5}
        end),
        case TopicErr of
            ?ERR_NONE -> ok;
            ?ERR_UNKNOWN_TOPIC_OR_PARTITION -> ok;
            _ -> throw(P:server_error(TopicErr, {metadata, Name}))
        end,
        {{Name, Parts}, T3}
    end),
    #{brokers => Brokers, topics => maps:from_list(Topics)}.

to_bin(B) when is_binary(B) -> B;
to_bin(L) when is_list(L) -> list_to_binary(L);
to_bin(A) when is_atom(A) -> atom_to_binary(A, utf8).

to_list(B) when is_binary(B) -> binary_to_list(B);
to_list(L) when is_list(L) -> L.
