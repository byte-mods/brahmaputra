%% @doc A batching producer.
%%
%% A gen_server that buffers records per partition and sends each buffer
%% as one Produce request. A buffer goes out when it reaches `batch_size'
%% bytes or `linger_ms' after its first record arrived (an
%% `erlang:send_after/3' timer per partition), whichever comes first.
%%
%% `buffer_memory' bounds the bytes held client-side. A `send' that would
%% exceed it is parked (its caller stays blocked) until a flush frees room
%% or `max_block_ms' passes, when it fails with `{error, {buffer_full, _}}'
%% rather than letting a producer faster than its broker grow without
%% limit.
%%
%% Share one producer between processes rather than starting one per
%% message: the batching is the point.
-module(brahmaputra_producer).
-behaviour(gen_server).

-include("brahmaputra.hrl").

-export([start_link/2, stop/1, close/1, send/3, send/4, send_to/4, send_to/5,
         send_sync/3, send_sync/4, flush/1, router/1, default_config/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(state, {config :: map(),
                codec :: brahmaputra_protocol:codec(),
                router :: pid(),
                buffers = #{} :: #{{binary(), integer()} => [map()]},
                sizes = #{} :: #{{binary(), integer()} => non_neg_integer()},
                timers = #{} :: #{{binary(), integer()} => reference()},
                buffered_bytes = 0 :: non_neg_integer(),
                round_robin = 0 :: non_neg_integer(),
                waiters = queue:new() :: queue:queue(),
                last_error = ok :: ok | {error, term()}}).

%% @doc Defaults, named as Kafka names them.
%%
%% <ul>
%%   <li>`acks' — 0 fire-and-forget, 1 leader append, -1 (or `all') every
%%       in-sync replica.</li>
%%   <li>`batch_size' — flush a partition buffer at this many bytes.</li>
%%   <li>`linger_ms' — flush a non-empty buffer this long after its first
%%       record; 0 sends each record immediately.</li>
%%   <li>`compression' — `none' or `gzip' built in; `lz4', `zstd',
%%       `snappy' after {@link brahmaputra_protocol:register_codec/3}.</li>
%%   <li>`request_timeout_ms', `retries', `retry_backoff_ms',
%%       `delivery_timeout_ms' — retries apply only to errors the broker
%%       returns before appending, so a retry cannot duplicate.</li>
%%   <li>`buffer_memory', `max_block_ms' — the bounded client buffer.</li>
%% </ul>
default_config() ->
    #{client_id => ?DEFAULT_CLIENT_ID,
      acks => 1,
      batch_size => 16 * 1024,
      linger_ms => 5,
      compression => none,
      request_timeout_ms => 30000,
      retries => 5,
      retry_backoff_ms => 100,
      delivery_timeout_ms => 120000,
      buffer_memory => 32 * 1024 * 1024,
      max_block_ms => 60000,
      connect_timeout_ms => 30000}.

-spec start_link(term(), map()) -> {ok, pid()} | {error, term()}.
start_link(Address, Opts) ->
    Config0 = maps:merge(default_config(), Opts),
    Config = Config0#{acks := normalize_acks(maps:get(acks, Config0))},
    case brahmaputra_protocol:parse_compression(maps:get(compression, Config)) of
        {ok, Codec} ->
            %% An unregistered codec fails here rather than on the first
            %% (possibly background) flush.
            case brahmaputra_protocol:codec_available(Codec) of
                true -> gen_server:start_link(?MODULE, {Address, Config, Codec}, []);
                false -> {error, {codec_not_registered, Codec}}
            end;
        {error, _} = E -> E
    end.

%% @doc Flush every buffer, then stop.
-spec close(pid()) -> ok | {error, term()}.
close(Producer) ->
    Result = flush(Producer),
    stop(Producer),
    Result.

stop(Producer) ->
    try gen_server:stop(Producer) catch exit:_ -> ok end.

%% @doc Buffer one record, partitioned by murmur2(key) when Key is a
%% binary and round-robin when it is `undefined'. Value `undefined' is a
%% tombstone, distinct from `<<>>'.
%%
%% Options: `headers' — `[{Key, Value | undefined}]'; `timestamp' — unix
%% ms, defaulting to now.
send(Producer, Topic, Value) -> send(Producer, Topic, Value, #{}).

send(Producer, Topic, Value, Opts) ->
    call(Producer, {send, to_bin(Topic), undefined, Value, Opts}).

%% @doc Buffer one record on an explicit partition, bypassing the
%% partitioner.
send_to(Producer, Topic, Partition, Value) -> send_to(Producer, Topic, Partition, Value, #{}).

send_to(Producer, Topic, Partition, Value, Opts) ->
    call(Producer, {send, to_bin(Topic), Partition, Value, Opts}).

%% @doc Send one record on its own and return its offset. A full round
%% trip per record: correct, and slow. Options are those of {@link send/4}
%% plus `partition', an explicit partition.
send_sync(Producer, Topic, Value) -> send_sync(Producer, Topic, Value, #{}).

send_sync(Producer, Topic, Value, Opts) ->
    call(Producer, {send_sync, to_bin(Topic), Value, Opts}).

%% @doc Send everything buffered and wait for acknowledgement. Also
%% reports a failure from a background (linger-triggered) flush since the
%% last call.
-spec flush(pid()) -> ok | {error, term()}.
flush(Producer) -> call(Producer, flush).

router(Producer) -> call(Producer, router).

call(Producer, Msg) -> gen_server:call(Producer, Msg, infinity).

%% ---------------------------------------------------------------------------
%% gen_server
%% ---------------------------------------------------------------------------

init({Address, Config, Codec}) ->
    process_flag(trap_exit, true),
    case brahmaputra_router:start_link(Address, Config) of
        {ok, Router} -> {ok, #state{config = Config, codec = Codec, router = Router}};
        {error, Reason} -> {stop, Reason}
    end.

handle_call({send, Topic, Partition0, Value, Opts}, From, State) ->
    Key = maps:get(key, Opts, undefined),
    case choose_partition(Topic, Partition0, Key, State) of
        {ok, Partition, S1} ->
            Headers = maps:get(headers, Opts, []),
            Item = #{key => Key, value => Value, headers => Headers,
                     created_ms => maps:get(timestamp, Opts, now_ms())},
            Size = record_size(Item),
            admit({Topic, Partition}, Item, Size, From, S1);
        {error, _} = E ->
            {reply, E, State}
    end;
handle_call({send_sync, Topic, Value, Opts}, _From, State) ->
    Key = maps:get(key, Opts, undefined),
    case choose_partition(Topic, maps:get(partition, Opts, undefined), Key, State) of
        {ok, Partition, S1} ->
            Item = #{key => Key, value => Value, headers => maps:get(headers, Opts, []),
                     created_ms => maps:get(timestamp, Opts, now_ms())},
            %% Anything already buffered for this partition goes first, so a
            %% synchronous send never overtakes an earlier asynchronous one.
            case flush_slot({Topic, Partition}, S1) of
                {ok, S2} -> {reply, produce(Topic, Partition, [Item], S2), S2};
                {{error, _} = E, S2} -> {reply, E, S2}
            end;
        {error, _} = E ->
            {reply, E, State}
    end;
handle_call(flush, _From, State) ->
    {Result, S1} = flush_all(State),
    Reply = case {Result, S1#state.last_error} of
                {ok, Last} -> Last;
                {Err, _} -> Err
            end,
    {reply, Reply, S1#state{last_error = ok}};
handle_call(router, _From, State) ->
    {reply, State#state.router, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({linger, Slot}, State) ->
    S0 = State#state{timers = maps:remove(Slot, State#state.timers)},
    {Result, S1} = flush_slot(Slot, S0),
    %% A background flush that fails must not crash the producer; the next
    %% explicit flush surfaces the error to a caller who can act on it.
    S2 = case Result of
             ok -> S1;
             {error, _} -> S1#state{last_error = Result}
         end,
    {noreply, S2};
handle_info({block_timeout, Ref}, State) ->
    Waiters = queue:filter(
                fun({R, From, _, _, _}) when R =:= Ref ->
                        gen_server:reply(From, {error, {buffer_full,
                            #{buffered_bytes => State#state.buffered_bytes,
                              buffer_memory => maps:get(buffer_memory, State#state.config),
                              max_block_ms => maps:get(max_block_ms, State#state.config)}}}),
                        false;
                   (_) -> true
                end, State#state.waiters),
    {noreply, State#state{waiters = Waiters}};
handle_info({'EXIT', Router, Reason}, State = #state{router = Router}) ->
    {stop, {router_down, Reason}, State};
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    queue:fold(fun({_, From, _, _, _}, _) -> gen_server:reply(From, {error, closed}) end,
               ok, State#state.waiters),
    brahmaputra_router:stop(State#state.router).

%% ---------------------------------------------------------------------------
%% buffering
%% ---------------------------------------------------------------------------

admit(Slot, Item, Size, From, State = #state{config = Config}) ->
    Limit = maps:get(buffer_memory, Config),
    Fits = Limit =< 0 orelse Size >= Limit
        orelse State#state.buffered_bytes + Size =< Limit,
    %% Parked senders keep FIFO order: a small record may not overtake a
    %% large one that has been waiting.
    case Fits andalso queue:is_empty(State#state.waiters) of
        true ->
            enqueue(Slot, Item, Size, State);
        false ->
            Ref = make_ref(),
            _ = erlang:send_after(maps:get(max_block_ms, Config), self(),
                                  {block_timeout, Ref}),
            Waiters = queue:in({Ref, From, Slot, Item, Size}, State#state.waiters),
            {noreply, State#state{waiters = Waiters}}
    end.

%% Buffer one admitted record. A record larger than the whole budget is
%% admitted rather than waiting forever; refusing oversized records is the
%% broker's job.
enqueue(Slot, Item, Size, State0 = #state{config = Config}) ->
    Buffer = maps:get(Slot, State0#state.buffers, []),
    SlotSize = maps:get(Slot, State0#state.sizes, 0) + Size,
    State1 = State0#state{buffers = maps:put(Slot, [Item | Buffer], State0#state.buffers),
                          sizes = maps:put(Slot, SlotSize, State0#state.sizes),
                          buffered_bytes = State0#state.buffered_bytes + Size},
    Linger = maps:get(linger_ms, Config),
    case Linger =< 0 orelse SlotSize >= maps:get(batch_size, Config) of
        true ->
            {Result, State2} = flush_slot(Slot, State1),
            {reply, Result, State2};
        false ->
            State2 = case maps:is_key(Slot, State1#state.timers) of
                         true -> State1;
                         false ->
                             TRef = erlang:send_after(Linger, self(), {linger, Slot}),
                             State1#state{timers = maps:put(Slot, TRef, State1#state.timers)}
                     end,
            {reply, ok, State2}
    end.

%% Admit parked senders, oldest first, while there is room.
drain_waiters(State = #state{config = Config}) ->
    Limit = maps:get(buffer_memory, Config),
    case queue:peek(State#state.waiters) of
        {value, {_Ref, From, Slot, Item, Size}}
          when State#state.buffered_bytes + Size =< Limit ->
            S1 = State#state{waiters = queue:drop(State#state.waiters)},
            case enqueue(Slot, Item, Size, S1) of
                {reply, Reply, S2} ->
                    gen_server:reply(From, Reply),
                    drain_waiters(S2)
            end;
        _ ->
            State
    end.

flush_all(State) ->
    Slots = [Slot || {Slot, [_ | _]} <- maps:to_list(State#state.buffers)],
    lists:foldl(fun(Slot, {ok, S}) -> flush_slot(Slot, S);
                   (Slot, {Err, S}) -> {_, S1} = flush_slot(Slot, S), {Err, S1}
                end, {ok, State}, Slots).

flush_slot(Slot = {Topic, Partition}, State) ->
    case maps:get(Slot, State#state.buffers, []) of
        [] ->
            {ok, State};
        Reversed ->
            case maps:take(Slot, State#state.timers) of
                {TRef, Timers} -> _ = erlang:cancel_timer(TRef);
                error -> Timers = State#state.timers
            end,
            Size = maps:get(Slot, State#state.sizes, 0),
            S1 = State#state{buffers = maps:remove(Slot, State#state.buffers),
                             sizes = maps:remove(Slot, State#state.sizes),
                             timers = Timers,
                             buffered_bytes = max(0, State#state.buffered_bytes - Size)},
            Result = case produce(Topic, Partition, lists:reverse(Reversed), S1) of
                         {ok, _} -> ok;
                         {error, _} = E -> E
                     end,
            {Result, drain_waiters(S1)}
    end.

record_size(#{key := Key, value := Value, headers := Headers}) ->
    bsize(Key) + bsize(Value) + 16
        + lists:sum([bsize(K) + bsize(V) + 4 || {K, V} <- Headers]).

bsize(undefined) -> 0;
bsize(B) -> byte_size(B).

choose_partition(_Topic, Partition, _Key, State) when is_integer(Partition) ->
    {ok, Partition, State};
choose_partition(Topic, undefined, Key, State) ->
    case brahmaputra_router:partitions(State#state.router, Topic) of
        {ok, Partitions} when Key =/= undefined ->
            {ok, brahmaputra_protocol:partition_for_key(Key, Partitions), State};
        {ok, Partitions} ->
            N = State#state.round_robin,
            {ok, lists:nth(N rem length(Partitions) + 1, Partitions),
             State#state{round_robin = N + 1}};
        {error, _} = E ->
            E
    end.

%% ---------------------------------------------------------------------------
%% produce
%% ---------------------------------------------------------------------------

produce(Topic, Partition, Items, #state{config = Config, codec = Codec, router = Router}) ->
    P = brahmaputra_protocol,
    %% The batch stores one max timestamp and a delta per record, so the
    %% rebasing happens here.
    MaxTs = lists:max([T || #{created_ms := T} <- Items]),
    Records = [#{key => K, value => V, headers => H, timestamp_delta => T - MaxTs}
               || #{key := K, value := V, headers := H, created_ms := T} <- Items],
    case P:encode_record_batch(Records, MaxTs, Codec) of
        {error, _} = E ->
            E;
        {ok, Encoded} ->
            Acks = maps:get(acks, Config),
            Body = P:body([P:enc_string(Topic), P:enc_int32(Partition), P:enc_int32(Acks),
                           P:enc_int32(maps:get(request_timeout_ms, Config)),
                           P:enc_int64(byte_size(Encoded)), Encoded]),
            case Acks of
                0 ->
                    case brahmaputra_router:conn_for(Router, Topic, Partition) of
                        {ok, Conn} ->
                            case brahmaputra_conn:send_oneway(Conn, ?API_PRODUCE, Body) of
                                ok -> {ok, -1};
                                {error, _} = E -> E
                            end;
                        {error, _} = E ->
                            E
                    end;
                _ ->
                    Deadline = now_ms() + maps:get(delivery_timeout_ms, Config),
                    produce_attempt(Topic, Partition, Body, maps:get(retries, Config),
                                    Deadline, Config, Router)
            end
    end.

produce_attempt(Topic, Partition, Body, AttemptsLeft, Deadline, Config, Router) ->
    P = brahmaputra_protocol,
    Timeout = maps:get(request_timeout_ms, Config) + 5000,
    Response =
        case brahmaputra_router:conn_for(Router, Topic, Partition) of
            {ok, Conn} -> brahmaputra_conn:request(Conn, ?API_PRODUCE, Body, Timeout);
            {error, _} = E0 -> E0
        end,
    Decoded =
        case Response of
            {ok, Resp} ->
                P:decode_body(Resp, fun(R0) ->
                    {_, R1} = P:dec_string(R0),
                    {_, R2} = P:dec_int32(R1),
                    {Code, R3} = P:dec_int32(R2),
                    {BaseOffset, R4} = P:dec_int64(R3),
                    {_LogAppendTime, _} = P:dec_int64(R4),
                    {Code, BaseOffset}
                end);
            {error, _} = E1 ->
                E1
        end,
    case Decoded of
        {ok, {?ERR_NONE, BaseOffset}} ->
            {ok, BaseOffset};
        {ok, {Code, _}} ->
            Retry = P:retriable(Code) andalso AttemptsLeft > 0 andalso now_ms() < Deadline,
            case Retry of
                false ->
                    {error, P:server_error(Code, {produce, Topic, Partition})};
                true ->
                    case lists:member(Code, [?ERR_NOT_LEADER_OR_FOLLOWER,
                                             ?ERR_FENCED_LEADER_EPOCH,
                                             ?ERR_UNKNOWN_LEADER_EPOCH]) of
                        %% A stale route is the most common retriable
                        %% cause; resending to the same broker repeats it.
                        true -> _ = brahmaputra_router:refresh(Router, Topic);
                        false -> ok
                    end,
                    timer:sleep(maps:get(retry_backoff_ms, Config)),
                    produce_attempt(Topic, Partition, Body, AttemptsLeft - 1, Deadline,
                                    Config, Router)
            end;
        {error, _} = E ->
            E
    end.

normalize_acks(all) -> -1;
normalize_acks(<<"all">>) -> -1;
normalize_acks("all") -> -1;
normalize_acks(N) when is_integer(N) -> N.

now_ms() -> erlang:system_time(millisecond).

to_bin(B) when is_binary(B) -> B;
to_bin(L) when is_list(L) -> list_to_binary(L).
