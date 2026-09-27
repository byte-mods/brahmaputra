%% @doc A partition consumer with no group coordination.
%%
%% Reads one partition at a time. The handle is a plain map over a linked
%% {@link brahmaputra_router}; every fetch goes straight to the partition
%% leader's connection, so several processes may share one handle.
-module(brahmaputra_consumer).

-include("brahmaputra.hrl").

-export([new/2, close/1, router/1, config/1, partitions/2, list_offsets/4,
         fetch/5, fetch_verbose/5, default_config/0, earliest/0, latest/0]).

-export_type([consumer/0, consumed_record/0]).

-type consumer() :: #{router := pid(), config := map()}.
-type consumed_record() ::
        #{topic := binary(), partition := integer(), offset := integer(),
          key := binary() | undefined, value := binary() | undefined,
          timestamp := integer(),
          headers := [brahmaputra_protocol:header()]}.

%% @doc Defaults, named as Kafka names them.
%%
%% <ul>
%%   <li>`fetch_max_bytes' — caps one response.</li>
%%   <li>`fetch_min_bytes' — return early once this much is ready.</li>
%%   <li>`fetch_max_wait_ms' — long-poll ceiling when caught up.</li>
%%   <li>`max_poll_records' — used by the group consumer.</li>
%%   <li>`isolation_level' — `read_uncommitted' or `read_committed'.</li>
%%   <li>`client_rack' — this consumer's failure domain.</li>
%% </ul>
default_config() ->
    #{client_id => ?DEFAULT_CLIENT_ID,
      fetch_max_bytes => 8 * 1024 * 1024,
      fetch_min_bytes => 1,
      fetch_max_wait_ms => 500,
      isolation_level => read_uncommitted,
      client_rack => <<>>,
      max_poll_records => 500,
      request_timeout_ms => 30000,
      connect_timeout_ms => 30000}.

earliest() -> ?EARLIEST.
latest() -> ?LATEST.

-spec new(term(), map()) -> {ok, consumer()} | {error, term()}.
new(Address, Opts) ->
    Config = maps:merge(default_config(), Opts),
    case brahmaputra_router:start_link(Address, Config) of
        {ok, Router} -> {ok, #{router => Router, config => Config}};
        {error, _} = E -> E
    end.

close(#{router := Router}) ->
    unlink(Router),
    brahmaputra_router:stop(Router).

router(#{router := Router}) -> Router.
config(#{config := Config}) -> Config.

partitions(#{router := Router}, Topic) ->
    brahmaputra_router:partitions(Router, Topic).

%% @doc Resolve `earliest', `latest' or a unix-ms timestamp to an offset.
-spec list_offsets(consumer(), binary(), integer(), earliest | latest | integer()) ->
          {ok, integer()} | {error, term()}.
list_offsets(C, Topic, Partition, earliest) -> list_offsets(C, Topic, Partition, ?EARLIEST);
list_offsets(C, Topic, Partition, latest) -> list_offsets(C, Topic, Partition, ?LATEST);
list_offsets(#{router := Router, config := Config}, Topic0, Partition, Timestamp) ->
    P = brahmaputra_protocol,
    Topic = to_bin(Topic0),
    Body = P:body([P:enc_string(Topic), P:enc_int32(Partition), P:enc_int64(Timestamp)]),
    maybe_request(Router, Topic, Partition, ?API_LIST_OFFSETS, Body,
                  maps:get(request_timeout_ms, Config),
                  fun(Resp) ->
                      P:decode_body(Resp, fun(R0) ->
                          {_, R1} = P:dec_string(R0),
                          {_, R2} = P:dec_int32(R1),
                          {Code, R3} = P:dec_int32(R2),
                          {Offset, R4} = P:dec_int64(R3),
                          {_, _} = P:dec_int64(R4),
                          {Code, Offset}
                      end)
                  end,
                  fun({?ERR_NONE, Offset}) -> {ok, Offset};
                     ({Code, _}) -> {error, P:server_error(Code, {list_offsets, Topic, Partition})}
                  end).

%% @doc Fetch from one partition starting at Offset.
-spec fetch(consumer(), binary(), integer(), integer(), integer()) ->
          {ok, [consumed_record()]} | {error, term()}.
fetch(C, Topic, Partition, Offset, MaxWaitMs) ->
    case fetch_verbose(C, Topic, Partition, Offset, MaxWaitMs) of
        {ok, Records, _HighWatermark} -> {ok, Records};
        {error, _} = E -> E
    end.

%% @doc Like {@link fetch/5} but also returns the high watermark.
-spec fetch_verbose(consumer(), binary(), integer(), integer(), integer()) ->
          {ok, [consumed_record()], integer()} | {error, term()}.
fetch_verbose(#{router := Router, config := Config}, Topic0, Partition, Offset, MaxWait0) ->
    P = brahmaputra_protocol,
    Topic = to_bin(Topic0),
    MaxWait = min(MaxWait0, maps:get(fetch_max_wait_ms, Config)),
    Isolation = case maps:get(isolation_level, Config) of
                    read_committed -> ?READ_COMMITTED;
                    ?READ_COMMITTED -> ?READ_COMMITTED;
                    _ -> ?READ_UNCOMMITTED
                end,
    Body = P:body([P:enc_string(Topic), P:enc_int32(Partition), P:enc_int64(Offset),
                   P:enc_int32(maps:get(fetch_max_bytes, Config)),
                   P:enc_int32(MaxWait),
                   P:enc_int32(maps:get(fetch_min_bytes, Config)),
                   P:enc_int32(Isolation),
                   P:enc_string(maps:get(client_rack, Config))]),
    Timeout = maps:get(request_timeout_ms, Config) + MaxWait,
    maybe_request(Router, Topic, Partition, ?API_FETCH, Body, Timeout,
                  fun decode_fetch/1,
                  fun({?ERR_NONE, HighWatermark, Batches}) ->
                          {ok, to_consumed(Topic, Partition, Offset, Batches), HighWatermark};
                     ({Code, _, _}) ->
                          {error, P:server_error(Code, {fetch, Topic, Partition})}
                  end).

%% Send to the leader; on NOT_LEADER_OR_FOLLOWER refresh the route once and
%% resend, since the most common cause is a leader that moved.
maybe_request(Router, Topic, Partition, ApiKey, Body, Timeout, Decode, Finish) ->
    Attempt =
        fun() ->
            case brahmaputra_router:conn_for(Router, Topic, Partition) of
                {ok, Conn} ->
                    case brahmaputra_conn:request(Conn, ApiKey, Body, Timeout) of
                        {ok, Resp} -> Decode(Resp);
                        {error, _} = E -> E
                    end;
                {error, _} = E ->
                    E
            end
        end,
    case Attempt() of
        {ok, Decoded} when element(1, Decoded) =:= ?ERR_NOT_LEADER_OR_FOLLOWER ->
            case brahmaputra_router:refresh(Router, Topic) of
                {ok, _} ->
                    case Attempt() of
                        {ok, D2} -> Finish(D2);
                        {error, _} = E -> E
                    end;
                {error, _} = E ->
                    E
            end;
        {ok, Decoded} ->
            Finish(Decoded);
        {error, _} = E ->
            E
    end.

decode_fetch(Resp) ->
    P = brahmaputra_protocol,
    Decoded = P:decode_body(Resp, fun(R0) ->
        {_, R1} = P:dec_string(R0),
        {_, R2} = P:dec_int32(R1),
        {Code, R3} = P:dec_int32(R2),
        {HighWatermark, R4} = P:dec_int64(R3),
        {_LastStable, R5} = P:dec_int64(R4),
        {BatchesLen, R6} = P:dec_int64(R5),
        %% Read though unused: the batches trail the struct, so skipping a
        %% field would decode them from the wrong offset.
        {_PreferredReplica, R7} = P:dec_int32(R6),
        BatchesLen =< byte_size(R7) orelse throw({decode_error, fetch_batches_overrun}),
        {Code, HighWatermark, binary:part(R7, 0, BatchesLen)}
    end),
    case Decoded of
        {ok, {Code, HighWatermark, Raw}} ->
            case P:decode_record_batches(Raw) of
                {ok, Batches} -> {ok, {Code, HighWatermark, Batches}};
                {error, _} = E -> E
            end;
        {error, _} = E ->
            E
    end.

to_consumed(Topic, Partition, FromOffset, Batches) ->
    lists:append(
      [begin
           Indexed = lists:zip(lists:seq(0, length(Records) - 1), Records),
           %% A batch can start before the requested offset; skip what the
           %% caller has already seen.
           [#{topic => Topic, partition => Partition, offset => Base + I,
              key => K, value => V, timestamp => MaxTs + Delta, headers => H}
            || {I, #{key := K, value := V, timestamp_delta := Delta, headers := H}}
                   <- Indexed,
               Base + I >= FromOffset]
       end
       || #{base_offset := Base, max_timestamp := MaxTs, records := Records} <- Batches]).

to_bin(B) when is_binary(B) -> B;
to_bin(L) when is_list(L) -> list_to_binary(L).
