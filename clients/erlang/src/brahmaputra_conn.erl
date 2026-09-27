%% @doc One TCP connection to one broker.
%%
%% A gen_server that owns the socket. Requests are pipelined: each is
%% written immediately and parked under its correlation id until the
%% matching response arrives, so a slow long-poll fetch does not hold up a
%% heartbeat sharing the same connection. The socket runs with
%% `{packet, 4}', which is exactly the frame's big-endian int32 length
%% prefix.
-module(brahmaputra_conn).
-behaviour(gen_server).

-include("brahmaputra.hrl").

-export([start_link/3, start_link/4, start/4, request/3, request/4, send_oneway/3, stop/1,
         set_request_timeout/2, broken/1, api_versions/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% Bounds one round trip when the caller names no timeout. It must exceed
%% the longest the broker may legitimately hold a request (a fetch
%% long-poll, an acks=all wait, a JoinGroup waiting out a rebalance); its
%% job is to turn a wedged broker into an error, not a caller blocked
%% forever.
-define(DEFAULT_REQUEST_TIMEOUT, 120000).

-record(state, {socket :: gen_tcp:socket(),
                client_id :: binary(),
                request_timeout = ?DEFAULT_REQUEST_TIMEOUT :: timeout(),
                next = 0 :: integer(),
                pending = #{} :: #{integer() => {gen_server:from(), reference()}}}).

%% @doc Connect to Host:Port. Returns once the socket is open, or
%% `{error, Reason}' when it could not be.
-spec start_link(string() | binary(), inet:port_number(), binary()) ->
          {ok, pid()} | {error, term()}.
start_link(Host, Port, ClientId) ->
    start_link(Host, Port, ClientId, 30000).

start_link(Host, Port, ClientId, ConnectTimeout) ->
    gen_server:start_link(?MODULE, {to_list(Host), Port, ClientId, ConnectTimeout}, []).

%% @doc Like {@link start_link/4} but unlinked, for a caller that does not
%% trap exits and would rather see `{error, closed}' than share a crash.
start(Host, Port, ClientId, ConnectTimeout) ->
    gen_server:start(?MODULE, {to_list(Host), Port, ClientId, ConnectTimeout}, []).

%% @doc Change the round-trip bound used by {@link request/3}.
set_request_timeout(Conn, Timeout) ->
    gen_server:call(Conn, {set_request_timeout, Timeout}, infinity).

%% @doc Whether this connection failed and must not be reused. A timeout,
%% I/O error or undecodable frame leaves the byte stream at an unknown
%% position, so the connection stops rather than carrying on; the router
%% notices and redials.
-spec broken(pid()) -> boolean().
broken(Conn) ->
    try gen_server:call(Conn, ping, 5000) of
        pong -> false
    catch
        exit:_ -> true
    end.

%% @doc Send one request and wait for its response body, bounded by this
%% connection's request timeout.
-spec request(pid(), integer(), iodata()) -> {ok, binary()} | {error, term()}.
request(Conn, ApiKey, Body) ->
    request(Conn, ApiKey, Body, default).

-spec request(pid(), integer(), iodata(), timeout() | default) ->
          {ok, binary()} | {error, term()}.
request(Conn, ApiKey, Body, Timeout) ->
    try
        gen_server:call(Conn, {request, ApiKey, Body, Timeout}, infinity)
    catch
        exit:{noproc, _} -> {error, closed};
        exit:{normal, _} -> {error, closed};
        exit:{Reason, _} -> {error, {connection_down, Reason}}
    end.

%% @doc Send without awaiting a response (acks=0): the broker sends none.
-spec send_oneway(pid(), integer(), iodata()) -> ok | {error, term()}.
send_oneway(Conn, ApiKey, Body) ->
    try
        gen_server:call(Conn, {oneway, ApiKey, Body}, infinity)
    catch
        exit:{Reason, _} -> {error, {connection_down, Reason}}
    end.

stop(Conn) ->
    try gen_server:stop(Conn) catch exit:_ -> ok end.

%% @doc Ask the broker what it speaks. Returns the api ranges and the
%% broker's version string.
-spec api_versions(pid()) -> {ok, [{integer(), integer(), integer()}], binary()} | {error, term()}.
api_versions(Conn) ->
    P = brahmaputra_protocol,
    Body = P:body([P:enc_string(<<"brahmaputra-erlang">>), P:enc_string(<<"0.1.0">>)]),
    case request(Conn, ?API_API_VERSIONS, Body) of
        {ok, Resp} ->
            Decoded = P:decode_body(Resp, fun(R0) ->
                {Code, R1} = P:dec_int32(R0),
                {Ranges, R2} = P:dec_array(R1, fun(B0) ->
                    {K, B1} = P:dec_int32(B0),
                    {Min, B2} = P:dec_int32(B1),
                    {Max, B3} = P:dec_int32(B2),
                    {{K, Min, Max}, B3}
                end),
                {Version, _} = P:dec_string(R2),
                {Code, Ranges, Version}
            end),
            case Decoded of
                {ok, {?ERR_NONE, Ranges, Version}} -> {ok, Ranges, Version};
                {ok, {Code, _, _}} -> {error, P:server_error(Code, api_versions)};
                {error, _} = E -> E
            end;
        {error, _} = E ->
            E
    end.

%% ---------------------------------------------------------------------------
%% gen_server
%% ---------------------------------------------------------------------------

init({Host, Port, ClientId, ConnectTimeout}) ->
    %% Responses are small and latency matters more than packet count;
    %% without nodelay every request pays Nagle plus delayed ACK.
    Opts = [binary, {packet, 4}, {active, once}, {nodelay, true}, {keepalive, true}],
    case gen_tcp:connect(Host, Port, Opts, ConnectTimeout) of
        {ok, Socket} -> {ok, #state{socket = Socket, client_id = ClientId}};
        {error, Reason} -> {stop, {connect_failed, Reason}}
    end.

handle_call(ping, _From, State) ->
    {reply, pong, State};
handle_call({set_request_timeout, Timeout}, _From, State) ->
    {reply, ok, State#state{request_timeout = Timeout}};
handle_call({request, ApiKey, Body, default}, From, State) ->
    handle_call({request, ApiKey, Body, State#state.request_timeout}, From, State);
handle_call({request, ApiKey, Body, Timeout}, From, State) ->
    Corr = next_corr(State#state.next),
    Frame = brahmaputra_protocol:encode_frame(ApiKey, Corr, State#state.client_id, Body),
    case gen_tcp:send(State#state.socket, Frame) of
        ok ->
            TRef = case Timeout of
                       infinity -> make_ref();
                       _ -> erlang:send_after(Timeout, self(), {request_timeout, Corr})
                   end,
            Pending = maps:put(Corr, {From, TRef}, State#state.pending),
            {noreply, State#state{next = Corr, pending = Pending}};
        {error, Reason} ->
            {stop, {shutdown, {send_failed, Reason}}, {error, {send_failed, Reason}}, State}
    end;
handle_call({oneway, ApiKey, Body}, _From, State) ->
    Corr = next_corr(State#state.next),
    Frame = brahmaputra_protocol:encode_frame(ApiKey, Corr, State#state.client_id, Body),
    case gen_tcp:send(State#state.socket, Frame) of
        ok -> {reply, ok, State#state{next = Corr}};
        {error, Reason} -> {stop, {shutdown, {send_failed, Reason}}, {error, Reason}, State}
    end.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({tcp, Socket, Payload}, State = #state{socket = Socket}) ->
    ok = inet:setopts(Socket, [{active, once}]),
    case brahmaputra_protocol:decode_frame(Payload) of
        {ok, Corr, Body} ->
            case maps:take(Corr, State#state.pending) of
                {{From, TRef}, Pending} ->
                    _ = erlang:cancel_timer(TRef),
                    gen_server:reply(From, {ok, Body}),
                    {noreply, State#state{pending = Pending}};
                error ->
                    %% A response nobody waits on: its request timed out.
                    {noreply, State}
            end;
        {error, Reason} ->
            {stop, {shutdown, {bad_frame, Reason}}, State}
    end;
handle_info({tcp_closed, Socket}, State = #state{socket = Socket}) ->
    {stop, normal, State};
handle_info({tcp_error, Socket, Reason}, State = #state{socket = Socket}) ->
    {stop, {shutdown, {tcp_error, Reason}}, State};
handle_info({request_timeout, Corr}, State) ->
    case maps:take(Corr, State#state.pending) of
        {{From, _}, Pending} ->
            %% The response may still be on its way and the broker is not
            %% answering in time: give up on this connection entirely
            %% instead of leaving it pooled for the next request.
            gen_server:reply(From, {error, timeout}),
            {stop, {shutdown, request_timeout}, State#state{pending = Pending}};
        error ->
            {noreply, State}
    end;
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    maps:foreach(fun(_, {From, _}) -> gen_server:reply(From, {error, closed}) end,
                 State#state.pending),
    _ = gen_tcp:close(State#state.socket),
    ok.

next_corr(N) when N >= 16#7FFFFFFF -> 1;
next_corr(N) -> N + 1.

to_list(B) when is_binary(B) -> binary_to_list(B);
to_list(L) -> L.
