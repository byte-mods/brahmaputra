defmodule Brahmaputra.Connection do
  @moduledoc """
  One TCP connection to one broker.

  The process owns the socket and serialises request/response pairs, so
  there is at most one request in flight per connection — which is also
  what keeps a partition's appends in order. Frames use `:gen_tcp`'s
  `packet: 4` mode, which is exactly the protocol's big-endian int32 length
  prefix.

  Any I/O failure, timeout or correlation mismatch leaves the byte stream at
  an unknown position — a partial frame may have been written, or a late
  response may still arrive — so the socket is closed and the connection is
  marked broken rather than reused. It tells the process that opened it
  (`{:connection_broken, pid}`) before replying to the failed request, so a
  `Brahmaputra.Router` has already forgotten it by the time the caller
  retries, and redials.
  """
  use GenServer

  alias Brahmaputra.{Error, Protocol}

  @doc """
  Bounds one request/response round trip. It must exceed the longest the
  broker may legitimately hold a request (a fetch long-poll, an acks=all
  wait, a JoinGroup waiting out a rebalance), so it is generous; its job is
  to turn a wedged broker into an error instead of a process blocked forever.
  """
  def default_request_timeout, do: 120_000

  # -- API --------------------------------------------------------------------

  @doc """
  Opens a connection. `opts`: `:client_id`, `:connect_timeout` (ms),
  `:request_timeout` (ms, default `default_request_timeout/0`; 0 disables).
  """
  def start_link(host, port, opts \\ []) do
    Brahmaputra.Util.start_linked(__MODULE__, {host, port, self(), opts})
  end

  @doc """
  Sends one request and returns `{:ok, body}` for the matching response.
  `timeout` overrides the connection's request timeout for this call.
  """
  def request(conn, api_key, body, timeout \\ nil) do
    GenServer.call(conn, {:request, api_key, body, timeout}, :infinity)
  catch
    :exit, _ -> {:error, broken_error()}
  end

  @doc "Sends without awaiting a response (acks=0: the broker sends none)."
  def send_oneway(conn, api_key, body) do
    GenServer.call(conn, {:oneway, api_key, body}, :infinity)
  catch
    :exit, _ -> {:error, broken_error()}
  end

  @doc "Changes how long one round trip may take before the connection is abandoned."
  def set_request_timeout(conn, timeout_ms),
    do: GenServer.call(conn, {:set_request_timeout, timeout_ms}, :infinity)

  @doc "Whether this connection failed and must not be reused."
  def broken?(conn) do
    GenServer.call(conn, :broken?, :infinity)
  catch
    :exit, _ -> true
  end

  @doc """
  Asks the broker what it speaks: `{:ok, [{api_key, min, max}], broker_version}`.
  The one call that works across a version mismatch.
  """
  def api_versions(conn, client_name \\ "brahmaputra-elixir") do
    body = Protocol.body([Protocol.w_string(client_name), Protocol.w_string(Brahmaputra.version())])

    with {:ok, resp} <- request(conn, Protocol.api(:api_versions), body) do
      Brahmaputra.Consumer.guard(fn ->
        {code, rest} = resp |> Protocol.open_body() |> Protocol.r_int32()
        if code != 0, do: throw({:server_error, Protocol.server_error(code, "api_versions")})

        {ranges, rest} =
          Protocol.r_array(rest, fn data ->
            {key, data} = Protocol.r_int32(data)
            {min, data} = Protocol.r_int32(data)
            {max, data} = Protocol.r_int32(data)
            {{key, min, max}, data}
          end)

        {broker_version, _} = Protocol.r_string(rest)
        {:ok, ranges, broker_version}
      end)
    end
  end

  def close(conn) do
    GenServer.stop(conn, :normal)
  catch
    :exit, _ -> :ok
  end

  defp broken_error, do: %Error{message: "connection is broken; the router will redial"}

  # -- Server -----------------------------------------------------------------

  @impl true
  def init({host, port, owner, opts}) do
    host = if is_binary(host), do: String.to_charlist(host), else: host
    timeout = Keyword.get(opts, :connect_timeout, 30_000)

    socket_opts = [
      :binary,
      packet: 4,
      active: false,
      # Responses are small and latency matters more than packet count;
      # without nodelay every request pays Nagle plus the peer's delayed ACK.
      nodelay: true,
      send_timeout: 30_000,
      send_timeout_close: true
    ]

    case :gen_tcp.connect(host, port, socket_opts, timeout) do
      {:ok, socket} ->
        {:ok,
         %{
           socket: socket,
           owner: owner,
           client_id: Keyword.get(opts, :client_id, "brahmaputra-elixir"),
           timeout: Keyword.get(opts, :request_timeout, default_request_timeout()),
           next: 0,
           broken: false
         }}

      {:error, reason} ->
        {:stop, %Error{message: "connect #{host}:#{port} failed: #{inspect(reason)}"}}
    end
  end

  @impl true
  def handle_call({:request, _, _, _}, _from, %{broken: true} = state),
    do: {:reply, {:error, broken_error()}, state}

  def handle_call({:oneway, _, _}, _from, %{broken: true} = state),
    do: {:reply, {:error, broken_error()}, state}

  def handle_call({:request, api_key, body, timeout}, _from, state) do
    correlation_id = state.next + 1
    state = %{state | next: correlation_id}
    frame = Protocol.encode_frame_payload(api_key, correlation_id, state.client_id, body)
    recv_timeout = recv_timeout(timeout || state.timeout)

    with :ok <- :gen_tcp.send(state.socket, frame),
         {:ok, body} <- await(state.socket, correlation_id, recv_timeout) do
      {:reply, {:ok, body}, state}
    else
      # Includes a timeout: the response may still be on its way, and
      # reading on from here would pair it with the next request.
      {:error, %Error{} = error} -> {:reply, {:error, error}, fail(state)}
      {:error, reason} -> {:reply, {:error, %Error{message: "connection error: #{inspect(reason)}"}}, fail(state)}
    end
  end

  def handle_call({:oneway, api_key, body}, _from, state) do
    correlation_id = state.next + 1
    state = %{state | next: correlation_id}
    frame = Protocol.encode_frame_payload(api_key, correlation_id, state.client_id, body)

    case :gen_tcp.send(state.socket, frame) do
      :ok -> {:reply, :ok, state}
      {:error, reason} -> {:reply, {:error, %Error{message: "send failed: #{inspect(reason)}"}}, fail(state)}
    end
  end

  def handle_call({:set_request_timeout, ms}, _from, state), do: {:reply, :ok, %{state | timeout: ms}}
  def handle_call(:broken?, _from, state), do: {:reply, state.broken, state}

  @impl true
  def terminate(_reason, state) do
    :gen_tcp.close(state.socket)
  end

  defp recv_timeout(ms) when is_integer(ms) and ms > 0, do: ms
  defp recv_timeout(_), do: :infinity

  # Marks the connection unusable and tells its owner, before the caller
  # hears about the failure.
  defp fail(state) do
    :gen_tcp.close(state.socket)
    send(state.owner, {:connection_broken, self()})
    %{state | broken: true}
  end

  defp await(socket, correlation_id, timeout) do
    with {:ok, payload} <- :gen_tcp.recv(socket, 0, timeout),
         {:ok, got, body} <- Protocol.decode_frame_payload(payload) do
      if got == correlation_id do
        {:ok, body}
      else
        # The stream has desynchronised; continuing would pair every later
        # response with the wrong request.
        {:error, %Error{message: "correlation id mismatch: expected #{correlation_id}, got #{got}"}}
      end
    end
  end
end
