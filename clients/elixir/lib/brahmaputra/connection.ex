defmodule Brahmaputra.Connection do
  @moduledoc """
  One TCP connection to one broker.

  The process owns the socket and serialises request/response pairs, which
  is what keeps correlation ids and responses paired. Frames use `:gen_tcp`'s
  `packet: 4` mode, which is exactly the protocol's big-endian int32 length
  prefix.
  """
  use GenServer

  alias Brahmaputra.{Error, Protocol}

  @default_request_timeout 60_000

  # -- API --------------------------------------------------------------------

  @doc "Opens a connection. `opts`: `:client_id`, `:connect_timeout` (ms)."
  def start_link(host, port, opts \\ []) do
    GenServer.start_link(__MODULE__, {host, port, opts})
  end

  @doc "Sends one request and returns the matching response body."
  def request(conn, api_key, body, timeout \\ @default_request_timeout) do
    GenServer.call(conn, {:request, api_key, body, timeout}, timeout + 5_000)
  catch
    :exit, reason -> {:error, %Error{message: "connection request failed: #{inspect(reason)}"}}
  end

  @doc "Sends without awaiting a response (acks=0: the broker sends none)."
  def send_oneway(conn, api_key, body) do
    GenServer.call(conn, {:oneway, api_key, body})
  catch
    :exit, reason -> {:error, %Error{message: "connection send failed: #{inspect(reason)}"}}
  end

  def close(conn) do
    GenServer.stop(conn, :normal)
  catch
    :exit, _ -> :ok
  end

  # -- Server -----------------------------------------------------------------

  @impl true
  def init({host, port, opts}) do
    host = if is_binary(host), do: String.to_charlist(host), else: host
    timeout = Keyword.get(opts, :connect_timeout, 30_000)

    # Responses are small and latency matters more than packet count; without
    # nodelay every request pays Nagle plus the peer's delayed ACK.
    case :gen_tcp.connect(host, port, [:binary, packet: 4, active: false, nodelay: true], timeout) do
      {:ok, socket} ->
        {:ok, %{socket: socket, client_id: Keyword.get(opts, :client_id, "brahmaputra-elixir"), next: 0}}

      {:error, reason} ->
        {:stop, %Error{message: "connect #{host}:#{port} failed: #{inspect(reason)}"}}
    end
  end

  @impl true
  def handle_call({:request, api_key, body, timeout}, _from, state) do
    correlation_id = state.next + 1
    state = %{state | next: correlation_id}
    frame = Protocol.encode_frame_payload(api_key, correlation_id, state.client_id, body)

    with :ok <- :gen_tcp.send(state.socket, frame),
         {:ok, body} <- await(state.socket, correlation_id, timeout) do
      {:reply, {:ok, body}, state}
    else
      {:error, %Error{} = error} ->
        {:stop, :normal, {:error, error}, state}

      {:error, reason} ->
        {:stop, :normal, {:error, %Error{message: "connection error: #{inspect(reason)}"}}, state}
    end
  end

  def handle_call({:oneway, api_key, body}, _from, state) do
    correlation_id = state.next + 1
    state = %{state | next: correlation_id}
    frame = Protocol.encode_frame_payload(api_key, correlation_id, state.client_id, body)

    case :gen_tcp.send(state.socket, frame) do
      :ok -> {:reply, :ok, state}
      {:error, reason} -> {:stop, :normal, {:error, %Error{message: "send failed: #{inspect(reason)}"}}, state}
    end
  end

  @impl true
  def terminate(_reason, state) do
    :gen_tcp.close(state.socket)
  end

  defp await(socket, correlation_id, timeout) do
    with {:ok, payload} <- :gen_tcp.recv(socket, 0, timeout),
         {:ok, got, body} <- Protocol.decode_frame_payload(payload) do
      cond do
        got == correlation_id ->
          {:ok, body}

        # An answer to an earlier request we stopped waiting on; drop it.
        got < correlation_id ->
          await(socket, correlation_id, timeout)

        # Anything else means the stream has desynchronised; continuing would
        # pair every later response with the wrong request.
        true ->
          {:error, %Error{message: "correlation id mismatch: expected #{correlation_id}, got #{got}"}}
      end
    end
  end
end
