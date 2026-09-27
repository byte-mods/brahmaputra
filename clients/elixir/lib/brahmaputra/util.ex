defmodule Brahmaputra.Util do
  @moduledoc false

  # GenServer.start_link would deliver an init failure to the caller as an
  # exit signal as well as an {:error, _}; starting unlinked and linking on
  # success returns the error as a value without killing the caller.
  def start_linked(module, arg) do
    case GenServer.start(module, arg) do
      {:ok, pid} ->
        Process.link(pid)
        {:ok, pid}

      other ->
        other
    end
  end

  def now_ms, do: System.system_time(:millisecond)
  def mono_ms, do: System.monotonic_time(:millisecond)
end
