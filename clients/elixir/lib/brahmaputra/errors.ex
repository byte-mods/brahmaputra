defmodule Brahmaputra.Error do
  @moduledoc "A client-side failure: a malformed response, a full buffer, a closed connection."
  defexception [:message]
end

defmodule Brahmaputra.ServerError do
  @moduledoc "A non-zero error code returned by the broker."
  defexception [:code, :context]

  @impl true
  def message(%{code: code, context: context}) do
    name = Brahmaputra.Protocol.error_name(code)

    if context in [nil, ""],
      do: "broker returned #{name}[#{code}]",
      else: "broker returned #{name}[#{code}] (#{context})"
  end
end

defmodule Brahmaputra.NoOffsetForPartitionError do
  @moduledoc """
  Returned when `auto_offset_reset: :none` is in force and a partition has
  no position to resume from.
  """
  defexception message: "no committed offset for partition", topic: nil, partition: nil
end
