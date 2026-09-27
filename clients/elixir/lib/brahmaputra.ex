defmodule Brahmaputra do
  @moduledoc """
  Native Elixir client for the Brahmaputra log broker.

      {:ok, producer} = Brahmaputra.Producer.start_link("127.0.0.1", 9092)
      :ok = Brahmaputra.Producer.send(producer, "orders", ~s({"id":1}), key: "user-7")
      :ok = Brahmaputra.Producer.flush(producer)

      {:ok, group} = Brahmaputra.GroupConsumer.start_link("127.0.0.1", 9092, "billing")
      :ok = Brahmaputra.GroupConsumer.subscribe(group, ["orders"])
      {:ok, records} = Brahmaputra.GroupConsumer.poll(group, 500)
      :ok = Brahmaputra.GroupConsumer.commit(group)

  See `Brahmaputra.Producer`, `Brahmaputra.Consumer` and
  `Brahmaputra.GroupConsumer` for configuration.
  """

  @version Mix.Project.config()[:version]

  @doc "This client's version."
  def version, do: @version

  @doc "Kafka's murmur2 hash; `murmur2(\"\") == 275646681`."
  defdelegate murmur2(data), to: Brahmaputra.Protocol

  @doc "`murmur2(key) % length(partitions)`, Kafka's default partitioner."
  defdelegate partition_for_key(key, partitions), to: Brahmaputra.Protocol

  @doc """
  Registers a compression codec this driver does not carry itself
  (`:lz4`, `:zstd` or `:snappy`). `none` and `gzip` are built in.
  """
  defdelegate register_codec(codec, compress_fun, decompress_fun), to: Brahmaputra.Protocol
end
