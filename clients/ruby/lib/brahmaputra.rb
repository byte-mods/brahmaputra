# frozen_string_literal: true

require "monitor"

require_relative "brahmaputra/version"
require_relative "brahmaputra/errors"
require_relative "brahmaputra/protocol"
require_relative "brahmaputra/config"
require_relative "brahmaputra/connection"
require_relative "brahmaputra/producer"
require_relative "brahmaputra/consumer"
require_relative "brahmaputra/assignors"
require_relative "brahmaputra/group_consumer"

# Native Ruby client for the Brahmaputra log broker.
module Brahmaputra
  module_function

  # Kafka's murmur2 (murmur2("") == 275646681).
  def murmur2(data) = Protocol.murmur2(data)

  # The partition Kafka's default partitioner picks for key.
  def partition_for_key(key, partitions) = Protocol.partition_for_key(key, partitions)

  def crc32c(data) = Protocol.crc32c(data.b)

  # Register a compression codec the standard library lacks (lz4, zstd,
  # snappy). Both callables take and return a binary String.
  def register_codec(name, compress:, decompress:)
    Protocol::Compression.register(name, compress: compress, decompress: decompress)
  end
end
