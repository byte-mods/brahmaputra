# frozen_string_literal: true

module Brahmaputra
  # Configuration is a Hash keyed by Kafka's dotted names ("linger.ms").
  # Symbols and snake_case are accepted too (linger_ms: 5), and an unknown
  # key is an error rather than a silently ignored typo.
  module Config
    ALIASES = {
      "assignor" => "partition.assignment.strategy",
      "compression" => "compression.type",
      "bootstrap" => "bootstrap.servers",
      "rack" => "client.rack"
    }.freeze

    COMMON = {
      "bootstrap.servers" => "127.0.0.1:9092",
      "client.id" => "brahmaputra-ruby",
      "socket.connection.setup.timeout.ms" => 30_000,
      "request.timeout.ms" => 30_000
    }.freeze

    def self.build(defaults, config, overrides)
      merged = COMMON.merge(defaults)
      (config || {}).merge(overrides || {}).each do |key, value|
        name = key.to_s.tr("_", ".")
        name = ALIASES.fetch(name, name)
        raise ArgumentError, "unknown configuration #{key.inspect}" unless merged.key?(name)

        merged[name] = value
      end
      merged.freeze
    end

    def self.router_for(config)
      Router.new(
        config["bootstrap.servers"],
        client_id: config["client.id"],
        connect_timeout_ms: config["socket.connection.setup.timeout.ms"],
        request_timeout_ms: config["request.timeout.ms"]
      )
    end
  end
end
