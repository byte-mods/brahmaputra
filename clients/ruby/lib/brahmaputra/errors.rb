# frozen_string_literal: true

module Brahmaputra
  # Broker error codes, as the protocol crate numbers them.
  module ErrorCode
    NONE = 0
    UNKNOWN_TOPIC_OR_PARTITION = 1
    OFFSET_OUT_OF_RANGE = 2
    INVALID_REQUEST = 3
    UNSUPPORTED_VERSION = 4
    INTERNAL = 5
    NOT_LEADER_OR_FOLLOWER = 6
    FENCED_BROKER_EPOCH = 7
    FENCED_LEADER_EPOCH = 8
    UNKNOWN_LEADER_EPOCH = 9
    NOT_ENOUGH_REPLICAS = 10
    FENCED_PRODUCER_EPOCH = 11
    OUT_OF_ORDER_SEQUENCE = 12
    UNKNOWN_MEMBER_ID = 13
    REBALANCE_IN_PROGRESS = 14
    NOT_COORDINATOR = 15
    ILLEGAL_GENERATION = 16
    COORDINATOR_LOAD_IN_PROGRESS = 17
    SASL_AUTHENTICATION_FAILED = 18
    AUTHORIZATION_FAILED = 19

    NAMES = constants.to_h { |name| [const_get(name), name.to_s] }.freeze

    # Codes the broker only ever returns *before* it appends anything, so a
    # retry cannot duplicate a record.
    RETRIABLE = [
      NOT_LEADER_OR_FOLLOWER, FENCED_LEADER_EPOCH, UNKNOWN_LEADER_EPOCH,
      NOT_ENOUGH_REPLICAS, COORDINATOR_LOAD_IN_PROGRESS, INTERNAL
    ].freeze

    # Codes that mean the cached route is stale.
    STALE_ROUTE = [NOT_LEADER_OR_FOLLOWER, FENCED_LEADER_EPOCH, UNKNOWN_LEADER_EPOCH].freeze

    def self.name_of(code) = NAMES.fetch(code, "UNKNOWN")
  end

  # Base class of every error this library raises.
  class Error < StandardError; end

  # The bytes on the wire did not decode.
  class ProtocolError < Error; end

  # Could not reach a broker, or the connection broke mid-request.
  class ConnectionError < Error; end

  # A client-side deadline (request.timeout.ms, delivery.timeout.ms) passed.
  class TimeoutError < Error; end

  # buffer.memory stayed full for longer than max.block.ms.
  class BufferFullError < Error; end

  # auto.offset.reset=none and a partition has no committed position.
  class NoOffsetForPartitionError < Error; end

  # The broker answered with a non-zero error code.
  class ServerError < Error
    attr_reader :code

    def initialize(code, context = nil)
      @code = code
      suffix = context ? " (#{context})" : ""
      super("broker returned #{ErrorCode.name_of(code)}[#{code}]#{suffix}")
    end

    def retriable? = ErrorCode::RETRIABLE.include?(@code)
  end
end
