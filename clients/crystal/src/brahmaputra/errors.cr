module Brahmaputra
  # Base class for every error this driver raises.
  class Error < Exception
  end

  # The connection failed (I/O error, timeout, correlation mismatch). The
  # connection it happened on is closed and marked broken; the router
  # redials on next use.
  class ConnectionError < Error
  end

  # A request's round trip exceeded the connection's request timeout.
  class RequestTimeoutError < ConnectionError
  end

  # Bytes from the broker that do not decode: truncated, oversized,
  # negative lengths, CRC mismatch.
  class DecodeError < Error
  end

  # Misconfiguration or misuse detected on the client side.
  class ConfigError < Error
  end

  # `auto.offset.reset=none` and there is no position to resume from.
  class NoOffsetForPartitionError < Error
    def initialize(topic : String, partition : Int32)
      super("no committed offset for partition #{topic}-#{partition} and auto.offset.reset=none")
    end
  end

  # `buffer.memory` stayed full for longer than `max.block.ms`.
  class BufferFullError < Error
  end

  # A non-zero error code from the broker.
  class ServerError < Error
    getter code : Int32
    getter context : String

    def initialize(@code : Int32, @context : String = "")
      name = ErrorCode.name_of(@code)
      if @context.empty?
        super("broker returned #{name}[#{@code}]")
      else
        super("broker returned #{name}[#{@code}] (#{@context})")
      end
    end
  end

  # Error codes the broker returns in a response's error_code field.
  module ErrorCode
    NONE                         =  0
    UNKNOWN_TOPIC_OR_PARTITION   =  1
    OFFSET_OUT_OF_RANGE          =  2
    INVALID_REQUEST              =  3
    UNSUPPORTED_VERSION          =  4
    INTERNAL                     =  5
    NOT_LEADER_OR_FOLLOWER       =  6
    FENCED_BROKER_EPOCH          =  7
    FENCED_LEADER_EPOCH          =  8
    UNKNOWN_LEADER_EPOCH         =  9
    NOT_ENOUGH_REPLICAS          = 10
    FENCED_PRODUCER_EPOCH        = 11
    OUT_OF_ORDER_SEQUENCE        = 12
    UNKNOWN_MEMBER_ID            = 13
    REBALANCE_IN_PROGRESS        = 14
    NOT_COORDINATOR              = 15
    ILLEGAL_GENERATION           = 16
    COORDINATOR_LOAD_IN_PROGRESS = 17
    SASL_AUTHENTICATION_FAILED   = 18
    AUTHORIZATION_FAILED         = 19

    NAMES = {
       0 => "NONE",
       1 => "UNKNOWN_TOPIC_OR_PARTITION",
       2 => "OFFSET_OUT_OF_RANGE",
       3 => "INVALID_REQUEST",
       4 => "UNSUPPORTED_VERSION",
       5 => "INTERNAL",
       6 => "NOT_LEADER_OR_FOLLOWER",
       7 => "FENCED_BROKER_EPOCH",
       8 => "FENCED_LEADER_EPOCH",
       9 => "UNKNOWN_LEADER_EPOCH",
      10 => "NOT_ENOUGH_REPLICAS",
      11 => "FENCED_PRODUCER_EPOCH",
      12 => "OUT_OF_ORDER_SEQUENCE",
      13 => "UNKNOWN_MEMBER_ID",
      14 => "REBALANCE_IN_PROGRESS",
      15 => "NOT_COORDINATOR",
      16 => "ILLEGAL_GENERATION",
      17 => "COORDINATOR_LOAD_IN_PROGRESS",
      18 => "SASL_AUTHENTICATION_FAILED",
      19 => "AUTHORIZATION_FAILED",
    }

    def self.name_of(code : Int32) : String
      NAMES[code]? || "UNKNOWN"
    end

    # Codes the broker returns strictly before it appends, so a retry
    # cannot duplicate a record.
    def self.retriable?(code : Int32) : Bool
      code.in?(NOT_LEADER_OR_FOLLOWER, FENCED_LEADER_EPOCH, UNKNOWN_LEADER_EPOCH,
        NOT_ENOUGH_REPLICAS, COORDINATOR_LOAD_IN_PROGRESS, INTERNAL)
    end
  end
end
