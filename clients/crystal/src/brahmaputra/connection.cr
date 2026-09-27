require "socket"

module Brahmaputra
  # Offset sentinels for `Consumer#list_offsets`.
  EARLIEST = -2_i64
  LATEST   = -1_i64

  # Bounds one request/response round trip. It must exceed the longest the
  # broker may legitimately hold a request (fetch long-poll, acks=all,
  # JoinGroup waiting out a rebalance), so it is generous: its job is to
  # turn a wedged broker into an error instead of a fiber blocked forever.
  DEFAULT_REQUEST_TIMEOUT = 120.seconds
  DEFAULT_CONNECT_TIMEOUT = 30.seconds

  def self.now_ms : Int64
    Time.utc.to_unix_ms
  end

  # Splits "host:port" (the port after the last colon).
  def self.parse_address(address : String) : {String, Int32}
    index = address.rindex(':') || raise ConfigError.new("address #{address.inspect} is not host:port")
    host = address[0, index]
    host = host[1..-2] if host.starts_with?('[') && host.ends_with?(']')
    port = address[(index + 1)..].to_i? || raise ConfigError.new("address #{address.inspect} has no numeric port")
    {host, port}
  end

  # One TCP connection to one broker.
  #
  # A mutex serialises request/response pairs, so there is at most one
  # request in flight per connection. Any I/O failure, timeout or
  # correlation mismatch leaves the byte stream at an unknown position, so
  # the connection is closed and marked broken rather than reused; the
  # Router notices and redials.
  class Connection
    getter address : String
    getter client_id : String
    property request_timeout : Time::Span?

    def self.dial(address : String, client_id : String = "brahmaputra-crystal",
                  connect_timeout : Time::Span = DEFAULT_CONNECT_TIMEOUT) : Connection
      host, port = Brahmaputra.parse_address(address)
      socket = begin
        TCPSocket.new(host, port, connect_timeout: connect_timeout)
      rescue ex : Socket::Error | IO::Error
        raise ConnectionError.new("connect to #{address}: #{ex.message}")
      end
      # Responses are small and latency matters more than packet count.
      socket.tcp_nodelay = true
      socket.sync = false
      new(socket, address, client_id)
    end

    def initialize(@socket : TCPSocket, @address : String, @client_id : String)
      @mutex = Mutex.new
      @next_id = 0
      @broken = false
      @request_timeout = DEFAULT_REQUEST_TIMEOUT
    end

    # True once this connection failed; it must not be reused.
    def broken? : Bool
      @broken
    end

    def close : Nil
      @broken = true
      @socket.close rescue nil
    end

    private def fail(message : String, timeout = false) : NoReturn
      @broken = true
      @socket.close rescue nil
      if timeout
        raise RequestTimeoutError.new("#{@address}: #{message}; connection closed")
      end
      raise ConnectionError.new("#{@address}: #{message}; connection closed")
    end

    private def arm_timeout : Nil
      t = @request_timeout
      t = nil if t && t <= Time::Span.zero
      @socket.read_timeout = t
      @socket.write_timeout = t
    end

    private def write_frame(api_key : Int16, correlation_id : Int32, body : Bytes) : Nil
      @socket.write(Protocol.encode_frame(api_key, correlation_id, @client_id, body))
      @socket.flush
    end

    # Sends one request and returns the matching response body.
    def request(api_key : Int16, body : Bytes) : Bytes
      @mutex.synchronize do
        raise ConnectionError.new("connection to #{@address} is broken; the router will redial") if @broken
        @next_id = @next_id &+ 1
        correlation_id = @next_id
        started = Time.monotonic
        begin
          arm_timeout
          write_frame(api_key, correlation_id, body)
          payload = read_frame(started)
        rescue ex : IO::TimeoutError
          fail("request timed out after #{@request_timeout}", timeout: true)
        rescue ex : IO::Error | Socket::Error
          # Includes EOF: the response may still be on its way, and reading
          # on would pair it with the next request.
          fail("#{ex.class}: #{ex.message}")
        end
        got, response = begin
          Protocol.decode_frame_payload(payload)
        rescue ex : DecodeError
          fail(ex.message || "bad frame")
        end
        if got != correlation_id
          fail("correlation id mismatch: expected #{correlation_id}, got #{got}")
        end
        response
      end
    end

    # Sends without awaiting a response (acks=0).
    def send_oneway(api_key : Int16, body : Bytes) : Nil
      @mutex.synchronize do
        raise ConnectionError.new("connection to #{@address} is broken; the router will redial") if @broken
        @next_id = @next_id &+ 1
        begin
          arm_timeout
          write_frame(api_key, @next_id, body)
        rescue ex : IO::TimeoutError
          fail("write timed out", timeout: true)
        rescue ex : IO::Error | Socket::Error
          fail("#{ex.class}: #{ex.message}")
        end
      end
    end

    # The whole round trip is bounded, not each read: a broker trickling
    # one byte per timeout would otherwise hold the caller indefinitely.
    private def read_frame(started : Time::Span) : Bytes
      header = Bytes.new(4)
      read_bounded(header, started)
      length = IO::ByteFormat::BigEndian.decode(Int32, header)
      raise IO::Error.new("negative frame length #{length}") if length < 0
      payload = Bytes.new(length)
      read_bounded(payload, started)
      payload
    end

    private def read_bounded(buffer : Bytes, started : Time::Span) : Nil
      filled = 0
      while filled < buffer.size
        if t = @request_timeout
          if t > Time::Span.zero
            remaining = t - (Time.monotonic - started)
            raise IO::TimeoutError.new("round trip exceeded #{t}") if remaining <= Time::Span.zero
            @socket.read_timeout = remaining
          end
        end
        n = @socket.read(buffer[filled..])
        raise IO::EOFError.new("broker closed the connection") if n == 0
        filled += n
      end
    end

    # One entry of an ApiVersions response.
    record ApiVersionRange, api_key : Int32, min_version : Int32, max_version : Int32

    # Asks the broker what it speaks. Returns the ranges and the broker's
    # version string.
    def api_versions : {Array(ApiVersionRange), String}
      w = Protocol::Writer.new.string("brahmaputra-crystal").string(VERSION)
      r = Protocol::Reader.new(request(Protocol::API_VERSIONS, w.to_slice))
      code = r.int32
      raise ServerError.new(code, "api_versions") unless code == ErrorCode::NONE
      ranges = Array(ApiVersionRange).new(r.count) { ApiVersionRange.new(r.int32, r.int32, r.int32) }
      {ranges, r.string}
    end
  end

  record BrokerInfo, node_id : Int32, host : String, port : Int32, rack : String
  record PartitionInfo, partition : Int32, leader : Int32, replicas : Array(Int32),
    isr : Array(Int32), leader_epoch : Int32
  record TopicInfo, name : String, partitions : Array(PartitionInfo)

  class ClusterMetadata
    getter brokers : Array(BrokerInfo)
    getter topics : Array(TopicInfo)

    def initialize(@brokers, @topics)
    end

    # A topic's partition ids, ascending.
    def partitions_of(topic : String) : Array(Int32)
      info = @topics.find { |t| t.name == topic }
      return [] of Int32 unless info
      info.partitions.map(&.partition).sort!
    end

    # The broker id leading a partition, or -1.
    def leader_of(topic : String, partition : Int32) : Int32
      info = @topics.find { |t| t.name == topic }
      return -1 unless info
      info.partitions.find { |p| p.partition == partition }.try(&.leader) || -1
    end

    def self.decode(r : Protocol::Reader) : ClusterMetadata
      # error_code, brokers, controller_id, topics — in schema order. The
      # leading code is request-level and distinct from the per-topic one.
      code = r.int32
      raise ServerError.new(code, "metadata") unless code == ErrorCode::NONE
      brokers = Array(BrokerInfo).new(r.count) { BrokerInfo.new(r.int32, r.string, r.int32, r.string) }
      r.int32 # controller_id
      topics = Array(TopicInfo).new(r.count) do
        name = r.string
        topic_error = r.int32
        partitions = Array(PartitionInfo).new(r.count) do
          partition = r.int32
          leader = r.int32
          replicas = Array(Int32).new(r.count) { r.int32 }
          isr = Array(Int32).new(r.count) { r.int32 }
          PartitionInfo.new(partition, leader, replicas, isr, r.int32)
        end
        if topic_error != ErrorCode::NONE && topic_error != ErrorCode::UNKNOWN_TOPIC_OR_PARTITION
          raise ServerError.new(topic_error, "metadata for #{name}")
        end
        TopicInfo.new(name, partitions)
      end
      new(brokers, topics)
    end
  end

  # Keeps connections to every broker and routes by partition leader.
  #
  # Metadata is cached and refreshed only when a request says the route was
  # stale. A connection that failed is replaced on its next use — the seed
  # included — rather than kept, so one dropped socket does not fail every
  # later request for the life of the client.
  class Router
    getter client_id : String
    getter request_timeout : Time::Span?
    @seed : Connection
    @metadata : ClusterMetadata?

    def initialize(@seed_address : String, @client_id : String,
                   @connect_timeout : Time::Span = DEFAULT_CONNECT_TIMEOUT,
                   @request_timeout : Time::Span? = DEFAULT_REQUEST_TIMEOUT)
      @mutex = Mutex.new
      @conns = {} of Int32 => Connection
      @metadata = nil
      @seed = dial(@seed_address)
    end

    private def dial(address : String) : Connection
      conn = Connection.dial(address, @client_id, @connect_timeout)
      conn.request_timeout = @request_timeout
      conn
    end

    # Changes the round-trip timeout for every current and future connection.
    def request_timeout=(timeout : Time::Span?)
      @mutex.synchronize do
        @request_timeout = timeout
        @seed.request_timeout = timeout
        @conns.each_value { |c| c.request_timeout = timeout }
      end
    end

    def close : Nil
      @mutex.synchronize do
        @conns.each_value { |c| c.close unless c.same?(@seed) }
        @conns.clear
        @seed.close
      end
    end

    # The seed connection, redialled if it has failed.
    def seed : Connection
      @mutex.synchronize { live_seed_locked }
    end

    private def live_seed_locked : Connection
      return @seed unless @seed.broken?
      old = @seed
      @seed = dial(@seed_address)
      @conns.each { |id, c| @conns[id] = @seed if c.same?(old) }
      @seed
    end

    def metadata(topics : Array(String) = [] of String, refresh : Bool = false) : ClusterMetadata
      @mutex.synchronize do
        cached = @metadata
        return cached if cached && !refresh
        seed = live_seed_locked
        body = Protocol::Writer.new.string_array(topics).to_slice
        metadata = ClusterMetadata.decode(Protocol::Reader.new(seed.request(Protocol::METADATA, body)))
        @metadata = metadata
        metadata
      end
    end

    def refresh(topic : String) : ClusterMetadata
      metadata([topic], refresh: true)
    end

    # A topic's partitions, refreshing once if the cached image has none (a
    # topic auto-created on first reference is not in it yet).
    def partitions(topic : String) : Array(Int32)
      partitions = metadata([topic]).partitions_of(topic)
      partitions = refresh(topic).partitions_of(topic) if partitions.empty?
      raise Error.new("topic #{topic.inspect} has no partitions") if partitions.empty?
      partitions
    end

    # The connection to a partition's leader.
    def connection_for(topic : String, partition : Int32) : Connection
      metadata = metadata([topic])
      leader = metadata.leader_of(topic, partition)
      if leader < 0
        metadata = refresh(topic)
        leader = metadata.leader_of(topic, partition)
      end
      raise Error.new("no leader for #{topic}-#{partition}") if leader < 0

      @mutex.synchronize do
        if conn = @conns[leader]?
          return conn unless conn.broken?
          @conns.delete(leader)
          conn.close unless conn.same?(@seed)
        end
        broker = metadata.brokers.find { |b| b.node_id == leader }
        raise Error.new("broker #{leader} is not in the metadata") unless broker
        # A single-broker cluster advertises the address it was configured
        # with, which may not be the one we dialled; reuse the seed.
        conn = if metadata.brokers.size == 1
                 live_seed_locked
               else
                 dial("#{broker.host}:#{broker.port}")
               end
        @conns[leader] = conn
        conn
      end
    end
  end
end
