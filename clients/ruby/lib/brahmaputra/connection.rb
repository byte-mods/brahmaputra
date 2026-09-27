# frozen_string_literal: true

require "socket"
require "securerandom"

module Brahmaputra
  # One TCP connection to one broker.
  #
  # Requests are serialised under a lock and each waits for its own reply,
  # so the stream can never pair a response with the wrong request; a
  # mismatched correlation id means the stream desynchronised and the
  # connection is dropped rather than trusted.
  class Connection
    SCRAM_MECHANISM = "SCRAM-SHA-256"

    ApiVersion = Struct.new(:api_key, :min_version, :max_version)

    attr_reader :host, :port

    def initialize(host, port, client_id: "brahmaputra-ruby", connect_timeout_ms: 30_000,
                   request_timeout_ms: 30_000)
      @host = host
      @port = port.to_i
      @client_id = client_id
      @request_timeout_ms = request_timeout_ms
      @lock = Monitor.new
      @correlation = 0
      begin
        @socket = Socket.tcp(host, @port, connect_timeout: connect_timeout_ms / 1000.0)
      rescue SystemCallError, SocketError, IOError => e
        raise ConnectionError, "connect to #{host}:#{port} failed: #{e.message}"
      end
      # Responses are small and latency matters more than packet count;
      # without this every request pays Nagle plus the peer's delayed ACK.
      @socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)
      @closed = false
    end

    def address = "#{@host}:#{@port}"

    def closed? = @closed

    def close
      @closed = true
      @socket.close unless @socket.closed?
    rescue IOError
      nil
    end

    # Send one request and return the matching response body.
    #
    # timeout_ms bounds the wait for the reply; any failure closes the
    # connection, since a half-read frame leaves the stream unusable.
    def request(api_key, body, timeout_ms: nil)
      @lock.synchronize do
        raise ConnectionError, "connection to #{address} is closed" if @closed

        correlation_id = next_correlation
        deadline = monotonic + (timeout_ms || @request_timeout_ms) / 1000.0
        begin
          @socket.write(Protocol.encode_frame(api_key, correlation_id, @client_id, body))
          length = read_exact(4, deadline).unpack1("l>")
          raise ProtocolError, "negative frame length #{length}" if length.negative?

          got, response = Protocol.decode_frame_payload(read_exact(length, deadline))
          if got != correlation_id
            raise ProtocolError, "correlation id mismatch: expected #{correlation_id}, got #{got}"
          end

          response
        rescue SystemCallError, IOError => e
          close
          raise ConnectionError, "#{address}: #{e.message}"
        rescue ProtocolError, TimeoutError, ConnectionError
          close
          raise
        end
      end
    end

    # Send without awaiting a response (acks=0: the broker sends none).
    def send_oneway(api_key, body)
      @lock.synchronize do
        raise ConnectionError, "connection to #{address} is closed" if @closed

        @socket.write(Protocol.encode_frame(api_key, next_correlation, @client_id, body))
        nil
      rescue SystemCallError, IOError => e
        close
        raise ConnectionError, "#{address}: #{e.message}"
      end
    end

    # Returns [Array<ApiVersion>, broker_version].
    def api_versions
      body = Protocol.body_writer.string("brahmaputra-ruby").string(VERSION).bytes
      reader = Protocol.body_reader(request(Protocol::ApiKey::API_VERSIONS, body))
      code = reader.int32
      raise ServerError.new(code, "api_versions") unless code == ErrorCode::NONE

      versions = Array.new(reader.int32) { ApiVersion.new(reader.int32, reader.int32, reader.int32) }
      [versions, reader.string]
    end

    # Bind a principal to this connection using SCRAM-SHA-256. The password
    # never crosses the wire. Returns [principal, role].
    def authenticate(username, password)
      require "openssl"
      client_nonce = SecureRandom.base64(18).tr(",", ".")
      bare = "n=#{username},r=#{client_nonce}"
      first = authenticate_step(username, "", SCRAM_MECHANISM, "n,,#{bare}")
      raise ProtocolError, "broker ended the SCRAM exchange before it began" if first[:done]

      server_first = first[:payload]
      fields = server_first.split(",").to_h { |part| part.split("=", 2) }
      nonce = fields["r"]
      salt = fields["s"]
      iterations = fields["i"].to_i
      if nonce.nil? || salt.nil? || iterations <= 0
        raise ProtocolError, "malformed SCRAM server-first message"
      end
      unless nonce.start_with?(client_nonce)
        raise ProtocolError, "SCRAM server nonce does not extend the client nonce"
      end

      without_proof = "c=biws,r=#{nonce}"
      auth_message = "#{bare},#{server_first},#{without_proof}"
      salted = OpenSSL::KDF.pbkdf2_hmac(password, salt: salt.unpack1("m"), iterations: iterations,
                                                  length: 32, hash: "sha256")
      client_key = OpenSSL::HMAC.digest("sha256", salted, "Client Key")
      stored_key = OpenSSL::Digest::SHA256.digest(client_key)
      signature = OpenSSL::HMAC.digest("sha256", stored_key, auth_message)
      proof = client_key.bytes.zip(signature.bytes).map { |a, b| a ^ b }.pack("C*")
      final = authenticate_step(username, "", SCRAM_MECHANISM, "#{without_proof},p=#{[proof].pack('m0')}")
      [final[:principal], final[:role]]
    end

    # Send the password itself, as SASL/PLAIN does. The broker refuses this on
    # a plaintext listener.
    def authenticate_plain(username, password)
      result = authenticate_step(username, password, "PLAIN", "")
      [result[:principal], result[:role]]
    end

    private

    def authenticate_step(username, password, mechanism, payload)
      body = Protocol.body_writer.string(username).string(password).string(mechanism).string(payload).bytes
      reader = Protocol.body_reader(request(Protocol::ApiKey::AUTHENTICATE, body))
      code = reader.int32
      result = { principal: reader.string, role: reader.string, payload: reader.string, done: reader.bool }
      raise ServerError.new(code, "authenticate") unless code == ErrorCode::NONE

      result
    end

    def next_correlation
      @correlation = (@correlation + 1) & 0x7FFF_FFFF
    end

    def monotonic = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    def read_exact(count, deadline)
      buf = Protocol.binary(count)
      while buf.bytesize < count
        chunk = @socket.read_nonblock(count - buf.bytesize, exception: false)
        case chunk
        when :wait_readable
          remaining = deadline - monotonic
          raise TimeoutError, "request to #{address} timed out" if remaining <= 0

          @socket.wait_readable(remaining)
        when nil
          raise ConnectionError, "connection to #{address} closed by broker"
        else
          buf << chunk
        end
      end
      buf
    end
  end

  BrokerInfo = Struct.new(:node_id, :host, :port, :rack)
  PartitionInfo = Struct.new(:partition, :leader, :replicas, :isr, :leader_epoch)

  # A snapshot of what the cluster told us.
  ClusterMetadata = Struct.new(:brokers, :topics) do
    def partitions_of(topic) = (topics[topic] || []).map(&:partition).sort

    def leader_of(topic, partition)
      info = (topics[topic] || []).find { |entry| entry.partition == partition }
      info ? info.leader : -1
    end

    def broker(node_id) = brokers.find { |entry| entry.node_id == node_id }
  end

  # Keeps connections to every broker and routes by partition leader.
  #
  # Metadata is cached and refreshed only when a request says the route was
  # stale, so the control plane stays off the data path.
  class Router
    attr_reader :client_id

    def initialize(bootstrap, client_id: "brahmaputra-ruby", connect_timeout_ms: 30_000,
                   request_timeout_ms: 30_000)
      @bootstrap = parse_bootstrap(bootstrap)
      @client_id = client_id
      @connect_timeout_ms = connect_timeout_ms
      @request_timeout_ms = request_timeout_ms
      @lock = Monitor.new
      @connections = {}
      @brokers = []
      @topics = {}
      @seed = nil
      seed
    end

    # The bootstrap connection, redialled if it broke.
    def seed
      @lock.synchronize do
        return @seed if @seed && !@seed.closed?

        errors = []
        @bootstrap.each do |host, port|
          @seed = dial(host, port)
          return @seed
        rescue ConnectionError => e
          errors << e.message
        end
        raise ConnectionError, "no bootstrap server reachable: #{errors.join('; ')}"
      end
    end

    def close
      @lock.synchronize do
        @connections.each_value { |connection| connection.close unless connection.equal?(@seed) }
        @connections.clear
        @seed&.close
      end
    end

    # Cluster metadata. With refresh: false a cached image is returned when it
    # already knows every requested topic.
    def metadata(topics = nil, refresh: false)
      topics = Array(topics)
      @lock.synchronize do
        known = !@brokers.empty? && topics.all? { |topic| @topics.key?(topic) }
        return snapshot if !refresh && known

        body = Protocol.body_writer.string_array(topics).bytes
        brokers, fetched = decode_metadata(Protocol.body_reader(seed.request(Protocol::ApiKey::METADATA, body)))
        @brokers = brokers
        @topics.merge!(fetched)
        snapshot
      end
    end

    def refresh(topic) = metadata([topic], refresh: true)

    # Partition ids of a topic, sorted. A topic auto-created on first use is
    # not in the cache yet; one refresh tells "new" from "absent".
    def partitions(topic)
      found = metadata([topic]).partitions_of(topic)
      found = refresh(topic).partitions_of(topic) if found.empty?
      raise Error, "topic #{topic} has no partitions" if found.empty?

      found
    end

    # The connection to the current leader of topic-partition.
    def connection_for(topic, partition)
      @lock.synchronize do
        meta = metadata([topic])
        leader = meta.leader_of(topic, partition)
        if leader.negative?
          meta = refresh(topic)
          leader = meta.leader_of(topic, partition)
        end
        raise Error, "no leader for #{topic}-#{partition}" if leader.negative?

        existing = @connections[leader]
        return existing if existing && !existing.closed?

        broker = meta.broker(leader)
        raise Error, "broker #{leader} is not in the metadata" unless broker

        # A single-broker cluster advertises the address it was configured
        # with, which may not be the one we dialled; reuse the seed.
        @connections[leader] = if meta.brokers.size == 1
                                 seed
                               else
                                 dial(broker.host, broker.port)
                               end
      end
    end

    private

    def snapshot = ClusterMetadata.new(@brokers.dup, @topics.dup)

    def dial(host, port)
      Connection.new(host, port, client_id: @client_id, connect_timeout_ms: @connect_timeout_ms,
                                 request_timeout_ms: @request_timeout_ms)
    end

    def parse_bootstrap(bootstrap)
      list = bootstrap.is_a?(Array) ? bootstrap : bootstrap.to_s.split(",")
      parsed = list.map(&:strip).reject(&:empty?).map do |entry|
        host, _, port = entry.rpartition(":")
        raise ArgumentError, "bootstrap server #{entry.inspect} is not host:port" if host.empty?

        [host.delete_prefix("[").delete_suffix("]"), Integer(port)]
      end
      raise ArgumentError, "bootstrap.servers is empty" if parsed.empty?

      parsed
    end

    # Field order is the schema's: error_code, brokers, controller_id,
    # topics. The leading code is request-level (an authorization denial);
    # the per-topic one is what "no such topic" uses.
    def decode_metadata(reader)
      code = reader.int32
      raise ServerError.new(code, "metadata") unless code == ErrorCode::NONE

      brokers = Array.new(reader.int32) do
        BrokerInfo.new(reader.int32, reader.string, reader.int32, reader.string)
      end
      reader.skip_int32 # controller_id
      topics = {}
      reader.int32.times do
        name = reader.string
        topic_error = reader.int32
        partitions = Array.new(reader.int32) do
          partition = reader.int32
          leader = reader.int32
          replicas = Array.new(reader.int32) { reader.int32 }
          isr = Array.new(reader.int32) { reader.int32 }
          PartitionInfo.new(partition, leader, replicas, isr, reader.int32)
        end
        unless [ErrorCode::NONE, ErrorCode::UNKNOWN_TOPIC_OR_PARTITION].include?(topic_error)
          raise ServerError.new(topic_error, "metadata for #{name}")
        end

        topics[name] = partitions unless partitions.empty?
      end
      [brokers, topics]
    end
  end
end
