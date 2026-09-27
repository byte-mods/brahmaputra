/**
 * One TCP connection per broker, and the router that picks the right one
 * by partition leader.
 */
module brahmaputra.connection;

import brahmaputra.protocol;

import core.atomic : atomicLoad, atomicStore;
import core.stdc.errno : errno, EINTR, EAGAIN, EWOULDBLOCK;
import core.sync.mutex : Mutex;
import core.time : Duration, MonoTime, dur, msecs, seconds;
import std.algorithm.sorting : sort;
import std.conv : to;
import std.format : format;
import std.socket;

/// Bounds one request/response round trip on the socket. It must exceed the
/// longest the broker may legitimately hold a request (a fetch long-poll,
/// an acks=all wait, a JoinGroup waiting out a rebalance), so it is
/// generous; its job is to turn a wedged broker into an error instead of a
/// thread blocked forever.
enum Duration DEFAULT_REQUEST_TIMEOUT = 120.seconds;

/// Default time allowed to open a TCP connection.
enum Duration DEFAULT_CONNECT_TIMEOUT = 30.seconds;

version (linux)
    private enum int SEND_FLAGS = 0x4000; // MSG_NOSIGNAL: a dead peer is an error, not SIGPIPE
else
    private enum int SEND_FLAGS = 0;

/// Splits "host:port".
void splitAddress(string address, out string host, out ushort port)
{
    import std.string : lastIndexOf;

    const colon = address.lastIndexOf(':');
    if (colon <= 0)
        throw new ConnectionException("address must be host:port, got " ~ address);
    host = address[0 .. colon];
    if (host.length > 2 && host[0] == '[' && host[$ - 1] == ']')
        host = host[1 .. $ - 1];
    try
        port = address[colon + 1 .. $].to!ushort;
    catch (Exception)
        throw new ConnectionException("bad port in address " ~ address);
}

/// One `ApiVersions` entry.
struct ApiVersionRange
{
    int apiKey;
    int minVersion;
    int maxVersion;
}

/// What `Connection.apiVersions` returns.
struct ApiVersionsResult
{
    ApiVersionRange[] ranges;
    string brokerVersion;
}

/// Result of `Connection.authenticate`.
struct AuthResult
{
    string principal;
    string role;
}

/**
 * One TCP connection to one broker.
 *
 * A mutex serialises request/response pairs, so there is at most one
 * request in flight per connection. Any I/O failure, timeout or
 * correlation mismatch leaves the byte stream at an unknown position, so
 * the connection is closed and marked `broken` rather than reused; the
 * `Router` notices and redials.
 */
final class Connection
{
    private Socket sock;
    private string address_;
    private string clientId;
    private Duration timeout;
    private Mutex mu;
    private int next;
    private shared bool broken_;

    private this(Socket sock, string address, string clientId)
    {
        this.sock = sock;
        this.address_ = address;
        this.clientId = clientId;
        this.timeout = DEFAULT_REQUEST_TIMEOUT;
        this.mu = new Mutex;
    }

    /// Opens a connection to one broker ("host:port").
    static Connection dial(string address, string clientId,
        Duration connectTimeout = DEFAULT_CONNECT_TIMEOUT)
    {
        string host;
        ushort port;
        splitAddress(address, host, port);
        Address[] candidates;
        try
            candidates = getAddress(host, port);
        catch (SocketException e)
            throw new ConnectionException("cannot resolve " ~ address ~ ": " ~ e.msg, e);
        if (candidates.length == 0)
            throw new ConnectionException("cannot resolve " ~ address);

        Exception last;
        foreach (candidate; candidates)
        {
            try
                return new Connection(connectSocket(candidate, connectTimeout), address, clientId);
            catch (Exception e)
                last = e;
        }
        throw new ConnectionException("cannot connect to " ~ address ~ ": " ~ last.msg, last);
    }

    private static Socket connectSocket(Address target, Duration connectTimeout)
    {
        auto s = new Socket(target.addressFamily, SocketType.STREAM, ProtocolType.TCP);
        scope (failure)
            s.close();
        if (connectTimeout <= Duration.zero)
            s.connect(target);
        else
        {
            s.blocking = false;
            s.connect(target);
            const deadline = MonoTime.currTime + connectTimeout;
            auto writable = new SocketSet(1);
            while (true)
            {
                writable.reset();
                writable.add(s);
                auto remaining = deadline - MonoTime.currTime;
                if (remaining <= Duration.zero)
                    throw new ConnectionException("connect timed out");
                const n = Socket.select(null, writable, null, remaining);
                if (n < 0)
                    continue; // interrupted
                if (n == 0)
                    throw new ConnectionException("connect timed out");
                break;
            }
            int err;
            s.getOption(SocketOptionLevel.SOCKET, SocketOption.ERROR, err);
            if (err != 0)
                throw new SocketOSException("connect failed", err);
            s.blocking = true;
        }
        // Responses are small and latency matters more than packet count.
        s.setOption(SocketOptionLevel.TCP, SocketOption.TCP_NODELAY, true);
        return s;
    }

    /// Changes how long one round trip may take before the connection is
    /// abandoned. Zero or negative disables the bound.
    void setRequestTimeout(Duration value)
    {
        mu.lock();
        scope (exit)
            mu.unlock();
        timeout = value;
    }

    /// Whether this connection failed and must not be reused.
    @property bool broken() const
    {
        return atomicLoad(broken_);
    }

    /// The host:port this connection was dialled to.
    @property string address() const
    {
        return address_;
    }

    /// Closes the socket. Safe to call from any thread, even while another
    /// thread is inside a request: that request fails with an error.
    void close()
    {
        atomicStore(broken_, true);
        try
            sock.shutdown(SocketShutdown.BOTH);
        catch (Exception)
        {
        }
        if (mu.tryLock())
        {
            scope (exit)
                mu.unlock();
            sock.close();
        }
        // Otherwise the thread holding the lock sees the shutdown, fails and
        // closes the socket itself.
    }

    // Called with mu held.
    private void failLocked()
    {
        atomicStore(broken_, true);
        try
            sock.shutdown(SocketShutdown.BOTH);
        catch (Exception)
        {
        }
        sock.close();
    }

    private MonoTime deadlineFromNow() const
    {
        return timeout > Duration.zero ? MonoTime.currTime + timeout : MonoTime.max;
    }

    private void armTimeout(SocketOption option, MonoTime deadline)
    {
        Duration value = Duration.zero; // zero: no timeout
        if (deadline != MonoTime.max)
        {
            value = deadline - MonoTime.currTime;
            if (value <= Duration.zero)
                throw new ConnectionException(format("request to %s timed out", address_));
            if (value < 1.msecs)
                value = 1.msecs;
        }
        sock.setOption(SocketOptionLevel.SOCKET, option, value);
    }

    private void sendAll(const(ubyte)[] data, MonoTime deadline)
    {
        while (data.length > 0)
        {
            armTimeout(SocketOption.SNDTIMEO, deadline);
            const n = sock.send(data, cast(SocketFlags) SEND_FLAGS);
            if (n == Socket.ERROR)
            {
                const e = errno;
                if (e == EINTR || e == EAGAIN || e == EWOULDBLOCK)
                    continue; // armTimeout throws once the deadline passes
                throw new ConnectionException(format("write to %s failed: %s", address_,
                        lastSocketError()));
            }
            data = data[n .. $];
        }
    }

    private void receiveExact(ubyte[] into, MonoTime deadline)
    {
        while (into.length > 0)
        {
            armTimeout(SocketOption.RCVTIMEO, deadline);
            const n = sock.receive(into);
            if (n == 0)
                throw new ConnectionException(format("%s closed the connection", address_));
            if (n == Socket.ERROR)
            {
                const e = errno;
                if (e == EINTR || e == EAGAIN || e == EWOULDBLOCK)
                    continue;
                throw new ConnectionException(format("read from %s failed: %s", address_,
                        lastSocketError()));
            }
            into = into[n .. $];
        }
    }

    private ubyte[] readFrame(MonoTime deadline)
    {
        ubyte[4] header;
        receiveExact(header[], deadline);
        const length = readBE!int(header[], 0);
        if (length < 0)
            throw new ProtocolException(format("negative frame length %d", length));
        auto payload = new ubyte[length];
        receiveExact(payload, deadline);
        return payload;
    }

    /// Sends one request and returns the matching response body.
    const(ubyte)[] request(short apiKey, const(ubyte)[] body_)
    {
        mu.lock();
        scope (exit)
            mu.unlock();
        if (broken)
            throw new ConnectionException("connection is broken; the router will redial");
        const correlationId = ++next;
        const deadline = deadlineFromNow();
        try
        {
            sendAll(encodeFrame(apiKey, correlationId, clientId, body_), deadline);
            // A timeout here leaves the response possibly still on its way;
            // reading on would pair it with the next request.
            const payload = readFrame(deadline);
            int got;
            auto responseBody = decodeFramePayload(payload, got);
            if (got != correlationId)
                throw new ProtocolException(format(
                        "correlation id mismatch: expected %d, got %d", correlationId, got));
            return responseBody;
        }
        catch (BrahmaputraException e)
        {
            failLocked();
            throw e;
        }
        catch (SocketException e)
        {
            failLocked();
            throw new ConnectionException(e.msg, e);
        }
    }

    /// Sends without awaiting a response (acks=0).
    void sendOneway(short apiKey, const(ubyte)[] body_)
    {
        mu.lock();
        scope (exit)
            mu.unlock();
        if (broken)
            throw new ConnectionException("connection is broken; the router will redial");
        const correlationId = ++next;
        try
            sendAll(encodeFrame(apiKey, correlationId, clientId, body_), deadlineFromNow());
        catch (BrahmaputraException e)
        {
            failLocked();
            throw e;
        }
        catch (SocketException e)
        {
            failLocked();
            throw new ConnectionException(e.msg, e);
        }
    }

    /// Asks the broker what it speaks. Works across a version mismatch.
    ApiVersionsResult apiVersions()
    {
        auto w = BodyWriter.start();
        w.str("brahmaputra-d");
        w.str("0.1.0");
        auto r = BodyReader(request(ApiKey.apiVersions, w.data));
        const code = r.i32();
        if (code != ErrorCode.none)
            throw new ServerException(code, "api_versions");
        ApiVersionsResult result;
        foreach (_; 0 .. r.count())
        {
            ApiVersionRange range;
            range.apiKey = r.i32();
            range.minVersion = r.i32();
            range.maxVersion = r.i32();
            result.ranges ~= range;
        }
        result.brokerVersion = r.str();
        return result;
    }

    /**
     * Binds a principal to this connection using SCRAM-SHA-256; the
     * password never crosses the wire. (The broker refuses credentials on a
     * plaintext listener, so this needs TLS in front of it.)
     */
    AuthResult authenticate(string username, string password)
    {
        import std.algorithm.searching : startsWith;
        import std.array : join;

        const clientNonce = scramNonce();
        const bare = "n=" ~ username ~ ",r=" ~ clientNonce;
        auto first = authenticateStep(username, "", "SCRAM-SHA-256", "n,," ~ bare);
        if (first.code != ErrorCode.none)
            throw new ServerException(first.code, "authenticate");
        if (first.done)
            throw new BrahmaputraException("broker ended the SCRAM exchange before it began");
        const serverFirst = first.payload;
        const nonce = scramField(serverFirst, "r");
        const salt = scramField(serverFirst, "s");
        const iterationsField = scramField(serverFirst, "i");
        if (nonce is null || salt is null || iterationsField is null)
            throw new BrahmaputraException("malformed SCRAM server-first message");
        int iterations;
        try
            iterations = iterationsField.to!int;
        catch (Exception)
            iterations = 0;
        if (iterations <= 0)
            throw new BrahmaputraException("malformed SCRAM iteration count");
        if (!nonce.startsWith(clientNonce))
            throw new BrahmaputraException("SCRAM server nonce does not extend the client nonce");
        const withoutProof = "c=biws,r=" ~ nonce;
        const authMessage = [bare, serverFirst, withoutProof].join(",");
        const proof = scramClientProof(password, salt, iterations, authMessage);
        auto second = authenticateStep(username, "", "SCRAM-SHA-256",
            withoutProof ~ ",p=" ~ proof);
        if (second.code != ErrorCode.none)
            throw new ServerException(second.code, "authenticate");
        return AuthResult(second.principal, second.role);
    }

    /// Sends the password itself, as SASL/PLAIN does.
    AuthResult authenticatePlain(string username, string password)
    {
        auto step = authenticateStep(username, password, "PLAIN", "");
        if (step.code != ErrorCode.none)
            throw new ServerException(step.code, "authenticate");
        return AuthResult(step.principal, step.role);
    }

    private static struct AuthStep
    {
        int code;
        string principal;
        string role;
        string payload;
        bool done;
    }

    private AuthStep authenticateStep(string username, string password,
        string mechanism, string payload)
    {
        auto w = BodyWriter.start();
        w.str(username);
        w.str(password);
        w.str(mechanism);
        w.str(payload);
        auto r = BodyReader(request(ApiKey.authenticate, w.data));
        AuthStep step;
        step.code = r.i32();
        step.principal = r.str();
        step.role = r.str();
        step.payload = r.str();
        step.done = r.boolean();
        return step;
    }
}

private string scramNonce()
{
    import std.array : replace;
    import std.base64 : Base64;
    import std.random : Random, uniform, unpredictableSeed;

    auto rng = Random(unpredictableSeed);
    ubyte[18] raw;
    foreach (ref b; raw)
        b = cast(ubyte) uniform(0, 256, rng);
    return Base64.encode(raw[]).idup.replace(",", ".");
}

private string scramField(string message, string key)
{
    import std.algorithm.iteration : splitter;
    import std.algorithm.searching : startsWith;

    foreach (part; message.splitter(','))
        if (part.startsWith(key ~ "="))
            return part[key.length + 1 .. $];
    return null;
}

private ubyte[32] scramHmac(const(ubyte)[] key, const(ubyte)[] message)
{
    import std.digest.hmac : hmac;
    import std.digest.sha : SHA256;

    return hmac!SHA256(message, key);
}

private string scramClientProof(string password, string salt, int iterations, string authMessage)
{
    import std.base64 : Base64, Base64Exception;
    import std.digest.sha : sha256Of;

    ubyte[] decodedSalt;
    try
        decodedSalt = Base64.decode(salt);
    catch (Base64Exception)
        throw new BrahmaputraException("malformed SCRAM salt");
    // PBKDF2-HMAC-SHA256, one output block.
    auto pw = cast(const(ubyte)[]) password;
    auto u = scramHmac(pw, decodedSalt ~ cast(ubyte[])[0, 0, 0, 1]);
    ubyte[32] salted = u;
    foreach (_; 1 .. iterations)
    {
        u = scramHmac(pw, u[]);
        foreach (j; 0 .. 32)
            salted[j] ^= u[j];
    }
    auto clientKey = scramHmac(salted[], cast(const(ubyte)[]) "Client Key");
    auto storedKey = sha256Of(clientKey[]);
    auto signature = scramHmac(storedKey[], cast(const(ubyte)[]) authMessage);
    ubyte[32] proof;
    foreach (j; 0 .. 32)
        proof[j] = clientKey[j] ^ signature[j];
    return Base64.encode(proof[]).idup;
}

// ---------------------------------------------------------------------------
// Metadata and routing
// ---------------------------------------------------------------------------

/// One broker in the cluster.
struct BrokerInfo
{
    int nodeId;
    string host;
    int port;
    /// Failure domain, empty when the broker was started without --rack.
    string rack;
}

/// One partition's placement.
struct PartitionInfo
{
    int partition;
    int leader;
    int[] replicas;
    int[] isr;
    int leaderEpoch;
}

/// One topic's placement.
struct TopicInfo
{
    string name;
    PartitionInfo[] partitions;
}

/// A metadata snapshot.
final class ClusterMetadata
{
    BrokerInfo[] brokers;
    TopicInfo[] topics;

    /// A topic's partition ids in ascending order (empty if unknown).
    int[] partitionsOf(string topic) const
    {
        foreach (ref t; topics)
        {
            if (t.name != topic)
                continue;
            int[] out_;
            foreach (ref p; t.partitions)
                out_ ~= p.partition;
            out_.sort();
            return out_;
        }
        return null;
    }

    /// The broker id leading a partition, or -1.
    int leaderOf(string topic, int partition) const
    {
        foreach (ref t; topics)
        {
            if (t.name != topic)
                continue;
            foreach (ref p; t.partitions)
                if (p.partition == partition)
                    return p.leader;
        }
        return -1;
    }
}

private ClusterMetadata decodeMetadata(ref BodyReader r)
{
    // Field order is the schema's: error_code, brokers, controller_id,
    // topics. The leading code is request-level (an authorization denial);
    // "no such topic" is the per-topic code.
    auto metadata = new ClusterMetadata;
    const code = r.i32();
    if (code != ErrorCode.none)
        throw new ServerException(code, "metadata");
    foreach (_; 0 .. r.count())
    {
        BrokerInfo broker;
        broker.nodeId = r.i32();
        broker.host = r.str();
        broker.port = r.i32();
        broker.rack = r.str();
        metadata.brokers ~= broker;
    }
    r.i32(); // controller_id
    foreach (_; 0 .. r.count())
    {
        TopicInfo topic;
        topic.name = r.str();
        const topicError = r.i32();
        foreach (__; 0 .. r.count())
        {
            PartitionInfo info;
            info.partition = r.i32();
            info.leader = r.i32();
            foreach (___; 0 .. r.count())
                info.replicas ~= r.i32();
            foreach (___; 0 .. r.count())
                info.isr ~= r.i32();
            info.leaderEpoch = r.i32();
            topic.partitions ~= info;
        }
        if (topicError != ErrorCode.none && topicError != ErrorCode.unknownTopicOrPartition)
            throw new ServerException(topicError, "metadata for " ~ topic.name);
        metadata.topics ~= topic;
    }
    return metadata;
}

/**
 * Keeps connections to every broker and routes by partition leader.
 *
 * Metadata is cached and refreshed only when a request says the route was
 * stale. A connection that failed is replaced on its next use — the seed
 * included — so one dropped socket does not fail every later request.
 */
final class Router
{
    private string clientId;
    private Duration connectTimeout;
    private Duration requestTimeout;
    private string seedAddress;
    private Connection seed_;
    private Mutex mu;
    private Connection[int] conns;
    private ClusterMetadata cached;

    this(string address, string clientId, Duration connectTimeout = DEFAULT_CONNECT_TIMEOUT,
        Duration requestTimeout = DEFAULT_REQUEST_TIMEOUT)
    {
        this.clientId = clientId;
        this.connectTimeout = connectTimeout;
        this.requestTimeout = requestTimeout;
        this.seedAddress = address;
        this.mu = new Mutex;
        this.seed_ = dialConfigured(address);
    }

    private Connection dialConfigured(string address)
    {
        auto conn = Connection.dial(address, clientId, connectTimeout);
        conn.setRequestTimeout(requestTimeout);
        return conn;
    }

    /// Closes every connection.
    void close()
    {
        mu.lock();
        scope (exit)
            mu.unlock();
        foreach (conn; conns.byValue)
            if (conn !is seed_)
                conn.close();
        conns = null;
        seed_.close();
    }

    /// The connection this router was opened with, redialled if it failed.
    Connection seed()
    {
        mu.lock();
        scope (exit)
            mu.unlock();
        try
            return liveSeedLocked();
        catch (ConnectionException)
            return seed_;
    }

    // Called with mu held.
    private Connection liveSeedLocked()
    {
        if (!seed_.broken)
            return seed_;
        auto conn = dialConfigured(seedAddress);
        auto old = seed_;
        seed_ = conn;
        foreach (id, ref cachedConn; conns)
            if (cachedConn is old)
                cachedConn = conn;
        return conn;
    }

    /// Cluster metadata, from cache unless `refresh`.
    ClusterMetadata metadata(const(string)[] topics, bool refresh)
    {
        mu.lock();
        scope (exit)
            mu.unlock();
        if (!refresh && cached !is null)
            return cached;
        auto seedConn = liveSeedLocked();
        auto w = BodyWriter.start();
        w.strArray(topics);
        auto r = BodyReader(seedConn.request(ApiKey.metadata, w.data));
        cached = decodeMetadata(r);
        return cached;
    }

    /// Re-reads metadata for one topic.
    ClusterMetadata refresh(string topic)
    {
        return metadata([topic], true);
    }

    /// A topic's partitions, auto-creating it if the broker does so on
    /// first reference.
    int[] partitions(string topic)
    {
        auto md = metadata([topic], false);
        auto result = md.partitionsOf(topic);
        if (result.length == 0)
        {
            // A new topic is not in the cached image yet; one refresh
            // distinguishes "new" from "absent".
            md = refresh(topic);
            result = md.partitionsOf(topic);
        }
        if (result.length == 0)
            throw new BrahmaputraException(format("topic %s has no partitions", topic));
        return result;
    }

    /// The connection to a partition's leader.
    Connection connFor(string topic, int partition)
    {
        auto md = metadata([topic], false);
        auto leader = md.leaderOf(topic, partition);
        if (leader < 0)
        {
            md = refresh(topic);
            leader = md.leaderOf(topic, partition);
        }
        if (leader < 0)
            throw new BrahmaputraException(format("no leader for %s-%d", topic, partition));

        mu.lock();
        scope (exit)
            mu.unlock();
        if (auto existing = leader in conns)
        {
            auto conn = *existing;
            if (!conn.broken)
                return conn;
            conns.remove(leader);
            if (conn !is seed_)
                conn.close();
        }
        foreach (ref broker; md.brokers)
        {
            if (broker.nodeId != leader)
                continue;
            // A single-broker cluster advertises the address it was
            // configured with, which may not be the one we dialled; reuse
            // the seed rather than opening a second connection to it.
            if (md.brokers.length == 1)
            {
                auto seedConn = liveSeedLocked();
                conns[leader] = seedConn;
                return seedConn;
            }
            auto conn = dialConfigured(format("%s:%d", broker.host, broker.port));
            conns[leader] = conn;
            return conn;
        }
        throw new BrahmaputraException(format("broker %d is not in the metadata", leader));
    }
}
