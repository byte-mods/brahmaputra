using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Brahmaputra;

/// <summary>One entry of an ApiVersions response.</summary>
public readonly record struct ApiVersionRange(int ApiKey, int MinVersion, int MaxVersion);

/// <summary>The result of a successful authentication.</summary>
public readonly record struct AuthenticatedPrincipal(string Principal, string Role);

/// <summary>
/// One TCP connection to one broker. Requests are serialised: each one is
/// written and its response read before the next begins, and the response is
/// matched by correlation id.
/// </summary>
/// <remarks>
/// A request that is cancelled or times out part-way leaves the stream at an
/// unknown position, so the connection closes itself and reports
/// <see cref="IsBroken"/>; the router then replaces it on next use.
/// </remarks>
public sealed class BrokerConnection : IDisposable
{
    private readonly TcpClient _client;
    private readonly NetworkStream _stream;
    private readonly string _clientId;
    private readonly SemaphoreSlim _lock = new(1, 1);
    private readonly TimeSpan _requestTimeout;
    private int _next;
    private volatile bool _broken;

    /// <summary>The address this connection was dialled with.</summary>
    public string Address { get; }

    /// <summary>True once the connection has failed or been closed and must not be reused.</summary>
    public bool IsBroken => _broken;

    private BrokerConnection(TcpClient client, string address, string clientId, TimeSpan requestTimeout)
    {
        _client = client;
        _stream = client.GetStream();
        _clientId = clientId;
        _requestTimeout = requestTimeout;
        Address = address;
    }

    /// <summary>Opens a connection to <c>host:port</c>.</summary>
    public static async Task<BrokerConnection> ConnectAsync(
        string address, string clientId, TimeSpan connectTimeout, TimeSpan requestTimeout,
        CancellationToken cancellationToken = default)
    {
        (string host, int port) = ParseAddress(address);
        var client = new TcpClient { NoDelay = true };
        // Responses are small and latency matters more than packet count;
        // without NoDelay every request pays Nagle plus the peer's delayed ACK.
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(connectTimeout);
        try
        {
            await client.ConnectAsync(host, port, cts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            client.Dispose();
            throw new BrahmaputraException($"connect to {address} timed out after {connectTimeout.TotalMilliseconds:0}ms");
        }
        catch (SocketException e)
        {
            client.Dispose();
            throw new BrahmaputraException($"connect to {address} failed: {e.Message}", e);
        }
        catch
        {
            client.Dispose();
            throw;
        }
        return new BrokerConnection(client, address, clientId, requestTimeout);
    }

    /// <summary>Opens a connection synchronously.</summary>
    public static BrokerConnection Connect(string address, string clientId, TimeSpan connectTimeout, TimeSpan requestTimeout) =>
        ConnectAsync(address, clientId, connectTimeout, requestTimeout).GetAwaiter().GetResult();

    internal static (string Host, int Port) ParseAddress(string address)
    {
        int colon = address.LastIndexOf(':');
        if (colon <= 0 || !int.TryParse(address.AsSpan(colon + 1), out int port))
            throw new ArgumentException($"address \"{address}\" is not host:port");
        string host = address[..colon].Trim('[', ']');
        return (host, port);
    }

    /// <summary>Closes the socket.</summary>
    public void Dispose()
    {
        _broken = true;
        _client.Dispose();
    }

    /// <summary>Sends one request and returns the matching response body.</summary>
    public async Task<byte[]> RequestAsync(
        ApiKey apiKey, byte[] body, CancellationToken cancellationToken = default, TimeSpan? extraTimeout = null)
    {
        await _lock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_broken) throw new BrahmaputraException($"connection to {Address} is closed");
            int correlationId = ++_next;
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(_requestTimeout + (extraTimeout ?? TimeSpan.Zero));
            try
            {
                await _stream.WriteAsync(Frame.Encode(apiKey, correlationId, _clientId, body), cts.Token).ConfigureAwait(false);
                byte[] payload = await ReadFrameAsync(cts.Token).ConfigureAwait(false);
                (int got, byte[] responseBody) = Frame.DecodePayload(payload);
                if (got != correlationId)
                {
                    // A response for a request we are not waiting on means the
                    // stream has desynchronised; continuing would pair every
                    // later response with the wrong request.
                    throw new BrahmaputraException($"correlation id mismatch: expected {correlationId}, got {got}");
                }
                return responseBody;
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                Dispose();
                throw new TimeoutException($"{apiKey} request to {Address} timed out");
            }
            catch
            {
                Dispose();
                throw;
            }
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>Sends one request synchronously.</summary>
    public byte[] Request(ApiKey apiKey, byte[] body) => RequestAsync(apiKey, body).GetAwaiter().GetResult();

    /// <summary>Sends without awaiting a response (acks=0).</summary>
    public async Task SendOnewayAsync(ApiKey apiKey, byte[] body, CancellationToken cancellationToken = default)
    {
        await _lock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_broken) throw new BrahmaputraException($"connection to {Address} is closed");
            int correlationId = ++_next;
            try
            {
                await _stream.WriteAsync(Frame.Encode(apiKey, correlationId, _clientId, body), cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                Dispose();
                throw;
            }
        }
        finally
        {
            _lock.Release();
        }
    }

    private async Task<byte[]> ReadFrameAsync(CancellationToken cancellationToken)
    {
        byte[] header = new byte[4];
        await _stream.ReadExactlyAsync(header, cancellationToken).ConfigureAwait(false);
        int length = BinaryPrimitives.ReadInt32BigEndian(header);
        if (length < 0) throw new BrahmaputraException($"negative frame length {length}");
        byte[] payload = new byte[length];
        await _stream.ReadExactlyAsync(payload, cancellationToken).ConfigureAwait(false);
        return payload;
    }

    /// <summary>
    /// Asks the broker what it speaks. This is the one call that works across a
    /// version mismatch.
    /// </summary>
    public async Task<(IReadOnlyList<ApiVersionRange> Versions, string BrokerVersion)> ApiVersionsAsync(
        CancellationToken cancellationToken = default)
    {
        var w = new BodyWriter();
        w.String("brahmaputra-dotnet");
        w.String("0.1.0");
        var r = new BodyReader(await RequestAsync(ApiKey.ApiVersions, w.ToArray(), cancellationToken).ConfigureAwait(false));
        int code = r.Int32();
        if (code != 0) throw new ServerException(code, "api_versions");
        int count = r.Count();
        var ranges = new List<ApiVersionRange>(count);
        for (int i = 0; i < count; i++) ranges.Add(new ApiVersionRange(r.Int32(), r.Int32(), r.Int32()));
        return (ranges, r.String());
    }

    /// <summary>Synchronous <see cref="ApiVersionsAsync"/>.</summary>
    public (IReadOnlyList<ApiVersionRange> Versions, string BrokerVersion) ApiVersions() =>
        ApiVersionsAsync().GetAwaiter().GetResult();

    // -----------------------------------------------------------------------
    // Authentication
    // -----------------------------------------------------------------------

    private const string ScramMechanism = "SCRAM-SHA-256";

    /// <summary>
    /// Binds a principal to this connection using SCRAM-SHA-256. The password
    /// never crosses the wire. Note the broker refuses credentials on a
    /// plaintext listener, and this client does not yet speak TLS.
    /// </summary>
    public async Task<AuthenticatedPrincipal> AuthenticateAsync(
        string username, string password, CancellationToken cancellationToken = default)
    {
        string clientNonce = Convert.ToBase64String(RandomNumberGenerator.GetBytes(18)).Replace(',', '.');
        string bare = $"n={username},r={clientNonce}";
        var first = await AuthenticateStepAsync(username, "", ScramMechanism, "n,," + bare, cancellationToken).ConfigureAwait(false);
        if (first.Code != 0) throw new ServerException(first.Code, "authenticate");
        if (first.Done) throw new BrahmaputraException("broker ended the SCRAM exchange before it began");
        string serverFirst = first.Payload;
        string nonce = ScramField(serverFirst, "r") ?? throw new BrahmaputraException("malformed SCRAM server-first message");
        string salt = ScramField(serverFirst, "s") ?? throw new BrahmaputraException("malformed SCRAM server-first message");
        string iterationsField = ScramField(serverFirst, "i") ?? throw new BrahmaputraException("malformed SCRAM server-first message");
        if (!int.TryParse(iterationsField, out int iterations) || iterations <= 0)
            throw new BrahmaputraException("malformed SCRAM iteration count");
        // The server must have kept this client's nonce, which is what makes
        // the exchange this one rather than a replay of an earlier one.
        if (!nonce.StartsWith(clientNonce, StringComparison.Ordinal))
            throw new BrahmaputraException("SCRAM server nonce does not extend the client nonce");
        string withoutProof = "c=biws,r=" + nonce;
        string authMessage = string.Join(",", bare, serverFirst, withoutProof);

        byte[] saltBytes;
        try { saltBytes = Convert.FromBase64String(salt); }
        catch (FormatException) { throw new BrahmaputraException("malformed SCRAM salt"); }
        byte[] salted = Rfc2898DeriveBytes.Pbkdf2(Encoding.UTF8.GetBytes(password), saltBytes, iterations, HashAlgorithmName.SHA256, 32);
        byte[] clientKey = HMACSHA256.HashData(salted, Encoding.UTF8.GetBytes("Client Key"));
        byte[] storedKey = SHA256.HashData(clientKey);
        byte[] signature = HMACSHA256.HashData(storedKey, Encoding.UTF8.GetBytes(authMessage));
        byte[] proof = new byte[clientKey.Length];
        for (int i = 0; i < proof.Length; i++) proof[i] = (byte)(clientKey[i] ^ signature[i]);

        var final = await AuthenticateStepAsync(
            username, "", ScramMechanism, withoutProof + ",p=" + Convert.ToBase64String(proof), cancellationToken).ConfigureAwait(false);
        if (final.Code != 0) throw new ServerException(final.Code, "authenticate");
        return new AuthenticatedPrincipal(final.Principal, final.Role);
    }

    /// <summary>
    /// Sends the password itself, exactly as SASL/PLAIN does. The broker refuses
    /// it on a plaintext listener.
    /// </summary>
    public async Task<AuthenticatedPrincipal> AuthenticatePlainAsync(
        string username, string password, CancellationToken cancellationToken = default)
    {
        var step = await AuthenticateStepAsync(username, password, "PLAIN", "", cancellationToken).ConfigureAwait(false);
        if (step.Code != 0) throw new ServerException(step.Code, "authenticate");
        return new AuthenticatedPrincipal(step.Principal, step.Role);
    }

    private async Task<(int Code, string Principal, string Role, string Payload, bool Done)> AuthenticateStepAsync(
        string username, string password, string mechanism, string payload, CancellationToken cancellationToken)
    {
        var w = new BodyWriter();
        w.String(username);
        w.String(password);
        w.String(mechanism);
        w.String(payload);
        var r = new BodyReader(await RequestAsync(ApiKey.Authenticate, w.ToArray(), cancellationToken).ConfigureAwait(false));
        return (r.Int32(), r.String(), r.String(), r.String(), r.Bool());
    }

    private static string? ScramField(string message, string key)
    {
        foreach (string part in message.Split(','))
            if (part.StartsWith(key + "=", StringComparison.Ordinal)) return part[(key.Length + 1)..];
        return null;
    }
}
