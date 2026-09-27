namespace Brahmaputra.FSharp

open System

/// One raw TCP connection to one broker. Most code never needs this: the
/// producer and consumers route through a pooled router. It is here for
/// probing a broker directly.
[<Sealed>]
type Connection internal (inner: Brahmaputra.BrokerConnection) =
    /// The driver object underneath.
    member _.Underlying = inner

    /// The address this connection was dialled with.
    member _.Address = inner.Address

    /// True once a request timed out, hit an I/O error or saw a correlation
    /// mismatch; a broken connection is closed and must not be reused.
    member _.IsBroken = inner.IsBroken

    interface IDisposable with
        member _.Dispose() = inner.Dispose()

/// Opens and uses raw broker connections.
[<RequireQualifiedAccess>]
module Connection =
    /// Dials `host:port`. `requestTimeout` bounds every request's round trip.
    let connect (address: string) (clientId: string) (connectTimeout: TimeSpan) (requestTimeout: TimeSpan) =
        Attempt.run (fun () ->
            new Connection(Brahmaputra.BrokerConnection.Connect(address, clientId, connectTimeout, requestTimeout)))

    /// Asks the broker which API versions it speaks.
    let apiVersions (connection: Connection) : Result<ApiVersions, BrahmaputraError> =
        Attempt.run (fun () -> connection.Underlying.ApiVersions().ToTuple() |> Convert.apiVersions)

    /// Asynchronous `apiVersions`.
    let apiVersionsAsync (connection: Connection) : Async<Result<ApiVersions, BrahmaputraError>> =
        async {
            let! result = Attempt.runAsync (fun token -> connection.Underlying.ApiVersionsAsync(token))
            return result |> Result.map (fun v -> Convert.apiVersions (v.ToTuple()))
        }

    /// True once the connection failed and was closed.
    let isBroken (connection: Connection) = connection.IsBroken

    /// Closes the socket.
    let close (connection: Connection) = (connection :> IDisposable).Dispose()

/// Kafka-compatible key partitioning.
[<RequireQualifiedAccess>]
module Partitioner =
    /// Kafka's murmur2 hash. `murmur2 [||] = 275646681u`.
    let murmur2 (key: byte[]) : uint32 =
        Brahmaputra.Partitioner.Murmur2(ReadOnlySpan<byte>(key))

    /// The partition a key maps to: murmur2(key) mod partitions, over ids in ascending order.
    let partitionForKey (key: byte[]) (partitions: int list) : int =
        Brahmaputra.Partitioner.PartitionForKey(ReadOnlySpan<byte>(key), ResizeArray(partitions))

/// Compression codec registry. `none` and `gzip` are built in.
[<RequireQualifiedAccess>]
module Codecs =
    /// Plugs in lz4, zstd or snappy. For lz4 the broker expects a little-endian
    /// uint32 of the uncompressed length followed by a raw LZ4 block.
    let register (codec: Compression) (compress: byte[] -> byte[]) (decompress: byte[] -> byte[]) =
        Attempt.run (fun () ->
            Brahmaputra.Codecs.Register(Convert.compression codec, Func<_, _>(compress), Func<_, _>(decompress)))

    /// Parses Kafka's `compression.type` spelling.
    let parse (name: string) : Result<Compression, BrahmaputraError> =
        match name.ToLowerInvariant() with
        | "none" -> Ok Compression.None
        | "gzip" -> Ok Compression.Gzip
        | "lz4" -> Ok Compression.Lz4
        | "zstd" -> Ok Compression.Zstd
        | "snappy" -> Ok Compression.Snappy
        | other -> Error(BrahmaputraError.InvalidArgument(sprintf "unknown compression \"%s\"" other))
