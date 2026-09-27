// Three encodings share one connection and they do not agree with each
// other, so keeping them straight is most of the work:
//
//   - The frame header is fixed big-endian: an int32 length prefix, then
//     apiKey/apiVersion/correlationId and an int16-prefixed client id.
//   - A request body is BitPacker: every integer is a zigzag varint, every
//     string and array is a varint count followed by its contents, and the
//     whole body is prefixed with the schema version string.
//   - A record batch is neither. Fixed big-endian header fields and plain
//     (non-zigzag) varints inside each record, because the broker stamps
//     offsets into it in place and validates its CRC without decoding it.
//
// Mixing those up produces a frame the broker rejects with no useful error,
// so each encoder here is explicit about which one it is.

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Text;

namespace Brahmaputra;

/// <summary>Wire-level constants shared by every request.</summary>
public static class Wire
{
    /// <summary>The BitPacker schema version every body carries first.</summary>
    public const string SchemaVersion = "1.0.0";

    /// <summary>
    /// The wire version this client speaks. The broker requires an exact match
    /// and answers UnsupportedVersion otherwise. Version 4 added tombstones,
    /// <c>client.rack</c> on fetch and a rack per broker in Metadata.
    /// </summary>
    public const short ApiVersion = 4;

    /// <summary>ListOffsets sentinel: the oldest retained offset.</summary>
    public const long Earliest = -2;

    /// <summary>ListOffsets sentinel: the next offset to be written.</summary>
    public const long Latest = -1;

    /// <summary>Fetch isolation level: see everything written (the default).</summary>
    public const int ReadUncommitted = 0;

    /// <summary>Fetch isolation level: stop at the last stable offset.</summary>
    public const int ReadCommitted = 1;
}

/// <summary>API keys, in wire order.</summary>
public enum ApiKey : short
{
#pragma warning disable CS1591
    Produce = 0,
    Fetch = 1,
    ListOffsets = 2,
    Metadata = 3,
    ReplicaFetch = 4,
    OffsetsForLeaderEpoch = 5,
    InitProducerId = 6,
    JoinGroup = 7,
    SyncGroup = 8,
    Heartbeat = 9,
    OffsetCommit = 10,
    OffsetFetch = 11,
    ListGroups = 12,
    DescribeGroup = 13,
    ApiVersions = 14,
    ProduceMulti = 15,
    FetchMulti = 16,
    Authenticate = 17,
    LeaveGroup = 18,
#pragma warning restore CS1591
}

/// <summary>Error codes the broker returns in a response's error_code field.</summary>
public enum ErrorCode
{
#pragma warning disable CS1591
    None = 0,
    UnknownTopicOrPartition = 1,
    OffsetOutOfRange = 2,
    InvalidRequest = 3,
    UnsupportedVersion = 4,
    Internal = 5,
    NotLeaderOrFollower = 6,
    FencedBrokerEpoch = 7,
    FencedLeaderEpoch = 8,
    UnknownLeaderEpoch = 9,
    NotEnoughReplicas = 10,
    FencedProducerEpoch = 11,
    OutOfOrderSequence = 12,
    UnknownMemberId = 13,
    RebalanceInProgress = 14,
    NotCoordinator = 15,
    IllegalGeneration = 16,
    CoordinatorLoadInProgress = 17,
    SaslAuthenticationFailed = 18,
    AuthorizationFailed = 19,
#pragma warning restore CS1591
}

/// <summary>Base class of every error this client raises.</summary>
public class BrahmaputraException : Exception
{
    /// <summary>Creates an exception with a message.</summary>
    public BrahmaputraException(string message) : base(message) { }

    /// <summary>Creates an exception with a message and a cause.</summary>
    public BrahmaputraException(string message, Exception inner) : base(message, inner) { }
}

/// <summary>A non-zero error code from the broker.</summary>
public sealed class ServerException : BrahmaputraException
{
    /// <summary>The raw error code.</summary>
    public int Code { get; }

    /// <summary>What the client was doing when the broker refused.</summary>
    public string Context { get; }

    /// <summary>The code as an enum; unknown codes map to their integer value.</summary>
    public ErrorCode Error => (ErrorCode)Code;

    /// <summary>Creates a server error.</summary>
    public ServerException(int code, string context)
        : base(Describe(code, context))
    {
        Code = code;
        Context = context;
    }

    private static string Describe(int code, string context)
    {
        string name = Enum.IsDefined(typeof(ErrorCode), code)
            ? ToScreamingSnake(((ErrorCode)code).ToString())
            : "UNKNOWN";
        return context.Length == 0
            ? $"broker returned {name}[{code}]"
            : $"broker returned {name}[{code}] ({context})";
    }

    private static string ToScreamingSnake(string name)
    {
        var sb = new StringBuilder();
        for (int i = 0; i < name.Length; i++)
        {
            if (i > 0 && char.IsUpper(name[i])) sb.Append('_');
            sb.Append(char.ToUpperInvariant(name[i]));
        }
        return sb.ToString();
    }

    /// <summary>
    /// Whether a code means "this send did not happen". Every code here is one
    /// the broker returns strictly before it appends, so a retry cannot
    /// duplicate a record.
    /// </summary>
    public static bool IsRetriable(int code) => (ErrorCode)code switch
    {
        ErrorCode.NotLeaderOrFollower or ErrorCode.FencedLeaderEpoch or
        ErrorCode.UnknownLeaderEpoch or ErrorCode.NotEnoughReplicas or
        ErrorCode.CoordinatorLoadInProgress or ErrorCode.Internal => true,
        _ => false,
    };
}

/// <summary>
/// Raised when <see cref="AutoOffsetReset.None"/> is in force and there is no
/// position to resume from.
/// </summary>
public sealed class NoOffsetForPartitionException : BrahmaputraException
{
    /// <summary>The partition with no position.</summary>
    public TopicPartition Partition { get; }

    /// <summary>Creates the exception.</summary>
    public NoOffsetForPartitionException(TopicPartition partition)
        : base($"no committed offset for partition {partition} and auto.offset.reset is none")
    {
        Partition = partition;
    }
}

/// <summary>Raised when the producer's bounded buffer stayed full for max.block.ms.</summary>
public sealed class BufferFullException : BrahmaputraException
{
    /// <summary>Creates the exception.</summary>
    public BufferFullException(string message) : base(message) { }
}

/// <summary>A topic and one of its partitions.</summary>
public readonly record struct TopicPartition(string Topic, int Partition) : IComparable<TopicPartition>
{
    /// <inheritdoc/>
    public int CompareTo(TopicPartition other)
    {
        int byTopic = string.CompareOrdinal(Topic, other.Topic);
        return byTopic != 0 ? byTopic : Partition.CompareTo(other.Partition);
    }

    /// <inheritdoc/>
    public override string ToString() => $"{Topic}-{Partition}";
}

// ---------------------------------------------------------------------------
// BitPacker primitives
// ---------------------------------------------------------------------------

/// <summary>
/// Builds a BitPacker body. Every integer goes out zigzag-varint encoded,
/// which is why this cannot share code with the record-batch encoder.
/// </summary>
public sealed class BodyWriter
{
    private byte[] _buf = new byte[256];
    private int _len;

    /// <summary>A writer already carrying the schema version every body starts with.</summary>
    public BodyWriter() => String(Wire.SchemaVersion);

    private void Ensure(int extra)
    {
        if (_len + extra <= _buf.Length) return;
        Array.Resize(ref _buf, Math.Max(_buf.Length * 2, _len + extra));
    }

    /// <summary>Appends an unsigned LEB128 varint.</summary>
    public void UVarint(ulong value)
    {
        Ensure(10);
        while (value >= 0x80)
        {
            _buf[_len++] = (byte)(value | 0x80);
            value >>= 7;
        }
        _buf[_len++] = (byte)value;
    }

    /// <summary>Appends a zigzag int32.</summary>
    public void Int32(int value) => UVarint((uint)((value << 1) ^ (value >> 31)));

    /// <summary>Appends a zigzag int64.</summary>
    public void Int64(long value) => UVarint((ulong)((value << 1) ^ (value >> 63)));

    /// <summary>Appends a one-byte bool.</summary>
    public void Bool(bool value)
    {
        Ensure(1);
        _buf[_len++] = value ? (byte)1 : (byte)0;
    }

    /// <summary>Appends a length-prefixed UTF-8 string.</summary>
    public void String(string? value)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(value ?? string.Empty);
        Int32(bytes.Length);
        Raw(bytes);
    }

    /// <summary>Appends a count-prefixed array of strings.</summary>
    public void StringArray(IReadOnlyCollection<string>? values)
    {
        Int32(values?.Count ?? 0);
        if (values == null) return;
        foreach (string value in values) String(value);
    }

    /// <summary>Appends bytes verbatim.</summary>
    public void Raw(ReadOnlySpan<byte> data)
    {
        Ensure(data.Length);
        data.CopyTo(_buf.AsSpan(_len));
        _len += data.Length;
    }

    /// <summary>The body so far.</summary>
    public byte[] ToArray() => _buf.AsSpan(0, _len).ToArray();
}

/// <summary>Reads a BitPacker body. Throws on truncation.</summary>
public sealed class BodyReader
{
    private readonly byte[] _data;
    private int _pos;

    /// <summary>
    /// A reader positioned past the schema version, which it verifies. A
    /// mismatch means broker and client disagree about the message shapes
    /// themselves, so failing loudly beats decoding garbage.
    /// </summary>
    public BodyReader(byte[] data)
    {
        _data = data;
        string version = String();
        if (version != Wire.SchemaVersion)
        {
            throw new BrahmaputraException(
                $"schema version mismatch: broker speaks \"{version}\", this client speaks \"{Wire.SchemaVersion}\"");
        }
    }

    /// <summary>Reads the leading error code of a body without keeping the reader.</summary>
    public static int PeekErrorCode(byte[] body)
    {
        try { return new BodyReader(body).Int32(); }
        catch (BrahmaputraException) { return 0; }
    }

    /// <summary>Reads an unsigned varint.</summary>
    public ulong UVarint()
    {
        ulong result = 0;
        int shift = 0;
        while (true)
        {
            if (_pos >= _data.Length) throw new BrahmaputraException("truncated varint");
            byte b = _data[_pos++];
            result |= (ulong)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) return result;
            shift += 7;
            if (shift > 63) throw new BrahmaputraException("varint overflows 64 bits");
        }
    }

    /// <summary>Reads a zigzag int32.</summary>
    public int Int32()
    {
        uint v = (uint)UVarint();
        return (int)(v >> 1) ^ -(int)(v & 1);
    }

    /// <summary>Reads a zigzag int64.</summary>
    public long Int64()
    {
        ulong v = UVarint();
        return (long)(v >> 1) ^ -(long)(v & 1);
    }

    /// <summary>Reads a one-byte bool.</summary>
    public bool Bool()
    {
        if (_pos >= _data.Length) throw new BrahmaputraException("truncated bool");
        return _data[_pos++] != 0;
    }

    /// <summary>Reads a length-prefixed UTF-8 string.</summary>
    public string String()
    {
        int length = Int32();
        if (length < 0 || _pos + length > _data.Length) throw new BrahmaputraException("truncated string");
        string value = Encoding.UTF8.GetString(_data, _pos, length);
        _pos += length;
        return value;
    }

    /// <summary>Reads a count-prefixed array of strings.</summary>
    public List<string> StringArray()
    {
        int count = Count();
        var list = new List<string>(count);
        for (int i = 0; i < count; i++) list.Add(String());
        return list;
    }

    /// <summary>Reads an array count, rejecting one that cannot fit in what remains.</summary>
    public int Count()
    {
        int count = Int32();
        // Every element takes at least one byte, so a count beyond the
        // remaining bytes is corrupt and allocating on it would be a hazard.
        if (count < 0 || count > _data.Length - _pos) throw new BrahmaputraException("corrupt array count");
        return count;
    }

    /// <summary>Everything not yet read.</summary>
    public ReadOnlyMemory<byte> Rest()
    {
        var rest = new ReadOnlyMemory<byte>(_data, _pos, _data.Length - _pos);
        _pos = _data.Length;
        return rest;
    }
}

// ---------------------------------------------------------------------------
// Frames
// ---------------------------------------------------------------------------

/// <summary>Frame header encoding: fixed big-endian, independent of the schema.</summary>
public static class Frame
{
    /// <summary>Builds one complete frame, length prefix included.</summary>
    public static byte[] Encode(ApiKey apiKey, int correlationId, string clientId, byte[] body)
    {
        byte[] client = Encoding.UTF8.GetBytes(clientId);
        int payloadLen = 8 + 2 + client.Length + body.Length;
        byte[] frame = new byte[4 + payloadLen];
        Span<byte> span = frame;
        BinaryPrimitives.WriteInt32BigEndian(span, payloadLen);
        BinaryPrimitives.WriteInt16BigEndian(span[4..], (short)apiKey);
        BinaryPrimitives.WriteInt16BigEndian(span[6..], Wire.ApiVersion);
        BinaryPrimitives.WriteInt32BigEndian(span[8..], correlationId);
        BinaryPrimitives.WriteInt16BigEndian(span[12..], (short)client.Length);
        client.CopyTo(span[14..]);
        body.CopyTo(span[(14 + client.Length)..]);
        return frame;
    }

    /// <summary>Splits a response payload (after the length prefix) into correlation id and body.</summary>
    public static (int CorrelationId, byte[] Body) DecodePayload(byte[] payload)
    {
        if (payload.Length < 10) throw new BrahmaputraException("frame payload shorter than its header");
        int correlationId = BinaryPrimitives.ReadInt32BigEndian(payload.AsSpan(4));
        short clientLen = BinaryPrimitives.ReadInt16BigEndian(payload.AsSpan(8));
        int offset = 10 + Math.Max((int)clientLen, 0);
        if (offset > payload.Length) throw new BrahmaputraException("frame client id runs past the payload");
        return (correlationId, payload.AsSpan(offset).ToArray());
    }
}

// ---------------------------------------------------------------------------
// CRC32C
// ---------------------------------------------------------------------------

/// <summary>
/// The Castagnoli CRC record batches carry. Not the zlib CRC32 that most
/// standard libraries mean by "crc32".
/// </summary>
public static class Crc32C
{
    private static readonly uint[] Table = BuildTable();

    private static uint[] BuildTable()
    {
        var table = new uint[256];
        for (uint i = 0; i < 256; i++)
        {
            uint crc = i;
            for (int bit = 0; bit < 8; bit++)
                crc = (crc & 1) != 0 ? (crc >> 1) ^ 0x82F63B78u : crc >> 1;
            table[i] = crc;
        }
        return table;
    }

    /// <summary>Computes CRC32C over data.</summary>
    public static uint Compute(ReadOnlySpan<byte> data)
    {
        uint crc = 0xFFFFFFFFu;
        foreach (byte b in data) crc = Table[(crc ^ b) & 0xFF] ^ (crc >> 8);
        return crc ^ 0xFFFFFFFFu;
    }

    /// <summary>Computes CRC32C over a UTF-8 string.</summary>
    public static uint Compute(string text) => Compute(Encoding.UTF8.GetBytes(text));
}

// ---------------------------------------------------------------------------
// Partitioning
// ---------------------------------------------------------------------------

/// <summary>Kafka's default partitioner.</summary>
public static class Partitioner
{
    /// <summary>
    /// Kafka's 32-bit murmur2, so a key lands on the same partition here as it
    /// would from any other Brahmaputra or Kafka producer.
    /// </summary>
    public static uint Murmur2(ReadOnlySpan<byte> data)
    {
        const uint seed = 0x9747b28c;
        const uint m = 0x5bd1e995;
        const int r = 24;
        unchecked
        {
            int length = data.Length;
            uint h = seed ^ (uint)length;
            int chunks = length / 4;
            for (int i = 0; i < chunks; i++)
            {
                uint k = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(i * 4, 4));
                k *= m;
                k ^= k >> r;
                k *= m;
                h *= m;
                h ^= k;
            }
            int tail = chunks * 4;
            switch (length - tail)
            {
                case 3:
                    h ^= (uint)data[tail + 2] << 16;
                    h ^= (uint)data[tail + 1] << 8;
                    h ^= data[tail];
                    h *= m;
                    break;
                case 2:
                    h ^= (uint)data[tail + 1] << 8;
                    h ^= data[tail];
                    h *= m;
                    break;
                case 1:
                    h ^= data[tail];
                    h *= m;
                    break;
            }
            h ^= h >> 13;
            h *= m;
            h ^= h >> 15;
            return h;
        }
    }

    /// <summary>murmur2(key) % partitions, over partition ids in ascending order.</summary>
    public static int PartitionForKey(ReadOnlySpan<byte> key, IReadOnlyList<int> partitions)
    {
        if (partitions.Count == 0) throw new ArgumentException("no partitions", nameof(partitions));
        return partitions[(int)((Murmur2(key) & 0x7fffffff) % (uint)partitions.Count)];
    }
}
