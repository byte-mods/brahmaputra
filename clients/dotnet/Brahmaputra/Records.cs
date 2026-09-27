using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;

namespace Brahmaputra;

/// <summary>Compression codecs, matching the broker's attribute values.</summary>
public enum CompressionType
{
#pragma warning disable CS1591
    None = 0,
    Lz4 = 1,
    Zstd = 2,
    Snappy = 3,
    Gzip = 4,
#pragma warning restore CS1591
}

/// <summary>
/// Compression codecs. <c>none</c> and <c>gzip</c> are built in; the others are
/// opt-in through <see cref="Register"/>, so an application that does not want
/// an lz4 or zstd dependency does not acquire one by using this client.
/// </summary>
public static class Codecs
{
    private const int MaxDecompressedBytes = 256 * 1024 * 1024;

    private static readonly ConcurrentDictionary<CompressionType, (Func<byte[], byte[]> Compress, Func<byte[], byte[]> Decompress)>
        External = new();

    /// <summary>
    /// Plugs in a codec this package does not carry. The lz4 payload the broker
    /// expects is a little-endian uint32 of the uncompressed length followed by
    /// a raw LZ4 block, not the LZ4 frame format.
    /// </summary>
    public static void Register(CompressionType codec, Func<byte[], byte[]> compress, Func<byte[], byte[]> decompress)
    {
        if (codec is CompressionType.None or CompressionType.Gzip)
            throw new ArgumentException($"{codec} is built in and cannot be replaced", nameof(codec));
        External[codec] = (compress, decompress);
    }

    /// <summary>Maps Kafka's <c>compression.type</c> spelling onto a codec.</summary>
    public static CompressionType Parse(string name) => name.ToLowerInvariant() switch
    {
        "none" => CompressionType.None,
        "lz4" => CompressionType.Lz4,
        "zstd" => CompressionType.Zstd,
        "snappy" => CompressionType.Snappy,
        "gzip" => CompressionType.Gzip,
        _ => throw new ArgumentException($"unknown compression \"{name}\" (none, lz4, zstd, snappy, gzip)"),
    };

    /// <summary>Compresses a records payload.</summary>
    public static byte[] Compress(CompressionType codec, byte[] payload)
    {
        switch (codec)
        {
            case CompressionType.None:
                return payload;
            case CompressionType.Gzip:
            {
                using var output = new MemoryStream();
                using (var gzip = new GZipStream(output, CompressionLevel.Optimal, leaveOpen: true))
                    gzip.Write(payload, 0, payload.Length);
                return output.ToArray();
            }
            default:
                if (External.TryGetValue(codec, out var fns)) return fns.Compress(payload);
                throw new BrahmaputraException(
                    $"{codec.ToString().ToLowerInvariant()} compression is not registered; call Codecs.Register or use none/gzip");
        }
    }

    /// <summary>Decompresses a records payload.</summary>
    public static byte[] Decompress(CompressionType codec, byte[] payload)
    {
        switch (codec)
        {
            case CompressionType.None:
                return payload;
            case CompressionType.Gzip:
            {
                using var input = new GZipStream(new MemoryStream(payload), CompressionMode.Decompress);
                using var output = new MemoryStream();
                byte[] chunk = new byte[64 * 1024];
                int read;
                while ((read = input.Read(chunk, 0, chunk.Length)) > 0)
                {
                    // Capped so a corrupt or hostile batch cannot name gigabytes
                    // of output that this process allocates before rejecting it.
                    if (output.Length + read > MaxDecompressedBytes)
                        throw new BrahmaputraException("decompressed batch exceeds 256 MiB");
                    output.Write(chunk, 0, read);
                }
                return output.ToArray();
            }
            default:
                if (External.TryGetValue(codec, out var fns)) return fns.Decompress(payload);
                throw new BrahmaputraException(
                    $"{codec.ToString().ToLowerInvariant()} decompression is not registered; call Codecs.Register");
        }
    }
}

/// <summary>
/// An ordered, possibly repeating annotation on a record. <see cref="Value"/>
/// may be null, which is distinct from empty.
/// </summary>
public sealed record RecordHeader(string Key, byte[]? Value)
{
    /// <summary>A header with a UTF-8 string value.</summary>
    public RecordHeader(string key, string? value)
        : this(key, value == null ? null : Encoding.UTF8.GetBytes(value)) { }
}

/// <summary>One record inside a batch, as the batch encoding sees it.</summary>
public sealed class BatchRecord
{
    /// <summary>Key, or null.</summary>
    public byte[]? Key { get; set; }

    /// <summary>Value; null is a tombstone, distinct from empty.</summary>
    public byte[]? Value { get; set; }

    /// <summary>Milliseconds relative to the batch's max timestamp (normally zero or negative).</summary>
    public long TimestampDelta { get; set; }

    /// <summary>Headers, in order.</summary>
    public IReadOnlyList<RecordHeader> Headers { get; set; } = Array.Empty<RecordHeader>();
}

/// <summary>One batch read back off the wire.</summary>
public sealed class DecodedBatch
{
    /// <summary>Offset of the first record.</summary>
    public long BaseOffset { get; init; }

    /// <summary>Newest record timestamp; record deltas are relative to it.</summary>
    public long MaxTimestamp { get; init; }

    /// <summary>The records.</summary>
    public List<BatchRecord> Records { get; init; } = new();
}

/// <summary>Record batch encoding: big-endian header, plain varints inside.</summary>
public static class RecordBatch
{
    private const int HeaderLen = 12;
    private const int MinBatchLength = 4 + 1 + 4 + 2 + 4 + 8;
    private const int ProducerExtensionLen = 8 + 2 + 4;
    private const byte MagicV1 = 1;
    private const byte MagicV2 = 2;
    private const int CompressionMask = 0x0007;
    private const int HeadersBit = 0x0008;
    // Some record in this batch has a null value: a tombstone. Set only when
    // one is present, so a batch without one encodes exactly as it always did.
    private const int NullValueBit = 0x0040;

    /// <summary>
    /// Encodes one batch exactly as the broker expects it. The broker never
    /// re-encodes it: it stamps base_offset and leader_epoch in place (both sit
    /// before the CRC) and writes these bytes to disk.
    /// </summary>
    public static byte[] Encode(IReadOnlyList<BatchRecord> records, long maxTimestamp, CompressionType codec)
    {
        bool hasHeaders = records.Any(r => r.Headers.Count > 0);
        // A null value is a tombstone and needs the widened length encoding; an
        // empty non-null value is an ordinary record and must not trigger it.
        bool hasNullValues = records.Any(r => r.Value == null);

        var payload = new MemoryStream();
        var rec = new MemoryStream();
        foreach (BatchRecord record in records)
        {
            rec.SetLength(0);
            if (record.Key == null)
            {
                PutUVarint(rec, 0);
            }
            else
            {
                PutUVarint(rec, (ulong)record.Key.Length + 1);
                rec.Write(record.Key);
            }
            if (hasNullValues)
            {
                if (record.Value == null)
                {
                    PutUVarint(rec, 0);
                }
                else
                {
                    PutUVarint(rec, (ulong)record.Value.Length + 1);
                    rec.Write(record.Value);
                }
            }
            else
            {
                PutUVarint(rec, (ulong)record.Value!.Length);
                rec.Write(record.Value);
            }
            long delta = record.TimestampDelta;
            PutUVarint(rec, (ulong)((delta << 1) ^ (delta >> 63)));
            if (hasHeaders)
            {
                PutUVarint(rec, (ulong)record.Headers.Count);
                foreach (RecordHeader header in record.Headers)
                {
                    byte[] key = Encoding.UTF8.GetBytes(header.Key);
                    PutUVarint(rec, (ulong)key.Length);
                    rec.Write(key);
                    if (header.Value == null)
                    {
                        PutUVarint(rec, 0);
                    }
                    else
                    {
                        PutUVarint(rec, (ulong)header.Value.Length + 1);
                        rec.Write(header.Value);
                    }
                }
            }
            PutUVarint(payload, (ulong)rec.Length);
            rec.Position = 0;
            rec.CopyTo(payload);
        }

        byte[] compressed = Codecs.Compress(codec, payload.ToArray());

        int attributes = (int)codec & CompressionMask;
        if (hasHeaders) attributes |= HeadersBit;
        if (hasNullValues) attributes |= NullValueBit;
        int batchLength = MinBatchLength + compressed.Length;

        byte[] output = new byte[HeaderLen + batchLength];
        Span<byte> span = output;
        // base_offset (0..8) and leader_epoch (12..16) are stamped by the broker.
        BinaryPrimitives.WriteInt32BigEndian(span[8..], batchLength);
        span[16] = MagicV1;
        const int crcAt = 17;
        BinaryPrimitives.WriteUInt16BigEndian(span[21..], (ushort)attributes);
        BinaryPrimitives.WriteInt32BigEndian(span[23..], Math.Max(records.Count - 1, 0));
        BinaryPrimitives.WriteInt64BigEndian(span[27..], maxTimestamp);
        compressed.CopyTo(span[35..]);
        BinaryPrimitives.WriteUInt32BigEndian(span[crcAt..], Crc32C.Compute(span[(crcAt + 4)..]));
        return output;
    }

    /// <summary>Decodes every batch in a fetch response's record set.</summary>
    public static List<DecodedBatch> DecodeAll(ReadOnlySpan<byte> data)
    {
        var batches = new List<DecodedBatch>();
        int pos = 0;
        while (pos < data.Length)
        {
            batches.Add(Decode(data, pos, out int next));
            pos = next;
        }
        return batches;
    }

    /// <summary>Decodes one batch starting at offset.</summary>
    public static DecodedBatch Decode(ReadOnlySpan<byte> data, int offset, out int next)
    {
        if (data.Length - offset < HeaderLen) throw new BrahmaputraException("truncated batch header");
        long baseOffset = BinaryPrimitives.ReadInt64BigEndian(data[offset..]);
        int batchLength = BinaryPrimitives.ReadInt32BigEndian(data[(offset + 8)..]);
        if (batchLength < MinBatchLength) throw new BrahmaputraException("batch_length too small");
        int bodyAt = offset + HeaderLen;
        int end = bodyAt + batchLength;
        if (end > data.Length) throw new BrahmaputraException("truncated batch body");

        byte magic = data[bodyAt + 4];
        if (magic != MagicV1 && magic != MagicV2) throw new BrahmaputraException($"unsupported magic {magic}");
        int crcAt = bodyAt + 5;
        uint stored = BinaryPrimitives.ReadUInt32BigEndian(data[crcAt..]);
        uint computed = Crc32C.Compute(data[(crcAt + 4)..end]);
        if (stored != computed)
            throw new BrahmaputraException($"crc mismatch: stored {stored:x8}, computed {computed:x8}");

        int cursor = crcAt + 4;
        int attributes = BinaryPrimitives.ReadUInt16BigEndian(data[cursor..]);
        long maxTimestamp = BinaryPrimitives.ReadInt64BigEndian(data[(cursor + 6)..]);
        cursor += 14;
        if (magic == MagicV2) cursor += ProducerExtensionLen;
        if (cursor > end) throw new BrahmaputraException("truncated batch header");

        byte[] payload = Codecs.Decompress((CompressionType)(attributes & CompressionMask), data[cursor..end].ToArray());
        var records = DecodeRecords(payload, (attributes & HeadersBit) != 0, (attributes & NullValueBit) != 0);
        next = end;
        return new DecodedBatch { BaseOffset = baseOffset, MaxTimestamp = maxTimestamp, Records = records };
    }

    private static List<BatchRecord> DecodeRecords(byte[] payload, bool hasHeaders, bool hasNullValues)
    {
        var records = new List<BatchRecord>();
        int pos = 0;
        while (pos < payload.Length)
        {
            ulong length = GetUVarint(payload, ref pos);
            if (length > (ulong)(payload.Length - pos)) throw new BrahmaputraException("truncated record");
            int end = pos + (int)length;
            var record = new BatchRecord();

            ulong keyLenPlusOne = GetUVarint(payload, ref pos);
            if (keyLenPlusOne > 0) record.Key = Take(payload, ref pos, keyLenPlusOne - 1, end);

            ulong rawValueLen = GetUVarint(payload, ref pos);
            if (hasNullValues && rawValueLen == 0)
            {
                // A tombstone: left null, which distinguishes it from empty.
                record.Value = null;
            }
            else
            {
                record.Value = Take(payload, ref pos, hasNullValues ? rawValueLen - 1 : rawValueLen, end);
            }

            ulong rawDelta = GetUVarint(payload, ref pos);
            record.TimestampDelta = (long)(rawDelta >> 1) ^ -(long)(rawDelta & 1);

            if (hasHeaders)
            {
                ulong count = GetUVarint(payload, ref pos);
                if (count > (ulong)(end - pos)) throw new BrahmaputraException("record header count exceeds record");
                var headers = new List<RecordHeader>((int)count);
                for (ulong i = 0; i < count; i++)
                {
                    ulong keyLen = GetUVarint(payload, ref pos);
                    string key = Encoding.UTF8.GetString(Take(payload, ref pos, keyLen, end));
                    ulong valuePlusOne = GetUVarint(payload, ref pos);
                    byte[]? value = valuePlusOne == 0 ? null : Take(payload, ref pos, valuePlusOne - 1, end);
                    headers.Add(new RecordHeader(key, value));
                }
                record.Headers = headers;
            }

            if (pos != end) throw new BrahmaputraException("trailing bytes in record");
            records.Add(record);
        }
        return records;
    }

    private static byte[] Take(byte[] data, ref int pos, ulong size, int end)
    {
        if (size > (ulong)(end - pos)) throw new BrahmaputraException("truncated record field");
        byte[] value = data.AsSpan(pos, (int)size).ToArray();
        pos += (int)size;
        return value;
    }

    private static void PutUVarint(Stream stream, ulong value)
    {
        while (value >= 0x80)
        {
            stream.WriteByte((byte)(value | 0x80));
            value >>= 7;
        }
        stream.WriteByte((byte)value);
    }

    private static ulong GetUVarint(byte[] data, ref int pos)
    {
        ulong result = 0;
        int shift = 0;
        while (true)
        {
            if (pos >= data.Length) throw new BrahmaputraException("truncated varint in record");
            byte b = data[pos++];
            result |= (ulong)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) return result;
            shift += 7;
            if (shift > 63) throw new BrahmaputraException("varint overflows 64 bits");
        }
    }
}
