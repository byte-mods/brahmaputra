"""Brahmaputra wire protocol: framing, BitPacker bodies, record batches.

Three encodings live in one connection and they do not agree with each
other, so keeping them straight is most of the work:

* The **frame header** is fixed big-endian — an i32 length prefix, then
  api_key/api_version/correlation_id and a length-prefixed client id.
* A **request body** is BitPacker: every integer is a zigzag varint, every
  string and array is a varint count followed by its contents, and the
  whole body is prefixed with the schema version string.
* A **record batch** is neither. It is a hand-rolled format with fixed
  big-endian header fields and *plain* (non-zigzag) varints inside each
  record, because the broker stamps offsets into it in place and validates
  its CRC without decoding it.

Mixing those up produces a frame the broker rejects with no useful error,
so each encoder here is deliberately explicit about which one it is.
"""

from __future__ import annotations

import gzip
import importlib
import struct
import zlib
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple, Union

# The BitPacker schema version every body carries as its first field.
SCHEMA_VERSION = "1.0.0"
# Wire version this client speaks. The broker requires an exact match.
# Version 3 added transactions: Fetch carries an isolation_level, and
# MetadataResponse carries a request-level error code so an authorization
# denial is no longer reported as an unknown topic.
API_VERSION = 4

# Isolation levels for a fetch. READ_UNCOMMITTED is the default and is what
# every non-transactional topic gives either way.
READ_UNCOMMITTED = 0
READ_COMMITTED = 1

BATCH_HEADER_LEN = 12
MIN_BATCH_LENGTH = 4 + 1 + 4 + 2 + 4 + 8
PRODUCER_EXTENSION_LEN = 8 + 2 + 4

MAGIC_V1 = 1
MAGIC_V2 = 2

COMPRESSION_MASK = 0x0007
HEADERS_BIT = 0x0008
# Some record in this batch has a null value: a tombstone, which deletes its
# key on a compacted topic. Set only when one is present, so a batch without
# one encodes exactly as it always did.
NULL_VALUE_BIT = 0x0040


class ApiKey:
    PRODUCE = 0
    FETCH = 1
    LIST_OFFSETS = 2
    METADATA = 3
    REPLICA_FETCH = 4
    OFFSETS_FOR_LEADER_EPOCH = 5
    INIT_PRODUCER_ID = 6
    JOIN_GROUP = 7
    SYNC_GROUP = 8
    HEARTBEAT = 9
    OFFSET_COMMIT = 10
    OFFSET_FETCH = 11
    LIST_GROUPS = 12
    DESCRIBE_GROUP = 13
    API_VERSIONS = 14
    PRODUCE_MULTI = 15
    FETCH_MULTI = 16
    AUTHENTICATE = 17
    LEAVE_GROUP = 18


class ErrorCode:
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


ERROR_NAMES = {
    value: name
    for name, value in vars(ErrorCode).items()
    if not name.startswith("_") and isinstance(value, int)
}

# Codes the broker only ever returns *before* it appends anything, so a
# retry cannot duplicate a record. Anything not here is returned to the
# caller as-is: a malformed request or a failed authorization fails
# identically however often it is sent, and the idempotence errors mean the
# producer's sequence state is already broken.
RETRIABLE_ERRORS = frozenset(
    {
        ErrorCode.NOT_LEADER_OR_FOLLOWER,
        ErrorCode.FENCED_LEADER_EPOCH,
        ErrorCode.UNKNOWN_LEADER_EPOCH,
        ErrorCode.NOT_ENOUGH_REPLICAS,
        ErrorCode.COORDINATOR_LOAD_IN_PROGRESS,
        ErrorCode.INTERNAL,
    }
)


class BrahmaputraError(Exception):
    """Base class for every error this client raises."""


class ProtocolError(BrahmaputraError):
    """The bytes on the wire were not what the protocol allows."""


class ServerError(BrahmaputraError):
    """The broker answered with a non-zero error code."""

    def __init__(self, code: int, context: str = "") -> None:
        name = ERROR_NAMES.get(code, "UNKNOWN")
        suffix = f" ({context})" if context else ""
        super().__init__(f"broker returned {name}[{code}]{suffix}")
        self.code = code


class NoOffsetForPartition(BrahmaputraError):
    """`auto.offset.reset=none` and there is no position to resume from."""


# --------------------------------------------------------------------------
# BitPacker primitives
# --------------------------------------------------------------------------


class Writer:
    """Builds a BitPacker body.

    Every integer goes out zigzag-varint encoded. That is why this cannot
    share code with the record-batch encoder below, which uses fixed-width
    fields and plain varints.
    """

    __slots__ = ("_buf",)

    def __init__(self) -> None:
        self._buf = bytearray()

    def raw(self, data: bytes) -> None:
        self._buf += data

    def uvarint(self, value: int) -> None:
        if value < 0:
            raise ValueError("uvarint cannot encode a negative value")
        while True:
            byte = value & 0x7F
            value >>= 7
            if value:
                self._buf.append(byte | 0x80)
            else:
                self._buf.append(byte)
                return

    def i32(self, value: int) -> None:
        self.uvarint(zigzag_encode(value, 32))

    def i64(self, value: int) -> None:
        self.uvarint(zigzag_encode(value, 64))

    def boolean(self, value: bool) -> None:
        self._buf.append(1 if value else 0)

    def string(self, value: str) -> None:
        encoded = value.encode("utf-8")
        self.i32(len(encoded))
        self._buf += encoded

    def string_array(self, values: List[str]) -> None:
        self.i32(len(values))
        for value in values:
            self.string(value)

    def bytes(self) -> bytes:
        return bytes(self._buf)


class Reader:
    """Reads a BitPacker body."""

    __slots__ = ("_data", "_pos")

    def __init__(self, data: bytes) -> None:
        self._data = data
        self._pos = 0

    @property
    def remaining(self) -> int:
        return len(self._data) - self._pos

    def uvarint(self) -> int:
        result = 0
        shift = 0
        while True:
            if self._pos >= len(self._data):
                raise ProtocolError("truncated varint")
            byte = self._data[self._pos]
            self._pos += 1
            result |= (byte & 0x7F) << shift
            if not byte & 0x80:
                return result
            shift += 7
            if shift > 63:
                raise ProtocolError("varint overflows 64 bits")

    def i32(self) -> int:
        return zigzag_decode(self.uvarint())

    def i64(self) -> int:
        return zigzag_decode(self.uvarint())

    def boolean(self) -> bool:
        if self._pos >= len(self._data):
            raise ProtocolError("truncated bool")
        value = self._data[self._pos]
        self._pos += 1
        return value != 0

    def string(self) -> str:
        length = self.i32()
        if length < 0 or self._pos + length > len(self._data):
            raise ProtocolError("truncated string")
        value = self._data[self._pos : self._pos + length]
        self._pos += length
        return value.decode("utf-8")

    def string_array(self) -> List[str]:
        return [self.string() for _ in range(self.i32())]

    def rest(self) -> bytes:
        value = self._data[self._pos :]
        self._pos = len(self._data)
        return value


def zigzag_encode(value: int, bits: int) -> int:
    """Map a signed integer onto the unsigned range varints can carry.

    Small negatives must stay small, or every negative sentinel the
    protocol uses (a null length, `latest` = -1) would cost ten bytes.
    """
    mask = (1 << bits) - 1
    return ((value << 1) ^ (value >> (bits - 1))) & mask


def zigzag_decode(value: int) -> int:
    return (value >> 1) ^ -(value & 1)


def body_writer() -> Writer:
    """A writer already carrying the schema version every body starts with."""
    writer = Writer()
    writer.string(SCHEMA_VERSION)
    return writer


def body_reader(data: bytes) -> Reader:
    """A reader positioned past the schema version, which is verified.

    A mismatch here means the broker and this client disagree about the
    message shapes themselves, so failing loudly beats decoding garbage
    into plausible-looking fields.
    """
    reader = Reader(data)
    version = reader.string()
    if version != SCHEMA_VERSION:
        raise ProtocolError(
            f"schema version mismatch: broker speaks {version!r}, "
            f"this client speaks {SCHEMA_VERSION!r}"
        )
    return reader


# --------------------------------------------------------------------------
# Frames
# --------------------------------------------------------------------------


def encode_frame(api_key: int, correlation_id: int, client_id: Optional[str], body: bytes) -> bytes:
    """One complete frame, length prefix included.

    Note the header is *fixed* big-endian while `body` is BitPacker. The
    broker has to read the header before it knows which body decoder to
    use, so the header cannot depend on the schema.
    """
    payload = bytearray()
    payload += struct.pack(">hhi", api_key, API_VERSION, correlation_id)
    if client_id is None:
        payload += struct.pack(">h", -1)
    else:
        encoded = client_id.encode("utf-8")
        payload += struct.pack(">h", len(encoded))
        payload += encoded
    payload += body
    return struct.pack(">i", len(payload)) + bytes(payload)


def decode_frame_payload(payload: bytes) -> Tuple[int, int, bytes]:
    """Split a frame payload into (api_key, correlation_id, body)."""
    if len(payload) < 10:
        raise ProtocolError("frame payload shorter than its header")
    api_key, _api_version, correlation_id = struct.unpack_from(">hhi", payload, 0)
    offset = 8
    (client_len,) = struct.unpack_from(">h", payload, offset)
    offset += 2
    if client_len >= 0:
        offset += client_len
    if offset > len(payload):
        raise ProtocolError("frame client id runs past the payload")
    return api_key, correlation_id, payload[offset:]


# --------------------------------------------------------------------------
# CRC32C (Castagnoli), for record batches
# --------------------------------------------------------------------------


def _build_crc32c_table() -> List[int]:
    # Castagnoli polynomial, reflected. Kafka and Brahmaputra both use
    # CRC32C rather than the zlib CRC32, so the stdlib is no help here.
    poly = 0x82F63B78
    table = []
    for index in range(256):
        crc = index
        for _ in range(8):
            crc = (crc >> 1) ^ (poly if crc & 1 else 0)
        table.append(crc)
    return table


_CRC32C_TABLE = _build_crc32c_table()


def crc32c(data: bytes) -> int:
    crc = 0xFFFFFFFF
    for byte in data:
        crc = _CRC32C_TABLE[(crc ^ byte) & 0xFF] ^ (crc >> 8)
    return crc ^ 0xFFFFFFFF


# --------------------------------------------------------------------------
# Compression
# --------------------------------------------------------------------------


class Compression:
    NONE = 0
    LZ4 = 1
    ZSTD = 2
    SNAPPY = 3
    GZIP = 4

    _NAMES = {
        "none": NONE,
        "lz4": LZ4,
        "zstd": ZSTD,
        "snappy": SNAPPY,
        "gzip": GZIP,
    }

    @classmethod
    def parse(cls, name: str) -> int:
        try:
            return cls._NAMES[name]
        except KeyError:
            raise ValueError(
                f"unknown compression {name!r}; expected one of {sorted(cls._NAMES)}"
            ) from None

    @classmethod
    def name(cls, value: int) -> str:
        for name, code in cls._NAMES.items():
            if code == value:
                return name
        return f"unknown({value})"


#: Cap on a decompressed batch, so a corrupt or hostile batch cannot name
#: gigabytes of output that this process allocates before rejecting it.
MAX_DECOMPRESSED_BYTES = 256 * 1024 * 1024

_compressors: Dict[int, Callable[[bytes], bytes]] = {}
_decompressors: Dict[int, Callable[[bytes], bytes]] = {}


def register_codec(
    codec: Union[int, str],
    compress_fn: Callable[[bytes], bytes],
    decompress_fn: Callable[[bytes], bytes],
) -> None:
    """Plug in a codec this package does not carry itself.

    `none` and `gzip` are built in. lz4, zstd and snappy are opt-in so an
    application that does not want those dependencies does not acquire
    them. A registered codec takes precedence over the optional-package
    fallback below.

    The lz4 payload the broker expects is a little-endian u32 of the
    uncompressed length followed by a raw LZ4 *block* — not the LZ4 frame
    format — and snappy is raw (unframed) snappy.
    """
    if isinstance(codec, str):
        codec = Compression.parse(codec)
    if codec in (Compression.NONE, Compression.GZIP):
        raise ValueError("none and gzip are built in and cannot be replaced")
    _compressors[codec] = compress_fn
    _decompressors[codec] = decompress_fn


def compress(codec: int, payload: bytes) -> bytes:
    """Compress a records payload.

    Codecs other than none/gzip come from :func:`register_codec`, or, as a
    convenience, from the optional package if it happens to be installed.
    Nothing is imported at module load, and the error names what to add.
    """
    if codec == Compression.NONE:
        return payload
    if codec == Compression.GZIP:
        return gzip.compress(payload)
    registered = _compressors.get(codec)
    if registered is not None:
        return registered(payload)
    if codec == Compression.LZ4:
        return _lz4_compress(payload)
    if codec == Compression.ZSTD:
        zstd = _require("zstandard", "zstd")
        return zstd.ZstdCompressor(level=3).compress(payload)
    if codec == Compression.SNAPPY:
        snappy = _require("snappy", "snappy", pip_name="python-snappy")
        return snappy.compress(payload)
    raise ProtocolError(f"unsupported compression {codec}")


def decompress(codec: int, payload: bytes) -> bytes:
    if codec == Compression.NONE:
        return payload
    if codec == Compression.GZIP:
        return _gunzip(payload)
    registered = _decompressors.get(codec)
    if registered is not None:
        return registered(payload)
    if codec == Compression.LZ4:
        return _lz4_decompress(payload)
    if codec == Compression.ZSTD:
        zstd = _require("zstandard", "zstd")
        # The broker's zstd encoder streams, so its frames need not carry a
        # content size; a one-shot `decompress` would refuse those.
        reader = zstd.ZstdDecompressor().decompressobj()
        return reader.decompress(payload)
    if codec == Compression.SNAPPY:
        snappy = _require("snappy", "snappy", pip_name="python-snappy")
        return snappy.decompress(payload)
    raise ProtocolError(f"unsupported compression {codec}")


def _gunzip(payload: bytes) -> bytes:
    out = bytearray()
    inflater = zlib.decompressobj(wbits=16 + zlib.MAX_WBITS)
    data = payload
    while data:
        out += inflater.decompress(data, MAX_DECOMPRESSED_BYTES + 1 - len(out))
        if len(out) > MAX_DECOMPRESSED_BYTES:
            raise ProtocolError("gzip batch decompresses past the size cap")
        data = inflater.unconsumed_tail
        if inflater.eof:
            # Concatenated gzip members are legal; carry on with the next.
            data = inflater.unused_data + data
            if not data:
                break
            inflater = zlib.decompressobj(wbits=16 + zlib.MAX_WBITS)
        elif not data:
            break
    return bytes(out)


def _require(module: str, codec: str, pip_name: Optional[str] = None):
    try:
        return importlib.import_module(module)
    except ImportError:
        raise BrahmaputraError(
            f"{codec} compression is not registered and the optional "
            f"{pip_name or module!r} package is missing; call register_codec() "
            f"or pip install {pip_name or module}"
        ) from None


def _lz4_compress(payload: bytes) -> bytes:
    # The broker uses lz4_flex's `compress_prepend_size`: a little-endian
    # u32 of the uncompressed length, then a raw LZ4 block. That is *not*
    # the LZ4 frame format, so the frame API in python-lz4 cannot be used.
    lz4_block = _require("lz4.block", "lz4", pip_name="lz4")
    compressed = lz4_block.compress(payload, store_size=False)
    return struct.pack("<I", len(payload)) + compressed


def _lz4_decompress(payload: bytes) -> bytes:
    lz4_block = _require("lz4.block", "lz4", pip_name="lz4")
    if len(payload) < 4:
        raise ProtocolError("lz4 payload shorter than its size prefix")
    (size,) = struct.unpack_from("<I", payload, 0)
    if size > MAX_DECOMPRESSED_BYTES:
        raise ProtocolError("lz4 batch claims more than the size cap")
    return lz4_block.decompress(payload[4:], uncompressed_size=size)


# --------------------------------------------------------------------------
# Record batches
# --------------------------------------------------------------------------


@dataclass
class RecordHeader:
    """An ordered, possibly repeating annotation on a record."""

    key: str
    value: Optional[bytes] = None


@dataclass
class Record:
    #: `None` is a tombstone: on a compacted topic it deletes the key, and it
    #: reaches consumers as a null value. Distinct from `b""`, which is an
    #: ordinary record that happens to carry no bytes.
    value: Optional[bytes]
    key: Optional[bytes] = None
    timestamp_delta: int = 0
    headers: List[RecordHeader] = field(default_factory=list)

    def timestamp(self, max_timestamp: int) -> int:
        return max_timestamp + self.timestamp_delta

    def header(self, key: str) -> Optional[bytes]:
        for header in self.headers:
            if header.key == key:
                return header.value
        return None


def encode_record_batch(
    records: List[Record],
    max_timestamp: int,
    base_offset: int = 0,
    leader_epoch: int = 0,
    compression: int = Compression.NONE,
    producer: Optional[Tuple[int, int, int]] = None,
) -> bytes:
    """Encode one record batch exactly as the broker expects it.

    The broker never re-encodes this: it validates the header, stamps
    `base_offset` and `leader_epoch` in place (both sit before the CRC, so
    it stays valid), and writes these bytes to disk. Getting this wrong
    therefore corrupts the log rather than merely failing a request.
    """
    has_headers = any(record.headers for record in records)
    # A None value is a tombstone and needs the widened length encoding; an
    # empty bytes value is an ordinary record and must not trigger it.
    has_null_values = any(record.value is None for record in records)

    payload = bytearray()
    for record in records:
        rec = bytearray()
        if record.key is None:
            _put_uvarint(rec, 0)
        else:
            _put_uvarint(rec, len(record.key) + 1)
            rec += record.key
        if has_null_values:
            if record.value is None:
                _put_uvarint(rec, 0)
            else:
                _put_uvarint(rec, len(record.value) + 1)
                rec += record.value
        else:
            _put_uvarint(rec, len(record.value))
            rec += record.value
        _put_uvarint(rec, zigzag_encode(record.timestamp_delta, 64))
        if has_headers:
            _put_uvarint(rec, len(record.headers))
            for header in record.headers:
                key = header.key.encode("utf-8")
                _put_uvarint(rec, len(key))
                rec += key
                if header.value is None:
                    _put_uvarint(rec, 0)
                else:
                    _put_uvarint(rec, len(header.value) + 1)
                    rec += header.value
        _put_uvarint(payload, len(rec))
        payload += rec

    compressed = compress(compression, bytes(payload))
    attributes = compression & COMPRESSION_MASK
    if has_headers:
        attributes |= HEADERS_BIT
    if has_null_values:
        attributes |= NULL_VALUE_BIT

    magic = MAGIC_V2 if producer is not None else MAGIC_V1
    extension = PRODUCER_EXTENSION_LEN if producer is not None else 0
    batch_length = MIN_BATCH_LENGTH + extension + len(compressed)

    out = bytearray()
    out += struct.pack(">qi", base_offset, batch_length)
    out += struct.pack(">ib", leader_epoch, magic)
    crc_at = len(out)
    out += b"\x00\x00\x00\x00"
    out += struct.pack(">Hiq", attributes, max(len(records) - 1, 0), max_timestamp)
    if producer is not None:
        producer_id, producer_epoch, base_sequence = producer
        out += struct.pack(">qhi", producer_id, producer_epoch, base_sequence)
    out += compressed

    crc = crc32c(bytes(out[crc_at + 4 :]))
    struct.pack_into(">I", out, crc_at, crc)
    return bytes(out)


@dataclass
class DecodedBatch:
    base_offset: int
    max_timestamp: int
    records: List[Record]


def decode_record_batch(data: bytes, offset: int) -> Tuple[DecodedBatch, int]:
    """Decode one batch starting at `offset`; returns it and the next offset."""
    if len(data) - offset < BATCH_HEADER_LEN:
        raise ProtocolError("truncated batch header")
    base_offset, batch_length = struct.unpack_from(">qi", data, offset)
    if batch_length < MIN_BATCH_LENGTH:
        raise ProtocolError("batch_length too small")
    body_at = offset + BATCH_HEADER_LEN
    end = body_at + batch_length
    if end > len(data):
        raise ProtocolError("truncated batch body")

    leader_epoch, magic = struct.unpack_from(">ib", data, body_at)
    del leader_epoch
    if magic not in (MAGIC_V1, MAGIC_V2):
        raise ProtocolError(f"unsupported magic {magic}")
    crc_at = body_at + 5
    (stored_crc,) = struct.unpack_from(">I", data, crc_at)
    computed = crc32c(data[crc_at + 4 : end])
    if computed != stored_crc:
        raise ProtocolError(
            f"crc mismatch: stored {stored_crc:#010x}, computed {computed:#010x}"
        )

    cursor = crc_at + 4
    attributes, _last_delta, max_timestamp = struct.unpack_from(">Hiq", data, cursor)
    cursor += 14
    if magic == MAGIC_V2:
        cursor += PRODUCER_EXTENSION_LEN

    payload = decompress(attributes & COMPRESSION_MASK, data[cursor:end])
    records = _decode_records(
        payload,
        bool(attributes & HEADERS_BIT),
        bool(attributes & NULL_VALUE_BIT),
    )
    return DecodedBatch(base_offset, max_timestamp, records), end


def _decode_records(
    payload: bytes, has_headers: bool, has_null_values: bool
) -> List[Record]:
    records: List[Record] = []
    pos = 0
    while pos < len(payload):
        length, pos = _get_uvarint(payload, pos)
        if pos + length > len(payload):
            raise ProtocolError("truncated record")
        end = pos + length

        key_len_plus_one, pos = _get_uvarint(payload, pos)
        if key_len_plus_one == 0:
            key = None
        else:
            size = key_len_plus_one - 1
            if pos + size > end:
                raise ProtocolError("truncated record key")
            key = payload[pos : pos + size]
            pos += size

        raw_value_len, pos = _get_uvarint(payload, pos)
        if has_null_values and raw_value_len == 0:
            # A tombstone. None rather than b"", which is what
            # distinguishes a deletion from a record whose value is empty.
            value = None
        else:
            value_len = raw_value_len - 1 if has_null_values else raw_value_len
            if pos + value_len > end:
                raise ProtocolError("truncated record value")
            value = payload[pos : pos + value_len]
            pos += value_len

        raw_delta, pos = _get_uvarint(payload, pos)
        timestamp_delta = zigzag_decode(raw_delta)

        headers: List[RecordHeader] = []
        if has_headers:
            count, pos = _get_uvarint(payload, pos)
            if count > end - pos:
                raise ProtocolError("record header count exceeds record")
            for _ in range(count):
                key_len, pos = _get_uvarint(payload, pos)
                if pos + key_len > end:
                    raise ProtocolError("truncated header key")
                header_key = payload[pos : pos + key_len].decode("utf-8", "replace")
                pos += key_len
                value_plus_one, pos = _get_uvarint(payload, pos)
                if value_plus_one == 0:
                    header_value = None
                else:
                    size = value_plus_one - 1
                    if pos + size > end:
                        raise ProtocolError("truncated header value")
                    header_value = payload[pos : pos + size]
                    pos += size
                headers.append(RecordHeader(header_key, header_value))

        if pos != end:
            raise ProtocolError("trailing bytes in record")
        records.append(Record(value, key, timestamp_delta, headers))
    return records


def split_batches(data: bytes) -> List[bytes]:
    """Split a run of concatenated batches into individual batch slices."""
    out = []
    pos = 0
    while pos < len(data):
        if len(data) - pos < BATCH_HEADER_LEN:
            break
        (_base, batch_length) = struct.unpack_from(">qi", data, pos)
        end = pos + BATCH_HEADER_LEN + batch_length
        if end > len(data):
            break
        out.append(data[pos:end])
        pos = end
    return out


def _put_uvarint(buf: bytearray, value: int) -> None:
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            buf.append(byte | 0x80)
        else:
            buf.append(byte)
            return


def _get_uvarint(data: bytes, pos: int) -> Tuple[int, int]:
    result = 0
    shift = 0
    while True:
        if pos >= len(data):
            raise ProtocolError("truncated varint in record")
        byte = data[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return result, pos
        shift += 7
        if shift > 63:
            raise ProtocolError("varint overflows 64 bits")


# --------------------------------------------------------------------------
# Partitioning
# --------------------------------------------------------------------------


def murmur2(data: bytes) -> int:
    """Kafka's 32-bit murmur2, so a key lands on the same partition here.

    Reproduced rather than imported because the whole point is that a
    Python producer and a Rust producer writing the same key must agree,
    and "some murmur2" is not good enough — it has to be this one.
    """
    seed = 0x9747B28C
    m = 0x5BD1E995
    r = 24
    length = len(data)
    h = (seed ^ length) & 0xFFFFFFFF

    chunks = length // 4
    for i in range(chunks):
        offset = i * 4
        k = (
            data[offset]
            | (data[offset + 1] << 8)
            | (data[offset + 2] << 16)
            | (data[offset + 3] << 24)
        )
        k = (k * m) & 0xFFFFFFFF
        k ^= k >> r
        k = (k * m) & 0xFFFFFFFF
        h = (h * m) & 0xFFFFFFFF
        h ^= k

    remaining = length & 3
    tail = chunks * 4
    if remaining >= 3:
        h ^= data[tail + 2] << 16
    if remaining >= 2:
        h ^= data[tail + 1] << 8
    if remaining >= 1:
        h ^= data[tail]
        h = (h * m) & 0xFFFFFFFF

    h ^= h >> 13
    h = (h * m) & 0xFFFFFFFF
    h ^= h >> 15
    return h


def partition_for_key(key: bytes, partitions: List[int]) -> int:
    """`murmur2(key) % partitions`, matching Kafka's default partitioner."""
    return partitions[(murmur2(key) & 0x7FFFFFFF) % len(partitions)]
