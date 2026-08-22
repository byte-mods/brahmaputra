// Package brahmaputra is a client for the Brahmaputra log streaming
// platform.
//
// Three encodings share one connection and they do not agree with each
// other, so keeping them straight is most of the work:
//
//   - The frame header is fixed big-endian: an int32 length prefix, then
//     apiKey/apiVersion/correlationID and a length-prefixed client id.
//   - A request body is BitPacker: every integer is a zigzag varint, every
//     string and array is a varint count followed by its contents, and the
//     whole body is prefixed with the schema version string.
//   - A record batch is neither. Fixed big-endian header fields and plain
//     (non-zigzag) varints inside each record, because the broker stamps
//     offsets into it in place and validates its CRC without decoding it.
//
// Mixing those up produces a frame the broker rejects with no useful
// error, so each encoder here is explicit about which one it is.
package brahmaputra

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

// SchemaVersion is the BitPacker schema version every body carries first.
const SchemaVersion = "1.0.0"

// APIVersion is the wire version this client speaks. The broker requires
// an exact match and answers UnsupportedVersion otherwise.
//
// Version 3 added transactions: Fetch and FetchMulti carry an
// isolation_level, and MetadataResponse carries a request-level error
// code so an authorization denial is no longer reported as an unknown
// topic.
const APIVersion int16 = 3

// Isolation levels for a fetch. ReadUncommitted is the default and is what
// every non-transactional topic gives either way.
const (
	ReadUncommitted int32 = 0
	ReadCommitted   int32 = 1
)

const (
	batchHeaderLen       = 12
	minBatchLength       = 4 + 1 + 4 + 2 + 4 + 8
	producerExtensionLen = 8 + 2 + 4

	magicV1 = 1
	magicV2 = 2

	compressionMask = 0x0007
	headersBit      = 0x0008
)

// API keys, in wire order.
const (
	APIProduce               int16 = 0
	APIFetch                 int16 = 1
	APIListOffsets           int16 = 2
	APIMetadata              int16 = 3
	APIReplicaFetch          int16 = 4
	APIOffsetsForLeaderEpoch int16 = 5
	APIInitProducerID        int16 = 6
	APIJoinGroup             int16 = 7
	APISyncGroup             int16 = 8
	APIHeartbeat             int16 = 9
	APIOffsetCommit          int16 = 10
	APIOffsetFetch           int16 = 11
	APIListGroups            int16 = 12
	APIDescribeGroup         int16 = 13
	APIAPIVersions           int16 = 14
	APIProduceMulti          int16 = 15
	APIFetchMulti            int16 = 16
	APIAuthenticate          int16 = 17
	APILeaveGroup            int16 = 18
)

// Error codes the broker returns in a response's error_code field.
const (
	ErrNone                     int32 = 0
	ErrUnknownTopicOrPartition  int32 = 1
	ErrOffsetOutOfRange         int32 = 2
	ErrInvalidRequest           int32 = 3
	ErrUnsupportedVersion       int32 = 4
	ErrInternal                 int32 = 5
	ErrNotLeaderOrFollower      int32 = 6
	ErrFencedBrokerEpoch        int32 = 7
	ErrFencedLeaderEpoch        int32 = 8
	ErrUnknownLeaderEpoch       int32 = 9
	ErrNotEnoughReplicas        int32 = 10
	ErrFencedProducerEpoch      int32 = 11
	ErrOutOfOrderSequence       int32 = 12
	ErrUnknownMemberID          int32 = 13
	ErrRebalanceInProgress      int32 = 14
	ErrNotCoordinator           int32 = 15
	ErrIllegalGeneration        int32 = 16
	ErrCoordinatorLoadInProgres int32 = 17
	ErrSaslAuthenticationFailed int32 = 18
	ErrAuthorizationFailed      int32 = 19
)

var errorNames = map[int32]string{
	ErrNone:                     "NONE",
	ErrUnknownTopicOrPartition:  "UNKNOWN_TOPIC_OR_PARTITION",
	ErrOffsetOutOfRange:         "OFFSET_OUT_OF_RANGE",
	ErrInvalidRequest:           "INVALID_REQUEST",
	ErrUnsupportedVersion:       "UNSUPPORTED_VERSION",
	ErrInternal:                 "INTERNAL",
	ErrNotLeaderOrFollower:      "NOT_LEADER_OR_FOLLOWER",
	ErrFencedBrokerEpoch:        "FENCED_BROKER_EPOCH",
	ErrFencedLeaderEpoch:        "FENCED_LEADER_EPOCH",
	ErrUnknownLeaderEpoch:       "UNKNOWN_LEADER_EPOCH",
	ErrNotEnoughReplicas:        "NOT_ENOUGH_REPLICAS",
	ErrFencedProducerEpoch:      "FENCED_PRODUCER_EPOCH",
	ErrOutOfOrderSequence:       "OUT_OF_ORDER_SEQUENCE",
	ErrUnknownMemberID:          "UNKNOWN_MEMBER_ID",
	ErrRebalanceInProgress:      "REBALANCE_IN_PROGRESS",
	ErrNotCoordinator:           "NOT_COORDINATOR",
	ErrIllegalGeneration:        "ILLEGAL_GENERATION",
	ErrCoordinatorLoadInProgres: "COORDINATOR_LOAD_IN_PROGRESS",
	ErrSaslAuthenticationFailed: "SASL_AUTHENTICATION_FAILED",
	ErrAuthorizationFailed:      "AUTHORIZATION_FAILED",
}

// ServerError is a non-zero error code from the broker.
type ServerError struct {
	Code    int32
	Context string
}

func (e *ServerError) Error() string {
	name, ok := errorNames[e.Code]
	if !ok {
		name = "UNKNOWN"
	}
	if e.Context == "" {
		return fmt.Sprintf("broker returned %s[%d]", name, e.Code)
	}
	return fmt.Sprintf("broker returned %s[%d] (%s)", name, e.Code, e.Context)
}

func serverError(code int32, context string) error {
	return &ServerError{Code: code, Context: context}
}

// ErrNoOffsetForPartition is returned when AutoOffsetResetNone is in force
// and there is no position to resume from.
var ErrNoOffsetForPartition = errors.New("no committed offset for partition")

// retriable reports whether a code means "this send did not happen".
//
// Every code here is one the broker returns strictly before it appends, so
// a retry cannot duplicate a record. Anything else is returned as-is: a
// malformed request or a failed authorization fails identically however
// often it is sent, and the idempotence errors mean the producer's
// sequence state is already broken.
func retriable(code int32) bool {
	switch code {
	case ErrNotLeaderOrFollower, ErrFencedLeaderEpoch, ErrUnknownLeaderEpoch,
		ErrNotEnoughReplicas, ErrCoordinatorLoadInProgres, ErrInternal:
		return true
	}
	return false
}

// ---------------------------------------------------------------------------
// BitPacker primitives
// ---------------------------------------------------------------------------

// Writer builds a BitPacker body. Every integer goes out zigzag-varint
// encoded, which is why this cannot share code with the record-batch
// encoder below.
type Writer struct {
	buf []byte
}

// NewBodyWriter returns a writer already carrying the schema version that
// every body starts with.
func NewBodyWriter() *Writer {
	w := &Writer{buf: make([]byte, 0, 256)}
	w.String(SchemaVersion)
	return w
}

func (w *Writer) Uvarint(value uint64) {
	for value >= 0x80 {
		w.buf = append(w.buf, byte(value)|0x80)
		value >>= 7
	}
	w.buf = append(w.buf, byte(value))
}

func (w *Writer) Int32(value int32) { w.Uvarint(uint64(uint32((value << 1) ^ (value >> 31)))) }
func (w *Writer) Int64(value int64) { w.Uvarint(uint64((value << 1) ^ (value >> 63))) }

func (w *Writer) Bool(value bool) {
	if value {
		w.buf = append(w.buf, 1)
	} else {
		w.buf = append(w.buf, 0)
	}
}

func (w *Writer) String(value string) {
	w.Int32(int32(len(value)))
	w.buf = append(w.buf, value...)
}

func (w *Writer) StringArray(values []string) {
	w.Int32(int32(len(values)))
	for _, value := range values {
		w.String(value)
	}
}

func (w *Writer) Raw(data []byte) { w.buf = append(w.buf, data...) }
func (w *Writer) Bytes() []byte   { return w.buf }

// Reader reads a BitPacker body.
type Reader struct {
	data []byte
	pos  int
	err  error
}

// NewBodyReader returns a reader positioned past the schema version, which
// it verifies. A mismatch means broker and client disagree about the
// message shapes themselves, so failing loudly beats decoding garbage into
// plausible-looking fields.
func NewBodyReader(data []byte) (*Reader, error) {
	r := &Reader{data: data}
	version := r.String()
	if r.err != nil {
		return nil, r.err
	}
	if version != SchemaVersion {
		return nil, fmt.Errorf(
			"schema version mismatch: broker speaks %q, this client speaks %q",
			version, SchemaVersion)
	}
	return r, nil
}

func (r *Reader) Err() error { return r.err }

func (r *Reader) fail(format string, args ...any) {
	if r.err == nil {
		r.err = fmt.Errorf(format, args...)
	}
}

func (r *Reader) Uvarint() uint64 {
	var result uint64
	var shift uint
	for {
		if r.pos >= len(r.data) {
			r.fail("truncated varint")
			return 0
		}
		b := r.data[r.pos]
		r.pos++
		result |= uint64(b&0x7F) << shift
		if b&0x80 == 0 {
			return result
		}
		shift += 7
		if shift > 63 {
			r.fail("varint overflows 64 bits")
			return 0
		}
	}
}

func (r *Reader) Int32() int32 {
	v := r.Uvarint()
	return int32(v>>1) ^ -int32(v&1)
}

func (r *Reader) Int64() int64 {
	v := r.Uvarint()
	return int64(v>>1) ^ -int64(v&1)
}

func (r *Reader) Bool() bool {
	if r.pos >= len(r.data) {
		r.fail("truncated bool")
		return false
	}
	v := r.data[r.pos]
	r.pos++
	return v != 0
}

func (r *Reader) String() string {
	length := int(r.Int32())
	if r.err != nil {
		return ""
	}
	if length < 0 || r.pos+length > len(r.data) {
		r.fail("truncated string")
		return ""
	}
	value := string(r.data[r.pos : r.pos+length])
	r.pos += length
	return value
}

func (r *Reader) StringArray() []string {
	count := int(r.Int32())
	if r.err != nil || count < 0 {
		return nil
	}
	out := make([]string, 0, count)
	for i := 0; i < count; i++ {
		out = append(out, r.String())
	}
	return out
}

func (r *Reader) Rest() []byte {
	value := r.data[r.pos:]
	r.pos = len(r.data)
	return value
}

// ---------------------------------------------------------------------------
// Frames
// ---------------------------------------------------------------------------

// EncodeFrame builds one complete frame, length prefix included.
//
// The header is fixed big-endian while body is BitPacker: the broker has
// to read the header before it knows which body decoder to use, so the
// header cannot depend on the schema.
func EncodeFrame(apiKey int16, correlationID int32, clientID string, body []byte) []byte {
	payloadLen := 8 + 2 + len(clientID) + len(body)
	out := make([]byte, 0, 4+payloadLen)
	out = binary.BigEndian.AppendUint32(out, uint32(payloadLen))
	out = binary.BigEndian.AppendUint16(out, uint16(apiKey))
	out = binary.BigEndian.AppendUint16(out, uint16(APIVersion))
	out = binary.BigEndian.AppendUint32(out, uint32(correlationID))
	out = binary.BigEndian.AppendUint16(out, uint16(int16(len(clientID))))
	out = append(out, clientID...)
	out = append(out, body...)
	return out
}

// DecodeFramePayload splits a frame payload into its correlation id and body.
func DecodeFramePayload(payload []byte) (correlationID int32, body []byte, err error) {
	if len(payload) < 10 {
		return 0, nil, errors.New("frame payload shorter than its header")
	}
	correlationID = int32(binary.BigEndian.Uint32(payload[4:8]))
	clientLen := int16(binary.BigEndian.Uint16(payload[8:10]))
	offset := 10
	if clientLen >= 0 {
		offset += int(clientLen)
	}
	if offset > len(payload) {
		return 0, nil, errors.New("frame client id runs past the payload")
	}
	return correlationID, payload[offset:], nil
}

// ---------------------------------------------------------------------------
// CRC32C
// ---------------------------------------------------------------------------

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// CRC32C is the Castagnoli CRC record batches carry, not the zlib CRC32.
func CRC32C(data []byte) uint32 { return crc32.Checksum(data, castagnoli) }

// ---------------------------------------------------------------------------
// Compression
// ---------------------------------------------------------------------------

// Compression codecs, matching the broker's attribute values.
type Compression int

const (
	CompressionNone   Compression = 0
	CompressionLZ4    Compression = 1
	CompressionZstd   Compression = 2
	CompressionSnappy Compression = 3
	CompressionGzip   Compression = 4
)

// ParseCompression maps Kafka's compression.type spelling onto a codec.
func ParseCompression(name string) (Compression, error) {
	switch name {
	case "none":
		return CompressionNone, nil
	case "lz4":
		return CompressionLZ4, nil
	case "zstd":
		return CompressionZstd, nil
	case "snappy":
		return CompressionSnappy, nil
	case "gzip":
		return CompressionGzip, nil
	}
	return 0, fmt.Errorf("unknown compression %q (none, lz4, zstd, snappy, gzip)", name)
}

func (c Compression) String() string {
	switch c {
	case CompressionNone:
		return "none"
	case CompressionLZ4:
		return "lz4"
	case CompressionZstd:
		return "zstd"
	case CompressionSnappy:
		return "snappy"
	case CompressionGzip:
		return "gzip"
	}
	return fmt.Sprintf("unknown(%d)", int(c))
}

// compress applies a codec to a records payload.
//
// Only gzip is in the standard library. Rather than force every user of
// this client to vendor three compression libraries, the others are
// optional: they work if the corresponding build tag package is present,
// and otherwise return an error naming what to add. gzip and none always
// work, which is enough to talk to any broker.
func compress(codec Compression, payload []byte) ([]byte, error) {
	switch codec {
	case CompressionNone:
		return payload, nil
	case CompressionGzip:
		var out bytes.Buffer
		writer := gzip.NewWriter(&out)
		if _, err := writer.Write(payload); err != nil {
			return nil, err
		}
		if err := writer.Close(); err != nil {
			return nil, err
		}
		return out.Bytes(), nil
	case CompressionLZ4, CompressionZstd, CompressionSnappy:
		if fn := externalCompressors[codec]; fn != nil {
			return fn(payload)
		}
		return nil, fmt.Errorf(
			"%s compression is not registered; call Register%sCodec or use none/gzip",
			codec, codec)
	}
	return nil, fmt.Errorf("unsupported compression %d", int(codec))
}

func decompress(codec Compression, payload []byte) ([]byte, error) {
	switch codec {
	case CompressionNone:
		return payload, nil
	case CompressionGzip:
		reader, err := gzip.NewReader(bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		defer reader.Close()
		// Capped so a corrupt or hostile batch cannot name gigabytes of
		// output that this process allocates before it can reject it.
		return io.ReadAll(io.LimitReader(reader, maxDecompressedBytes))
	case CompressionLZ4, CompressionZstd, CompressionSnappy:
		if fn := externalDecompressors[codec]; fn != nil {
			return fn(payload)
		}
		return nil, fmt.Errorf(
			"%s decompression is not registered; call Register%sCodec", codec, codec)
	}
	return nil, fmt.Errorf("unsupported compression %d", int(codec))
}

const maxDecompressedBytes = 256 * 1024 * 1024

var (
	externalCompressors   = map[Compression]func([]byte) ([]byte, error){}
	externalDecompressors = map[Compression]func([]byte) ([]byte, error){}
)

// RegisterCodec plugs in a compression codec this package does not carry
// itself, so an application that wants lz4 or zstd pays for that
// dependency and one that does not, does not.
//
// The lz4 payload the broker expects is a little-endian uint32 of the
// uncompressed length followed by a raw LZ4 block — not the LZ4 frame
// format — so a frame-format library will not interoperate.
func RegisterCodec(
	codec Compression,
	compressFn func([]byte) ([]byte, error),
	decompressFn func([]byte) ([]byte, error),
) {
	externalCompressors[codec] = compressFn
	externalDecompressors[codec] = decompressFn
}

// ---------------------------------------------------------------------------
// Record batches
// ---------------------------------------------------------------------------

// RecordHeader is an ordered, possibly repeating annotation on a record.
// Value may be nil, which is distinct from empty.
type RecordHeader struct {
	Key   string
	Value []byte
}

// Record is one record inside a batch.
type Record struct {
	Key   []byte
	Value []byte
	// TimestampDelta is milliseconds relative to the batch's max timestamp,
	// so it is normally zero or negative.
	TimestampDelta int64
	Headers        []RecordHeader
}

// Timestamp resolves this record's absolute time given its batch's base.
func (r *Record) Timestamp(maxTimestamp int64) int64 { return maxTimestamp + r.TimestampDelta }

// Header returns the first value stored under key, if any.
func (r *Record) Header(key string) []byte {
	for i := range r.Headers {
		if r.Headers[i].Key == key {
			return r.Headers[i].Value
		}
	}
	return nil
}

// EncodeRecordBatch encodes one batch exactly as the broker expects it.
//
// The broker never re-encodes this: it validates the header, stamps
// baseOffset and leaderEpoch in place (both sit before the CRC, so it
// stays valid), and writes these bytes to disk. Getting this wrong
// corrupts the log rather than merely failing a request.
func EncodeRecordBatch(
	records []Record,
	maxTimestamp int64,
	codec Compression,
) ([]byte, error) {
	hasHeaders := false
	for i := range records {
		if len(records[i].Headers) > 0 {
			hasHeaders = true
			break
		}
	}

	var payload []byte
	for i := range records {
		record := &records[i]
		var rec []byte
		if record.Key == nil {
			rec = appendUvarint(rec, 0)
		} else {
			rec = appendUvarint(rec, uint64(len(record.Key))+1)
			rec = append(rec, record.Key...)
		}
		rec = appendUvarint(rec, uint64(len(record.Value)))
		rec = append(rec, record.Value...)
		rec = appendUvarint(rec, uint64((record.TimestampDelta<<1)^(record.TimestampDelta>>63)))
		if hasHeaders {
			rec = appendUvarint(rec, uint64(len(record.Headers)))
			for _, header := range record.Headers {
				rec = appendUvarint(rec, uint64(len(header.Key)))
				rec = append(rec, header.Key...)
				if header.Value == nil {
					rec = appendUvarint(rec, 0)
				} else {
					rec = appendUvarint(rec, uint64(len(header.Value))+1)
					rec = append(rec, header.Value...)
				}
			}
		}
		payload = appendUvarint(payload, uint64(len(rec)))
		payload = append(payload, rec...)
	}

	compressed, err := compress(codec, payload)
	if err != nil {
		return nil, err
	}

	attributes := uint16(codec) & compressionMask
	if hasHeaders {
		attributes |= headersBit
	}
	batchLength := minBatchLength + len(compressed)

	out := make([]byte, 0, batchHeaderLen+batchLength)
	out = binary.BigEndian.AppendUint64(out, 0) // base_offset, stamped by the broker
	out = binary.BigEndian.AppendUint32(out, uint32(batchLength))
	out = binary.BigEndian.AppendUint32(out, 0) // leader_epoch, likewise
	out = append(out, magicV1)
	crcAt := len(out)
	out = append(out, 0, 0, 0, 0)
	out = binary.BigEndian.AppendUint16(out, attributes)
	lastDelta := len(records) - 1
	if lastDelta < 0 {
		lastDelta = 0
	}
	out = binary.BigEndian.AppendUint32(out, uint32(int32(lastDelta)))
	out = binary.BigEndian.AppendUint64(out, uint64(maxTimestamp))
	out = append(out, compressed...)

	binary.BigEndian.PutUint32(out[crcAt:crcAt+4], CRC32C(out[crcAt+4:]))
	return out, nil
}

// DecodedBatch is one batch read back off the wire.
type DecodedBatch struct {
	BaseOffset   int64
	MaxTimestamp int64
	Records      []Record
}

// DecodeRecordBatch decodes one batch starting at offset, returning it and
// the offset just past it.
func DecodeRecordBatch(data []byte, offset int) (DecodedBatch, int, error) {
	var batch DecodedBatch
	if len(data)-offset < batchHeaderLen {
		return batch, 0, errors.New("truncated batch header")
	}
	baseOffset := int64(binary.BigEndian.Uint64(data[offset : offset+8]))
	batchLength := int(int32(binary.BigEndian.Uint32(data[offset+8 : offset+12])))
	if batchLength < minBatchLength {
		return batch, 0, errors.New("batch_length too small")
	}
	bodyAt := offset + batchHeaderLen
	end := bodyAt + batchLength
	if end > len(data) {
		return batch, 0, errors.New("truncated batch body")
	}

	magic := data[bodyAt+4]
	if magic != magicV1 && magic != magicV2 {
		return batch, 0, fmt.Errorf("unsupported magic %d", magic)
	}
	crcAt := bodyAt + 5
	stored := binary.BigEndian.Uint32(data[crcAt : crcAt+4])
	computed := CRC32C(data[crcAt+4 : end])
	if stored != computed {
		return batch, 0, fmt.Errorf("crc mismatch: stored %#08x, computed %#08x", stored, computed)
	}

	cursor := crcAt + 4
	attributes := binary.BigEndian.Uint16(data[cursor : cursor+2])
	maxTimestamp := int64(binary.BigEndian.Uint64(data[cursor+6 : cursor+14]))
	cursor += 14
	if magic == magicV2 {
		cursor += producerExtensionLen
	}

	payload, err := decompress(Compression(attributes&compressionMask), data[cursor:end])
	if err != nil {
		return batch, 0, err
	}
	records, err := decodeRecords(payload, attributes&headersBit != 0)
	if err != nil {
		return batch, 0, err
	}
	return DecodedBatch{baseOffset, maxTimestamp, records}, end, nil
}

func decodeRecords(payload []byte, hasHeaders bool) ([]Record, error) {
	var records []Record
	pos := 0
	for pos < len(payload) {
		length, next, err := getUvarint(payload, pos)
		if err != nil {
			return nil, err
		}
		pos = next
		if pos+int(length) > len(payload) {
			return nil, errors.New("truncated record")
		}
		end := pos + int(length)

		var record Record
		keyLenPlusOne, next, err := getUvarint(payload, pos)
		if err != nil {
			return nil, err
		}
		pos = next
		if keyLenPlusOne > 0 {
			size := int(keyLenPlusOne - 1)
			record.Key = append([]byte(nil), payload[pos:pos+size]...)
			pos += size
		}

		valueLen, next, err := getUvarint(payload, pos)
		if err != nil {
			return nil, err
		}
		pos = next
		record.Value = append([]byte(nil), payload[pos:pos+int(valueLen)]...)
		pos += int(valueLen)

		rawDelta, next, err := getUvarint(payload, pos)
		if err != nil {
			return nil, err
		}
		pos = next
		record.TimestampDelta = int64(rawDelta>>1) ^ -int64(rawDelta&1)

		if hasHeaders {
			count, next, err := getUvarint(payload, pos)
			if err != nil {
				return nil, err
			}
			pos = next
			// A count is a promise about bytes that follow; if it exceeds
			// what is left it is corrupt, and allocating on it would let a
			// two-byte record ask for gigabytes.
			if count > uint64(end-pos) {
				return nil, errors.New("record header count exceeds record")
			}
			for i := uint64(0); i < count; i++ {
				keyLen, next, err := getUvarint(payload, pos)
				if err != nil {
					return nil, err
				}
				pos = next
				header := RecordHeader{Key: string(payload[pos : pos+int(keyLen)])}
				pos += int(keyLen)
				valuePlusOne, next, err := getUvarint(payload, pos)
				if err != nil {
					return nil, err
				}
				pos = next
				if valuePlusOne > 0 {
					size := int(valuePlusOne - 1)
					header.Value = append([]byte(nil), payload[pos:pos+size]...)
					pos += size
				}
				record.Headers = append(record.Headers, header)
			}
		}

		if pos != end {
			return nil, errors.New("trailing bytes in record")
		}
		records = append(records, record)
	}
	return records, nil
}

func appendUvarint(buf []byte, value uint64) []byte {
	for value >= 0x80 {
		buf = append(buf, byte(value)|0x80)
		value >>= 7
	}
	return append(buf, byte(value))
}

func getUvarint(data []byte, pos int) (uint64, int, error) {
	var result uint64
	var shift uint
	for {
		if pos >= len(data) {
			return 0, 0, errors.New("truncated varint in record")
		}
		b := data[pos]
		pos++
		result |= uint64(b&0x7F) << shift
		if b&0x80 == 0 {
			return result, pos, nil
		}
		shift += 7
		if shift > 63 {
			return 0, 0, errors.New("varint overflows 64 bits")
		}
	}
}

// ---------------------------------------------------------------------------
// Partitioning
// ---------------------------------------------------------------------------

// Murmur2 is Kafka's 32-bit murmur2, so a key lands on the same partition
// here as it would there.
//
// Reproduced rather than imported because the point is that a Go producer
// and a Rust producer writing the same key must agree, and "some murmur2"
// is not good enough — it has to be this one.
func Murmur2(data []byte) uint32 {
	const seed uint32 = 0x9747b28c
	const m uint32 = 0x5bd1e995
	const r = 24

	length := len(data)
	h := seed ^ uint32(length)
	chunks := length / 4

	for i := 0; i < chunks; i++ {
		offset := i * 4
		k := uint32(data[offset]) |
			uint32(data[offset+1])<<8 |
			uint32(data[offset+2])<<16 |
			uint32(data[offset+3])<<24
		k *= m
		k ^= k >> r
		k *= m
		h *= m
		h ^= k
	}

	tail := chunks * 4
	switch length - tail {
	case 3:
		h ^= uint32(data[tail+2]) << 16
		h ^= uint32(data[tail+1]) << 8
		h ^= uint32(data[tail])
		h *= m
	case 2:
		h ^= uint32(data[tail+1]) << 8
		h ^= uint32(data[tail])
		h *= m
	case 1:
		h ^= uint32(data[tail])
		h *= m
	}

	h ^= h >> 13
	h *= m
	h ^= h >> 15
	return h
}

// PartitionForKey is murmur2(key) % partitions, Kafka's default partitioner.
func PartitionForKey(key []byte, partitions []int32) int32 {
	return partitions[int(Murmur2(key)&0x7fffffff)%len(partitions)]
}

// Skip advances past fields this client does not use.
//
// Named rather than discarding a return value, because a response's fields
// must still be *read* in order even when their values are ignored — the
// encoding is positional, so skipping by not reading would misalign
// everything after it.
func (r *Reader) Skip(fields ...func()) {
	for _, read := range fields {
		read()
	}
}

// SkipString reads and discards one string field.
func (r *Reader) SkipString() { _ = r.String() }

// SkipInt32 reads and discards one int32 field.
func (r *Reader) SkipInt32() { _ = r.Int32() }

// SkipInt64 reads and discards one int64 field.
func (r *Reader) SkipInt64() { _ = r.Int64() }
