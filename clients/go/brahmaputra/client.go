package brahmaputra

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"sync"
	"time"
)

// Offset sentinels for ListOffsets.
const (
	Earliest int64 = -2
	Latest   int64 = -1
)

func nowMillis() int64 { return time.Now().UnixMilli() }

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

// Conn is one TCP connection to one broker, multiplexed by correlation id.
//
// The broker may answer out of order, so responses are matched by
// correlation id. A mutex serialises request/response pairs; this is
// enough for a producer that batches, and matches how the Rust client
// behaves.
type Conn struct {
	conn     net.Conn
	clientID string
	mu       sync.Mutex
	next     int32
}

// Dial opens a connection to one broker.
func Dial(address, clientID string, timeout time.Duration) (*Conn, error) {
	conn, err := net.DialTimeout("tcp", address, timeout)
	if err != nil {
		return nil, err
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		// Responses are small and latency matters more than packet count;
		// without this every request pays Nagle plus the peer's delayed ACK.
		_ = tcp.SetNoDelay(true)
	}
	return &Conn{conn: conn, clientID: clientID}, nil
}

func (c *Conn) Close() error { return c.conn.Close() }

// Request sends one request and returns the matching response body.
func (c *Conn) Request(apiKey int16, body []byte) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.next++
	correlationID := c.next
	if _, err := c.conn.Write(EncodeFrame(apiKey, correlationID, c.clientID, body)); err != nil {
		return nil, err
	}
	payload, err := c.readFrame()
	if err != nil {
		return nil, err
	}
	got, responseBody, err := DecodeFramePayload(payload)
	if err != nil {
		return nil, err
	}
	if got != correlationID {
		// A response for a request we are not waiting on can only mean the
		// stream has desynchronised; continuing would pair every later
		// response with the wrong request.
		return nil, fmt.Errorf("correlation id mismatch: expected %d, got %d", correlationID, got)
	}
	return responseBody, nil
}

// SendOneway sends without awaiting a response (acks=0).
func (c *Conn) SendOneway(apiKey int16, body []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.next++
	_, err := c.conn.Write(EncodeFrame(apiKey, c.next, c.clientID, body))
	return err
}

func (c *Conn) readFrame() ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(c.conn, header[:]); err != nil {
		return nil, err
	}
	length := int32(binary.BigEndian.Uint32(header[:]))
	if length < 0 {
		return nil, fmt.Errorf("negative frame length %d", length)
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(c.conn, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

// Authenticate binds a principal to this connection.
//
// The password crosses the wire in the clear exactly as SASL/PLAIN does,
// so the broker refuses this on a plaintext listener.
func (c *Conn) Authenticate(username, password string) (principal, role string, err error) {
	w := NewBodyWriter()
	w.String(username)
	w.String(password)
	body, err := c.Request(APIAuthenticate, w.Bytes())
	if err != nil {
		return "", "", err
	}
	r, err := NewBodyReader(body)
	if err != nil {
		return "", "", err
	}
	code := r.Int32()
	principal = r.String()
	role = r.String()
	if r.Err() != nil {
		return "", "", r.Err()
	}
	if code != ErrNone {
		return "", "", serverError(code, "authenticate")
	}
	return principal, role, nil
}

// APIVersionRange is one entry of an ApiVersions response.
type APIVersionRange struct {
	APIKey     int32
	MinVersion int32
	MaxVersion int32
}

// APIVersions asks the broker what it speaks. This is the one call that
// works across a version mismatch, so it is what a client uses to decide
// whether it can talk to a broker at all.
func (c *Conn) APIVersions() ([]APIVersionRange, string, error) {
	w := NewBodyWriter()
	w.String("brahmaputra-go")
	w.String("0.1.0")
	body, err := c.Request(APIAPIVersions, w.Bytes())
	if err != nil {
		return nil, "", err
	}
	r, err := NewBodyReader(body)
	if err != nil {
		return nil, "", err
	}
	code := r.Int32()
	if code != ErrNone {
		return nil, "", serverError(code, "api_versions")
	}
	count := int(r.Int32())
	ranges := make([]APIVersionRange, 0, count)
	for i := 0; i < count; i++ {
		ranges = append(ranges, APIVersionRange{r.Int32(), r.Int32(), r.Int32()})
	}
	brokerVersion := r.String()
	return ranges, brokerVersion, r.Err()
}

// ---------------------------------------------------------------------------
// Metadata and routing
// ---------------------------------------------------------------------------

type BrokerInfo struct {
	NodeID int32
	Host   string
	Port   int32
}

type PartitionInfo struct {
	Partition   int32
	Leader      int32
	Replicas    []int32
	ISR         []int32
	LeaderEpoch int32
}

type TopicInfo struct {
	Name       string
	Partitions []PartitionInfo
}

type ClusterMetadata struct {
	Brokers []BrokerInfo
	Topics  []TopicInfo
}

// PartitionsOf returns a topic's partition ids in ascending order.
func (m *ClusterMetadata) PartitionsOf(topic string) []int32 {
	for i := range m.Topics {
		if m.Topics[i].Name == topic {
			out := make([]int32, 0, len(m.Topics[i].Partitions))
			for _, partition := range m.Topics[i].Partitions {
				out = append(out, partition.Partition)
			}
			sort.Slice(out, func(a, b int) bool { return out[a] < out[b] })
			return out
		}
	}
	return nil
}

// LeaderOf returns the broker id leading a partition, or -1.
func (m *ClusterMetadata) LeaderOf(topic string, partition int32) int32 {
	for i := range m.Topics {
		if m.Topics[i].Name != topic {
			continue
		}
		for _, info := range m.Topics[i].Partitions {
			if info.Partition == partition {
				return info.Leader
			}
		}
	}
	return -1
}

func decodeMetadata(r *Reader) (*ClusterMetadata, error) {
	// Field order is exactly the schema's: brokers, controller_id, topics.
	// There is no leading error code — a per-topic one lives inside
	// TopicInfo instead.
	metadata := &ClusterMetadata{}
	for count := int(r.Int32()); count > 0; count-- {
		metadata.Brokers = append(metadata.Brokers, BrokerInfo{r.Int32(), r.String(), r.Int32()})
	}
	r.SkipInt32() // controller_id
	for count := int(r.Int32()); count > 0; count-- {
		name := r.String()
		topicError := r.Int32()
		var partitions []PartitionInfo
		for pcount := int(r.Int32()); pcount > 0; pcount-- {
			info := PartitionInfo{Partition: r.Int32(), Leader: r.Int32()}
			for rc := int(r.Int32()); rc > 0; rc-- {
				info.Replicas = append(info.Replicas, r.Int32())
			}
			for ic := int(r.Int32()); ic > 0; ic-- {
				info.ISR = append(info.ISR, r.Int32())
			}
			info.LeaderEpoch = r.Int32()
			partitions = append(partitions, info)
		}
		if topicError != ErrNone && topicError != ErrUnknownTopicOrPartition {
			return nil, serverError(topicError, "metadata for "+name)
		}
		metadata.Topics = append(metadata.Topics, TopicInfo{name, partitions})
	}
	return metadata, r.Err()
}

// Router keeps connections to every broker and routes by partition leader.
//
// Metadata is cached and refreshed only when a request comes back saying
// the route was stale, because refreshing per request would put the
// control plane on the data path.
type Router struct {
	clientID string
	timeout  time.Duration
	seed     *Conn
	mu       sync.Mutex
	conns    map[int32]*Conn
	metadata *ClusterMetadata
}

func NewRouter(address, clientID string, timeout time.Duration) (*Router, error) {
	seed, err := Dial(address, clientID, timeout)
	if err != nil {
		return nil, err
	}
	return &Router{
		clientID: clientID,
		timeout:  timeout,
		seed:     seed,
		conns:    map[int32]*Conn{},
	}, nil
}

func (router *Router) Close() error {
	router.mu.Lock()
	defer router.mu.Unlock()
	for _, conn := range router.conns {
		if conn != router.seed {
			_ = conn.Close()
		}
	}
	router.conns = map[int32]*Conn{}
	return router.seed.Close()
}

// Seed returns the connection this router was opened with.
func (router *Router) Seed() *Conn { return router.seed }

func (router *Router) Metadata(topics []string, refresh bool) (*ClusterMetadata, error) {
	router.mu.Lock()
	defer router.mu.Unlock()
	if !refresh && router.metadata != nil {
		return router.metadata, nil
	}
	w := NewBodyWriter()
	w.StringArray(topics)
	body, err := router.seed.Request(APIMetadata, w.Bytes())
	if err != nil {
		return nil, err
	}
	r, err := NewBodyReader(body)
	if err != nil {
		return nil, err
	}
	metadata, err := decodeMetadata(r)
	if err != nil {
		return nil, err
	}
	router.metadata = metadata
	return metadata, nil
}

func (router *Router) Refresh(topic string) (*ClusterMetadata, error) {
	return router.Metadata([]string{topic}, true)
}

// Partitions returns a topic's partitions, creating it implicitly if the
// broker auto-creates on first reference.
func (router *Router) Partitions(topic string) ([]int32, error) {
	metadata, err := router.Metadata([]string{topic}, false)
	if err != nil {
		return nil, err
	}
	partitions := metadata.PartitionsOf(topic)
	if len(partitions) == 0 {
		// A topic auto-created on first produce is not in the cached image
		// yet; one refresh distinguishes "new" from "absent".
		if metadata, err = router.Refresh(topic); err != nil {
			return nil, err
		}
		partitions = metadata.PartitionsOf(topic)
	}
	if len(partitions) == 0 {
		return nil, fmt.Errorf("topic %q has no partitions", topic)
	}
	return partitions, nil
}

// ConnFor returns the connection to a partition's leader.
func (router *Router) ConnFor(topic string, partition int32) (*Conn, error) {
	metadata, err := router.Metadata([]string{topic}, false)
	if err != nil {
		return nil, err
	}
	leader := metadata.LeaderOf(topic, partition)
	if leader < 0 {
		if metadata, err = router.Refresh(topic); err != nil {
			return nil, err
		}
		leader = metadata.LeaderOf(topic, partition)
	}
	if leader < 0 {
		return nil, fmt.Errorf("no leader for %s-%d", topic, partition)
	}

	router.mu.Lock()
	defer router.mu.Unlock()
	if conn, ok := router.conns[leader]; ok {
		return conn, nil
	}
	for _, broker := range metadata.Brokers {
		if broker.NodeID != leader {
			continue
		}
		// A single-broker cluster advertises the address it was configured
		// with, which may not be the one we dialled; reuse the seed rather
		// than opening a second connection to ourselves.
		if len(metadata.Brokers) == 1 {
			router.conns[leader] = router.seed
			return router.seed, nil
		}
		conn, err := Dial(
			fmt.Sprintf("%s:%d", broker.Host, broker.Port), router.clientID, router.timeout)
		if err != nil {
			return nil, err
		}
		router.conns[leader] = conn
		return conn, nil
	}
	return nil, fmt.Errorf("broker %d is not in the metadata", leader)
}

// ---------------------------------------------------------------------------
// Producer
// ---------------------------------------------------------------------------

// ProducerConfig is named as Kafka names its producer settings, so someone
// who knows Kafka does not have to learn a new vocabulary. Where a default
// differs from Kafka's it is called out.
type ProducerConfig struct {
	ClientID string
	// Acks: 0 fire-and-forget, 1 leader append, -1 every in-sync replica.
	Acks int32
	// BatchSize flushes a partition buffer once it holds this many bytes.
	BatchSize int
	// LingerMs flushes every non-empty buffer at least this often. 0 sends
	// each record immediately. Kafka defaults to 0; this defaults to 5
	// because an unbatched producer is slow enough to look broken.
	LingerMs int
	// Compression: none, lz4, zstd, snappy or gzip. Codecs other than none
	// and gzip must be registered with RegisterCodec first.
	Compression string
	// RequestTimeoutMs is the broker-side wait for acknowledgements.
	RequestTimeoutMs int32
	// Retries of a send the broker refused with a retriable error — one it
	// returns before appending, so a retry cannot duplicate.
	Retries int
	// RetryBackoffMs waits between retries. A tight retry loop against a
	// recovering broker slows the recovery it is waiting for.
	RetryBackoffMs int
	// DeliveryTimeoutMs caps the whole send, first attempt through last
	// retry. Retries alone do not bound latency: N retries that each take
	// RequestTimeoutMs is an unbounded wait.
	DeliveryTimeoutMs int
	// BufferMemory caps unflushed record bytes held client-side.
	BufferMemory int
	// MaxBlockMs is how long Send may block on a full buffer before failing.
	MaxBlockMs int
	// DialTimeout for opening broker connections.
	DialTimeout time.Duration
}

// DefaultProducerConfig returns the settings a producer uses unless told
// otherwise.
func DefaultProducerConfig() ProducerConfig {
	return ProducerConfig{
		ClientID:          "brahmaputra-go",
		Acks:              1,
		BatchSize:         16 * 1024,
		LingerMs:          5,
		Compression:       "none",
		RequestTimeoutMs:  30_000,
		Retries:           5,
		RetryBackoffMs:    100,
		DeliveryTimeoutMs: 120_000,
		BufferMemory:      32 * 1024 * 1024,
		MaxBlockMs:        60_000,
		DialTimeout:       30 * time.Second,
	}
}

type buffered struct {
	record    Record
	createdMs int64
}

type topicPartition struct {
	topic     string
	partition int32
}

// Producer batches records per partition and sends each batch as one
// Produce request. Share one across goroutines rather than creating one
// per message: the batching is the point.
type Producer struct {
	config ProducerConfig
	codec  Compression
	router *Router

	mu            sync.Mutex
	cond          *sync.Cond
	buffers       map[topicPartition][]buffered
	sizes         map[topicPartition]int
	bufferedBytes int
	roundRobin    int
	closed        bool
	done          chan struct{}
}

// NewProducer connects and starts the linger ticker.
func NewProducer(address string, config ProducerConfig) (*Producer, error) {
	codec, err := ParseCompression(config.Compression)
	if err != nil {
		return nil, err
	}
	router, err := NewRouter(address, config.ClientID, config.DialTimeout)
	if err != nil {
		return nil, err
	}
	producer := &Producer{
		config:  config,
		codec:   codec,
		router:  router,
		buffers: map[topicPartition][]buffered{},
		sizes:   map[topicPartition]int{},
		done:    make(chan struct{}),
	}
	producer.cond = sync.NewCond(&producer.mu)
	if config.LingerMs > 0 {
		go producer.lingerLoop()
	}
	return producer, nil
}

// Router exposes the routing layer, for callers that need metadata.
func (p *Producer) Router() *Router { return p.router }

// Close flushes, stops the ticker and releases connections.
func (p *Producer) Close() error {
	if err := p.Flush(); err != nil {
		return err
	}
	p.mu.Lock()
	p.closed = true
	p.cond.Broadcast()
	p.mu.Unlock()
	if p.config.LingerMs > 0 {
		select {
		case <-p.done:
		case <-time.After(2 * time.Second):
		}
	}
	return p.router.Close()
}

// Send buffers one record. Call Flush to await delivery.
//
// Returning without an offset is deliberate: with batching the offset is
// not known until the batch goes out, and pretending otherwise would mean
// a synchronous round trip per record. Use SendSync when you need one.
func (p *Producer) Send(topic string, value, key []byte, headers ...RecordHeader) error {
	partition, err := p.choosePartition(topic, key)
	if err != nil {
		return err
	}
	return p.SendTo(topic, partition, value, key, headers...)
}

// SendTo buffers one record on an explicit partition, bypassing the
// partitioner.
func (p *Producer) SendTo(
	topic string, partition int32, value, key []byte, headers ...RecordHeader,
) error {
	record := Record{Key: key, Value: value, Headers: headers}
	size := len(value) + len(key) + 16
	for _, header := range headers {
		size += len(header.Key) + len(header.Value) + 4
	}
	if err := p.reserve(size); err != nil {
		return err
	}

	slot := topicPartition{topic, partition}
	p.mu.Lock()
	p.buffers[slot] = append(p.buffers[slot], buffered{record, nowMillis()})
	p.sizes[slot] += size
	full := p.sizes[slot] >= p.config.BatchSize
	p.mu.Unlock()

	if p.config.LingerMs == 0 || full {
		return p.flushPartition(slot)
	}
	return nil
}

// SendSync sends one record on its own and returns its offset. A full
// round trip per record — correct, and slow.
func (p *Producer) SendSync(
	topic string, value, key []byte, headers ...RecordHeader,
) (int64, error) {
	partition, err := p.choosePartition(topic, key)
	if err != nil {
		return -1, err
	}
	return p.produce(topic, partition, []buffered{{Record{key, value, 0, headers}, nowMillis()}})
}

// Flush sends every buffered record and waits for acknowledgement.
func (p *Producer) Flush() error {
	p.mu.Lock()
	slots := make([]topicPartition, 0, len(p.buffers))
	for slot, records := range p.buffers {
		if len(records) > 0 {
			slots = append(slots, slot)
		}
	}
	p.mu.Unlock()
	for _, slot := range slots {
		if err := p.flushPartition(slot); err != nil {
			return err
		}
	}
	return nil
}

func (p *Producer) choosePartition(topic string, key []byte) (int32, error) {
	partitions, err := p.router.Partitions(topic)
	if err != nil {
		return 0, err
	}
	if key != nil {
		return PartitionForKey(key, partitions), nil
	}
	p.mu.Lock()
	index := p.roundRobin % len(partitions)
	p.roundRobin++
	p.mu.Unlock()
	return partitions[index], nil
}

// reserve blocks until size more bytes may be buffered.
//
// This is what makes BufferMemory real: a producer faster than its broker
// is slowed down here rather than allowed to grow without limit and die
// holding records nobody has acknowledged.
func (p *Producer) reserve(size int) error {
	limit := p.config.BufferMemory
	p.mu.Lock()
	defer p.mu.Unlock()
	if limit <= 0 || size >= limit {
		// A record larger than the whole budget is admitted rather than
		// waiting forever on a condition that can never hold; refusing
		// oversized records is the broker's job (max.message.bytes).
		p.bufferedBytes += size
		return nil
	}

	deadline := time.Now().Add(time.Duration(p.config.MaxBlockMs) * time.Millisecond)
	for p.bufferedBytes+size > limit {
		if time.Now().After(deadline) {
			return fmt.Errorf(
				"producer buffer full: %d of %d bytes unflushed after MaxBlockMs=%d",
				p.bufferedBytes, limit, p.config.MaxBlockMs)
		}
		// sync.Cond has no timed wait, so a waker bounds the wait instead.
		timer := time.AfterFunc(20*time.Millisecond, func() {
			p.mu.Lock()
			p.cond.Broadcast()
			p.mu.Unlock()
		})
		p.cond.Wait()
		timer.Stop()
	}
	p.bufferedBytes += size
	return nil
}

func (p *Producer) release(size int) {
	p.mu.Lock()
	p.bufferedBytes -= size
	if p.bufferedBytes < 0 {
		p.bufferedBytes = 0
	}
	p.cond.Broadcast()
	p.mu.Unlock()
}

func (p *Producer) lingerLoop() {
	defer close(p.done)
	ticker := time.NewTicker(time.Duration(p.config.LingerMs) * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		p.mu.Lock()
		closed := p.closed
		p.mu.Unlock()
		if closed {
			return
		}
		// A background flush that fails must not kill the ticker; the next
		// explicit Flush surfaces the error to a caller who can act on it.
		_ = p.Flush()
	}
}

func (p *Producer) flushPartition(slot topicPartition) error {
	p.mu.Lock()
	batch := p.buffers[slot]
	if len(batch) == 0 {
		p.mu.Unlock()
		return nil
	}
	p.buffers[slot] = nil
	size := p.sizes[slot]
	delete(p.sizes, slot)
	p.mu.Unlock()

	p.release(size)
	_, err := p.produce(slot.topic, slot.partition, batch)
	return err
}

func (p *Producer) produce(topic string, partition int32, batch []buffered) (int64, error) {
	if len(batch) == 0 {
		return -1, nil
	}
	// The batch stores one base timestamp and a delta per record, so the
	// rebasing happens here; maxTimestamp becomes the newest record's time,
	// which is what makes it a truthful answer to "how recent is this batch".
	maxTimestamp := batch[0].createdMs
	for _, item := range batch[1:] {
		if item.createdMs > maxTimestamp {
			maxTimestamp = item.createdMs
		}
	}
	records := make([]Record, 0, len(batch))
	for _, item := range batch {
		item.record.TimestampDelta = item.createdMs - maxTimestamp
		records = append(records, item.record)
	}

	encoded, err := EncodeRecordBatch(records, maxTimestamp, p.codec)
	if err != nil {
		return -1, err
	}
	w := NewBodyWriter()
	w.String(topic)
	w.Int32(partition)
	w.Int32(p.config.Acks)
	w.Int32(p.config.RequestTimeoutMs)
	w.Int64(int64(len(encoded)))
	w.Raw(encoded)
	body := w.Bytes()

	if p.config.Acks == 0 {
		conn, err := p.router.ConnFor(topic, partition)
		if err != nil {
			return -1, err
		}
		return -1, conn.SendOneway(APIProduce, body)
	}

	deadline := time.Now().Add(time.Duration(p.config.DeliveryTimeoutMs) * time.Millisecond)
	attemptsLeft := p.config.Retries
	for {
		conn, err := p.router.ConnFor(topic, partition)
		if err != nil {
			return -1, err
		}
		response, err := conn.Request(APIProduce, body)
		if err != nil {
			return -1, err
		}
		r, err := NewBodyReader(response)
		if err != nil {
			return -1, err
		}
		r.SkipString() // topic
		r.SkipInt32()  // partition
		code := r.Int32()
		baseOffset := r.Int64()
		r.SkipInt64() // log_append_time_ms
		if r.Err() != nil {
			return -1, r.Err()
		}
		if code == ErrNone {
			return baseOffset, nil
		}
		if !retriable(code) || attemptsLeft <= 0 || time.Now().After(deadline) {
			return -1, serverError(code, fmt.Sprintf("produce to %s-%d", topic, partition))
		}
		attemptsLeft--
		if code == ErrNotLeaderOrFollower || code == ErrFencedLeaderEpoch ||
			code == ErrUnknownLeaderEpoch {
			// A stale route is the most common retriable cause, and
			// resending to the same broker would just repeat it.
			_, _ = p.router.Refresh(topic)
		}
		time.Sleep(time.Duration(p.config.RetryBackoffMs) * time.Millisecond)
	}
}

// ---------------------------------------------------------------------------
// Consumer
// ---------------------------------------------------------------------------

// ConsumedRecord is one record delivered to the application.
type ConsumedRecord struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	// Timestamp is absolute unix milliseconds, already resolved against
	// the batch base so a caller never has to know the batch existed.
	Timestamp int64
	Headers   []RecordHeader
}

// Header returns the first value stored under key, if any.
func (r *ConsumedRecord) Header(key string) []byte {
	for i := range r.Headers {
		if r.Headers[i].Key == key {
			return r.Headers[i].Value
		}
	}
	return nil
}

// ConsumerConfig is named as Kafka names its consumer settings.
type ConsumerConfig struct {
	ClientID string
	// FetchMaxBytes caps a response, split across the partitions in one
	// request.
	FetchMaxBytes int32
	// FetchMinBytes returns early once this many bytes are ready.
	FetchMinBytes int32
	// FetchMaxWaitMs is the long-poll ceiling when caught up.
	FetchMaxWaitMs int32
	// MaxPollRecords is how many records a poll returns; the rest stay
	// buffered and uncommitted.
	MaxPollRecords int
	DialTimeout    time.Duration
}

func DefaultConsumerConfig() ConsumerConfig {
	return ConsumerConfig{
		ClientID:       "brahmaputra-go",
		FetchMaxBytes:  8 * 1024 * 1024,
		FetchMinBytes:  1,
		FetchMaxWaitMs: 500,
		MaxPollRecords: 500,
		DialTimeout:    30 * time.Second,
	}
}

// Consumer reads one partition at a time, with no group coordination.
type Consumer struct {
	config ConsumerConfig
	router *Router
}

func NewConsumer(address string, config ConsumerConfig) (*Consumer, error) {
	router, err := NewRouter(address, config.ClientID, config.DialTimeout)
	if err != nil {
		return nil, err
	}
	return &Consumer{config: config, router: router}, nil
}

func (c *Consumer) Close() error   { return c.router.Close() }
func (c *Consumer) Router() *Router { return c.router }

func (c *Consumer) Partitions(topic string) ([]int32, error) {
	return c.router.Partitions(topic)
}

// ListOffsets resolves Earliest, Latest or a unix-ms timestamp.
func (c *Consumer) ListOffsets(topic string, partition int32, timestamp int64) (int64, error) {
	w := NewBodyWriter()
	w.String(topic)
	w.Int32(partition)
	w.Int64(timestamp)
	conn, err := c.router.ConnFor(topic, partition)
	if err != nil {
		return 0, err
	}
	response, err := conn.Request(APIListOffsets, w.Bytes())
	if err != nil {
		return 0, err
	}
	r, err := NewBodyReader(response)
	if err != nil {
		return 0, err
	}
	r.SkipString() // topic
	r.SkipInt32()  // partition
	code := r.Int32()
	offset := r.Int64()
	r.SkipInt64() // timestamp
	if r.Err() != nil {
		return 0, r.Err()
	}
	if code != ErrNone {
		return 0, serverError(code, fmt.Sprintf("list_offsets %s-%d", topic, partition))
	}
	return offset, nil
}

// Fetch reads from one partition starting at offset.
func (c *Consumer) Fetch(
	topic string, partition int32, offset int64, maxWaitMs int32,
) ([]ConsumedRecord, error) {
	records, _, err := c.FetchVerbose(topic, partition, offset, maxWaitMs)
	return records, err
}

// FetchVerbose also returns the partition's high watermark.
func (c *Consumer) FetchVerbose(
	topic string, partition int32, offset int64, maxWaitMs int32,
) ([]ConsumedRecord, int64, error) {
	if maxWaitMs > c.config.FetchMaxWaitMs {
		maxWaitMs = c.config.FetchMaxWaitMs
	}
	w := NewBodyWriter()
	w.String(topic)
	w.Int32(partition)
	w.Int64(offset)
	w.Int32(c.config.FetchMaxBytes)
	w.Int32(maxWaitMs)
	w.Int32(c.config.FetchMinBytes)
	body := w.Bytes()

	conn, err := c.router.ConnFor(topic, partition)
	if err != nil {
		return nil, 0, err
	}
	code, highWatermark, batches, err := c.fetchOnce(conn, body)
	if err != nil {
		return nil, 0, err
	}
	if code == ErrNotLeaderOrFollower {
		if _, err = c.router.Refresh(topic); err != nil {
			return nil, 0, err
		}
		if conn, err = c.router.ConnFor(topic, partition); err != nil {
			return nil, 0, err
		}
		if code, highWatermark, batches, err = c.fetchOnce(conn, body); err != nil {
			return nil, 0, err
		}
	}
	if code != ErrNone {
		return nil, 0, serverError(code, fmt.Sprintf("fetch %s-%d", topic, partition))
	}

	var out []ConsumedRecord
	for _, batch := range batches {
		for index := range batch.Records {
			recordOffset := batch.BaseOffset + int64(index)
			// A batch can start before the requested offset; skip what the
			// caller has already seen.
			if recordOffset < offset {
				continue
			}
			record := &batch.Records[index]
			out = append(out, ConsumedRecord{
				Topic:     topic,
				Partition: partition,
				Offset:    recordOffset,
				Key:       record.Key,
				Value:     record.Value,
				Timestamp: record.Timestamp(batch.MaxTimestamp),
				Headers:   record.Headers,
			})
		}
	}
	return out, highWatermark, nil
}

func (c *Consumer) fetchOnce(
	conn *Conn, body []byte,
) (int32, int64, []DecodedBatch, error) {
	response, err := conn.Request(APIFetch, body)
	if err != nil {
		return 0, 0, nil, err
	}
	r, err := NewBodyReader(response)
	if err != nil {
		return 0, 0, nil, err
	}
	r.SkipString() // topic
	r.SkipInt32()  // partition
	code := r.Int32()
	highWatermark := r.Int64()
	r.SkipInt64() // last_stable_offset
	batchesLength := r.Int64()
	if r.Err() != nil {
		return 0, 0, nil, r.Err()
	}
	trailing := r.Rest()
	if batchesLength > int64(len(trailing)) {
		return 0, 0, nil, errors.New("fetch response claims more batch bytes than it carries")
	}
	raw := trailing[:batchesLength]

	var batches []DecodedBatch
	for pos := 0; pos < len(raw); {
		batch, next, err := DecodeRecordBatch(raw, pos)
		if err != nil {
			return 0, 0, nil, err
		}
		batches = append(batches, batch)
		pos = next
	}
	return code, highWatermark, batches, nil
}
