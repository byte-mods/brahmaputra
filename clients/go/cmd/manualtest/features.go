package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	bp "github.com/byte-mods/brahmaputra/clients/go/brahmaputra"
)

// featureChecks covers the client feature checklist item by item: every
// configuration setting is shown to change behaviour, not merely accepted.
func featureChecks(address string) {
	quick := func() bp.ProducerConfig {
		config := bp.DefaultProducerConfig()
		config.LingerMs = 0
		return config
	}
	fetchAll := func(topic string, partition int32) []bp.ConsumedRecord {
		consumer := must(bp.NewConsumer(address, bp.DefaultConsumerConfig()))
		defer consumer.Close()
		var out []bp.ConsumedRecord
		for offset := int64(0); ; {
			batch, err := consumer.Fetch(topic, partition, offset, 100)
			if err != nil || len(batch) == 0 {
				return out
			}
			out = append(out, batch...)
			offset = batch[len(batch)-1].Offset + 1
		}
	}

	section("producer settings")
	{
		topic := unique("go-linger")
		config := bp.DefaultProducerConfig()
		config.LingerMs = 50
		producer := must(bp.NewProducer(address, config))
		must(0, producer.SendTo(topic, 0, []byte("lingered"), nil))
		time.Sleep(600 * time.Millisecond)
		got := fetchAll(topic, 0)
		check("linger.ms sends a batch without an explicit flush", len(got) == 1,
			fmt.Sprintf("got %d before any flush", len(got)))
		must(0, producer.Close())
	}
	{
		topic := unique("go-batchsize")
		config := bp.DefaultProducerConfig()
		config.LingerMs = 60_000
		config.BatchSize = 200
		producer := must(bp.NewProducer(address, config))
		for i := 0; i < 10; i++ {
			must(0, producer.SendTo(topic, 0, bytes.Repeat([]byte("b"), 50), nil))
		}
		got := fetchAll(topic, 0)
		check("batch.size sends a full batch before linger expires", len(got) >= 3,
			fmt.Sprintf("got %d of 10 with linger 60s", len(got)))
		must(0, producer.Close())
	}
	{
		topic := unique("go-closeflush")
		config := bp.DefaultProducerConfig()
		config.LingerMs = 60_000
		producer := must(bp.NewProducer(address, config))
		for i := 0; i < 5; i++ {
			must(0, producer.SendTo(topic, 0, []byte(fmt.Sprintf("c%d", i)), nil))
		}
		must(0, producer.Close())
		got := fetchAll(topic, 0)
		check("close flushes buffered records", len(got) == 5, fmt.Sprintf("got %d", len(got)))
	}
	{
		topic := unique("go-sync")
		producer := must(bp.NewProducer(address, quick()))
		first := must(producer.SendToSync(topic, 1, []byte("s0"), nil, time.Now().UnixMilli()))
		second := must(producer.SendToSync(topic, 1, []byte("s1"), nil, time.Now().UnixMilli()))
		keyed := must(producer.SendSync(topic, []byte("s2"), []byte("k")))
		must(0, producer.Close())
		got := fetchAll(topic, 1)
		check("send-and-wait returns the record's offset",
			first == 0 && second == 1 && keyed >= 0 && len(got) >= 2 &&
				string(got[1].Value) == "s1",
			fmt.Sprintf("offsets %d %d %d", first, second, keyed))
	}
	{
		topic := unique("go-roundrobin")
		producer := must(bp.NewProducer(address, quick()))
		partitions := must(producer.Router().Partitions(topic))
		for i := 0; i < 2*len(partitions); i++ {
			must(0, producer.Send(topic, []byte(fmt.Sprintf("rr%d", i)), nil))
		}
		must(0, producer.Close())
		even := true
		for _, partition := range partitions {
			if len(fetchAll(topic, partition)) != 2 {
				even = false
			}
		}
		check("null keys are spread round-robin", even && len(partitions) > 1,
			fmt.Sprintf("%d partitions", len(partitions)))
	}
	{
		topic := unique("go-timestamp")
		producer := must(bp.NewProducer(address, quick()))
		must(0, producer.SendToAt(topic, 0, []byte("t1"), nil, 1_600_000_001_000))
		must(0, producer.SendToAt(topic, 0, []byte("t2"), nil, 1_600_000_002_000))
		must(0, producer.SendToAt(topic, 0, []byte("t3"), nil, 1_600_000_003_000))
		must(0, producer.Close())
		got := fetchAll(topic, 0)
		check("an explicit record timestamp is kept",
			len(got) == 3 && got[0].Timestamp == 1_600_000_001_000 &&
				got[2].Timestamp == 1_600_000_003_000,
			fmt.Sprintf("%d records", len(got)))
		consumer := must(bp.NewConsumer(address, bp.DefaultConsumerConfig()))
		byTime := must(consumer.ListOffsets(topic, 0, 1_600_000_001_500))
		pastEnd := must(consumer.ListOffsets(topic, 0, 1_700_000_000_000))
		check("list offsets by timestamp finds the first record at or after it",
			byTime == 1 && pastEnd == 3, fmt.Sprintf("byTime=%d pastEnd=%d", byTime, pastEnd))
		consumer.Close()
	}
	{
		topic := unique("go-acksall")
		config := quick()
		config.Acks = -1
		config.RequestTimeoutMs = 1500
		producer := must(bp.NewProducer(address, config))
		offset, err := producer.SendSync(topic, []byte("durable"), nil)
		check("acks=all with request.timeout.ms is acknowledged", err == nil && offset >= 0,
			fmt.Sprint(err))
		must(0, producer.Close())
	}
	{
		var compressed, decompressed atomic.Int32
		bp.RegisterCodec(bp.CompressionLZ4,
			func(payload []byte) ([]byte, error) { compressed.Add(1); return lz4Encode(payload), nil },
			func(payload []byte) ([]byte, error) { decompressed.Add(1); return lz4Decode(payload) })
		topic := unique("go-lz4")
		config := quick()
		config.Compression = "lz4"
		producer := must(bp.NewProducer(address, config))
		body := bytes.Repeat([]byte("registered codec "), 30)
		for i := 0; i < 5; i++ {
			must(0, producer.SendTo(topic, 0, body, []byte{byte('0' + i)}))
		}
		must(0, producer.Close())
		got := fetchAll(topic, 0)
		ok := len(got) == 5
		for _, record := range got {
			ok = ok && bytes.Equal(record.Value, body)
		}
		check("a registered lz4 codec round-trips", ok && compressed.Load() > 0 &&
			decompressed.Load() > 0, fmt.Sprintf("got %d, compress=%d decompress=%d",
			len(got), compressed.Load(), decompressed.Load()))
	}

	section("retries (fault-injecting proxy)")
	{
		proxy := newFaultProxy(address)
		topic := unique("go-retry")
		config := quick()
		config.Retries = 5
		config.RetryBackoffMs = 50
		producer := must(bp.NewProducer(proxy.address, config))
		must(producer.Router().Partitions(topic))
		proxy.inject(bp.ErrNotLeaderOrFollower, 2)
		offset, err := producer.SendToSync(topic, 0, []byte("eventually"), nil, time.Now().UnixMilli())
		check("a retriable produce error is retried until it succeeds",
			err == nil && offset == 0 && proxy.produces() == 3,
			fmt.Sprintf("err=%v offset=%d attempts=%d", err, offset, proxy.produces()))
		must(0, producer.Close())

		config = quick()
		config.Retries = 2
		config.RetryBackoffMs = 200
		producer = must(bp.NewProducer(proxy.address, config))
		must(producer.Router().Partitions(topic))
		proxy.inject(bp.ErrNotLeaderOrFollower, -1)
		started := time.Now()
		_, err = producer.SendToSync(topic, 0, []byte("never"), nil, time.Now().UnixMilli())
		elapsed := time.Since(started)
		check("retry.backoff.ms spaces the retries",
			err != nil && proxy.produces() == 3 && elapsed >= 400*time.Millisecond,
			fmt.Sprintf("attempts=%d elapsed=%v", proxy.produces(), elapsed))
		producer.Close()

		config = quick()
		config.Retries = 5
		config.RetryBackoffMs = 1000
		producer = must(bp.NewProducer(proxy.address, config))
		must(producer.Router().Partitions(topic))
		proxy.inject(bp.ErrInvalidRequest, -1)
		started = time.Now()
		_, err = producer.SendToSync(topic, 0, []byte("rejected"), nil, time.Now().UnixMilli())
		check("a non-retriable produce error is not retried",
			err != nil && proxy.produces() == 1 && time.Since(started) < time.Second,
			fmt.Sprintf("attempts=%d", proxy.produces()))
		producer.Close()

		config = quick()
		config.Retries = 1_000_000
		config.RetryBackoffMs = 50
		config.DeliveryTimeoutMs = 600
		producer = must(bp.NewProducer(proxy.address, config))
		must(producer.Router().Partitions(topic))
		proxy.inject(bp.ErrNotLeaderOrFollower, -1)
		started = time.Now()
		_, err = producer.SendToSync(topic, 0, []byte("late"), nil, time.Now().UnixMilli())
		elapsed = time.Since(started)
		check("delivery.timeout.ms bounds the retries",
			err != nil && elapsed < 3*time.Second && proxy.produces() > 2,
			fmt.Sprintf("attempts=%d elapsed=%v", proxy.produces(), elapsed))
		producer.Close()
		proxy.close()
	}

	section("consumer settings")
	{
		topic := unique("go-fetch")
		producer := must(bp.NewProducer(address, quick()))
		for i := 0; i < 10; i++ {
			must(0, producer.SendTo(topic, 0, bytes.Repeat([]byte{byte('a' + i)}, 1000), nil))
		}
		must(0, producer.Close())

		consumer := must(bp.NewConsumer(address, bp.DefaultConsumerConfig()))
		records, highWatermark, err := consumer.FetchVerbose(topic, 0, 0, 100)
		check("fetch reports the high watermark", err == nil && highWatermark == 10 &&
			len(records) == 10, fmt.Sprintf("hw=%d err=%v", highWatermark, err))
		metadata := must(consumer.Router().Metadata([]string{topic}, true))
		partitions := metadata.PartitionsOf(topic)
		led := len(partitions) == 4
		for _, partition := range partitions {
			led = led && metadata.LeaderOf(topic, partition) >= 0
		}
		check("metadata lists every partition with a leader", led,
			fmt.Sprintf("%v", partitions))
		consumer.Close()

		small := bp.DefaultConsumerConfig()
		small.FetchMaxBytes = 2500
		capped := must(bp.NewConsumer(address, small))
		got := must(capped.Fetch(topic, 0, 0, 100))
		check("fetch.max.bytes caps a response", len(got) >= 1 && len(got) < 10,
			fmt.Sprintf("got %d of 10", len(got)))
		capped.Close()

		waiting := bp.DefaultConsumerConfig()
		waiting.FetchMinBytes = 1 << 20
		waiting.FetchMaxWaitMs = 400
		patient := must(bp.NewConsumer(address, waiting))
		started := time.Now()
		got = must(patient.Fetch(topic, 0, 0, 400))
		elapsed := time.Since(started)
		check("fetch.min.bytes waits up to fetch.max.wait.ms for more data",
			len(got) == 10 && elapsed >= 300*time.Millisecond && elapsed < 3*time.Second,
			fmt.Sprintf("got %d after %v", len(got), elapsed))
		patient.Close()
	}
	{
		// A length prefix larger than what follows, or negative, is an
		// error — never a slice past the end or a huge allocation.
		batch, _ := bp.EncodeRecordBatch([]bp.Record{{Value: []byte("x")}}, 0, bp.CompressionNone)
		oversized := append([]byte{}, batch...)
		binary.BigEndian.PutUint32(oversized[8:12], 0x7fffffff)
		_, _, errOversized := bp.DecodeRecordBatch(oversized, 0)
		negative := append([]byte{}, batch...)
		binary.BigEndian.PutUint32(negative[8:12], 0xfffffff0)
		_, _, errNegative := bp.DecodeRecordBatch(negative, 0)
		_, errTruncated := bp.NewBodyReader([]byte{0x7e, '1'})
		check("a truncated or oversized length is an error, not a crash",
			errOversized != nil && errNegative != nil && errTruncated != nil,
			fmt.Sprintf("%v / %v / %v", errOversized, errNegative, errTruncated))
	}

	section("consumer group settings")
	produceN := func(topic string, n int) {
		producer := must(bp.NewProducer(address, quick()))
		for i := 0; i < n; i++ {
			must(0, producer.Send(topic, []byte(fmt.Sprintf("m%d", i)), nil))
		}
		must(0, producer.Close())
	}
	manual := func() bp.GroupConfig {
		config := bp.DefaultGroupConfig()
		config.AutoCommitIntervalMs = 0
		return config
	}
	pollUntil := func(group *bp.GroupConsumer, want int, limit time.Duration) ([]bp.ConsumedRecord, int) {
		var got []bp.ConsumedRecord
		largest := 0
		deadline := time.Now().Add(limit)
		for len(got) < want && time.Now().Before(deadline) {
			records, err := group.Poll(300 * time.Millisecond)
			if err != nil {
				break
			}
			if len(records) > largest {
				largest = len(records)
			}
			got = append(got, records...)
		}
		return got, largest
	}
	sumCommitted := func(group *bp.GroupConsumer) int64 {
		committed, err := group.Committed(nil)
		if err != nil {
			return -1
		}
		total := int64(0)
		for _, offset := range committed {
			total += offset
		}
		return total
	}
	{
		topic := unique("go-maxpoll")
		produceN(topic, 20)
		config := manual()
		config.MaxPollRecords = 5
		group := must(bp.NewGroupConsumer(address, unique("go-maxpoll-grp"), config))
		group.Subscribe([]string{topic})
		got, largest := pollUntil(group, 20, 20*time.Second)
		check("max.poll.records caps one poll", len(got) == 20 && largest <= 5,
			fmt.Sprintf("got %d, largest poll %d", len(got), largest))
		must(0, group.Close())
	}
	{
		topic := unique("go-autocommit")
		produceN(topic, 12)
		config := bp.DefaultGroupConfig()
		config.AutoCommitIntervalMs = 200
		group := must(bp.NewGroupConsumer(address, unique("go-autocommit-grp"), config))
		group.Subscribe([]string{topic})
		pollUntil(group, 12, 20*time.Second)
		time.Sleep(300 * time.Millisecond)
		group.Poll(300 * time.Millisecond)
		total := sumCommitted(group)
		check("auto-commit commits delivered positions", total == 12, fmt.Sprint(total))
		must(0, group.Close())
	}
	{
		topic := unique("go-heartbeat")
		produceN(topic, 4)
		config := manual()
		config.SessionTimeoutMs = 1500
		config.HeartbeatIntervalMs = 300
		group := must(bp.NewGroupConsumer(address, unique("go-heartbeat-grp"), config))
		group.Subscribe([]string{topic})
		pollUntil(group, 4, 20*time.Second)
		generation := group.Generation()
		time.Sleep(4 * time.Second) // well past session.timeout.ms, no polls
		err := group.Commit()
		check("heartbeats keep an idle member in its group",
			err == nil && group.Generation() == generation, fmt.Sprint(err))
		must(0, group.Close())
	}
	{
		first, second := unique("go-multi-a"), unique("go-multi-b")
		produceN(first, 6)
		produceN(second, 7)
		group := must(bp.NewGroupConsumer(address, unique("go-multi-grp"), manual()))
		group.Subscribe([]string{first, second})
		got, _ := pollUntil(group, 13, 20*time.Second)
		topics := map[string]int{}
		for _, record := range got {
			topics[record.Topic]++
		}
		check("a member subscribed to two topics consumes both",
			topics[first] == 6 && topics[second] == 7, fmt.Sprint(topics))
		must(0, group.Close())
	}
	{
		topic := unique("go-static")
		produceN(topic, 4)
		groupID := unique("go-static-grp")
		config := manual()
		config.GroupInstanceID = "instance-1"
		original := must(bp.NewGroupConsumer(address, groupID, config))
		original.Subscribe([]string{topic})
		pollUntil(original, 4, 20*time.Second)
		memberID := original.MemberID()
		// The same instance comes back (a restart) before the old session
		// has expired: it must reclaim the slot, not join as a stranger.
		returning := must(bp.NewGroupConsumer(address, groupID, config))
		returning.Subscribe([]string{topic})
		returning.Poll(2 * time.Second)
		check("a returning static member reclaims its member id",
			memberID != "" && returning.MemberID() == memberID,
			fmt.Sprintf("%q then %q", memberID, returning.MemberID()))
		returning.Close()
		original.Close()
	}
	{
		topic := unique("go-leave")
		produceN(topic, 8)
		groupID := unique("go-leave-grp")
		config := manual()
		config.SessionTimeoutMs = 30_000
		config.RebalanceTimeoutMs = 10_000
		config.SocketTimeout = 30 * time.Second
		leaving := must(bp.NewGroupConsumer(address, groupID, config))
		leaving.Subscribe([]string{topic})
		pollUntil(leaving, 8, 20*time.Second)
		must(0, leaving.Close())
		successor := must(bp.NewGroupConsumer(address, groupID, config))
		successor.Subscribe([]string{topic})
		started := time.Now()
		for len(successor.Assignment()[topic]) < 4 && time.Since(started) < 15*time.Second {
			successor.Poll(200 * time.Millisecond)
		}
		elapsed := time.Since(started)
		check("close leaves the group so the next member is assigned at once",
			len(successor.Assignment()[topic]) == 4 && elapsed < 6*time.Second,
			fmt.Sprintf("assigned after %v", elapsed))
		must(0, successor.Close())
	}
	{
		topic := unique("go-fence")
		produceN(topic, 8)
		groupID := unique("go-fence-grp")
		config := manual()
		config.RebalanceTimeoutMs = 2000
		first := must(bp.NewGroupConsumer(address, groupID, config))
		first.Subscribe([]string{topic})
		pollUntil(first, 8, 20*time.Second)
		oldGeneration := first.Generation()

		// A second member joins while the first stops polling: the group
		// moves on without it, so its generation is superseded.
		second := must(bp.NewGroupConsumer(address, groupID, config))
		second.Subscribe([]string{topic})
		deadline := time.Now().Add(15 * time.Second)
		for second.Generation() <= oldGeneration && time.Now().Before(deadline) {
			second.Poll(200 * time.Millisecond)
		}
		err := first.Commit()
		check("a commit from a superseded generation is fenced", err != nil,
			fmt.Sprintf("old=%d new=%d", oldGeneration, second.Generation()))

		// Both members polling settle on a split of the partitions.
		var wg sync.WaitGroup
		for _, member := range []*bp.GroupConsumer{first, second} {
			wg.Add(1)
			go func(member *bp.GroupConsumer) {
				defer wg.Done()
				until := time.Now().Add(8 * time.Second)
				for time.Now().Before(until) {
					member.Poll(200 * time.Millisecond)
				}
			}(member)
		}
		wg.Wait()
		a, b := first.Assignment()[topic], second.Assignment()[topic]
		union := map[int32]int{}
		for _, partition := range append(append([]int32{}, a...), b...) {
			union[partition]++
		}
		disjoint := len(union) == 4
		for _, count := range union {
			disjoint = disjoint && count == 1
		}
		check("two members share the partitions without overlap",
			disjoint && len(a) > 0 && len(b) > 0, fmt.Sprintf("%v / %v", a, b))
		second.Close()
		first.Close()
	}
}

// ---------------------------------------------------------------------------
// LZ4 block codec for the registration check. The encoder emits one
// literal-only sequence (valid LZ4, just uncompressed); the decoder handles
// any block. The payload is a little-endian uint32 size, then the block.
// ---------------------------------------------------------------------------

func lz4Encode(src []byte) []byte {
	out := binary.LittleEndian.AppendUint32(nil, uint32(len(src)))
	n := len(src)
	if n < 15 {
		out = append(out, byte(n<<4))
	} else {
		out = append(out, 0xF0)
		rest := n - 15
		for rest >= 255 {
			out = append(out, 255)
			rest -= 255
		}
		out = append(out, byte(rest))
	}
	return append(out, src...)
}

func lz4Decode(src []byte) ([]byte, error) {
	if len(src) < 4 {
		return nil, errors.New("lz4: short input")
	}
	size := int(binary.LittleEndian.Uint32(src))
	out := make([]byte, 0, size)
	pos := 4
	readLen := func(base int) (int, error) {
		n := base
		if base == 15 {
			for {
				if pos >= len(src) {
					return 0, errors.New("lz4: truncated length")
				}
				b := int(src[pos])
				pos++
				n += b
				if b != 255 {
					break
				}
			}
		}
		return n, nil
	}
	for pos < len(src) {
		token := int(src[pos])
		pos++
		literals, err := readLen(token >> 4)
		if err != nil {
			return nil, err
		}
		if pos+literals > len(src) {
			return nil, errors.New("lz4: truncated literals")
		}
		out = append(out, src[pos:pos+literals]...)
		pos += literals
		if pos >= len(src) {
			break
		}
		if pos+2 > len(src) {
			return nil, errors.New("lz4: truncated offset")
		}
		offset := int(binary.LittleEndian.Uint16(src[pos:]))
		pos += 2
		match, err := readLen(token & 15)
		if err != nil {
			return nil, err
		}
		match += 4
		if offset == 0 || offset > len(out) {
			return nil, errors.New("lz4: bad offset")
		}
		for i := 0; i < match; i++ {
			out = append(out, out[len(out)-offset])
		}
	}
	if len(out) != size {
		return nil, errors.New("lz4: size mismatch")
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// faultProxy forwards frames to the broker one request at a time, but can
// answer Produce requests itself with an injected error code — the only way
// to make a healthy single broker return a retriable error on demand.
// ---------------------------------------------------------------------------

type faultProxy struct {
	address  string
	target   string
	listener net.Listener
	mu       sync.Mutex
	code     int32
	failures int // remaining injected failures; -1 is forever
	seen     int // produce requests received since the last inject
}

func newFaultProxy(target string) *faultProxy {
	listener := must(net.Listen("tcp", "127.0.0.1:0"))
	p := &faultProxy{address: listener.Addr().String(), target: target, listener: listener}
	go func() {
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			go p.serve(client)
		}
	}()
	return p
}

func (p *faultProxy) inject(code int32, failures int) {
	p.mu.Lock()
	p.code, p.failures, p.seen = code, failures, 0
	p.mu.Unlock()
}

func (p *faultProxy) produces() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.seen
}

func readFrameRaw(conn net.Conn) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return nil, err
	}
	payload := make([]byte, binary.BigEndian.Uint32(header[:]))
	if _, err := io.ReadFull(conn, payload); err != nil {
		return nil, err
	}
	return append(header[:], payload...), nil
}

func (p *faultProxy) serve(client net.Conn) {
	defer client.Close()
	upstream, err := net.Dial("tcp", p.target)
	if err != nil {
		return
	}
	defer upstream.Close()
	for {
		frame, err := readFrameRaw(client)
		if err != nil {
			return
		}
		apiKey := int16(binary.BigEndian.Uint16(frame[4:6]))
		if apiKey == bp.APIProduce {
			p.mu.Lock()
			p.seen++
			fail := p.failures != 0
			if p.failures > 0 {
				p.failures--
			}
			code := p.code
			p.mu.Unlock()
			if fail {
				clientLen := int(binary.BigEndian.Uint16(frame[12:14]))
				header := frame[4 : 14+clientLen]
				w := bp.NewBodyWriter()
				w.String("")
				w.Int32(0)
				w.Int32(code)
				w.Int64(-1)
				w.Int64(-1)
				body := append(append([]byte{}, header...), w.Bytes()...)
				response := binary.BigEndian.AppendUint32(nil, uint32(len(body)))
				if _, err := client.Write(append(response, body...)); err != nil {
					return
				}
				continue
			}
		}
		if _, err := upstream.Write(frame); err != nil {
			return
		}
		response, err := readFrameRaw(upstream)
		if err != nil {
			return
		}
		if _, err := client.Write(response); err != nil {
			return
		}
	}
}

func (p *faultProxy) close() { p.listener.Close() }
