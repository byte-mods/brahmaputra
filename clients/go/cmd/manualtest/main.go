// Command manualtest exercises the Go driver against a live broker.
//
//	brahmaputra-server --data-dir ./data --default-partitions 4
//	go run ./cmd/manualtest [host:port]
//
// Every check asserts a property of the system, not that a function ran:
// records come back byte-identical, keys pin partitions, headers survive,
// offsets are contiguous. It exits non-zero on the first failure.
package main

import (
	"bytes"
	"fmt"
	"os"
	"time"

	bp "github.com/byte-mods/brahmaputra/clients/go/brahmaputra"
)

var (
	passed int
	failed int
)

func check(name string, ok bool, detail string) {
	if ok {
		passed++
		fmt.Printf("  ok   %s\n", name)
		return
	}
	failed++
	if detail != "" {
		fmt.Printf("  FAIL %s: %s\n", name, detail)
	} else {
		fmt.Printf("  FAIL %s\n", name)
	}
}

func section(title string) { fmt.Printf("\n%s\n", title) }

func unique(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano()%1_000_000_000)
}

func must[T any](value T, err error) T {
	if err != nil {
		fmt.Printf("  FATAL %v\n", err)
		os.Exit(2)
	}
	return value
}

func main() {
	address := "127.0.0.1:9092"
	if len(os.Args) > 1 {
		address = os.Args[1]
	}

	section("connection and metadata")
	{
		consumer := must(bp.NewConsumer(address, bp.DefaultConsumerConfig()))
		defer consumer.Close()
		versions, brokerVersion, err := consumer.Router().Seed().APIVersions()
		check("ApiVersions answers", err == nil && len(versions) > 0, fmt.Sprint(err))
		check("broker reports a version", brokerVersion != "", brokerVersion)
		metadata := must(consumer.Router().Metadata(nil, true))
		check("metadata lists brokers", len(metadata.Brokers) >= 1,
			fmt.Sprintf("%d brokers", len(metadata.Brokers)))
	}

	section("produce and consume round trip")
	topic := unique("go-roundtrip")
	payloads := make([][]byte, 50)
	for i := range payloads {
		payloads[i] = []byte(fmt.Sprintf("record-%d", i))
	}
	{
		config := bp.DefaultProducerConfig()
		config.LingerMs = 0
		producer := must(bp.NewProducer(address, config))
		for _, payload := range payloads {
			if err := producer.SendTo(topic, 0, payload, nil); err != nil {
				fmt.Printf("  FATAL send: %v\n", err)
				os.Exit(2)
			}
		}
		must(0, producer.Flush())
		must(0, producer.Close())
	}
	{
		consumer := must(bp.NewConsumer(address, bp.DefaultConsumerConfig()))
		got := must(consumer.Fetch(topic, 0, 0, 500))
		check("every record comes back", len(got) == len(payloads),
			fmt.Sprintf("got %d", len(got)))
		identical := len(got) == len(payloads)
		for i := 0; identical && i < len(got); i++ {
			if !bytes.Equal(got[i].Value, payloads[i]) || got[i].Offset != int64(i) {
				identical = false
			}
		}
		check("values byte-identical and offsets contiguous", identical, "")
		consumer.Close()
	}

	section("compression codecs")
	// Only none and gzip ship in the driver; lz4/zstd/snappy are opt-in
	// via RegisterCodec so applications that do not want those
	// dependencies do not carry them.
	for _, codec := range []string{"none", "gzip"} {
		codecTopic := unique("go-" + codec)
		body := bytes.Repeat([]byte("the same line over and over. "), 40)
		config := bp.DefaultProducerConfig()
		config.LingerMs = 0
		config.Compression = codec
		producer := must(bp.NewProducer(address, config))
		for i := 0; i < 20; i++ {
			must(0, producer.SendTo(codecTopic, 0, append(append([]byte{}, body...),
				byte('0'+i%10)), nil))
		}
		must(0, producer.Flush())
		must(0, producer.Close())

		consumer := must(bp.NewConsumer(address, bp.DefaultConsumerConfig()))
		got := must(consumer.Fetch(codecTopic, 0, 0, 500))
		check(codec+": round trips", len(got) == 20 &&
			bytes.HasPrefix(got[0].Value, body), fmt.Sprintf("got %d records", len(got)))
		consumer.Close()
	}

	section("keys, partitioning and ordering")
	{
		keyTopic := unique("go-keys")
		config := bp.DefaultProducerConfig()
		config.LingerMs = 0
		producer := must(bp.NewProducer(address, config))
		partitions := must(producer.Router().Partitions(keyTopic))
		for i := 0; i < 30; i++ {
			must(0, producer.Send(keyTopic, []byte(fmt.Sprintf("v%d", i)), []byte("user-7")))
		}
		must(0, producer.Flush())
		must(0, producer.Close())

		target := bp.PartitionForKey([]byte("user-7"), partitions)
		consumer := must(bp.NewConsumer(address, bp.DefaultConsumerConfig()))
		onTarget := must(consumer.Fetch(keyTopic, target, 0, 500))
		check("a key pins every record to one partition", len(onTarget) == 30,
			fmt.Sprintf("partition %d holds %d of 30", target, len(onTarget)))

		ordered := len(onTarget) == 30
		for i := 0; ordered && i < len(onTarget); i++ {
			if string(onTarget[i].Value) != fmt.Sprintf("v%d", i) {
				ordered = false
			}
		}
		check("per-key order is preserved", ordered, "")

		strays := 0
		for _, partition := range partitions {
			if partition == target {
				continue
			}
			strays += len(must(consumer.Fetch(keyTopic, partition, 0, 200)))
		}
		check("no keyed record landed elsewhere", strays == 0, fmt.Sprintf("%d strays", strays))
		consumer.Close()
	}

	section("murmur2 agrees with the broker's partitioner")
	check("murmur2(\"\") is stable", bp.Murmur2(nil) == 275646681,
		fmt.Sprint(bp.Murmur2(nil)))
	check("murmur2 is deterministic",
		bp.Murmur2([]byte("user-7")) == bp.Murmur2([]byte("user-7")), "")
	check("different keys hash differently",
		bp.Murmur2([]byte("user-7")) != bp.Murmur2([]byte("user-8")), "")

	section("record headers and timestamps")
	{
		headerTopic := unique("go-headers")
		before := time.Now().UnixMilli() - 1000
		config := bp.DefaultProducerConfig()
		config.LingerMs = 0
		producer := must(bp.NewProducer(address, config))
		must(0, producer.SendTo(headerTopic, 0, []byte("annotated"), nil,
			bp.RecordHeader{Key: "trace-id", Value: []byte("abc-123")},
			bp.RecordHeader{Key: "content-type", Value: []byte("application/json")},
			bp.RecordHeader{Key: "tombstone-reason", Value: nil},
		))
		must(0, producer.SendTo(headerTopic, 0, []byte("plain"), nil))
		must(0, producer.Flush())
		must(0, producer.Close())
		after := time.Now().UnixMilli() + 1000

		consumer := must(bp.NewConsumer(address, bp.DefaultConsumerConfig()))
		got := must(consumer.Fetch(headerTopic, 0, 0, 500))
		check("both records arrive", len(got) == 2, fmt.Sprintf("got %d", len(got)))
		if len(got) == 2 {
			annotated, plain := got[0], got[1]
			check("headers survive the round trip", len(annotated.Headers) == 3,
				fmt.Sprintf("%d headers", len(annotated.Headers)))
			check("header values are exact",
				bytes.Equal(annotated.Header("trace-id"), []byte("abc-123")), "")
			check("a null header value stays null",
				len(annotated.Headers) == 3 && annotated.Headers[2].Value == nil, "")
			check("a record with no headers gains none from its batch",
				len(plain.Headers) == 0, fmt.Sprintf("%d headers", len(plain.Headers)))
			inWindow := true
			for _, record := range got {
				if record.Timestamp < before || record.Timestamp > after {
					inWindow = false
				}
			}
			check("timestamps are real wall-clock values", inWindow,
				fmt.Sprintf("%d,%d outside %d..%d", got[0].Timestamp, got[1].Timestamp,
					before, after))
		}
		consumer.Close()
	}

	section("offsets")
	{
		consumer := must(bp.NewConsumer(address, bp.DefaultConsumerConfig()))
		earliest := must(consumer.ListOffsets(topic, 0, bp.Earliest))
		latest := must(consumer.ListOffsets(topic, 0, bp.Latest))
		check("earliest is 0 on a fresh topic", earliest == 0, fmt.Sprint(earliest))
		check("latest equals the record count", latest == 50, fmt.Sprint(latest))
		consumer.Close()
	}

	section("acks")
	for _, acks := range []int32{0, 1, -1} {
		acksTopic := unique(fmt.Sprintf("go-acks%d", acks))
		config := bp.DefaultProducerConfig()
		config.LingerMs = 0
		config.Acks = acks
		producer := must(bp.NewProducer(address, config))
		must(0, producer.SendTo(acksTopic, 0, []byte("durable"), nil))
		must(0, producer.Flush())
		must(0, producer.Close())
		time.Sleep(400 * time.Millisecond)

		consumer := must(bp.NewConsumer(address, bp.DefaultConsumerConfig()))
		got := must(consumer.Fetch(acksTopic, 0, 0, 500))
		check(fmt.Sprintf("acks=%d stores the record", acks), len(got) == 1,
			fmt.Sprintf("got %d", len(got)))
		consumer.Close()
	}

	section("consumer group: assignment, commit, resume")
	{
		groupTopic := unique("go-group")
		groupID := unique("go-billing")
		config := bp.DefaultProducerConfig()
		config.LingerMs = 0
		producer := must(bp.NewProducer(address, config))
		for i := 0; i < 40; i++ {
			must(0, producer.Send(groupTopic, []byte(fmt.Sprintf("g%d", i)), nil))
		}
		must(0, producer.Flush())
		must(0, producer.Close())

		groupConfig := bp.DefaultGroupConfig()
		groupConfig.AutoCommitIntervalMs = 0
		consumer := must(bp.NewGroupConsumer(address, groupID, groupConfig))
		consumer.Subscribe([]string{groupTopic})

		var seen []bp.ConsumedRecord
		deadline := time.Now().Add(30 * time.Second)
		for len(seen) < 40 && time.Now().Before(deadline) {
			records, err := consumer.Poll(500 * time.Millisecond)
			if err != nil {
				fmt.Printf("  FATAL poll: %v\n", err)
				os.Exit(2)
			}
			seen = append(seen, records...)
		}
		check("the group consumes every record", len(seen) == 40,
			fmt.Sprintf("got %d", len(seen)))

		unique := map[string]bool{}
		for _, record := range seen {
			unique[fmt.Sprintf("%d-%d", record.Partition, record.Offset)] = true
		}
		check("no record is delivered twice", len(unique) == len(seen), "")

		must(0, consumer.Commit())
		committed := must(consumer.Committed(nil))
		total := int64(0)
		for _, offset := range committed {
			total += offset
		}
		check("commit records a position", total == 40, fmt.Sprint(total))
		must(0, consumer.Close())

		// A second consumer in the same group must resume, not replay.
		rejoined := must(bp.NewGroupConsumer(address, groupID, groupConfig))
		rejoined.Subscribe([]string{groupTopic})
		var replayed []bp.ConsumedRecord
		until := time.Now().Add(5 * time.Second)
		for time.Now().Before(until) {
			records, _ := rejoined.Poll(300 * time.Millisecond)
			replayed = append(replayed, records...)
		}
		check("a rejoining group resumes from its commit", len(replayed) == 0,
			fmt.Sprintf("replayed %d records it had already committed", len(replayed)))
		must(0, rejoined.Close())
	}

	section("auto.offset.reset")
	{
		resetTopic := unique("go-reset")
		config := bp.DefaultProducerConfig()
		config.LingerMs = 0
		producer := must(bp.NewProducer(address, config))
		for i := 0; i < 10; i++ {
			must(0, producer.Send(resetTopic, []byte(fmt.Sprintf("r%d", i)), nil))
		}
		must(0, producer.Flush())
		must(0, producer.Close())

		latestConfig := bp.DefaultGroupConfig()
		latestConfig.AutoCommitIntervalMs = 0
		latestConfig.AutoOffsetReset = bp.AutoOffsetResetLatest
		consumer := must(bp.NewGroupConsumer(address, unique("go-latest"), latestConfig))
		consumer.Subscribe([]string{resetTopic})
		var skipped []bp.ConsumedRecord
		until := time.Now().Add(4 * time.Second)
		for time.Now().Before(until) {
			records, _ := consumer.Poll(300 * time.Millisecond)
			skipped = append(skipped, records...)
		}
		check("latest skips records produced before the group existed",
			len(skipped) == 0, fmt.Sprintf("saw %d", len(skipped)))
		must(0, consumer.Close())

		noneConfig := bp.DefaultGroupConfig()
		noneConfig.AutoCommitIntervalMs = 0
		noneConfig.AutoOffsetReset = bp.AutoOffsetResetNone
		strict := must(bp.NewGroupConsumer(address, unique("go-none"), noneConfig))
		strict.Subscribe([]string{resetTopic})
		raised := false
		until = time.Now().Add(5 * time.Second)
		for time.Now().Before(until) && !raised {
			if _, err := strict.Poll(300 * time.Millisecond); err != nil {
				raised = err == bp.ErrNoOffsetForPartition ||
					bytes.Contains([]byte(err.Error()), []byte("no committed offset"))
			}
		}
		check("none refuses to guess a position", raised, "")
		must(0, strict.Close())
	}

	section("assignors")
	for _, assignor := range []string{bp.AssignorRange, bp.AssignorRoundRobin, bp.AssignorSticky} {
		assignorTopic := unique("go-" + assignor)
		config := bp.DefaultProducerConfig()
		config.LingerMs = 0
		producer := must(bp.NewProducer(address, config))
		for i := 0; i < 20; i++ {
			must(0, producer.Send(assignorTopic, []byte(fmt.Sprintf("a%d", i)), nil))
		}
		must(0, producer.Flush())
		must(0, producer.Close())

		groupConfig := bp.DefaultGroupConfig()
		groupConfig.AutoCommitIntervalMs = 0
		groupConfig.Assignor = assignor
		consumer := must(bp.NewGroupConsumer(address, unique("go-grp-"+assignor), groupConfig))
		consumer.Subscribe([]string{assignorTopic})
		var collected []bp.ConsumedRecord
		deadline := time.Now().Add(20 * time.Second)
		for len(collected) < 20 && time.Now().Before(deadline) {
			records, _ := consumer.Poll(500 * time.Millisecond)
			collected = append(collected, records...)
		}
		check(assignor+": consumes every record", len(collected) == 20,
			fmt.Sprintf("got %d", len(collected)))
		must(0, consumer.Close())
	}

	section("bounded client buffer")
	{
		bufferTopic := unique("go-buffer")
		config := bp.DefaultProducerConfig()
		config.LingerMs = 10_000 // never flush on time during this check
		config.BufferMemory = 2048
		config.MaxBlockMs = 300
		producer := must(bp.NewProducer(address, config))
		blocked := false
		for i := 0; i < 500 && !blocked; i++ {
			if err := producer.SendTo(bufferTopic, 0, bytes.Repeat([]byte("x"), 256), nil); err != nil {
				blocked = bytes.Contains([]byte(err.Error()), []byte("buffer full"))
			}
		}
		check("a full buffer blocks and then reports", blocked, "")
	}

	fmt.Printf("\n%d passed, %d failed\n", passed, failed)
	if failed > 0 {
		os.Exit(1)
	}
}
