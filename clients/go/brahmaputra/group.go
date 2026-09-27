package brahmaputra

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// OffsetsTopic is the internal topic whose partition leaders coordinate
// consumer groups.
const OffsetsTopic = "__consumer_offsets"

const (
	// coordinatorAttempts bounds retries after a coordinator move or load.
	coordinatorAttempts = 4
	// joinAttempts bounds join+sync rounds for a group that will not settle.
	joinAttempts = 4
)

// Where to start when a partition has no valid position — either the group
// never committed one, or the committed one has fallen off the front of
// the log because retention deleted it. Both are the same situation to a
// consumer, so they take one policy.
const (
	// AutoOffsetResetEarliest starts from the oldest record still retained.
	// Reprocesses history; never silently skips.
	AutoOffsetResetEarliest = "earliest"
	// AutoOffsetResetLatest starts from the end. Skips whatever was missed;
	// never reprocesses.
	AutoOffsetResetLatest = "latest"
	// AutoOffsetResetNone refuses to guess and returns
	// ErrNoOffsetForPartition — the honest choice when neither reprocessing
	// nor skipping is safe.
	AutoOffsetResetNone = "none"
)

// Partition assignment strategies.
const (
	AssignorRange      = "range"
	AssignorRoundRobin = "roundrobin"
	// AssignorSticky keeps members on the partitions they already hold.
	// Prefer it when consumers carry per-partition state, because every
	// partition that moves throws that state away.
	AssignorSticky = "sticky"
)

// GroupConfig is named as Kafka names its consumer-group settings.
type GroupConfig struct {
	ClientID string
	// SessionTimeoutMs: the coordinator evicts a member that stops
	// heartbeating for this long. Kafka defaults to 45s; this defaults to
	// 10s as the Rust client does.
	SessionTimeoutMs int32
	// RebalanceTimeoutMs is how long the coordinator waits for rejoins.
	RebalanceTimeoutMs int32
	// MaxPollIntervalMs is the longest gap between Poll calls before this
	// member is presumed stuck and leaves. Separate from the session
	// timeout on purpose: heartbeats prove the process is alive, this
	// proves the application is still consuming.
	MaxPollIntervalMs int
	// AutoCommitIntervalMs; 0 disables auto-commit.
	AutoCommitIntervalMs int
	AutoOffsetReset      string
	Assignor             string
	// GroupInstanceID gives this consumer a stable identity across
	// restarts (KIP-345), so a rolling restart does not rebalance twice
	// per instance. Empty means a dynamic member.
	GroupInstanceID string
	MaxPollRecords  int
	FetchMaxBytes   int32
	DialTimeout     time.Duration
}

func DefaultGroupConfig() GroupConfig {
	return GroupConfig{
		ClientID:             "brahmaputra-go",
		SessionTimeoutMs:     10_000,
		RebalanceTimeoutMs:   3_000,
		MaxPollIntervalMs:    300_000,
		AutoCommitIntervalMs: 5_000,
		AutoOffsetReset:      AutoOffsetResetEarliest,
		Assignor:             AssignorRange,
		MaxPollRecords:       500,
		FetchMaxBytes:        8 * 1024 * 1024,
		DialTimeout:          30 * time.Second,
	}
}

// GroupConsumer shares a topic's partitions with the rest of its group.
//
// Single-threaded by design, matching Kafka's consumer: use one per
// goroutine and give each its own client id.
type GroupConsumer struct {
	groupID  string
	config   GroupConfig
	consumer *Consumer

	subscribed []string
	// memberID, generation and joined are shared with the heartbeat
	// goroutine, which reads them every tick and clears joined when the
	// coordinator asks for a rejoin; every access goes through mu.
	memberID   string
	generation int32
	joined     bool
	assignment []topicPartition

	// positions is the next offset to *deliver* — what gets committed.
	// It only advances over records handed to the caller.
	positions map[topicPartition]int64
	// fetchPositions is the next offset to *fetch*. It runs ahead of
	// positions by exactly the records sitting in buffered.
	fetchPositions map[topicPartition]int64
	buffered       []ConsumedRecord

	mu         sync.Mutex
	lastPollMs int64
	// inPoll is true while Poll runs. MaxPollIntervalMs bounds the gap
	// *between* polls — time the application spends processing — so a poll
	// that is itself busy joining a slow rebalance must not count against it.
	inPoll       bool
	lastCommitMs int64
	closed       bool
	done         chan struct{}
}

// NewGroupConsumer connects and starts heartbeating.
func NewGroupConsumer(address, groupID string, config GroupConfig) (*GroupConsumer, error) {
	consumerConfig := DefaultConsumerConfig()
	consumerConfig.ClientID = config.ClientID
	consumerConfig.FetchMaxBytes = config.FetchMaxBytes
	consumerConfig.MaxPollRecords = config.MaxPollRecords
	consumerConfig.DialTimeout = config.DialTimeout

	consumer, err := NewConsumer(address, consumerConfig)
	if err != nil {
		return nil, err
	}
	group := &GroupConsumer{
		groupID:        groupID,
		config:         config,
		consumer:       consumer,
		generation:     -1,
		positions:      map[topicPartition]int64{},
		fetchPositions: map[topicPartition]int64{},
		lastPollMs:     nowMillis(),
		lastCommitMs:   nowMillis(),
		done:           make(chan struct{}),
	}
	go group.heartbeatLoop()
	return group, nil
}

// Subscribe sets the topics this member wants a share of.
func (g *GroupConsumer) Subscribe(topics []string) {
	g.subscribed = append([]string(nil), topics...)
	g.setJoined(false)
}

// membership snapshots the fields the heartbeat goroutine shares.
func (g *GroupConsumer) membership() (memberID string, generation int32, joined bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.memberID, g.generation, g.joined
}

func (g *GroupConsumer) setJoined(joined bool) {
	g.mu.Lock()
	g.joined = joined
	g.mu.Unlock()
}

// Close commits, leaves the group, then stops.
//
// Leaving is what separates a clean shutdown from a crash. Without it the
// coordinator cannot tell the difference and must wait out
// SessionTimeoutMs before reassigning, so a rolling restart of N instances
// costs N session timeouts of stalled partitions.
func (g *GroupConsumer) Close() error {
	g.mu.Lock()
	g.closed = true
	g.mu.Unlock()

	memberID, _, joined := g.membership()
	if joined {
		_ = g.Commit()
	}
	if memberID != "" {
		// Best effort: the caller is shutting down, and failing here costs
		// only the session timeout it was trying to avoid.
		_ = g.leave()
	}
	select {
	case <-g.done:
	case <-time.After(2 * time.Second):
	}
	return g.consumer.Close()
}

// Poll returns up to MaxPollRecords records, joining the group if needed.
func (g *GroupConsumer) Poll(timeout time.Duration) ([]ConsumedRecord, error) {
	if len(g.subscribed) == 0 {
		return nil, fmt.Errorf("subscribe to at least one topic before polling")
	}
	// Stamped on entry and again on return, and not enforced in between:
	// the interval bounds how long the *application* may go without asking
	// for records, and a poll that blocks — for its timeout, or on a slow
	// rebalance — is the consumer working normally.
	g.mu.Lock()
	g.lastPollMs = nowMillis()
	g.inPoll = true
	g.mu.Unlock()
	defer func() {
		g.mu.Lock()
		g.lastPollMs = nowMillis()
		g.inPoll = false
		g.mu.Unlock()
	}()

	deadline := time.Now().Add(timeout)
	for {
		// Checked every sweep, not only on entry: a rebalance the heartbeat
		// learns of mid-poll must stop this member fetching partitions it
		// may no longer own, rather than carrying on until the timeout.
		if _, _, joined := g.membership(); !joined {
			if err := g.join(); err != nil {
				return nil, err
			}
		}
		if len(g.buffered) > 0 {
			return g.takeBuffered(), nil
		}
		if len(g.assignment) == 0 {
			if time.Now().After(deadline) {
				return nil, nil
			}
			time.Sleep(50 * time.Millisecond)
			continue
		}

		gotAny := false
		for _, slot := range g.assignment {
			remaining := time.Until(deadline)
			if remaining < 0 {
				remaining = 0
			}
			waitMs := int32(remaining / time.Millisecond)
			if waitMs > 500 {
				waitMs = 500
			}
			offset := g.fetchPositions[slot]
			records, err := g.consumer.Fetch(slot.topic, slot.partition, offset, waitMs)
			if err != nil {
				var serverErr *ServerError
				if asServerError(err, &serverErr) {
					switch serverErr.Code {
					case ErrOffsetOutOfRange:
						// The committed offset fell off the log; restart
						// where the policy says.
						reset, resetErr := g.resetOffset(slot.topic, slot.partition)
						if resetErr != nil {
							return nil, resetErr
						}
						g.fetchPositions[slot] = reset
						g.positions[slot] = reset
						continue
					case ErrNotLeaderOrFollower:
						_, _ = g.consumer.Router().Refresh(slot.topic)
						continue
					}
				}
				return nil, err
			}
			if len(records) > 0 {
				gotAny = true
				g.fetchPositions[slot] = records[len(records)-1].Offset + 1
				g.buffered = append(g.buffered, records...)
			}
		}

		g.maybeAutoCommit()
		if len(g.buffered) > 0 {
			return g.takeBuffered(), nil
		}
		if !gotAny && time.Now().After(deadline) {
			return nil, nil
		}
	}
}

func (g *GroupConsumer) takeBuffered() []ConsumedRecord {
	limit := g.config.MaxPollRecords
	if limit <= 0 || limit > len(g.buffered) {
		limit = len(g.buffered)
	}
	delivered := g.buffered[:limit]
	g.buffered = g.buffered[limit:]
	for _, record := range delivered {
		// The consumed position advances only over records actually handed
		// to the caller; committing what was merely fetched would silently
		// skip records nobody processed.
		g.positions[topicPartition{record.Topic, record.Partition}] = record.Offset + 1
	}
	return delivered
}

// Commit records the delivered positions. At-least-once: call it after
// processing, not before.
func (g *GroupConsumer) Commit() error {
	if len(g.positions) == 0 {
		return nil
	}
	slots := make([]topicPartition, 0, len(g.positions))
	for slot := range g.positions {
		slots = append(slots, slot)
	}
	sort.Slice(slots, func(a, b int) bool {
		if slots[a].topic != slots[b].topic {
			return slots[a].topic < slots[b].topic
		}
		return slots[a].partition < slots[b].partition
	})

	memberID, generation, _ := g.membership()
	w := NewBodyWriter()
	w.String(g.groupID)
	w.Int32(generation)
	w.String(memberID)
	w.Int32(int32(len(slots)))
	for _, slot := range slots {
		w.String(slot.topic)
		w.Int32(slot.partition)
		w.Int64(g.positions[slot])
	}

	response, err := g.coordinatorRequest(APIOffsetCommit, w.Bytes())
	if err != nil {
		return err
	}
	r, err := NewBodyReader(response)
	if err != nil {
		return err
	}
	if code := r.Int32(); code != ErrNone {
		return serverError(code, "offset_commit")
	}
	g.lastCommitMs = nowMillis()
	return nil
}

// Committed reads the group's committed offsets. An empty slice asks for
// every partition the group holds.
func (g *GroupConsumer) Committed(
	partitions []topicPartition,
) (map[topicPartition]int64, error) {
	w := NewBodyWriter()
	w.String(g.groupID)
	w.Int32(int32(len(partitions)))
	for _, slot := range partitions {
		w.String(slot.topic)
		w.Int32(slot.partition)
	}
	response, err := g.coordinatorRequest(APIOffsetFetch, w.Bytes())
	if err != nil {
		return nil, err
	}
	r, err := NewBodyReader(response)
	if err != nil {
		return nil, err
	}
	if code := r.Int32(); code != ErrNone {
		return nil, serverError(code, "offset_fetch")
	}
	out := map[topicPartition]int64{}
	for count := int(r.Int32()); count > 0; count-- {
		topic := r.String()
		partition := r.Int32()
		out[topicPartition{topic, partition}] = r.Int64()
	}
	return out, r.Err()
}

func (g *GroupConsumer) maybeAutoCommit() {
	interval := g.config.AutoCommitIntervalMs
	if interval <= 0 || len(g.positions) == 0 {
		return
	}
	if nowMillis()-g.lastCommitMs < int64(interval) {
		return
	}
	// An auto-commit that fails is retried on the next poll; the explicit
	// Commit is what a caller relies on.
	_ = g.Commit()
}

func (g *GroupConsumer) resetOffset(topic string, partition int32) (int64, error) {
	switch g.config.AutoOffsetReset {
	case AutoOffsetResetEarliest:
		return g.consumer.ListOffsets(topic, partition, Earliest)
	case AutoOffsetResetLatest:
		return g.consumer.ListOffsets(topic, partition, Latest)
	case AutoOffsetResetNone:
		return 0, ErrNoOffsetForPartition
	}
	return 0, fmt.Errorf("unknown auto offset reset %q", g.config.AutoOffsetReset)
}

// ---------------------------------------------------------------------------
// Membership
// ---------------------------------------------------------------------------

func (g *GroupConsumer) join() error {
	for attempt := 0; attempt < joinAttempts; attempt++ {
		w := NewBodyWriter()
		w.String(g.groupID)
		w.Int32(g.config.SessionTimeoutMs)
		w.Int32(g.config.RebalanceTimeoutMs)
		currentMemberID, _, _ := g.membership()
		w.String(currentMemberID)
		w.StringArray(g.subscribed)
		w.String(g.config.GroupInstanceID)

		response, err := g.coordinatorRequest(APIJoinGroup, w.Bytes())
		if err != nil {
			return err
		}
		r, err := NewBodyReader(response)
		if err != nil {
			return err
		}
		code := r.Int32()
		if code == ErrRebalanceInProgress {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if code == ErrUnknownMemberID {
			// The coordinator dropped this member (session expiry, or
			// removed while it waited): join again as a new one.
			g.mu.Lock()
			g.memberID = ""
			g.mu.Unlock()
			continue
		}
		if code != ErrNone {
			return serverError(code, "join_group")
		}

		generation := r.Int32()
		memberID := r.String()
		leaderID := r.String()

		type memberInfo struct {
			id     string
			topics []string
			held   []topicPartition
		}
		var members []memberInfo
		for count := int(r.Int32()); count > 0; count-- {
			info := memberInfo{id: r.String(), topics: r.StringArray()}
			for held := int(r.Int32()); held > 0; held-- {
				info.held = append(info.held, topicPartition{r.String(), r.Int32()})
			}
			members = append(members, info)
		}
		if r.Err() != nil {
			return r.Err()
		}

		g.mu.Lock()
		g.memberID = memberID
		g.generation = generation
		g.mu.Unlock()

		var assignments []memberAssignment
		if memberID == leaderID {
			topicPartitions := map[string][]int32{}
			for _, member := range members {
				for _, topic := range member.topics {
					if _, ok := topicPartitions[topic]; ok {
						continue
					}
					partitions, err := g.consumer.Partitions(topic)
					if err != nil {
						return err
					}
					topicPartitions[topic] = partitions
				}
			}
			memberList := make([][2]any, 0, len(members))
			previous := map[string][]topicPartition{}
			for _, member := range members {
				memberList = append(memberList, [2]any{member.id, member.topics})
				previous[member.id] = member.held
			}
			computed, err := g.computeAssignment(memberList, topicPartitions, previous)
			if err != nil {
				return err
			}
			assignments = computed
		}

		ok, err := g.sync(assignments)
		if err != nil {
			return err
		}
		if ok {
			g.setJoined(true)
			return nil
		}
	}
	return fmt.Errorf("consumer group failed to stabilise after %d join attempts", joinAttempts)
}

type memberAssignment struct {
	memberID   string
	partitions []topicPartition
}

func (g *GroupConsumer) sync(assignments []memberAssignment) (bool, error) {
	memberID, generation, _ := g.membership()
	w := NewBodyWriter()
	w.String(g.groupID)
	w.Int32(generation)
	w.String(memberID)
	w.Int32(int32(len(assignments)))
	for _, assignment := range assignments {
		w.String(assignment.memberID)
		w.Int32(int32(len(assignment.partitions)))
		for _, slot := range assignment.partitions {
			w.String(slot.topic)
			w.Int32(slot.partition)
		}
	}

	response, err := g.coordinatorRequest(APISyncGroup, w.Bytes())
	if err != nil {
		return false, err
	}
	r, err := NewBodyReader(response)
	if err != nil {
		return false, err
	}
	code := r.Int32()
	if code == ErrRebalanceInProgress || code == ErrIllegalGeneration {
		return false, nil
	}
	if code == ErrUnknownMemberID {
		g.mu.Lock()
		g.memberID = ""
		g.mu.Unlock()
		return false, nil
	}
	if code != ErrNone {
		return false, serverError(code, "sync_group")
	}
	var assignment []topicPartition
	for count := int(r.Int32()); count > 0; count-- {
		assignment = append(assignment, topicPartition{r.String(), r.Int32()})
	}
	if r.Err() != nil {
		return false, r.Err()
	}
	return true, g.applyAssignment(assignment)
}

func (g *GroupConsumer) applyAssignment(assignment []topicPartition) error {
	g.assignment = assignment
	owned := map[topicPartition]bool{}
	for _, slot := range assignment {
		owned[slot] = true
	}
	for slot := range g.positions {
		if !owned[slot] {
			delete(g.positions, slot)
		}
	}
	// Buffered records sit ahead of the consumed position and were never
	// delivered, so a new assignment simply drops them.
	g.buffered = nil

	var needed []topicPartition
	for _, slot := range assignment {
		if _, ok := g.positions[slot]; !ok {
			needed = append(needed, slot)
		}
	}
	if len(needed) > 0 {
		committed, err := g.Committed(needed)
		if err != nil {
			return err
		}
		for _, slot := range needed {
			offset, ok := committed[slot]
			if !ok || offset < 0 {
				if offset, err = g.resetOffset(slot.topic, slot.partition); err != nil {
					return err
				}
			}
			g.positions[slot] = offset
		}
	}
	g.fetchPositions = map[topicPartition]int64{}
	for slot, offset := range g.positions {
		g.fetchPositions[slot] = offset
	}
	return nil
}

func (g *GroupConsumer) leave() error {
	memberID, _, _ := g.membership()
	w := NewBodyWriter()
	w.String(g.groupID)
	w.String(memberID)
	response, err := g.coordinatorRequest(APILeaveGroup, w.Bytes())
	if err != nil {
		return err
	}
	r, err := NewBodyReader(response)
	if err != nil {
		return err
	}
	if code := r.Int32(); code != ErrNone {
		return serverError(code, "leave_group")
	}
	g.setJoined(false)
	return nil
}

func (g *GroupConsumer) heartbeatLoop() {
	defer close(g.done)
	// This loop enforces two independent deadlines, so it has to wake often
	// enough for the shorter of them. Deriving the tick from the session
	// timeout alone would leave a long session with a short poll interval
	// unchecked until long after it stalled.
	heartbeatEvery := int(g.config.SessionTimeoutMs) / 3
	if heartbeatEvery < 1 {
		heartbeatEvery = 1
	}
	pollCheckEvery := g.config.MaxPollIntervalMs / 3
	if pollCheckEvery < 1 {
		pollCheckEvery = 1
	}
	interval := heartbeatEvery
	if pollCheckEvery < interval {
		interval = pollCheckEvery
	}

	ticker := time.NewTicker(time.Duration(interval) * time.Millisecond)
	defer ticker.Stop()
	leftForSlowPoll := false

	for range ticker.C {
		g.mu.Lock()
		closed := g.closed
		idleMs := nowMillis() - g.lastPollMs
		inPoll := g.inPoll
		g.mu.Unlock()
		if closed {
			return
		}
		memberID, generation, joined := g.membership()
		if !joined || memberID == "" {
			continue
		}

		if !inPoll && idleMs >= int64(g.config.MaxPollIntervalMs) {
			// The application has stopped consuming even though the process
			// is alive. Continuing to heartbeat would assert a liveness this
			// member no longer has, holding its partitions away from a
			// consumer that could make progress.
			if !leftForSlowPoll {
				_ = g.leave()
				leftForSlowPoll = true
				g.setJoined(false)
			}
			continue
		}
		leftForSlowPoll = false

		w := NewBodyWriter()
		w.String(g.groupID)
		w.Int32(generation)
		w.String(memberID)
		response, err := g.coordinatorRequest(APIHeartbeat, w.Bytes())
		if err != nil {
			continue // transient: retry next tick
		}
		r, err := NewBodyReader(response)
		if err != nil {
			continue
		}
		switch r.Int32() {
		case ErrRebalanceInProgress, ErrUnknownMemberID, ErrIllegalGeneration:
			// Only if nothing has changed since the snapshot: a heartbeat
			// for an old generation answering after the member already
			// rejoined must not send it round again.
			g.mu.Lock()
			if g.generation == generation && g.memberID == memberID {
				g.joined = false
			}
			g.mu.Unlock()
		}
	}
}

// ---------------------------------------------------------------------------
// Coordinator routing
// ---------------------------------------------------------------------------

func (g *GroupConsumer) coordinatorPartition() (int32, error) {
	partitions, err := g.consumer.Partitions(OffsetsTopic)
	if err != nil {
		return 0, err
	}
	return int32(CRC32C([]byte(g.groupID)) % uint32(len(partitions))), nil
}

// coordinatorRequest sends to the group's coordinator, following moves and
// waiting out loads.
func (g *GroupConsumer) coordinatorRequest(apiKey int16, body []byte) ([]byte, error) {
	for attempt := 0; attempt < coordinatorAttempts; attempt++ {
		partition, err := g.coordinatorPartition()
		if err != nil {
			return nil, err
		}
		conn, err := g.consumer.Router().ConnFor(OffsetsTopic, partition)
		if err != nil {
			return nil, err
		}
		response, err := conn.Request(apiKey, body)
		if err != nil {
			return nil, err
		}
		switch peekErrorCode(response) {
		case ErrCoordinatorLoadInProgres:
			time.Sleep(100 * time.Millisecond)
			continue
		case ErrNotCoordinator, ErrNotLeaderOrFollower:
			_, _ = g.consumer.Router().Refresh(OffsetsTopic)
			continue
		}
		return response, nil
	}
	return nil, fmt.Errorf(
		"group coordinator unavailable after %d attempts", coordinatorAttempts)
}

// peekErrorCode reads a response's leading error code without consuming
// the body. Every group response starts with one, which is what makes a
// generic coordinator-retry wrapper possible at all.
func peekErrorCode(body []byte) int32 {
	r, err := NewBodyReader(body)
	if err != nil {
		return ErrNone
	}
	return r.Int32()
}

func asServerError(err error, target **ServerError) bool {
	if converted, ok := err.(*ServerError); ok {
		*target = converted
		return true
	}
	return false
}

// ---------------------------------------------------------------------------
// Assignors
// ---------------------------------------------------------------------------

func (g *GroupConsumer) computeAssignment(
	members [][2]any,
	topicPartitions map[string][]int32,
	previous map[string][]topicPartition,
) ([]memberAssignment, error) {
	list := make([]assignorMember, 0, len(members))
	for _, entry := range members {
		list = append(list, assignorMember{
			id:     entry[0].(string),
			topics: entry[1].([]string),
		})
	}

	var assignment map[string][]topicPartition
	switch g.config.Assignor {
	case AssignorRange:
		assignment = rangeAssign(list, topicPartitions)
	case AssignorRoundRobin:
		assignment = roundRobinAssign(list, topicPartitions)
	case AssignorSticky:
		assignment = stickyAssign(list, topicPartitions, previous)
	default:
		return nil, fmt.Errorf("unknown assignor %q", g.config.Assignor)
	}

	out := make([]memberAssignment, 0, len(assignment))
	for memberID, partitions := range assignment {
		out = append(out, memberAssignment{memberID, partitions})
	}
	sort.Slice(out, func(a, b int) bool { return out[a].memberID < out[b].memberID })
	return out, nil
}

type assignorMember struct {
	id     string
	topics []string
}

func (m assignorMember) subscribes(topic string) bool {
	for _, candidate := range m.topics {
		if candidate == topic {
			return true
		}
	}
	return false
}

func emptyAssignment(members []assignorMember) map[string][]topicPartition {
	out := map[string][]topicPartition{}
	for _, member := range members {
		out[member.id] = nil
	}
	return out
}

func sortedTopics(topicPartitions map[string][]int32) []string {
	topics := make([]string, 0, len(topicPartitions))
	for topic := range topicPartitions {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	return topics
}

// rangeAssign gives each subscribed member a contiguous range per topic;
// the first (partitions % members) members take one extra.
func rangeAssign(
	members []assignorMember, topicPartitions map[string][]int32,
) map[string][]topicPartition {
	assignment := emptyAssignment(members)
	for _, topic := range sortedTopics(topicPartitions) {
		partitions := topicPartitions[topic]
		var subscribers []string
		for _, member := range members {
			if member.subscribes(topic) {
				subscribers = append(subscribers, member.id)
			}
		}
		sort.Strings(subscribers)
		if len(subscribers) == 0 {
			continue
		}
		base := len(partitions) / len(subscribers)
		extra := len(partitions) % len(subscribers)
		cursor := 0
		for index, memberID := range subscribers {
			count := base
			if index < extra {
				count++
			}
			for _, partition := range partitions[cursor : cursor+count] {
				assignment[memberID] = append(assignment[memberID],
					topicPartition{topic, partition})
			}
			cursor += count
		}
	}
	return assignment
}

// roundRobinAssign deals every partition around the circle of members
// sorted by id, skipping members not subscribed to a partition's topic.
func roundRobinAssign(
	members []assignorMember, topicPartitions map[string][]int32,
) map[string][]topicPartition {
	assignment := emptyAssignment(members)
	circle := append([]assignorMember(nil), members...)
	sort.Slice(circle, func(a, b int) bool { return circle[a].id < circle[b].id })
	if len(circle) == 0 {
		return assignment
	}
	cursor := 0
	for _, topic := range sortedTopics(topicPartitions) {
		for _, partition := range topicPartitions[topic] {
			start := cursor
			for {
				member := circle[cursor%len(circle)]
				cursor++
				if member.subscribes(topic) {
					assignment[member.id] = append(assignment[member.id],
						topicPartition{topic, partition})
					break
				}
				if cursor-start >= len(circle) {
					break // nobody subscribes to this topic
				}
			}
		}
	}
	return assignment
}

// stickyAssign keeps members on what they hold and moves only what balance
// requires.
//
// Mirrors the Rust implementation exactly, because members computing the
// assignment independently must agree — a leader running a different
// algorithm from its predecessor would reshuffle the whole group.
func stickyAssign(
	members []assignorMember,
	topicPartitions map[string][]int32,
	previous map[string][]topicPartition,
) map[string][]topicPartition {
	assignment := emptyAssignment(members)
	if len(members) == 0 {
		return assignment
	}

	subscribes := func(memberID, topic string) bool {
		for _, member := range members {
			if member.id == memberID {
				return member.subscribes(topic)
			}
		}
		return false
	}

	previousIDs := make([]string, 0, len(previous))
	for memberID := range previous {
		previousIDs = append(previousIDs, memberID)
	}
	sort.Strings(previousIDs)

	var unassigned []topicPartition
	claimed := map[topicPartition]string{}
	for _, topic := range sortedTopics(topicPartitions) {
		for _, partition := range topicPartitions[topic] {
			slot := topicPartition{topic, partition}
			holder := ""
			for _, memberID := range previousIDs {
				for _, held := range previous[memberID] {
					if held == slot && subscribes(memberID, topic) {
						holder = memberID
						break
					}
				}
				if holder != "" {
					break
				}
			}
			if holder == "" {
				unassigned = append(unassigned, slot)
			} else {
				claimed[slot] = holder
			}
		}
	}

	var eligible []string
	for _, member := range members {
		for _, topic := range member.topics {
			if _, ok := topicPartitions[topic]; ok {
				eligible = append(eligible, member.id)
				break
			}
		}
	}
	sort.Strings(eligible)
	if len(eligible) == 0 {
		return assignment
	}

	total := 0
	for _, partitions := range topicPartitions {
		total += len(partitions)
	}
	base := total / len(eligible)
	extra := total % len(eligible)
	quota := map[string]int{}
	for index, memberID := range eligible {
		quota[memberID] = base
		if index < extra {
			quota[memberID]++
		}
	}

	claimedSlots := make([]topicPartition, 0, len(claimed))
	for slot := range claimed {
		claimedSlots = append(claimedSlots, slot)
	}
	sortSlots(claimedSlots)

	kept := map[string][]topicPartition{}
	for _, slot := range claimedSlots {
		memberID := claimed[slot]
		if len(kept[memberID]) < quota[memberID] {
			kept[memberID] = append(kept[memberID], slot)
		} else {
			unassigned = append(unassigned, slot)
		}
	}
	for memberID, held := range kept {
		if _, ok := assignment[memberID]; ok {
			assignment[memberID] = held
		}
	}

	sortSlots(unassigned)
	for _, slot := range unassigned {
		taker := ""
		for _, memberID := range eligible {
			if subscribes(memberID, slot.topic) && len(assignment[memberID]) < quota[memberID] {
				taker = memberID
				break
			}
		}
		if taker == "" {
			// Quotas exhausted (possible with uneven subscriptions): an
			// unassigned partition is a stalled partition, so fall back to
			// any subscribed member rather than dropping it.
			for _, memberID := range eligible {
				if subscribes(memberID, slot.topic) {
					taker = memberID
					break
				}
			}
		}
		if taker != "" {
			assignment[taker] = append(assignment[taker], slot)
		}
	}

	for memberID := range assignment {
		sortSlots(assignment[memberID])
	}
	return assignment
}

func sortSlots(slots []topicPartition) {
	sort.Slice(slots, func(a, b int) bool {
		if slots[a].topic != slots[b].topic {
			return slots[a].topic < slots[b].topic
		}
		return slots[a].partition < slots[b].partition
	})
}
