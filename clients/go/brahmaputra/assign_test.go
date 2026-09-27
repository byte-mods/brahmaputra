package brahmaputra

import (
	"reflect"
	"testing"
)

// Partitions compare as integers, not strings: 2 sorts before 10, and the
// member over quota keeps its lowest-numbered partitions.
func TestStickyComparesPartitionsAsIntegers(t *testing.T) {
	members := []assignorMember{{"a", []string{"t"}}, {"b", []string{"t"}}}
	partitions := map[string][]int32{"t": {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}}
	previous := map[string][]topicPartition{
		"a": {{"t", 10}, {"t", 2}, {"t", 11}, {"t", 3}, {"t", 0}, {"t", 1}, {"t", 9}},
	}
	got := stickyAssign(members, partitions, previous)
	want := []topicPartition{{"t", 0}, {"t", 1}, {"t", 2}, {"t", 3}, {"t", 9}, {"t", 10}}
	if !reflect.DeepEqual(got["a"], want) {
		t.Fatalf("a kept %v, want %v", got["a"], want)
	}
	if len(got["b"]) != 6 {
		t.Fatalf("b got %v", got["b"])
	}
}

func TestRangeAndRoundRobinCoverEveryPartition(t *testing.T) {
	members := []assignorMember{{"a", []string{"t"}}, {"b", []string{"t"}}, {"c", []string{"t"}}}
	partitions := map[string][]int32{"t": {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}}
	for name, assign := range map[string]func([]assignorMember, map[string][]int32) map[string][]topicPartition{
		"range": rangeAssign, "roundrobin": roundRobinAssign,
	} {
		seen := map[int32]bool{}
		for _, slots := range assign(members, partitions) {
			for _, slot := range slots {
				if seen[slot.partition] {
					t.Fatalf("%s assigned %d twice", name, slot.partition)
				}
				seen[slot.partition] = true
			}
		}
		if len(seen) != 11 {
			t.Fatalf("%s covered %d of 11", name, len(seen))
		}
	}
}

func TestDecodingRejectsBadLengths(t *testing.T) {
	r := &Reader{data: []byte{0x01}} // zigzag -1: a negative string length
	if _ = r.String(); r.Err() == nil {
		t.Fatal("negative string length accepted")
	}
	payload := appendUvarint(nil, 1<<40) // record claims a terabyte
	if _, err := decodeRecords(payload, false, false); err == nil {
		t.Fatal("oversized record length accepted")
	}
}
