package placement

import (
	"crypto/sha256"
	"encoding/binary"
	"testing"

	"github.com/afk-surf/nimbo/internal/ids"
)

func brokers(n int) []ids.BrokerID {
	out := make([]ids.BrokerID, n)
	for i := range out {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(i))
		out[i] = ids.BrokerID(sha256.Sum256(b[:]))
	}
	return out
}

func TestNearestDistinctAndStable(t *testing.T) {
	ix := New(brokers(20), 16)
	for i := 0; i < 1000; i++ {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(i))
		key := ids.Sum(b[:])
		got := ix.Nearest(key, 3, nil)
		if len(got) != 3 || got[0] == got[1] || got[1] == got[2] || got[0] == got[2] {
			t.Fatalf("bad set %v", got)
		}
		// Removing a non-member must not change the set; removing a member
		// must keep the other two.
		var rest []ids.BrokerID
		for _, b := range ix.Brokers() {
			if b != got[0] {
				rest = append(rest, b)
			}
		}
		got2 := New(rest, 16).Nearest(key, 3, nil)
		if got2[0] != got[1] || got2[1] != got[2] {
			t.Fatalf("placement not monotone: %v vs %v", got, got2)
		}
	}
}

func TestBalance(t *testing.T) {
	bs := brokers(100)
	ix := New(bs, 16)
	load := map[ids.BrokerID]int{}
	const n = 30000
	for i := 0; i < n; i++ {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(i))
		for _, x := range ix.Nearest(ids.Sum(b[:]), 3, nil) {
			load[x]++
		}
	}
	max := 0
	for _, v := range load {
		if v > max {
			max = v
		}
	}
	mean := float64(3*n) / 100
	if r := float64(max) / mean; r > 1.6 {
		t.Fatalf("max/mean = %.2f", r)
	}
}
