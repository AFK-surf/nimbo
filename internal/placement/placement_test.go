package placement

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
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

// bruteNearest is the reference: per-Broker minimum XOR distance over all
// virtual IDs, sorted.
func bruteNearest(bs []ids.BrokerID, n int, key ids.Hash, k int, keep func(ids.BrokerID) bool) []ids.BrokerID {
	type c struct {
		d [32]byte
		b ids.BrokerID
	}
	var cs []c
	for _, b := range bs {
		if keep != nil && !keep(b) {
			continue
		}
		var best [32]byte
		for i := range best {
			best[i] = 0xff
		}
		for i := 0; i < n; i++ {
			v := VirtualID(b, i)
			var d [32]byte
			for j := range d {
				d[j] = v[j] ^ key[j]
			}
			if bytes.Compare(d[:], best[:]) < 0 {
				best = d
			}
		}
		cs = append(cs, c{best, b})
	}
	sort.Slice(cs, func(i, j int) bool { return bytes.Compare(cs[i].d[:], cs[j].d[:]) < 0 })
	var out []ids.BrokerID
	for i := 0; i < len(cs) && i < k; i++ {
		out = append(out, cs[i].b)
	}
	return out
}

func TestNearestMatchesBruteForce(t *testing.T) {
	for _, nb := range []int{1, 2, 3, 7, 50} {
		bs := brokers(nb)
		ix := New(bs, 16)
		for i := 0; i < 300; i++ {
			var buf [8]byte
			binary.BigEndian.PutUint64(buf[:], uint64(i*7919+nb))
			key := ids.Sum(buf[:])
			keep := func(b ids.BrokerID) bool { return b[0]%3 != 0 }
			for _, k := range []int{1, 3, 4, nb + 2} {
				for _, kp := range []func(ids.BrokerID) bool{nil, keep} {
					got := ix.Nearest(key, k, kp)
					want := bruteNearest(bs, 16, key, k, kp)
					if fmt.Sprint(got) != fmt.Sprint(want) {
						t.Fatalf("n=%d k=%d: got %v want %v", nb, k, got, want)
					}
				}
			}
		}
	}
}

func TestNearestOwner(t *testing.T) {
	for _, nb := range []int{1, 2, 5, 100} {
		ix := New(brokers(nb), 16)
		for i := 0; i < 2000; i++ {
			var buf [8]byte
			binary.BigEndian.PutUint64(buf[:], uint64(i))
			key := ids.Sum(buf[:])
			got, _ := ix.NearestOwner(key)
			fast, _ := ix.NearestOwnerFast(key)
			if want := ix.Nearest(key, 1, nil)[0]; got != want || fast != want {
				t.Fatalf("n=%d: %v / %v vs %v", nb, got, fast, want)
			}
		}
	}
}

func BenchmarkNearestOwner100(b *testing.B) {
	ix := New(brokersN(100), 16)
	var buf [8]byte
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		ix.NearestOwner(ids.Sum(buf[:]))
	}
}

func BenchmarkNearestOwnerFast100(b *testing.B) {
	ix := New(brokersN(100), 16)
	var buf [8]byte
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		ix.NearestOwnerFast(ids.Sum(buf[:]))
	}
}

func BenchmarkSHA256Only(b *testing.B) {
	var buf [8]byte
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		ids.Sum(buf[:])
	}
}
