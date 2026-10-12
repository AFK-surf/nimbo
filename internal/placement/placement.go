// Package placement implements XOR placement over virtual IDs (design §7.1).
package placement

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"sort"

	"github.com/afk-surf/nimbo/internal/ids"
)

// VirtualID returns SHA-256(broker_id ‖ i), with i as one byte.
func VirtualID(b ids.BrokerID, i int) [32]byte {
	var buf [33]byte
	copy(buf[:], b[:])
	buf[32] = byte(i)
	return sha256.Sum256(buf[:])
}

type vid struct {
	hi    uint64 // first 8 bytes, big endian, for fast comparison
	id    [32]byte
	owner int // index into Index.brokers
}

// Index is an immutable placement index over a set of Brokers.
type Index struct {
	brokers []ids.BrokerID // sorted
	vids    []vid
}

// New builds an index for the given Brokers with n virtual IDs each.
func New(brokers []ids.BrokerID, n int) *Index {
	bs := append([]ids.BrokerID(nil), brokers...)
	sort.Slice(bs, func(i, j int) bool { return bytes.Compare(bs[i][:], bs[j][:]) < 0 })
	ix := &Index{brokers: bs, vids: make([]vid, 0, len(bs)*n)}
	for bi, b := range bs {
		for i := 0; i < n; i++ {
			v := VirtualID(b, i)
			ix.vids = append(ix.vids, vid{hi: binary.BigEndian.Uint64(v[:8]), id: v, owner: bi})
		}
	}
	return ix
}

// Brokers returns the indexed Brokers in sorted order.
func (ix *Index) Brokers() []ids.BrokerID { return ix.brokers }

// Len is the number of Brokers.
func (ix *Index) Len() int { return len(ix.brokers) }

type cand struct {
	hi   uint64
	dist [32]byte
	b    int
}

func less(a, b *cand) bool {
	if a.hi != b.hi {
		return a.hi < b.hi
	}
	if c := bytes.Compare(a.dist[:], b.dist[:]); c != 0 {
		return c < 0
	}
	return a.b < b.b
}

// Nearest returns up to k distinct Brokers ordered by their minimum XOR
// distance to key, skipping Brokers for which keep returns false.
func (ix *Index) Nearest(key ids.Hash, k int, keep func(ids.BrokerID) bool) []ids.BrokerID {
	if k <= 0 || len(ix.brokers) == 0 {
		return nil
	}
	khi := binary.BigEndian.Uint64(key[:8])
	best := make([]cand, len(ix.brokers))
	seen := make([]bool, len(ix.brokers))
	for i := range ix.vids {
		v := &ix.vids[i]
		c := cand{hi: v.hi ^ khi, b: v.owner}
		if seen[v.owner] && c.hi > best[v.owner].hi {
			continue
		}
		for j := 0; j < 32; j++ {
			c.dist[j] = v.id[j] ^ key[j]
		}
		if !seen[v.owner] || less(&c, &best[v.owner]) {
			best[v.owner] = c
			seen[v.owner] = true
		}
	}
	cs := make([]cand, 0, len(best))
	for i := range best {
		if keep != nil && !keep(ix.brokers[i]) {
			continue
		}
		cs = append(cs, best[i])
	}
	sort.Slice(cs, func(i, j int) bool { return less(&cs[i], &cs[j]) })
	if len(cs) > k {
		cs = cs[:k]
	}
	out := make([]ids.BrokerID, len(cs))
	for i, c := range cs {
		out[i] = ix.brokers[c.b]
	}
	return out
}
