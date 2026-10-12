// Package placement implements XOR placement over virtual IDs (design §7.1).
package placement

import (
	"bytes"
	"crypto/sha256"
	"slices"
	"sort"
	"sync"

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
	id    [32]byte
	owner int32 // index into Index.brokers
}

// Index is an immutable placement index over a set of Brokers. Virtual IDs
// are kept sorted, so the XOR-nearest ones are found by walking the implied
// binary trie in order of XOR distance: O(k log n) per lookup instead of a
// scan of every virtual ID.
type Index struct {
	brokers []ids.BrokerID // sorted
	vids    []vid          // sorted by id

	tableOnce sync.Once
	table     []int32
}

// vidCache memoizes virtual IDs: they depend only on the Broker ID, and
// every membership change rebuilds the index.
var vidCache sync.Map // ids.BrokerID -> []([32]byte)

func virtualIDs(b ids.BrokerID, n int) [][32]byte {
	if v, ok := vidCache.Load(b); ok {
		if vs := v.([][32]byte); len(vs) == n {
			return vs
		}
	}
	vs := make([][32]byte, n)
	for i := range vs {
		vs[i] = VirtualID(b, i)
	}
	vidCache.Store(b, vs)
	return vs
}

// New builds an index for the given Brokers with n virtual IDs each.
func New(brokers []ids.BrokerID, n int) *Index {
	bs := append([]ids.BrokerID(nil), brokers...)
	slices.SortFunc(bs, func(a, b ids.BrokerID) int { return bytes.Compare(a[:], b[:]) })
	ix := &Index{brokers: bs, vids: make([]vid, 0, len(bs)*n)}
	for bi, b := range bs {
		for _, v := range virtualIDs(b, n) {
			ix.vids = append(ix.vids, vid{id: v, owner: int32(bi)})
		}
	}
	slices.SortFunc(ix.vids, func(a, b vid) int { return bytes.Compare(a.id[:], b.id[:]) })
	return ix
}

// Brokers returns the indexed Brokers in sorted order.
func (ix *Index) Brokers() []ids.BrokerID { return ix.brokers }

// Len is the number of Brokers.
func (ix *Index) Len() int { return len(ix.brokers) }

func bit(b *[32]byte, d int) byte { return (b[d>>3] >> (7 - uint(d&7))) & 1 }

const leafSize = 8

// walker emits virtual IDs in increasing XOR distance from key and collects
// the first k distinct owners that keep accepts. Since every virtual ID of a
// Broker is visited no earlier than its nearest one, the order of first
// visits is the order of the Brokers' minimum distances.
type walker struct {
	ix       *Index
	key      *ids.Hash
	k        int
	keep     func(ids.BrokerID) bool
	out      []ids.BrokerID
	seen     []int32
	rejected []int32
}

func (w *walker) emit(v *vid) bool {
	for _, o := range w.seen {
		if o == v.owner {
			return false
		}
	}
	for _, o := range w.rejected {
		if o == v.owner {
			return false
		}
	}
	b := w.ix.brokers[v.owner]
	if w.keep != nil && !w.keep(b) {
		w.rejected = append(w.rejected, v.owner)
		return false
	}
	w.seen = append(w.seen, v.owner)
	w.out = append(w.out, b)
	return len(w.out) >= w.k
}

// walk visits vids[lo:hi], which share their first d bits, in XOR order.
// It reports whether k owners have been collected.
func (w *walker) walk(lo, hi, d int) bool {
	if lo >= hi {
		return false
	}
	if hi-lo <= leafSize || d >= 256 {
		var idx [leafSize]int
		var dist [leafSize][32]byte
		n := 0
		for i := lo; i < hi; i++ {
			if n == leafSize {
				// Only reachable at d >= 256 (identical IDs); emit in order.
				break
			}
			idx[n] = i
			for j := 0; j < 32; j++ {
				dist[n][j] = w.ix.vids[i].id[j] ^ w.key[j]
			}
			n++
		}
		// Insertion sort by distance.
		for i := 1; i < n; i++ {
			for j := i; j > 0 && bytes.Compare(dist[j][:], dist[j-1][:]) < 0; j-- {
				dist[j], dist[j-1] = dist[j-1], dist[j]
				idx[j], idx[j-1] = idx[j-1], idx[j]
			}
		}
		for i := 0; i < n; i++ {
			if w.emit(&w.ix.vids[idx[i]]) {
				return true
			}
		}
		return false
	}
	vs := w.ix.vids
	mid := lo + sort.Search(hi-lo, func(i int) bool { return bit(&vs[lo+i].id, d) == 1 })
	if bit((*[32]byte)(w.key), d) == 0 {
		return w.walk(lo, mid, d+1) || w.walk(mid, hi, d+1)
	}
	return w.walk(mid, hi, d+1) || w.walk(lo, mid, d+1)
}

// Nearest returns up to k distinct Brokers ordered by their minimum XOR
// distance to key, skipping Brokers for which keep returns false.
func (ix *Index) Nearest(key ids.Hash, k int, keep func(ids.BrokerID) bool) []ids.BrokerID {
	if k <= 0 || len(ix.brokers) == 0 {
		return nil
	}
	w := walker{ix: ix, key: &key, k: min(k, len(ix.brokers)), keep: keep, out: make([]ids.BrokerID, 0, min(k, len(ix.brokers)))}
	w.seen = make([]int32, 0, w.k)
	w.walk(0, len(ix.vids), 0)
	return w.out
}

// NearestOwner returns the Broker owning the virtual ID nearest to key, with
// no allocation. It equals Nearest(key, 1, nil)[0]; single-use cap ID
// sampling calls it about once per Broker in the cluster per cap.
func (ix *Index) NearestOwner(key ids.Hash) (ids.BrokerID, bool) {
	vs := ix.vids
	if len(vs) == 0 {
		return ids.BrokerID{}, false
	}
	lo, hi, d := 0, len(vs), 0
	for hi-lo > leafSize && d < 256 {
		mid := lo + sort.Search(hi-lo, func(i int) bool { return bit(&vs[lo+i].id, d) == 1 })
		if bit((*[32]byte)(&key), d) == 0 {
			if mid > lo {
				hi = mid
			} else {
				lo = mid
			}
		} else if hi > mid {
			lo = mid
		} else {
			hi = mid
		}
		d++
	}
	best := lo
	var bd [32]byte
	for i := lo; i < hi; i++ {
		var dist [32]byte
		for j := 0; j < 32; j++ {
			dist[j] = vs[i].id[j] ^ key[j]
		}
		if i == lo || bytes.Compare(dist[:], bd[:]) < 0 {
			best, bd = i, dist
		}
	}
	return ix.brokers[vs[best].owner], true
}

// ownerTable maps the top 16 bits of a key to the owner of its nearest
// virtual ID when every key with that prefix has the same one, else -1.
// It makes NearestOwner a table lookup for most keys; it is built lazily,
// since only single-use cap ID sampling needs it.
func (ix *Index) ownerTable() []int32 {
	ix.tableOnce.Do(func() {
		t := make([]int32, 1<<16)
		vs := ix.vids
		unique := func(lo, hi int) int32 {
			o := vs[lo].owner
			for i := lo + 1; i < hi; i++ {
				if vs[i].owner != o {
					return -1
				}
			}
			return o
		}
		var rec func(lo, hi, d, base, size int)
		rec = func(lo, hi, d, base, size int) {
			if o := unique(lo, hi); o >= 0 {
				for i := base; i < base+size; i++ {
					t[i] = o
				}
				return
			}
			if size == 1 {
				t[base] = -1
				return
			}
			mid := lo + sort.Search(hi-lo, func(i int) bool { return bit(&vs[lo+i].id, d) == 1 })
			half := size / 2
			switch {
			case mid == lo:
				// No virtual ID has bit d = 0. Flipping bit d of a key
				// changes its distance to every remaining ID by the same
				// amount, so both halves have the same nearest owner.
				rec(mid, hi, d+1, base+half, half)
				copy(t[base:base+half], t[base+half:base+size])
			case mid == hi:
				rec(lo, mid, d+1, base, half)
				copy(t[base+half:base+size], t[base:base+half])
			default:
				rec(lo, mid, d+1, base, half)
				rec(mid, hi, d+1, base+half, half)
			}
		}
		if len(vs) > 0 {
			rec(0, len(vs), 0, 0, 1<<16)
		}
		ix.table = t
	})
	return ix.table
}

// NearestOwnerFast is NearestOwner using the prefix table where it decides.
func (ix *Index) NearestOwnerFast(key ids.Hash) (ids.BrokerID, bool) {
	if len(ix.vids) == 0 {
		return ids.BrokerID{}, false
	}
	if o := ix.ownerTable()[uint16(key[0])<<8|uint16(key[1])]; o >= 0 {
		return ix.brokers[o], true
	}
	return ix.NearestOwner(key)
}
