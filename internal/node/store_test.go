package node

import (
	"testing"
	"time"

	"github.com/afk-surf/nimbo/internal/ids"
	"github.com/afk-surf/nimbo/internal/peerproto"
)

// Relays carry a deadline as a relative TTL, so the receiver adds the
// in-flight time. A relay of the use that already set a record's deadline
// must not extend it again: two Brokers re-sending a record to each other
// would otherwise keep it alive long after its last use.
func TestRelayOfSameUseDoesNotExtend(t *testing.T) {
	b := newBench(t, 1)
	n := b.nodes[0]
	blob := []byte("relayed")
	h := ids.Sum(blob)
	w := &peerproto.Rec{Kind: peerproto.KindBlob, ID: h[:], Data: blob, TTLms: ms(n.tun.CapTTL), Origin: 7}
	r := n.mergeWire(w)
	if r == nil {
		t.Fatal("merge rejected")
	}
	dl := r.deadline
	b.advance(time.Second)
	n.mergeWire(w) // the same use, relayed again: one second later on arrival
	n.handleRCR(n.id, &peerproto.RCRBatch{Ops: []peerproto.Op{{Touch: &peerproto.Touch{Kind: w.Kind, ID: w.ID, TTLms: w.TTLms, Origin: 7}}}})
	if !r.deadline.Equal(dl) {
		t.Fatalf("relay of the same use extended the deadline by %v", r.deadline.Sub(dl))
	}
	w2 := *w
	w2.Origin = 9 // a later use
	n.mergeWire(&w2)
	if !r.deadline.After(dl) || r.origin != 9 {
		t.Fatal("a later use did not extend the deadline")
	}
}
