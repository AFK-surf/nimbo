package node

import (
	"bytes"
	"encoding/json"

	"github.com/afk-surf/nimbo/internal/caprec"
	"github.com/afk-surf/nimbo/internal/ids"
	"github.com/afk-surf/nimbo/internal/peerproto"
	"github.com/afk-surf/nimbo/internal/presence"
	"github.com/afk-surf/nimbo/pkg/wire"
)

// Capabilities: minting, transfer, validation, renewal and consumption
// (design §8).

type capErr struct{ code, detail string }

func cerr(code, detail string) *capErr { return &capErr{code, detail} }

// --- blobs ---

// prepBlob canonicalises and checks a schema or base supplied by an endpoint.
func (n *Node) prepBlob(kind peerproto.BlobKind, raw json.RawMessage) ([]byte, *capErr) {
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return nil, cerr(wire.ErrBadRequest, "invalid JSON")
	}
	b := buf.Bytes()
	switch kind {
	case peerproto.BlobSchema:
		if len(b) > n.lim.MaxSchemaBytes {
			return nil, cerr(wire.ErrTooLarge, "schema")
		}
		if _, err := n.schemas.Get(ids.Sum(b), b); err != nil {
			return nil, cerr(wire.ErrBadRequest, "schema: "+err.Error())
		}
	case peerproto.BlobBase:
		if len(b) > n.lim.MaxBaseBytes {
			return nil, cerr(wire.ErrTooLarge, "base")
		}
		var m map[string]json.RawMessage
		if json.Unmarshal(b, &m) != nil || m == nil {
			return nil, cerr(wire.ErrBadRequest, "base must be a JSON object")
		}
	}
	return b, nil
}

// blobLive reports whether this Broker's cached TTL for the blob is running.
func (n *Node) blobLive(h ids.Hash) bool {
	r := n.recs[blobKey(h)]
	return r != nil && n.now().Before(r.deadline)
}

// writeBlob makes a blob live: it uses the cached copy if its TTL is still
// running, otherwise it stores the bytes on the blob's placement set.
func (n *Node) writeBlob(kind peerproto.BlobKind, b []byte) ids.Hash {
	h := ids.Sum(b)
	if r := n.recs[blobKey(h)]; r != nil && n.now().Before(r.deadline) {
		n.useRec(r)
		return h
	}
	dl := n.now().Add(n.tun.CapTTL)
	if r := n.recs[blobKey(h)]; r != nil {
		r.deadline, r.touched = dl, dl
		n.setFlags(r, r.flags|fIssuer)
		n.replicate(r, modeStore, nil)
		return h
	}
	r := &record{key: blobKey(h), data: b, blobKind: kind, deadline: dl, touched: dl}
	n.putNew(r, fIssuer)
	n.replicate(r, modeStore, nil)
	return h
}

// prepBlobs validates optional schema and base, charges the session's
// new-blob budget, and writes them.
func (n *Node) prepBlobs(s *session, schemaRaw, baseRaw json.RawMessage) (sh, bh *ids.Hash, e *capErr) {
	var sb, bb []byte
	if len(schemaRaw) > 0 && string(schemaRaw) != "null" {
		if sb, e = n.prepBlob(peerproto.BlobSchema, schemaRaw); e != nil {
			return
		}
	}
	if len(baseRaw) > 0 && string(baseRaw) != "null" {
		if bb, e = n.prepBlob(peerproto.BlobBase, baseRaw); e != nil {
			return
		}
	}
	cost := 0
	if sb != nil && !n.blobLive(ids.Sum(sb)) {
		cost += len(sb)
	}
	if bb != nil && !n.blobLive(ids.Sum(bb)) {
		cost += len(bb)
	}
	if s.blobBytes+cost > n.lim.BlobBytesPer10s {
		return nil, nil, cerr(wire.ErrRateLimited, "new blob bytes")
	}
	s.blobBytes += cost
	if sb != nil {
		h := n.writeBlob(peerproto.BlobSchema, sb)
		sh = &h
	}
	if bb != nil {
		h := n.writeBlob(peerproto.BlobBase, bb)
		bh = &h
	}
	return
}

func (n *Node) useBlobs(c *caprec.Record) {
	for _, h := range []*ids.Hash{c.Schema, c.Base} {
		if h != nil {
			if r := n.recs[blobKey(*h)]; r != nil {
				n.useRec(r)
			}
		}
	}
}

// resolveBlobs fetches a record's schema and base blobs; it fails closed.
func (n *Node) resolveBlobs(c *caprec.Record, cb func(schema, base *record, e *capErr)) {
	var hs []ids.Hash
	if c.Schema != nil {
		hs = append(hs, *c.Schema)
	}
	if c.Base != nil {
		hs = append(hs, *c.Base)
	}
	got := make([]*record, len(hs))
	left := len(hs)
	finish := func() {
		var sr, br *record
		i := 0
		if c.Schema != nil {
			sr = got[i]
			i++
		}
		if c.Base != nil {
			br = got[i]
		}
		for _, r := range got {
			if r == nil {
				cb(nil, nil, cerr(wire.ErrUnavailable, "blob unresolved"))
				return
			}
		}
		cb(sr, br, nil)
	}
	if left == 0 {
		finish()
		return
	}
	for i, h := range hs {
		i := i
		n.lookupRec(blobKey(h), false, func(r *record, o lookupOutcome) {
			if o == lkFound {
				got[i] = r
			}
			left--
			if left == 0 {
				finish()
			}
		})
	}
}

// --- validation (design §8.4, steps 1-4) ---

type validated struct {
	r      *record
	schema *record
	base   *record
}

func (n *Node) validateCap(s *session, id ids.CapID, cb func(*validated, *capErr)) {
	if !n.noteUse(s, id) {
		cb(nil, cerr(wire.ErrRateLimited, "distinct caps"))
		return
	}
	rk := capKey(id)
	if n.recs[rk] == nil {
		if s.unknown >= n.lim.UnknownPer10s {
			cb(nil, cerr(wire.ErrRateLimited, "unknown cap lookups"))
			return
		}
		s.unknown++
	}
	n.lookupRec(rk, false, func(r *record, o lookupOutcome) {
		switch o {
		case lkNotFound:
			cb(nil, cerr(wire.ErrInvalidCap, ""))
			return
		case lkUnresolved:
			cb(nil, cerr(wire.ErrUnavailable, "cap lookup"))
			return
		}
		if r.cap == nil {
			cb(nil, cerr(wire.ErrInvalidCap, ""))
			return
		}
		if r.cap.Holder != s.key {
			cb(nil, cerr(wire.ErrNotHolder, ""))
			return
		}
		if r.consumed {
			cb(nil, cerr(wire.ErrConsumed, ""))
			return
		}
		next := func(r *record) {
			n.resolveBlobs(r.cap, func(sr, br *record, e *capErr) {
				if e != nil {
					cb(nil, e)
					return
				}
				cb(&validated{r: r, schema: sr, base: br}, nil)
			})
		}
		if !n.expired(r) {
			next(r)
			return
		}
		// A local copy can hold a stale TTL: re-read before rejecting.
		n.lookupRec(rk, true, func(r2 *record, o lookupOutcome) {
			if o == lkFound {
				r = r2
			}
			if n.expired(r) {
				cb(nil, cerr(wire.ErrExpired, ""))
				return
			}
			next(r)
		})
	})
}

// checkTarget applies step 5: a gone target fails the use.
func (n *Node) checkTarget(c *caprec.Record, cb func(presence.State, []ids.BrokerID, *capErr)) {
	if len(n.byKey[c.Target]) > 0 {
		cb(presence.Present, []ids.BrokerID{n.id}, nil)
		return
	}
	n.locate(c.Target, func(st presence.State, owners []ids.BrokerID) {
		if st == presence.Gone {
			cb(st, nil, cerr(wire.ErrTargetGone, ""))
			return
		}
		cb(st, owners, nil)
	})
}

// consume applies step 6 for a single-use cap at its issuer.
func (n *Node) consume(r *record, cb func(*capErr)) {
	issuer, ok := n.view.byShort[r.cap.Issuer]
	if !ok {
		cb(cerr(wire.ErrUnavailable, "issuer unknown"))
		return
	}
	done := func(code peerproto.ConsumeCode) {
		switch code {
		case peerproto.ConsumeOK:
			r.consumed = true
			cb(nil)
		case peerproto.ConsumeConsumed:
			r.consumed = true
			cb(cerr(wire.ErrConsumed, ""))
		default:
			cb(cerr(wire.ErrInvalidCap, "issuer lost the cap"))
		}
	}
	if issuer == n.id {
		done(n.handleConsume(&peerproto.Consume{ID: r.key.capID()}).Code)
		return
	}
	n.call(issuer, &peerproto.Envelope{Consume: &peerproto.Consume{ID: r.key.capID()}}, func(resp *peerproto.Envelope, err error) {
		if err != nil || resp.ConsumeResult == nil {
			cb(cerr(wire.ErrUnavailable, "issuer unreachable"))
			return
		}
		done(resp.ConsumeResult.Code)
	})
}

// --- record creation ---

func (n *Node) newCapID() ids.CapID {
	var id ids.CapID
	ids.RandBytes(n.env.Rand(), id[:])
	return id
}

// singleUseID draws IDs until this Broker is the nearest live Broker to
// SHA-256(ID) (design §8.3).
func (n *Node) singleUseID() (ids.CapID, bool) {
	// Expected tries: about the number of Brokers. Draw randomness in
	// batches and decide most candidates with the prefix table.
	var buf [64 * 16]byte
	for batch := 0; batch < 128; batch++ {
		ids.RandBytes(n.env.Rand(), buf[:])
		for i := 0; i < 64; i++ {
			var id ids.CapID
			copy(id[:], buf[i*16:])
			if near, ok := n.view.idx.NearestOwnerFast(id.PlacementKey()); ok && near == n.id {
				return id, true
			}
		}
	}
	return ids.CapID{}, false
}

// createCap stores a new cap record issued here. Single-use records stay on
// this Broker only; others are replicated to their placement set. If
// durable, done waits until 2 placement members (or all, if fewer) hold the
// record.
func (n *Node) createCap(c *caprec.Record, durable bool, done func(ids.CapID, *capErr)) {
	var id ids.CapID
	if c.SingleUse {
		var ok bool
		if id, ok = n.singleUseID(); !ok {
			done(id, cerr(wire.ErrUnavailable, "no single-use id"))
			return
		}
	} else {
		id = n.newCapID()
	}
	if c.Kind == caprec.KindMint {
		c.Lineage = ids.Lineage(id)
	}
	dl := n.now().Add(n.tun.CapTTL)
	r := &record{key: capKey(id), data: c.Encode(), cap: c, deadline: dl, touched: dl}
	if c.SingleUse {
		n.putNew(r, fHolder|fIssuer)
		done(id, nil)
		return
	}
	n.putNew(r, fIssuer)
	if !durable {
		n.replicate(r, modeStore, nil)
		done(id, nil)
		return
	}
	pset := n.view.placementSet(r.pkey, n.tun.Replicas)
	need := min(2, len(pset))
	have := 0
	if contains(pset, n.id) {
		have = 1
	}
	finished := false
	finish := func(e *capErr) {
		if !finished {
			finished = true
			done(id, e)
		}
	}
	if have >= need {
		n.replicate(r, modeStore, nil)
		finish(nil)
		return
	}
	n.replicate(r, modeStore, func(ok bool) {
		if ok {
			have++
			if have >= need {
				finish(nil)
			}
		}
	})
	n.after(n.tun.DurableTimeout, func() { finish(cerr(wire.ErrUnavailable, "durable write timed out")) })
}

// admit applies the per-Broker mint admission rate and replication
// back-pressure, and the per-session live-cap quota.
func (n *Node) admit(s *session) *capErr {
	if n.overloaded() || !n.mintTok.take(n.now(), 1) {
		return cerr(wire.ErrUnavailable, "overloaded")
	}
	if len(s.created) >= n.lim.MaxLiveCaps {
		for id := range s.created {
			if r := n.recs[capKey(id)]; r == nil || n.expired(r) {
				delete(s.created, id)
			}
		}
		if len(s.created) >= n.lim.MaxLiveCaps {
			return cerr(wire.ErrRateLimited, "live caps")
		}
	}
	return nil
}

// --- Actions ---

func (n *Node) actMint(s *session, in *wire.In) {
	if !s.mints.take(n.now(), 1) {
		n.fail(s, in.Rid, wire.ErrRateLimited, "mints")
		return
	}
	if e := n.admit(s); e != nil {
		n.fail(s, in.Rid, e.code, e.detail)
		return
	}
	sh, bh, e := n.prepBlobs(s, in.Schema, in.Base)
	if e != nil {
		n.fail(s, in.Rid, e.code, e.detail)
		return
	}
	c := &caprec.Record{
		Kind:      caprec.KindMint,
		Target:    s.key,
		Holder:    s.key,
		Issuer:    n.short,
		MintedAt:  n.env.Wall().UnixMilli(),
		Schema:    sh,
		Base:      bh,
		AutoReply: in.AutoReply,
		Hints:     []ids.Short{n.short},
	}
	n.stats.Minted++
	n.createCap(c, in.Durable, func(id ids.CapID, e *capErr) {
		if e != nil {
			n.fail(s, in.Rid, e.code, e.detail)
			return
		}
		s.created[id] = true
		n.ok(s, wire.Result{Rid: in.Rid, Cap: id.String()})
	})
}

func (n *Node) actTransfer(s *session, in *wire.In) {
	id, err := ids.ParseCapID(in.Cap)
	to, err2 := ids.ParseKey(in.To)
	if err != nil || err2 != nil {
		n.fail(s, in.Rid, wire.ErrBadRequest, "cap or to")
		return
	}
	if e := n.admit(s); e != nil {
		n.fail(s, in.Rid, e.code, e.detail)
		return
	}
	n.validateCap(s, id, func(v *validated, e *capErr) {
		if e != nil {
			n.fail(s, in.Rid, e.code, e.detail)
			return
		}
		n.checkTarget(v.r.cap, func(_ presence.State, _ []ids.BrokerID, e *capErr) {
			if e != nil {
				n.fail(s, in.Rid, e.code, e.detail)
				return
			}
			create := func() {
				c := v.r.cap.Derive(to, n.short, n.env.Wall().UnixMilli())
				n.stats.Transferred++
				n.createCap(c, in.Durable && !c.SingleUse, func(nid ids.CapID, e *capErr) {
					if e != nil {
						n.fail(s, in.Rid, e.code, e.detail)
						return
					}
					s.created[nid] = true
					n.ok(s, wire.Result{Rid: in.Rid, Cap: nid.String()})
				})
			}
			if v.r.singleUse() {
				n.consume(v.r, func(e *capErr) {
					if e != nil {
						n.fail(s, in.Rid, e.code, e.detail)
						return
					}
					n.useBlobs(v.r.cap)
					create()
				})
				return
			}
			n.useRec(v.r)
			n.useBlobs(v.r.cap)
			create()
		})
	})
}

func (n *Node) actRenew(s *session, in *wire.In) {
	if len(in.Caps) == 0 || len(in.Caps) > 256 {
		n.fail(s, in.Rid, wire.ErrBadRequest, "caps")
		return
	}
	out := make([]wire.RenewItem, len(in.Caps))
	left := len(in.Caps)
	finish := func() {
		left--
		if left == 0 {
			n.ok(s, wire.Result{Rid: in.Rid, Renewed: out})
		}
	}
	for i, cs := range in.Caps {
		i := i
		out[i].Cap = cs
		id, err := ids.ParseCapID(cs)
		if err != nil {
			out[i].Error = &wire.Error{Code: wire.ErrBadRequest}
			finish()
			continue
		}
		n.validateCap(s, id, func(v *validated, e *capErr) {
			if e != nil {
				out[i].Error = &wire.Error{Code: e.code, Detail: e.detail}
				finish()
				return
			}
			n.checkTarget(v.r.cap, func(_ presence.State, _ []ids.BrokerID, e *capErr) {
				if e != nil {
					out[i].Error = &wire.Error{Code: e.code, Detail: e.detail}
					finish()
					return
				}
				n.useRec(v.r)
				n.useBlobs(v.r.cap)
				out[i].OK = true
				out[i].TTLms = ms(n.remaining(v.r))
				finish()
			})
		})
	}
}

func (n *Node) actInspect(s *session, in *wire.In) {
	id, err := ids.ParseCapID(in.Cap)
	if err != nil {
		n.fail(s, in.Rid, wire.ErrBadRequest, "cap")
		return
	}
	n.validateCap(s, id, func(v *validated, e *capErr) {
		if e != nil {
			n.fail(s, in.Rid, e.code, e.detail)
			return
		}
		c := v.r.cap
		info := &wire.CapInfo{
			Lineage:   c.Lineage.String(),
			Kind:      c.Kind.String(),
			SingleUse: c.SingleUse,
			Target:    c.Target.String(),
			Holder:    c.Holder.String(),
			Issuer:    c.Issuer.String(),
			MintedAt:  c.MintedAt,
			AutoReply: c.AutoReply,
			TTLms:     ms(n.remaining(v.r)),
		}
		if v.schema != nil {
			info.Schema = v.schema.data
		}
		if v.base != nil {
			info.Base = v.base.data
		}
		n.ok(s, wire.Result{Rid: in.Rid, Info: info})
	})
}
