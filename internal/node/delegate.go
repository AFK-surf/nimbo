package node

import (
	"crypto/hmac"
	"time"

	"golang.org/x/crypto/curve25519"

	"github.com/afk-surf/nimbo/internal/caprec"
	"github.com/afk-surf/nimbo/internal/ids"
	"github.com/afk-surf/nimbo/pkg/wire"
)

// Delegated issuance (design §8.6): an endpoint that can prove ownership of
// another endpoint key gets a cap targeting that key and held by itself.
// The proof is an X25519 key agreement with a single-use Broker ephemeral
// key, bound to the grant; challenges live only in the session that asked.

type delegation struct {
	priv        [32]byte
	target      ids.Key
	schema      *ids.Hash
	base        *ids.Hash
	autoReply   bool
	allowStream bool
	deadline    time.Time
}

func (n *Node) actDelegateBegin(s *session, in *wire.In) {
	target, err := ids.ParseKey(in.Target)
	if err != nil {
		n.fail(s, in.Rid, wire.ErrBadRequest, "target")
		return
	}
	now := n.now()
	if len(s.delegations) >= n.lim.MaxPendingDelegations {
		for k, d := range s.delegations {
			if now.After(d.deadline) {
				delete(s.delegations, k)
			}
		}
	}
	if len(s.delegations) >= n.lim.MaxPendingDelegations {
		n.fail(s, in.Rid, wire.ErrRateLimited, "pending delegations")
		return
	}
	if !s.delegs.take(now, 1) {
		n.fail(s, in.Rid, wire.ErrRateLimited, "delegations")
		return
	}
	if !s.mints.take(now, 1) {
		n.fail(s, in.Rid, wire.ErrRateLimited, "mints")
		return
	}
	sh, bh, e := n.prepBlobs(s, in.Schema, in.Base)
	if e != nil {
		n.fail(s, in.Rid, e.code, e.detail)
		return
	}
	d := &delegation{target: target, schema: sh, base: bh, autoReply: in.AutoReply, allowStream: in.AllowStream, deadline: now.Add(n.tun.DelegationTTL)}
	ids.RandBytes(n.env.Rand(), d.priv[:])
	pub, err := curve25519.X25519(d.priv[:], curve25519.Basepoint)
	if err != nil {
		panic(err)
	}
	eph := ids.Key(pub)
	s.delegations[eph] = d
	r := wire.Result{Rid: in.Rid, Eph: eph.String(), Broker: n.id.String(), TTLms: ms(n.tun.DelegationTTL)}
	if sh != nil {
		r.SchemaHash = sh.String()
	}
	if bh != nil {
		r.BaseHash = bh.String()
	}
	n.ok(s, r)
}

func (n *Node) actDelegate(s *session, in *wire.In) {
	eph, err := ids.ParseKey(in.Eph)
	if err != nil {
		n.fail(s, in.Rid, wire.ErrBadRequest, "eph")
		return
	}
	d := s.delegations[eph]
	if d == nil || n.now().After(d.deadline) {
		delete(s.delegations, eph)
		n.fail(s, in.Rid, wire.ErrInvalidChallenge, "")
		return
	}
	// Single use, whatever the outcome.
	delete(s.delegations, eph)
	proof, err := ids.B32.DecodeString(in.Proof)
	if err != nil || len(proof) != 32 {
		n.fail(s, in.Rid, wire.ErrBadRequest, "proof")
		return
	}
	ss, err := curve25519.X25519(d.priv[:], d.target[:])
	if err != nil { // all-zero shared secret (low-order target key)
		n.fail(s, in.Rid, wire.ErrBadProof, "")
		return
	}
	g := wire.Grant{Broker: n.id, Eph: eph, Target: d.target, Holder: s.key, AutoReply: d.autoReply, AllowStream: d.allowStream}
	if d.schema != nil {
		g.Schema = (*[32]byte)(d.schema)
	}
	if d.base != nil {
		g.Base = (*[32]byte)(d.base)
	}
	if !hmac.Equal(proof, wire.DelegationProof(ss, &g)) {
		n.fail(s, in.Rid, wire.ErrBadProof, "")
		return
	}
	if e := n.admit(s); e != nil {
		n.fail(s, in.Rid, e.code, e.detail)
		return
	}
	// Routing finds the target through presence; hint this Broker only if
	// the target is connected here.
	var hints []ids.Short
	if len(n.byKey[d.target]) > 0 {
		hints = []ids.Short{n.short}
	}
	c := &caprec.Record{
		Kind:      caprec.KindMint,
		Target:    d.target,
		Holder:    s.key,
		Issuer:    n.short,
		MintedAt:  n.env.Wall().UnixMilli(),
		Schema:    d.schema,
		Base:      d.base,
		AutoReply: d.autoReply,
		Hints:     hints,

		AllowStream: d.allowStream,
	}
	n.useBlobs(c)
	n.stats.Delegated++
	n.createCap(c, in.Durable, func(id ids.CapID, e *capErr) {
		if e != nil {
			n.fail(s, in.Rid, e.code, e.detail)
			return
		}
		s.created[id] = true
		n.ok(s, wire.Result{Rid: in.Rid, Cap: id.String()})
	})
}
