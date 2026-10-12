package sim

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/afk-surf/nimbo/internal/ids"
	"github.com/afk-surf/nimbo/pkg/wire"
)

type sendStatus int

const (
	stPending sendStatus = iota
	stOK
	stFailed
	stUnknown
)

type useKind int

const (
	useSend useKind = iota
	useTransfer
	useRenew
)

type sendRec struct {
	nonce   string
	from    *Endpoint
	conn    *Conn
	cap     heldCap
	carried []heldCap
	ttl     time.Duration
	violate string
	reply   struct {
		auto, schema bool
		base         string
	}
	fifoSeq  int
	sentAt   time.Duration
	resultAt time.Duration
	status   sendStatus
	code     string
	msgID    string
	replyID  string
	expected bool

	deliveries  int
	deliveredAt time.Duration
	deliveredOn string
}

// Checker records the workload's observations and checks invariants.
type Checker struct {
	w              *World
	sends          map[string]*sendRec
	order          []*sendRec
	singleOK       map[ids.CapID]int
	replyDelivered map[string]int
	fifoLast       map[fifoKey]int
	attempts       map[*heldCap]int
	violations     []string
	stats          Stats
}

func newChecker(w *World) *Checker {
	return &Checker{
		w:              w,
		sends:          map[string]*sendRec{},
		singleOK:       map[ids.CapID]int{},
		replyDelivered: map[string]int{},
		fifoLast:       map[fifoKey]int{},
		attempts:       map[*heldCap]int{},
		stats:          Stats{Errors: map[string]int{}},
	}
}

func (c *Checker) violate(format string, args ...any) {
	if len(c.violations) < 200 {
		c.violations = append(c.violations, fmt.Sprintf("t=%.3fs ", c.w.now.Seconds())+fmt.Sprintf(format, args...))
	}
	c.w.logf("VIOLATION: "+format, args...)
}

func (c *Checker) noteErr(out *wire.Out) {
	if out == nil {
		c.stats.Errors["(closed)"]++
		return
	}
	if !out.OK && out.Error != nil {
		c.stats.Errors[out.Error.Code]++
	}
}

func (c *Checker) quietSettled() bool { return !c.w.faultsOn && c.w.now >= c.w.settledAt }

func (c *Checker) sent(rec *sendRec) {
	w := c.w
	rec.sentAt = w.now
	c.sends[rec.nonce] = rec
	c.order = append(c.order, rec)
	c.stats.Sends++
	h := rec.from.byID[rec.cap.id]
	first := h != nil && c.attempts[h] == 0
	exp := c.quietSettled() && rec.cap.fresh && first && !rec.cap.contested && rec.violate == "" &&
		w.now-rec.cap.lastUse < 40*time.Second &&
		!rec.cap.target.retired && rec.cap.target.connected() && rec.from.connected() &&
		w.now < w.cfg.FaultPhase+w.cfg.Quiet-20*time.Second
	for _, ch := range rec.carried {
		if !ch.fresh || ch.singleUse || w.now-ch.lastUse > 40*time.Second {
			exp = false
		}
	}
	rec.expected = exp
}

func (c *Checker) useAttempt(e *Endpoint, h *heldCap) { c.attempts[h]++ }

func (c *Checker) sendResult(rec *sendRec, out *wire.Out) {
	rec.resultAt = c.w.now
	c.noteErr(out)
	switch {
	case out == nil:
		rec.status = stUnknown
	case out.OK:
		rec.status = stOK
		rec.msgID, rec.replyID = out.Msg, out.ReplyID
		c.stats.SendOK++
		if rec.violate != "" {
			c.violate("send %s violating %s was accepted", rec.nonce, rec.violate)
		}
		if rec.cap.autoReply != (out.ReplyID != "") {
			c.violate("send %s: reply_id presence %v, cap auto_reply %v", rec.nonce, out.ReplyID != "", rec.cap.autoReply)
		}
	default:
		rec.status = stFailed
		rec.code = out.Error.Code
		if rec.violate == "" && (rec.code == wire.ErrSchemaViolation || rec.code == wire.ErrBaseConflict) {
			c.violate("send %s: unexpected %s (%s)", rec.nonce, rec.code, out.Error.Detail)
		}
		if rec.code == wire.ErrNotHolder || rec.code == wire.ErrBadRequest {
			c.violate("send %s on own cap failed with %s (%s)", rec.nonce, rec.code, out.Error.Detail)
		}
	}
	if rec.expected && (out == nil || !out.OK) {
		code := "(closed)"
		if out != nil {
			code = out.Error.Code + " " + out.Error.Detail
		}
		c.violate("quiet-phase send %s on fresh cap failed: %s", rec.nonce, code)
	}
}

// useResult checks the expiry bound and single use, and tracks last use.
func (c *Checker) useResult(e *Endpoint, h *heldCap, out *wire.Out, kind useKind) {
	transfer := kind == useTransfer
	now := c.w.now
	if out == nil {
		h.lastUse = now // outcome unknown: it may have been a use
		return
	}
	if !out.OK {
		c.noteErr(out)
		if transfer && out.Error.Code == wire.ErrUnavailable {
			h.lastUse = now
		}
		if out.Error.Code == wire.ErrNotHolder {
			c.violate("%s: use of own cap %s failed with not_holder", e.name, h.id)
		}
		return
	}
	bound := c.w.cfg.Tunables.CapTTL + c.w.cfg.Tunables.Grace + 5*time.Second
	if idle := now - h.lastUse; idle > bound {
		c.violate("%s: cap %s accepted after %v idle", e.name, h.id, idle)
	}
	h.lastUse = now
	if h.singleUse && kind != useRenew {
		c.singleOK[h.id]++
		if c.singleOK[h.id] > 1 {
			c.violate("single-use cap %s used successfully %d times", h.id, c.singleOK[h.id])
		}
	}
}

func (c *Checker) inspected(e *Endpoint, h *heldCap, out *wire.Out) {
	if out == nil || !out.OK {
		c.noteErr(out)
		return
	}
	bound := c.w.cfg.Tunables.CapTTL + c.w.cfg.Tunables.Grace + 5*time.Second
	if idle := c.w.now - h.lastUse; idle > bound {
		c.violate("%s: inspect accepted cap %s after %v idle", e.name, h.id, idle)
	}
	if out.Info == nil || out.Info.Holder != e.key.String() {
		c.violate("%s: inspect returned wrong holder", e.name)
	} else if out.Info.Target != h.target.key.String() {
		c.violate("%s: inspect returned wrong target", e.name)
	}
}

func (c *Checker) delivered(e *Endpoint, conn *Conn, out *wire.Out) {
	c.stats.Delivered++
	var p map[string]any
	if err := json.Unmarshal(out.Payload, &p); err != nil {
		c.violate("%s: delivered payload is not an object", e.name)
		return
	}
	nonce, _ := p["nonce"].(string)
	rec := c.sends[nonce]
	if rec == nil {
		c.violate("%s: delivery of a message nobody sent (%s)", e.name, out.Payload)
		return
	}
	rec.deliveries++
	if rec.deliveries > 1 {
		c.violate("message %s delivered %d times", nonce, rec.deliveries)
		return
	}
	rec.deliveredAt = c.w.now
	rec.deliveredOn = out.On
	if e != rec.cap.target {
		c.violate("message %s delivered to %s, target is %s", nonce, e.name, rec.cap.target.name)
	}
	if fmt.Sprint(p["from"]) != rec.from.name || p["seq"] == nil {
		c.violate("message %s payload altered: %s", nonce, out.Payload)
	}
	want := 3
	if rec.cap.base != "" {
		want++
		if p["tag"] != rec.cap.base {
			c.violate("message %s: base not merged (%s)", nonce, out.Payload)
		}
	}
	if len(p) != want {
		c.violate("message %s: unexpected payload fields %s", nonce, out.Payload)
	}
	if len(out.Caps) != len(rec.carried) {
		c.violate("message %s: %d carried caps delivered, %d sent", nonce, len(out.Caps), len(rec.carried))
	}
	if (out.Reply != "") != rec.cap.autoReply {
		c.violate("message %s: reply cap present=%v, auto_reply=%v", nonce, out.Reply != "", rec.cap.autoReply)
	}
	if rec.cap.singleUse {
		c.replyDelivered[rec.cap.replyOf]++
		if c.replyDelivered[rec.cap.replyOf] > 1 {
			c.violate("reply lineage of %s delivered %d times", rec.cap.replyOf, c.replyDelivered[rec.cap.replyOf])
		}
	}
	if rec.expected {
		k := fifoKey{rec.conn, rec.cap.id}
		if rec.fifoSeq < c.fifoLast[k] {
			c.violate("FIFO: message %s (seq %d) after seq %d on the same session and cap", nonce, rec.fifoSeq, c.fifoLast[k])
		}
		c.fifoLast[k] = max(c.fifoLast[k], rec.fifoSeq)
	}
	if rec.cap.target.retired {
		c.violate("message %s delivered to a retired endpoint", nonce)
	}
	e.onDeliver(conn, out, rec)
}

// expectedLineage resolves the lineage a delivery should report.
func (c *Checker) expectedLineage(h *heldCap) (string, bool) {
	if h.replyOf == "" {
		return h.lineage.String(), true
	}
	orig := c.sends[h.replyOf]
	if orig == nil || orig.replyID == "" {
		return "", false
	}
	return orig.replyID, true
}

func (c *Checker) finalChecks() {
	w := c.w
	end := w.cfg.FaultPhase + w.cfg.Quiet
	tun := w.cfg.Tunables
	for _, rec := range c.order {
		if rec.deliveries > 0 {
			if rec.status == stFailed {
				c.violate("message %s delivered although its send failed with %s", rec.nonce, rec.code)
			}
			if l, ok := c.expectedLineage(&rec.cap); ok && l != rec.deliveredOn {
				c.violate("message %s delivered with on=%s, want %s", rec.nonce, rec.deliveredOn, l)
			}
			limit := rec.sentAt + 15*time.Second + rec.ttl
			if rec.status == stOK {
				limit = rec.resultAt + rec.ttl + time.Second
			}
			if rec.deliveredAt > limit {
				c.violate("message %s delivered %v after its send, ttl %v", rec.nonce, rec.deliveredAt-rec.sentAt, rec.ttl)
			}
		}
		if rec.expected {
			c.stats.Expected++
			if rec.status == stOK {
				c.stats.ExpectedOK++
			}
			if rec.deliveries == 1 {
				c.stats.ExpectedDelivered++
			} else {
				c.violate("quiet-phase message %s (status %d %s) was not delivered", rec.nonce, rec.status, rec.code)
			}
		}
	}
	var ref *Broker
	for _, b := range w.brokers {
		if b.alive {
			ref = b
			break
		}
	}
	if ref == nil {
		c.violate("no broker alive at the end")
		return
	}
	// Membership agreement.
	var alive []ids.BrokerID
	for _, b := range w.brokers {
		if b.alive {
			alive = append(alive, b.id)
		}
	}
	for _, b := range w.brokers {
		if !b.alive {
			continue
		}
		if fmt.Sprint(b.node.Members()) != fmt.Sprint(sortIDs(alive)) {
			c.violate("%s: membership %d brokers, %d alive", b, len(b.node.Members()), len(alive))
		}
		if b.node.Fenced() {
			c.violate("%s is fenced at the end of the quiet phase", b)
		}
	}
	// Re-replication: every cap used successfully in the last 15s is held by
	// its whole placement set.
	for _, e := range w.eps {
		for _, h := range e.wallet {
			if h.singleUse || h.lastUse < end-15*time.Second || h.lastUse > end-3*time.Second || w.now-h.obtained < 5*time.Second {
				continue
			}
			for _, m := range ref.node.PlacementSet(h.id) {
				mb := w.byID[m]
				if _, ok := mb.node.HeldCap(h.id); !ok {
					c.violate("cap %s (used %v ago) missing on placement member %s", h.id, w.now-h.lastUse, mb)
				}
			}
		}
	}
	// Presence: every settled session is registered on its placement set.
	for _, e := range w.eps {
		if e.retired {
			continue
		}
		for _, cn := range e.conns {
			if !cn.open || !cn.ready || w.now-cn.readyAt < 10*time.Second || !cn.b.alive {
				continue
			}
			for _, m := range ref.node.PresencePlacement(e.key) {
				mb := w.byID[m]
				if n, ok := mb.node.HeldPresence(e.key, cn.b.id, cn.b.node.Incarnation()); !ok || n == 0 {
					c.violate("presence of %s on %s missing on placement member %s", e.name, cn.b, mb)
				}
			}
		}
	}
	_ = tun
}

func sortIDs(a []ids.BrokerID) []ids.BrokerID {
	out := append([]ids.BrokerID(nil), a...)
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && string(out[j][:]) < string(out[j-1][:]); j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}
