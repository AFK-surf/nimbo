// Package host is the production runtime for a node.Node: it runs the node's
// single event loop and implements node.Env with real clocks, crypto
// randomness, timers, S3, the Broker mesh and WebSocket sessions.
package host

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/afk-surf/nimbo/internal/ids"
	"github.com/afk-surf/nimbo/internal/node"
	"github.com/afk-surf/nimbo/internal/peerproto"
	"github.com/afk-surf/nimbo/internal/s3store"
)

// Mesh is the authenticated Broker-to-Broker transport.
type Mesh interface {
	// Accept returns the next inbound peer stream and the authenticated
	// Broker it comes from.
	Accept() (net.Conn, ids.BrokerID, error)
	// Dial opens a stream to a peer.
	Dial(ctx context.Context, to ids.BrokerID) (net.Conn, error)
	// SetPeers reconfigures the transport for the live member list.
	SetPeers(members []node.Member)
	Close() error
}

// Options configure a Host.
type Options struct {
	Node   node.Config
	Store  s3store.Store
	Mesh   Mesh
	Logger *slog.Logger
	// OnLeft is called after a graceful leave completes.
	OnLeft func()
	// OnUnknownPeer, if the mesh reports handshakes from unknown keys, is
	// wired by the caller to Host.UnknownPeer.
}

// Host runs one Broker.
type Host struct {
	opts  Options
	log   *slog.Logger
	node  *node.Node
	loop  chan func()
	done  chan struct{}
	close sync.Once

	links    map[linkKey]*link // loop-owned
	deferred []func()          // loop-owned: zero-delay timers, in order
	sessMu   sync.Mutex
	sessions map[node.SessionID]*Session
	nextSess atomic.Uint64
	members  atomic.Pointer[[]node.Member]
}

// New creates a Host; Start runs it.
func New(opts Options) *Host {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	h := &Host{
		opts:     opts,
		log:      opts.Logger,
		loop:     make(chan func(), 1<<16),
		done:     make(chan struct{}),
		links:    map[linkKey]*link{},
		sessions: map[node.SessionID]*Session{},
	}
	h.node = node.New(opts.Node, (*env)(h))
	return h
}

// Node returns the node. Only use it inside Do.
func (h *Host) Node() *node.Node { return h.node }

// Start starts the event loop, the mesh acceptor and the node.
func (h *Host) Start() {
	go h.run()
	go h.acceptPeers()
	h.Do(h.node.Start)
}

func (h *Host) run() {
	for {
		select {
		case f := <-h.loop:
			f()
			// Zero-delay timers run after the event that set them, in
			// order, before any other event (as in the simulator).
			for len(h.deferred) > 0 {
				d := h.deferred[0]
				h.deferred = h.deferred[1:]
				d()
			}
		case <-h.done:
			return
		}
	}
}

func (h *Host) defer0(f func()) { h.deferred = append(h.deferred, f) }

// Do runs f on the node loop.
func (h *Host) Do(f func()) {
	select {
	case h.loop <- f:
	case <-h.done:
	}
}

// Call runs f on the node loop and waits for it.
func (h *Host) Call(f func()) {
	ch := make(chan struct{})
	h.Do(func() { f(); close(ch) })
	select {
	case <-ch:
	case <-h.done:
	}
}

// Drain starts a graceful leave.
func (h *Host) Drain() { h.Do(h.node.Drain) }

// Done is closed when the host stops.
func (h *Host) Done() <-chan struct{} { return h.done }

// Close stops the host immediately (no graceful leave).
func (h *Host) Close() {
	h.close.Do(func() {
		h.Call(func() {
			h.node.Stop()
			for _, l := range h.links {
				l.stop()
			}
		})
		close(h.done)
		h.opts.Mesh.Close()
		h.sessMu.Lock()
		for _, s := range h.sessions {
			s.close()
		}
		h.sessMu.Unlock()
	})
}

// UnknownPeer is called by a mesh that sees a handshake from a node key not
// in the member list: the node checks S3 (fast join).
func (h *Host) UnknownPeer(id ids.BrokerID) {
	h.Do(func() { h.node.OnUnknownPeer(id) })
}

// Members returns the last member list reported by the node.
func (h *Host) Members() []node.Member {
	if p := h.members.Load(); p != nil {
		return *p
	}
	return nil
}

// env implements node.Env. All methods are called on the loop.
type env Host

func (e *env) h() *Host { return (*Host)(e) }

func (e *env) Now() time.Time  { return time.Now() }
func (e *env) Wall() time.Time { return time.Now().Round(0) }
func (e *env) Rand() io.Reader { return rand.Reader }

type zeroTimer struct{ stopped bool }

func (t *zeroTimer) Stop() bool { was := !t.stopped; t.stopped = true; return was }

type hostTimer struct{ t *time.Timer }

func (t hostTimer) Stop() bool { return t.t.Stop() }

func (e *env) AfterFunc(d time.Duration, f func()) node.Timer {
	h := e.h()
	if d <= 0 {
		t := &zeroTimer{}
		h.defer0(func() {
			if !t.stopped {
				f()
			}
		})
		return t
	}
	return hostTimer{time.AfterFunc(d, func() { h.Do(f) })}
}

func (e *env) SendPeer(to ids.BrokerID, m *peerproto.Envelope) { e.h().sendPeer(to, m) }

func (e *env) SendSession(id node.SessionID, frame []byte) bool {
	h := e.h()
	h.sessMu.Lock()
	s := h.sessions[id]
	h.sessMu.Unlock()
	if s == nil {
		return true
	}
	return s.enqueue(frame)
}

func (e *env) CloseSession(id node.SessionID) {
	h := e.h()
	h.sessMu.Lock()
	s := h.sessions[id]
	delete(h.sessions, id)
	h.sessMu.Unlock()
	if s != nil {
		s.closeAfterFlush()
	}
}

func (e *env) S3(req node.S3Request, done func(node.S3Response)) {
	h := e.h()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		resp := h.opts.Store.Do(ctx, req)
		h.Do(func() { done(resp) })
	}()
}

func (e *env) MembersChanged(ms []node.Member) {
	h := e.h()
	cp := append([]node.Member(nil), ms...)
	h.members.Store(&cp)
	go h.opts.Mesh.SetPeers(cp)
}

func (e *env) Left() {
	h := e.h()
	if h.opts.OnLeft != nil {
		go h.opts.OnLeft()
	}
}

func (e *env) Logf(format string, args ...any) {
	e.h().log.Info(fmt.Sprintf(format, args...))
}
