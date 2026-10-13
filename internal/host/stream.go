package host

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/afk-surf/nimbo/internal/ids"
	"github.com/afk-surf/nimbo/internal/node"
	"github.com/afk-surf/nimbo/internal/peerproto"
	"github.com/afk-surf/nimbo/pkg/wire"
)

// Stream data plane (design §9.7). A leg is one data-plane connection: an
// endpoint's data socket (/v1/stream) or a mesh data connection. The node
// decides which legs to splice; here two pump goroutines per splice copy
// chunks of at most 32 KiB, one at a time, so a slow reader stalls the
// writer through every TCP window on the path and memory stays at one chunk
// per direction per hop.
//
// Stream tokens arrive in the URL. Nothing here logs URLs or tokens.

const (
	attachWait     = 15 * time.Second // node answer to an attach (includes the dial to the holder's Broker)
	streamStatusIO = 2 * time.Second  // header write and status byte read on a data connection
	closeWait      = 5 * time.Second
)

// chunk is one piece of a message, or a close.
type chunk struct {
	binary bool
	data   []byte
	fin    bool
	close  bool
	code   int
	reason string
}

// streamEnd is the transport of a leg.
type streamEnd interface {
	// read returns the next chunk; a close chunk when the other side
	// closed cleanly, an error when the connection failed.
	read(buf []byte) (chunk, error)
	write(c chunk) error
	// sendClose sends a close (best effort).
	sendClose(code int, reason string)
	abort()
}

type streamLeg struct {
	h          *Host
	id         node.StreamLeg
	answer     chan node.AttachCode
	peerAnswer chan node.PeerStatus

	mu       sync.Mutex
	end      streamEnd
	failed   bool
	ready    chan struct{} // closed once end is set or the leg failed
	finished bool
	pair     *splicePair
	done     chan struct{}
}

func (h *Host) newLeg() *streamLeg {
	l := &streamLeg{
		h:          h,
		id:         node.StreamLeg(h.nextLeg.Add(1)),
		answer:     make(chan node.AttachCode, 1),
		peerAnswer: make(chan node.PeerStatus, 1),
		ready:      make(chan struct{}),
		done:       make(chan struct{}),
	}
	h.legMu.Lock()
	h.legs[l.id] = l
	h.legMu.Unlock()
	return l
}

func (h *Host) leg(id node.StreamLeg) *streamLeg {
	h.legMu.Lock()
	defer h.legMu.Unlock()
	return h.legs[id]
}

// setEnd makes the leg usable.
func (l *streamLeg) setEnd(e streamEnd) {
	l.mu.Lock()
	if l.finished || l.failed {
		l.mu.Unlock()
		e.abort()
		return
	}
	l.end = e
	close(l.ready)
	l.mu.Unlock()
}

// fail ends a leg that never became usable.
func (l *streamLeg) fail() {
	l.mu.Lock()
	if l.end == nil && !l.failed {
		l.failed = true
		close(l.ready)
	}
	l.mu.Unlock()
	l.finish()
}

// finish releases the leg and reports it to the node, once.
func (l *streamLeg) finish() {
	l.mu.Lock()
	if l.finished {
		l.mu.Unlock()
		return
	}
	l.finished = true
	if l.end == nil && !l.failed {
		l.failed = true
		close(l.ready)
	}
	e := l.end
	close(l.done)
	l.mu.Unlock()
	if e != nil {
		e.abort()
	}
	h := l.h
	h.legMu.Lock()
	delete(h.legs, l.id)
	h.legMu.Unlock()
	h.Do(func() { h.node.OnStreamLegClosed(l.id) })
}

// usable waits until the leg is ready and returns its end (nil if it
// failed).
func (l *streamLeg) usable(timeout time.Duration) streamEnd {
	select {
	case <-l.ready:
	case <-time.After(timeout):
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.finished {
		return nil
	}
	return l.end
}

// closeWith closes the leg (and its splice partner) with a code.
func (l *streamLeg) closeWith(code int, reason string) {
	l.mu.Lock()
	p := l.pair
	l.mu.Unlock()
	if p != nil {
		p.end(func() {
			var wg sync.WaitGroup
			for _, x := range []*streamLeg{p.a, p.b} {
				wg.Add(1)
				go func() {
					defer wg.Done()
					if e := x.usable(closeWait); e != nil {
						e.sendClose(code, reason)
					}
				}()
			}
			wg.Wait()
		})
		return
	}
	if e := l.usable(closeWait); e != nil {
		e.sendClose(code, reason)
	}
	l.finish()
}

// splicePair is two spliced legs. The first decision about how it ends
// wins; then both legs finish.
type splicePair struct {
	a, b *streamLeg
	once sync.Once
}

func (p *splicePair) end(notify func()) {
	p.once.Do(func() {
		notify()
		p.a.finish()
		p.b.finish()
	})
}

func (p *splicePair) run() {
	ea, eb := p.a.usable(attachWait), p.b.usable(attachWait)
	if ea == nil || eb == nil {
		p.end(func() {
			for _, e := range []streamEnd{ea, eb} {
				if e != nil {
					e.sendClose(wire.ClosePeerLost, wire.ReasonPeerLost)
				}
			}
		})
		return
	}
	for _, x := range []struct {
		l *streamLeg
		e streamEnd
	}{{p.a, ea}, {p.b, eb}} {
		if w, ok := x.e.(*wsEnd); ok {
			go w.pinger(x.l.done)
		}
	}
	go p.pump(ea, eb)
	go p.pump(eb, ea)
}

// pump copies src to dst. Only reads decide how the stream ends: a write
// failure means dst is gone, and the pump reading dst reports that.
func (p *splicePair) pump(src, dst streamEnd) {
	buf := make([]byte, peerproto.MaxChunk)
	for {
		c, err := src.read(buf)
		if err != nil {
			p.end(func() { dst.sendClose(wire.ClosePeerLost, wire.ReasonPeerLost) })
			return
		}
		if c.close {
			p.end(func() { dst.sendClose(c.code, c.reason) })
			return
		}
		if dst.write(c) != nil {
			return
		}
	}
}

// --- WebSocket ends ---

type wsEnd struct {
	c      *websocket.Conn
	ctx    context.Context
	cancel context.CancelFunc
	r      io.Reader
	rbin   bool
	w      io.WriteCloser
}

func newWSEnd(c *websocket.Conn) *wsEnd {
	ctx, cancel := context.WithCancel(context.Background())
	return &wsEnd{c: c, ctx: ctx, cancel: cancel}
}

func (e *wsEnd) read(buf []byte) (chunk, error) {
	if e.r == nil {
		typ, r, err := e.c.Reader(e.ctx)
		if err != nil {
			return e.closed(err)
		}
		e.r, e.rbin = r, typ == websocket.MessageBinary
	}
	n, err := io.ReadFull(e.r, buf)
	switch err {
	case nil:
		return chunk{binary: e.rbin, data: buf[:n]}, nil
	case io.EOF, io.ErrUnexpectedEOF:
		e.r = nil
		return chunk{binary: e.rbin, data: buf[:n], fin: true}, nil
	}
	return e.closed(err)
}

// closed turns a read error into a relayed close (the endpoint closed
// cleanly) or a failure.
func (e *wsEnd) closed(err error) (chunk, error) {
	var ce websocket.CloseError
	if errors.As(err, &ce) {
		return chunk{close: true, code: wire.EndpointCloseCode(int(ce.Code)), reason: ce.Reason}, nil
	}
	return chunk{}, err
}

func (e *wsEnd) write(c chunk) error {
	if e.w == nil {
		typ := websocket.MessageText
		if c.binary {
			typ = websocket.MessageBinary
		}
		w, err := e.c.Writer(e.ctx, typ)
		if err != nil {
			return err
		}
		e.w = w
	}
	if len(c.data) > 0 {
		if _, err := e.w.Write(c.data); err != nil {
			return err
		}
	}
	if c.fin {
		err := e.w.Close()
		e.w = nil
		return err
	}
	return nil
}

func (e *wsEnd) sendClose(code int, reason string) {
	if len(reason) > 123 {
		reason = reason[:123]
	}
	e.c.Close(websocket.StatusCode(code), reason)
}

func (e *wsEnd) abort() {
	e.cancel()
	e.c.CloseNow()
}

// pinger keeps a paired data socket alive (liveness is per hop).
func (e *wsEnd) pinger(done chan struct{}) {
	t := time.NewTicker(pingEvery)
	defer t.Stop()
	for {
		select {
		case <-done:
			return
		case <-t.C:
			ctx, cancel := context.WithTimeout(e.ctx, pongTimeout)
			err := e.c.Ping(ctx)
			cancel()
			if err != nil {
				e.c.CloseNow()
				return
			}
		}
	}
}

// --- mesh data connection ends ---

type meshEnd struct {
	c   net.Conn
	br  *bufio.Reader
	wmu sync.Mutex
	bw  *bufio.Writer
}

func newMeshEnd(c net.Conn, br *bufio.Reader) *meshEnd {
	keepAlive(c)
	return &meshEnd{c: c, br: br, bw: bufio.NewWriterSize(c, peerproto.MaxChunk+16)}
}

func keepAlive(c net.Conn) {
	if k, ok := c.(interface {
		SetKeepAlive(bool) error
		SetKeepAlivePeriod(time.Duration) error
	}); ok {
		k.SetKeepAlive(true)
		k.SetKeepAlivePeriod(pingEvery)
	}
}

func (e *meshEnd) read(buf []byte) (chunk, error) {
	flags, data, err := peerproto.ReadChunk(e.br, buf)
	if err != nil {
		return chunk{}, err
	}
	if flags&peerproto.ChunkClose != 0 {
		code, reason := peerproto.ParseCloseBody(data)
		return chunk{close: true, code: int(code), reason: reason}, nil
	}
	return chunk{binary: flags&peerproto.ChunkBinary != 0, fin: flags&peerproto.ChunkFin != 0, data: data}, nil
}

func (e *meshEnd) write(c chunk) error {
	var flags byte
	if c.binary {
		flags |= peerproto.ChunkBinary
	}
	if c.fin {
		flags |= peerproto.ChunkFin
	}
	e.wmu.Lock()
	defer e.wmu.Unlock()
	if err := peerproto.WriteChunk(e.bw, flags, c.data); err != nil {
		return err
	}
	return e.bw.Flush()
}

func (e *meshEnd) sendClose(code int, reason string) {
	e.wmu.Lock()
	defer e.wmu.Unlock()
	e.c.SetWriteDeadline(time.Now().Add(closeWait))
	if peerproto.WriteChunk(e.bw, peerproto.ChunkClose, peerproto.CloseBody(uint16(code), reason)) == nil {
		e.bw.Flush()
	}
}

func (e *meshEnd) abort() { e.c.Close() }

// --- node.Env ---

func (e *env) AnswerAttach(id node.StreamLeg, code node.AttachCode) {
	if l := e.h().leg(id); l != nil {
		select {
		case l.answer <- code:
		default:
		}
	}
}

func (e *env) AnswerStreamPeer(id node.StreamLeg, st node.PeerStatus) {
	if l := e.h().leg(id); l != nil {
		select {
		case l.peerAnswer <- st:
		default:
		}
	}
}

func (e *env) DialStreamPeer(to ids.BrokerID, id ids.MsgID) node.StreamLeg {
	h := e.h()
	l := h.newLeg()
	go h.dialStream(l, to, id)
	return l.id
}

func (e *env) SpliceStream(a, b node.StreamLeg) {
	h := e.h()
	la, lb := h.leg(a), h.leg(b)
	if la == nil || lb == nil {
		// One leg is already gone: the other closes as peer_lost.
		for _, l := range []*streamLeg{la, lb} {
			if l != nil {
				go l.closeWith(wire.ClosePeerLost, wire.ReasonPeerLost)
			}
		}
		return
	}
	p := &splicePair{a: la, b: lb}
	for _, l := range []*streamLeg{la, lb} {
		l.mu.Lock()
		l.pair = p
		l.mu.Unlock()
	}
	go p.run()
}

func (e *env) CloseStreamLeg(id node.StreamLeg, code int, reason string) {
	if l := e.h().leg(id); l != nil {
		go l.closeWith(code, reason)
	}
}

// --- data connections over the mesh ---

// dialStream opens a data connection for stream id to the holder's Broker.
func (h *Host) dialStream(l *streamLeg, to ids.BrokerID, id ids.MsgID) {
	st := node.PeerUnreachable
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	c, err := h.opts.Mesh.Dial(ctx, to)
	cancel()
	if err == nil {
		c.SetDeadline(time.Now().Add(streamStatusIO))
		err = peerproto.WriteStreamHeader(c, peerproto.StreamHeader{ID: id})
		var b [1]byte
		if err == nil {
			_, err = io.ReadFull(c, b[:])
		}
		if err == nil && b[0] >= byte(node.PeerPaired) && b[0] <= byte(node.PeerBusy) {
			st = node.PeerStatus(b[0])
		}
		c.SetDeadline(time.Time{})
		if st == node.PeerPaired {
			l.setEnd(newMeshEnd(c, bufio.NewReaderSize(c, peerproto.MaxChunk+16)))
		} else {
			c.Close()
		}
	}
	h.Do(func() { h.node.OnStreamDialed(l.id, st) })
	if st != node.PeerPaired {
		l.fail()
	}
}

// serveStreamPeer serves an inbound data connection (after its preamble).
func (h *Host) serveStreamPeer(c net.Conn, br *bufio.Reader, from ids.BrokerID) {
	c.SetReadDeadline(time.Now().Add(streamStatusIO))
	hdr, err := peerproto.ReadStreamHeader(br)
	c.SetReadDeadline(time.Time{})
	if err != nil {
		c.Close()
		return
	}
	l := h.newLeg()
	h.Do(func() { h.node.OnStreamPeer(l.id, from, hdr.ID) })
	var st node.PeerStatus
	select {
	case st = <-l.peerAnswer:
	case <-time.After(streamStatusIO):
	case <-h.done:
	}
	if st == 0 {
		st = node.PeerBusy
	}
	c.SetWriteDeadline(time.Now().Add(streamStatusIO))
	_, err = c.Write([]byte{byte(st)})
	c.SetWriteDeadline(time.Time{})
	if err != nil || st != node.PeerPaired {
		c.Close()
		l.fail()
		return
	}
	l.setEnd(newMeshEnd(c, br))
}

// --- endpoint data sockets ---

func headerHas(r *http.Request, name, token string) bool {
	for _, v := range r.Header.Values(name) {
		for _, p := range strings.Split(v, ",") {
			if strings.EqualFold(strings.TrimSpace(p), token) {
				return true
			}
		}
	}
	return false
}

// serveStream serves GET /v1/stream?token=...: the token is checked (and,
// for a target, the halves paired) before the upgrade, so a 101 means the
// socket is part of a stream.
func (h *Host) serveStream(w http.ResponseWriter, r *http.Request) {
	if !headerHas(r, "Upgrade", "websocket") || !headerHas(r, "Sec-WebSocket-Protocol", wire.StreamSubprotocol) {
		http.Error(w, "WebSocket with subprotocol "+wire.StreamSubprotocol+" required", http.StatusBadRequest)
		return
	}
	hash := node.StreamTokenHash(r.URL.Query().Get("token"))
	ip := clientIP(r)
	l := h.newLeg()
	h.Do(func() { h.node.OnStreamAttach(l.id, hash, ip) })
	code := node.AttachUnavailable
	select {
	case code = <-l.answer:
	case <-time.After(attachWait):
	case <-h.done:
	}
	if code != node.AttachOK {
		st := map[node.AttachCode]int{
			node.AttachUnauthorized: http.StatusUnauthorized,
			node.AttachForbidden:    http.StatusForbidden,
			node.AttachUnavailable:  http.StatusServiceUnavailable,
			node.AttachRateLimited:  http.StatusTooManyRequests,
		}[code]
		http.Error(w, http.StatusText(st), st)
		l.fail()
		return
	}
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols:       []string{wire.StreamSubprotocol},
		InsecureSkipVerify: true,
	})
	if err != nil {
		l.fail()
		return
	}
	c.SetReadLimit(-1)
	l.setEnd(newWSEnd(c))
}
