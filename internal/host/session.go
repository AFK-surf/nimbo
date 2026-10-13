package host

import (
	"context"
	"net"
	"net/http"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/afk-surf/nimbo/internal/node"
	"github.com/afk-surf/nimbo/pkg/wire"
)

// Endpoint WebSocket sessions (design §6.1).

const (
	sessQueueMsgs  = 1000
	sessQueueBytes = 1 << 20
	pingEvery      = 20 * time.Second
	pongTimeout    = 40 * time.Second // a session is dropped after ~60s of silence
	readLimit      = 256 << 10
)

// Session is one endpoint connection.
type Session struct {
	h      *Host
	id     node.SessionID
	c      *websocket.Conn
	out    chan []byte
	bytes  atomic.Int64
	once   sync.Once
	flush  chan struct{} // closed: drain queue, then close
	closed chan struct{}
}

func (s *Session) enqueue(frame []byte) bool {
	if s.bytes.Load()+int64(len(frame)) > sessQueueBytes {
		return false
	}
	select {
	case s.out <- frame:
		s.bytes.Add(int64(len(frame)))
		return true
	default:
		return false
	}
}

func (s *Session) close() {
	s.once.Do(func() {
		close(s.closed)
		s.c.Close(websocket.StatusNormalClosure, "")
	})
}

func (s *Session) closeAfterFlush() {
	select {
	case <-s.flush:
	default:
		close(s.flush)
	}
}

func (s *Session) writer() {
	ping := time.NewTicker(pingEvery)
	defer ping.Stop()
	write := func(b []byte) bool {
		s.bytes.Add(-int64(len(b)))
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return s.c.Write(ctx, websocket.MessageText, b) == nil
	}
	for {
		select {
		case <-s.closed:
			return
		case b := <-s.out:
			if !write(b) {
				s.close()
				return
			}
		case <-s.flush:
			for {
				select {
				case b := <-s.out:
					if !write(b) {
						s.close()
						return
					}
				default:
					s.close()
					return
				}
			}
		case <-ping.C:
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), pongTimeout)
				defer cancel()
				if s.c.Ping(ctx) != nil {
					s.close()
				}
			}()
		}
	}
}

// ServeHTTP serves the endpoint WebSocket at /v1/ws and stream data
// sockets at /v1/stream.
func (h *Host) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/v1/stream" {
		h.serveStream(w, r)
		return
	}
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols:       []string{wire.Subprotocol},
		InsecureSkipVerify: true, // endpoints are not browsers; caps, not origins, grant authority
	})
	if err != nil {
		return
	}
	if c.Subprotocol() != wire.Subprotocol {
		c.Close(websocket.StatusPolicyViolation, "subprotocol nimbo.v1 required")
		return
	}
	c.SetReadLimit(readLimit)
	ip := clientIP(r)
	s := &Session{
		h:      h,
		id:     node.SessionID(h.nextSess.Add(1)),
		c:      c,
		out:    make(chan []byte, sessQueueMsgs),
		flush:  make(chan struct{}),
		closed: make(chan struct{}),
	}
	h.sessMu.Lock()
	h.sessions[s.id] = s
	h.sessMu.Unlock()
	go s.writer()
	h.Do(func() { h.node.OnSessionOpen(s.id, ip) })
	defer func() {
		h.sessMu.Lock()
		owned := h.sessions[s.id] == s
		delete(h.sessions, s.id)
		h.sessMu.Unlock()
		s.close()
		if owned {
			h.Do(func() { h.node.OnSessionClosed(s.id) })
		}
	}()
	ctx := context.Background()
	for {
		typ, data, err := c.Read(ctx)
		if err != nil {
			return
		}
		if typ != websocket.MessageText {
			c.Close(websocket.StatusUnsupportedData, "text frames only")
			return
		}
		h.Do(func() { h.node.OnSessionFrame(s.id, data) })
	}
}

// clientIP is the connection's peer address. Behind a load balancer the
// listener must be wrapped with PROXY protocol, so RemoteAddr is the client.
func clientIP(r *http.Request) netip.Addr {
	ap, err := netip.ParseAddrPort(r.RemoteAddr)
	if err != nil {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			return netip.Addr{}
		}
		a, _ := netip.ParseAddr(host)
		return a.Unmap()
	}
	return ap.Addr().Unmap()
}
