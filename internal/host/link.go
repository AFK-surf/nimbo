package host

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"time"

	"github.com/afk-surf/nimbo/internal/ids"
	"github.com/afk-surf/nimbo/internal/peerproto"
)

// Peer links (design §5): frames to one Broker go over two lazily dialed
// streams, one for messages and one for RCR traffic, so a handoff burst
// cannot hold up messages. Each stream preserves order.

const (
	classMsg = 0
	classRCR = 1

	linkIdle    = 2 * time.Minute
	dialTimeout = 1500 * time.Millisecond // below the RPC timeout, so dial failures are "not sent"
	linkQueue   = 1 << 15
)

type linkKey struct {
	to    ids.BrokerID
	class int
}

type link struct {
	h    *Host
	key  linkKey
	q    chan *peerproto.Envelope
	quit chan struct{}
}

func classOf(e *peerproto.Envelope) int {
	if e.Forward != nil || e.ForwardResult != nil || e.Bounce != nil {
		return classMsg
	}
	return classRCR
}

// sendPeer runs on the loop.
func (h *Host) sendPeer(to ids.BrokerID, m *peerproto.Envelope) {
	k := linkKey{to, classOf(m)}
	l := h.links[k]
	if l == nil {
		l = &link{h: h, key: k, q: make(chan *peerproto.Envelope, linkQueue), quit: make(chan struct{})}
		h.links[k] = l
		go l.run()
	}
	select {
	case l.q <- m:
	default:
		h.defer0(func() { h.node.OnPeerSendFailed(to, m) })
	}
}

func (l *link) stop() {
	select {
	case <-l.quit:
	default:
		close(l.quit)
	}
}

// notSent reports frames that were never written.
func (l *link) notSent(ms ...*peerproto.Envelope) {
	l.h.Do(func() {
		for _, m := range ms {
			l.h.node.OnPeerSendFailed(l.key.to, m)
		}
	})
}

func (l *link) drainQueued() []*peerproto.Envelope {
	var out []*peerproto.Envelope
	for {
		select {
		case m := <-l.q:
			out = append(out, m)
		default:
			return out
		}
	}
}

func (l *link) run() {
	var conn net.Conn
	var bw *bufio.Writer
	idle := time.NewTimer(linkIdle)
	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()
	for {
		select {
		case <-l.quit:
			return
		case <-idle.C:
			if conn != nil {
				conn.Close()
				conn = nil
			}
			idle.Reset(linkIdle)
		case m := <-l.q:
			if conn == nil {
				ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
				c, err := l.h.opts.Mesh.Dial(ctx, l.key.to)
				cancel()
				if err == nil {
					c.SetWriteDeadline(time.Now().Add(5 * time.Second))
					_, err = c.Write(peerproto.Preamble[:])
					if err != nil {
						c.Close()
					}
				}
				if err != nil {
					l.notSent(append([]*peerproto.Envelope{m}, l.drainQueued()...)...)
					continue
				}
				conn, bw = c, bufio.NewWriterSize(c, 64<<10)
				go discard(c)
			}
			idle.Reset(linkIdle)
			err := l.write(conn, bw, m)
			if err != nil {
				// The frame may or may not have left: unknown, so it is not
				// reported. The next frame redials.
				conn.Close()
				conn = nil
			}
		}
	}
}

// write writes m and everything already queued, then flushes.
func (l *link) write(conn net.Conn, bw *bufio.Writer, m *peerproto.Envelope) error {
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	for {
		if err := peerproto.WriteFrame(bw, m); err != nil {
			return err
		}
		select {
		case m = <-l.q:
			continue
		default:
		}
		return bw.Flush()
	}
}

// discard reads the (unused) inbound half of an outbound stream so a peer
// closing it is noticed.
func discard(c net.Conn) {
	io.Copy(io.Discard, c)
	c.Close()
}

// acceptPeers serves inbound peer streams.
func (h *Host) acceptPeers() {
	for {
		c, from, err := h.opts.Mesh.Accept()
		if err != nil {
			select {
			case <-h.done:
				return
			default:
			}
			if errors.Is(err, net.ErrClosed) {
				return
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		go h.servePeer(c, from)
	}
}

func (h *Host) servePeer(c net.Conn, from ids.BrokerID) {
	defer c.Close()
	br := bufio.NewReaderSize(c, 64<<10)
	var pre [4]byte
	c.SetReadDeadline(time.Now().Add(10 * time.Second))
	if _, err := io.ReadFull(br, pre[:]); err != nil || pre != peerproto.Preamble {
		return
	}
	for {
		c.SetReadDeadline(time.Now().Add(linkIdle + time.Minute))
		e, err := peerproto.ReadFrame(br)
		if err != nil {
			return
		}
		h.Do(func() { h.node.OnPeerMessage(from, e) })
	}
}
