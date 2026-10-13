package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/afk-surf/nimbo/pkg/wire"
)

// Streams (design §9.7): a full-duplex WebSocket pipe to the target of a cap
// with allow_stream. The holder opens it with OpenStream; the target gets an
// offer (a Delivery with Stream set) and accepts it with AcceptStream,
// refuses it with RejectStream, or ignores it. Each side's data socket goes
// to its own Broker, at the same authority as its session.

// StreamOptions configure OpenStream. The payload, carried caps and reply
// options are those of an ordinary send; the offer is delivered like a
// message.
type StreamOptions struct {
	Payload any
	Caps    []string
	TTL     time.Duration // how long the offer may wait for delivery
	Reply   *wire.ReplyOpts
	// Detached opens the stream without attaching the data socket:
	// Stream.Token holds the token, and Attach attaches later (within the
	// offer's TTL plus the attach window).
	Detached bool
}

// Stream is one side of an established or pending stream.
type Stream struct {
	ID      string
	ReplyID string // set if the cap has auto_reply
	// Token is the holder's data-socket token of a detached stream.
	Token string
	// Conn is the data socket. Messages keep their type and boundaries.
	Conn   *websocket.Conn
	status chan wire.Out // holder only

	mu   sync.Mutex
	done bool
	err  error
}

// StreamError is a stream that failed before the halves paired.
type StreamError struct {
	Reason string // ttl_expired, receiver_busy, target_gone, unavailable, peer_attach_timeout, ...
	Detail string
}

func (e *StreamError) Error() string {
	if e.Detail != "" {
		return "nimbo: stream failed: " + e.Reason + ": " + e.Detail
	}
	return "nimbo: stream failed: " + e.Reason
}

// AttachError is an HTTP refusal of a data socket: 401 for an unknown, used
// or expired token (or the wrong Broker), 403 when the holder's half is
// gone, 503 when the holder's Broker is unreachable, 429 when rate limited.
type AttachError struct{ Status int }

func (e *AttachError) Error() string {
	return fmt.Sprintf("nimbo: stream attach refused (HTTP %d)", e.Status)
}

// ErrStreamStatusLost means the session that opened the stream closed before
// the stream's status arrived; the data socket still tells the outcome.
var ErrStreamStatusLost = errors.New("nimbo: session closed before the stream status")

// streamURL is the data-socket URL on the session's authority.
func (c *Client) streamURL(token string) (string, error) {
	u, err := url.Parse(c.opts.URL)
	if err != nil {
		return "", err
	}
	if strings.HasSuffix(u.Path, "/v1/ws") {
		u.Path = strings.TrimSuffix(u.Path, "/v1/ws") + "/v1/stream"
	} else {
		u.Path = "/v1/stream"
	}
	u.RawQuery = url.Values{"token": {token}}.Encode()
	return u.String(), nil
}

func (c *Client) attach(ctx context.Context, token string) (*websocket.Conn, error) {
	u, err := c.streamURL(token)
	if err != nil {
		return nil, err
	}
	dopts := &websocket.DialOptions{}
	if c.opts.DialOptions != nil {
		*dopts = *c.opts.DialOptions
	}
	dopts.Subprotocols = []string{wire.StreamSubprotocol}
	conn, resp, err := websocket.Dial(ctx, u, dopts)
	if err != nil {
		if resp != nil && resp.StatusCode != 101 {
			return nil, &AttachError{Status: resp.StatusCode}
		}
		return nil, err
	}
	conn.SetReadLimit(-1)
	return conn, nil
}

// OpenStream asks the target of capID for a stream and attaches this
// endpoint's data socket. Messages written before the target accepts wait
// in the socket's buffers. WaitAccepted reports the outcome.
func (c *Client) OpenStream(ctx context.Context, capID string, o StreamOptions) (*Stream, error) {
	payload := o.Payload
	if payload == nil {
		payload = map[string]any{}
	}
	pb, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	in := &wire.In{Op: "stream", Cap: capID, Payload: pb, Caps: o.Caps, Reply: o.Reply}
	if o.TTL > 0 {
		in.TTLms = o.TTL.Milliseconds()
	}
	r, err := c.request(ctx, in)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	status := c.streams[r.Stream]
	c.mu.Unlock()
	if status == nil {
		// The session closed between the result and now.
		status = make(chan wire.Out)
		close(status)
	}
	st := &Stream{ID: r.Stream, ReplyID: r.ReplyID, Token: r.Token, status: status}
	if o.Detached {
		return st, nil
	}
	return st, c.Attach(ctx, st)
}

// Attach attaches the data socket of a stream opened with Detached.
func (c *Client) Attach(ctx context.Context, st *Stream) error {
	if st.Conn != nil || st.Token == "" {
		return errors.New("nimbo: stream already attached")
	}
	conn, err := c.attach(ctx, st.Token)
	if err != nil {
		return err
	}
	st.Conn, st.Token = conn, ""
	return nil
}

// WaitAccepted waits for the stream's status on the holder's session: nil
// once the target attached, a *StreamError if the stream failed first. On
// the target's side it returns nil at once.
func (s *Stream) WaitAccepted(ctx context.Context) error {
	if s.status == nil {
		return nil
	}
	s.mu.Lock()
	if s.done {
		s.mu.Unlock()
		return s.err
	}
	s.mu.Unlock()
	var o wire.Out
	var ok bool
	select {
	case o, ok = <-s.status:
	case <-ctx.Done():
		return ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done { // another waiter took the status
		return s.err
	}
	s.done = true
	switch {
	case !ok:
		s.err = ErrStreamStatusLost
	case o.State != wire.StreamAccepted:
		s.err = &StreamError{Reason: o.Reason, Detail: o.Detail}
	}
	return s.err
}

// AcceptStream accepts a stream offer by attaching to this endpoint's
// Broker with the offer's token. It returns once the halves are paired.
func (c *Client) AcceptStream(ctx context.Context, d Delivery) (*Stream, error) {
	if d.StreamToken == "" {
		return nil, errors.New("nimbo: delivery is not a stream offer")
	}
	conn, err := c.attach(ctx, d.StreamToken)
	if err != nil {
		return nil, err
	}
	return &Stream{ID: d.Stream, Conn: conn}, nil
}

// RejectStream refuses a stream offer: it attaches and closes at once with
// 4901 (stream_rejected) and the reason, which the holder's data socket
// receives.
func (c *Client) RejectStream(ctx context.Context, d Delivery, reason string) error {
	st, err := c.AcceptStream(ctx, d)
	if err != nil {
		return err
	}
	return st.Close(wire.CloseStreamRejected, reason)
}

// Read reads one whole message.
func (s *Stream) Read(ctx context.Context) (websocket.MessageType, []byte, error) {
	return s.Conn.Read(ctx)
}

// Write writes one whole message.
func (s *Stream) Write(ctx context.Context, typ websocket.MessageType, p []byte) error {
	return s.Conn.Write(ctx, typ, p)
}

// Reader returns the next message as a stream of bytes.
func (s *Stream) Reader(ctx context.Context) (websocket.MessageType, io.Reader, error) {
	return s.Conn.Reader(ctx)
}

// Writer starts a message of any size.
func (s *Stream) Writer(ctx context.Context, typ websocket.MessageType) (io.WriteCloser, error) {
	return s.Conn.Writer(ctx, typ)
}

// NetConn exposes the stream as a net.Conn of binary messages.
func (s *Stream) NetConn(ctx context.Context) net.Conn {
	return websocket.NetConn(ctx, s.Conn, websocket.MessageBinary)
}

// Close closes the stream; the other side receives the code and reason
// (1000 and 3000-4899 pass through, 4901 refuses).
func (s *Stream) Close(code int, reason string) error {
	return s.Conn.Close(websocket.StatusCode(code), reason)
}

// CloseStatus returns the close code of an error from a stream read, or -1.
func CloseStatus(err error) int { return int(websocket.CloseStatus(err)) }
