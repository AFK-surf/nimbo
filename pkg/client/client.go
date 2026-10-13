// Package client is the Go endpoint SDK for Nimbo (design §6). An Endpoint
// is identified by an X25519 key; it connects to a Broker over WebSocket,
// authenticates with the handshake of §6.2, and then mints, transfers,
// renews and sends on caps. The client reconnects with jittered
// exponential backoff and honours closing.retry_after_ms.
package client

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/url"
	"sync"
	"time"

	"github.com/coder/websocket"
	"golang.org/x/crypto/curve25519"

	"github.com/afk-surf/nimbo/internal/ids"
	"github.com/afk-surf/nimbo/pkg/wire"
)

// Key is an endpoint's X25519 key pair.
type Key struct {
	Private [32]byte
	Public  [32]byte
}

// GenerateKey creates a new ephemeral endpoint key.
func GenerateKey() (Key, error) {
	var k Key
	if _, err := rand.Read(k.Private[:]); err != nil {
		return k, err
	}
	pub, err := curve25519.X25519(k.Private[:], curve25519.Basepoint)
	if err != nil {
		return k, err
	}
	copy(k.Public[:], pub)
	return k, nil
}

// PublicString is the endpoint key in base32, as used in transfer.to.
func (k Key) PublicString() string { return ids.B32.EncodeToString(k.Public[:]) }

// Error is a request failure reported by the Broker.
type Error struct {
	Code   string
	Detail string
}

func (e *Error) Error() string {
	if e.Detail != "" {
		return e.Code + ": " + e.Detail
	}
	return e.Code
}

// ErrClosed is returned for requests whose session closed before a result.
var ErrClosed = errors.New("nimbo: session closed before result")

// Delivery is a message received on one of the endpoint's caps.
type Delivery struct {
	Msg     string
	On      string // lineage ID: the minted cap or reply_id it arrived on
	Payload json.RawMessage
	Caps    []string // new cap IDs, now held by this endpoint
	Reply   string   // single-use reply cap, if the cap has auto_reply
	// Stream and StreamToken are set on a stream offer: AcceptStream or
	// RejectStream answers it, or ignoring it lets it expire.
	Stream      string
	StreamToken string
}

// Bounce reports an undeliverable message this endpoint sent.
type Bounce struct {
	Msg    string
	Reason string
}

// Options configure a Client.
type Options struct {
	// URL is wss://host[:port]/v1/ws.
	URL string
	Key Key
	// Authority overrides the host[:port] bound into the handshake proof.
	Authority string
	// MaxMessageBytes advertises the largest payload this endpoint accepts.
	MaxMessageBytes int
	// OnDeliver and OnBounce are called from the client's reader goroutine.
	OnDeliver func(Delivery)
	OnBounce  func(Bounce)
	// Dial overrides websocket dial options (e.g. a custom HTTP client).
	DialOptions *websocket.DialOptions
	ClientName  string
}

// Client is a reconnecting endpoint session.
type Client struct {
	opts Options

	mu      sync.Mutex
	conn    *websocket.Conn
	ready   chan struct{} // closed when a session is established
	rid     uint64
	pending map[uint64]chan *wire.Out
	closed  bool
	cancel  context.CancelFunc
	welcome *wire.Out
	// streams this session opened, awaiting their stream_status
	streams map[string]chan wire.Out
}

// Dial connects and keeps the session up until Close.
func Dial(ctx context.Context, opts Options) (*Client, error) {
	if opts.ClientName == "" {
		opts.ClientName = "nimbo-go/1"
	}
	c := &Client{opts: opts, ready: make(chan struct{}), pending: map[uint64]chan *wire.Out{}, streams: map[string]chan wire.Out{}}
	rctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	first := make(chan error, 1)
	go c.loop(rctx, first)
	select {
	case err := <-first:
		if err != nil {
			cancel()
			return nil, err
		}
		return c, nil
	case <-ctx.Done():
		cancel()
		return nil, ctx.Err()
	}
}

// Close ends the session for good.
func (c *Client) Close() error {
	c.mu.Lock()
	c.closed = true
	conn := c.conn
	c.mu.Unlock()
	c.cancel()
	if conn != nil {
		return conn.Close(websocket.StatusNormalClosure, "")
	}
	return nil
}

// Session returns the current session ID.
func (c *Client) Session() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.welcome == nil {
		return ""
	}
	return c.welcome.Session
}

func jitter(d time.Duration) time.Duration {
	n, _ := rand.Int(rand.Reader, big.NewInt(int64(d)+1))
	return d/2 + time.Duration(n.Int64())/2
}

func (c *Client) loop(ctx context.Context, first chan error) {
	backoff := 250 * time.Millisecond
	for {
		retryAfter, err := c.session(ctx, first)
		first = nil
		if ctx.Err() != nil {
			return
		}
		wait := jitter(backoff)
		if retryAfter > 0 {
			wait = retryAfter
		}
		if err == nil {
			backoff = 250 * time.Millisecond
		} else {
			backoff = min(2*backoff, 30*time.Second)
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return
		}
	}
}

// session runs one connection until it ends.
func (c *Client) session(ctx context.Context, first chan error) (time.Duration, error) {
	fail := func(err error) (time.Duration, error) {
		if first != nil {
			first <- err
		}
		return 0, err
	}
	u, err := url.Parse(c.opts.URL)
	if err != nil {
		return fail(err)
	}
	authority := c.opts.Authority
	if authority == "" {
		authority = u.Host
	}
	dopts := &websocket.DialOptions{}
	if c.opts.DialOptions != nil {
		*dopts = *c.opts.DialOptions
	}
	dopts.Subprotocols = []string{wire.Subprotocol}
	dctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	conn, _, err := websocket.Dial(dctx, c.opts.URL, dopts)
	cancel()
	if err != nil {
		return fail(err)
	}
	conn.SetReadLimit(1 << 20)
	defer conn.CloseNow()

	read := func() (*wire.Out, error) {
		_, b, err := conn.Read(ctx)
		if err != nil {
			return nil, err
		}
		var o wire.Out
		if err := json.Unmarshal(b, &o); err != nil {
			return nil, err
		}
		return &o, nil
	}
	write := func(v any) error {
		b, _ := json.Marshal(v)
		return conn.Write(ctx, websocket.MessageText, b)
	}
	ch, err := read()
	if err != nil {
		return fail(err)
	}
	if ch.Op == "closing" {
		return fail(&Error{Code: "closing", Detail: ch.Reason})
	}
	if ch.Op != "challenge" {
		return fail(fmt.Errorf("nimbo: expected challenge, got %q", ch.Op))
	}
	eph, err1 := ids.B32.DecodeString(ch.Eph)
	nonce, err2 := ids.B32.DecodeString(ch.Nonce)
	if err1 != nil || err2 != nil || len(eph) != 32 {
		return fail(errors.New("nimbo: bad challenge"))
	}
	ss, err := curve25519.X25519(c.opts.Key.Private[:], eph)
	if err != nil {
		return fail(err)
	}
	proof := wire.Proof(ss, nonce, authority, c.opts.Key.Public[:], eph)
	if err := write(wire.In{Op: "hello", Key: c.opts.Key.PublicString(), Authority: authority, Proof: ids.B32.EncodeToString(proof), Client: c.opts.ClientName, MaxMessageBytes: c.opts.MaxMessageBytes}); err != nil {
		return fail(err)
	}
	w, err := read()
	if err != nil {
		return fail(err)
	}
	if w.Op != "welcome" {
		if w.Op == "closing" {
			return fail(&Error{Code: "closing", Detail: w.Reason})
		}
		return fail(fmt.Errorf("nimbo: handshake rejected (%s)", w.Op))
	}
	c.mu.Lock()
	c.conn, c.welcome = conn, w
	close(c.ready)
	c.mu.Unlock()
	if first != nil {
		first <- nil
	}
	var retryAfter time.Duration
	defer func() {
		c.mu.Lock()
		c.conn = nil
		c.ready = make(chan struct{})
		for rid, p := range c.pending {
			close(p)
			delete(c.pending, rid)
		}
		// stream_status goes only to the session that opened the stream.
		for id, ch := range c.streams {
			close(ch)
			delete(c.streams, id)
		}
		c.mu.Unlock()
	}()
	for {
		o, err := read()
		if err != nil {
			return retryAfter, nil
		}
		switch o.Op {
		case "result":
			c.mu.Lock()
			p := c.pending[o.Rid]
			delete(c.pending, o.Rid)
			if o.OK && o.Stream != "" {
				// Before the result is handed over, so the status (which
				// follows it on the session) always finds its channel.
				c.streams[o.Stream] = make(chan wire.Out, 1)
			}
			c.mu.Unlock()
			if p != nil {
				p <- o
			}
		case "deliver":
			if c.opts.OnDeliver != nil {
				c.opts.OnDeliver(Delivery{Msg: o.Msg, On: o.On, Payload: o.Payload, Caps: o.Caps, Reply: o.Reply, Stream: o.Stream, StreamToken: o.StreamToken})
			}
		case "stream_status":
			c.mu.Lock()
			ch := c.streams[o.Stream]
			delete(c.streams, o.Stream)
			c.mu.Unlock()
			if ch != nil {
				ch <- *o
				close(ch)
			}
		case "bounce":
			if c.opts.OnBounce != nil {
				c.opts.OnBounce(Bounce{Msg: o.Msg, Reason: o.Reason})
			}
		case "closing":
			retryAfter = time.Duration(o.RetryAfterMs) * time.Millisecond
		}
	}
}

// request sends one Action and waits for its result. It waits for a live
// session first.
func (c *Client) request(ctx context.Context, in *wire.In) (*wire.Out, error) {
	for {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return nil, errClientClosed
		}
		conn, ready := c.conn, c.ready
		if conn == nil {
			c.mu.Unlock()
			select {
			case <-ready:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		c.rid++
		in.Rid = c.rid
		ch := make(chan *wire.Out, 1)
		c.pending[in.Rid] = ch
		c.mu.Unlock()
		b, _ := json.Marshal(in)
		if err := conn.Write(ctx, websocket.MessageText, b); err != nil {
			return nil, ErrClosed
		}
		select {
		case o, ok := <-ch:
			if !ok {
				return nil, ErrClosed
			}
			if !o.OK {
				if o.Error == nil {
					return nil, &Error{Code: "unknown"}
				}
				return o, &Error{Code: o.Error.Code, Detail: o.Error.Detail}
			}
			return o, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

var errClientClosed = errors.New("nimbo: client closed")

// MintOptions configure MintCap.
type MintOptions struct {
	Schema    json.RawMessage // optional JSON Schema
	Base      json.RawMessage // optional JSON object merged into payloads
	AutoReply bool
	Durable   bool
	// AllowStream lets holders of the cap open streams to this endpoint.
	AllowStream bool
}

// Mint creates a cap targeting and held by this endpoint.
func (c *Client) Mint(ctx context.Context, o MintOptions) (string, error) {
	r, err := c.request(ctx, &wire.In{Op: "mint", Schema: o.Schema, Base: o.Base, AutoReply: o.AutoReply, Durable: o.Durable, AllowStream: o.AllowStream})
	if err != nil {
		return "", err
	}
	return r.Cap, nil
}

// Transfer gives the holder of to a new cap for the same target.
func (c *Client) Transfer(ctx context.Context, capID, to string, durable bool) (string, error) {
	r, err := c.request(ctx, &wire.In{Op: "transfer", Cap: capID, To: to, Durable: durable})
	if err != nil {
		return "", err
	}
	return r.Cap, nil
}

// SendOptions configure SendMessage.
type SendOptions struct {
	Caps  []string // caps to carry to the receiver
	TTL   time.Duration
	Reply *wire.ReplyOpts
}

// Sent is the result of a send.
type Sent struct {
	Msg     string
	ReplyID string // set if the target cap has auto_reply
}

// Send sends a JSON object to the target of capID.
func (c *Client) Send(ctx context.Context, capID string, payload any, o SendOptions) (Sent, error) {
	pb, err := json.Marshal(payload)
	if err != nil {
		return Sent{}, err
	}
	in := &wire.In{Op: "send", Cap: capID, Payload: pb, Caps: o.Caps, Reply: o.Reply}
	if o.TTL > 0 {
		in.TTLms = o.TTL.Milliseconds()
	}
	r, err := c.request(ctx, in)
	if err != nil {
		return Sent{}, err
	}
	return Sent{Msg: r.Msg, ReplyID: r.ReplyID}, nil
}

// Renew refreshes caps' TTLs without new IDs.
func (c *Client) Renew(ctx context.Context, caps ...string) ([]wire.RenewItem, error) {
	r, err := c.request(ctx, &wire.In{Op: "renew", Caps: caps})
	if err != nil {
		return nil, err
	}
	return r.Renewed, nil
}

// Inspect returns a cap's record.
func (c *Client) Inspect(ctx context.Context, capID string) (*wire.CapInfo, error) {
	r, err := c.request(ctx, &wire.In{Op: "inspect", Cap: capID})
	if err != nil {
		return nil, err
	}
	return r.Info, nil
}
