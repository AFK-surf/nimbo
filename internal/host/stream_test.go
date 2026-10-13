package host_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/afk-surf/nimbo/internal/node"
	"github.com/afk-surf/nimbo/pkg/client"
	"github.com/afk-surf/nimbo/pkg/wire"
)

// grant mints a cap on target (with allow_stream unless noStream) and
// transfers it to holder, retrying "unavailable" while the mesh warms up.
func grant(t *testing.T, ctx context.Context, target *client.Client, holder client.Key, allow bool) string {
	t.Helper()
	for {
		capID, err := target.Mint(ctx, client.MintOptions{Durable: true, AllowStream: allow})
		var granted string
		if err == nil {
			granted, err = target.Transfer(ctx, capID, holder.PublicString(), true)
		}
		if err == nil {
			return granted
		}
		if !strings.Contains(err.Error(), "unavailable") || ctx.Err() != nil {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// offer waits for a stream offer.
func offer(t *testing.T, ch chan client.Delivery) client.Delivery {
	t.Helper()
	d := recv(t, ch)
	if d.Stream == "" || d.StreamToken == "" {
		t.Fatalf("delivery is not a stream offer: %+v", d)
	}
	return d
}

func readMsg(t *testing.T, ctx context.Context, st *client.Stream) (websocket.MessageType, []byte) {
	t.Helper()
	typ, b, err := st.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return typ, b
}

// expectClose reads until the stream closes and checks the close code.
func expectClose(t *testing.T, ctx context.Context, st *client.Stream, code int, reason string) {
	t.Helper()
	for {
		_, _, err := st.Read(ctx)
		if err == nil {
			continue
		}
		var ce websocket.CloseError
		if !errors.As(err, &ce) {
			t.Fatalf("want close %d, got %v", code, err)
		}
		if int(ce.Code) != code || (reason != "" && ce.Reason != reason) {
			t.Fatalf("want close %d %q, got %d %q", code, reason, ce.Code, ce.Reason)
		}
		return
	}
}

func TestStreamSameBroker(t *testing.T) {
	bs := startCluster(t, 3)
	testStream(t, bs[0], bs[0])
}

func TestStreamCrossBroker(t *testing.T) {
	bs := startCluster(t, 3)
	testStream(t, bs[0], bs[1])
}

func TestStreamTailscaleMesh(t *testing.T) {
	if testing.Short() {
		t.Skip("starts userspace WireGuard engines")
	}
	bs := startClusterMesh(t, 3, true)
	testStream(t, bs[0], bs[2])
}

func testStream(t *testing.T, hb, tb *broker) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	holder, holderKey, _ := dial(t, hb.url)
	target, _, targetIn := dial(t, tb.url)
	granted := grant(t, ctx, target, holderKey, true)

	st, err := holder.OpenStream(ctx, granted, client.StreamOptions{Payload: map[string]any{"proto": "echo/1"}})
	if err != nil {
		t.Fatal(err)
	}
	// Written before the target accepts: waits in the socket's buffers.
	if err := st.Write(ctx, websocket.MessageText, []byte("early")); err != nil {
		t.Fatal(err)
	}
	d := offer(t, targetIn)
	if d.Stream != st.ID || !strings.Contains(string(d.Payload), "echo/1") {
		t.Fatalf("offer %+v for stream %s", d, st.ID)
	}
	ts, err := target.AcceptStream(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.WaitAccepted(ctx); err != nil {
		t.Fatal(err)
	}
	if typ, b := readMsg(t, ctx, ts); typ != websocket.MessageText || string(b) != "early" {
		t.Fatalf("got %v %q", typ, b)
	}
	// Mixed text and binary, both ways, boundaries kept.
	for i := 0; i < 20; i++ {
		typ := websocket.MessageText
		if i%2 == 1 {
			typ = websocket.MessageBinary
		}
		msg := bytes.Repeat([]byte{byte('a' + i)}, i*100)
		if err := ts.Write(ctx, typ, msg); err != nil {
			t.Fatal(err)
		}
		gt, gb := readMsg(t, ctx, st)
		if gt != typ || !bytes.Equal(gb, msg) {
			t.Fatalf("message %d: got %v (%d bytes)", i, gt, len(gb))
		}
		if err := st.Write(ctx, typ, msg); err != nil {
			t.Fatal(err)
		}
		gt, gb = readMsg(t, ctx, ts)
		if gt != typ || !bytes.Equal(gb, msg) {
			t.Fatalf("echo %d: got %v (%d bytes)", i, gt, len(gb))
		}
	}
	// A large message is relayed in chunks.
	size := 64 << 20
	if testing.Short() {
		size = 8 << 20
	}
	big := make([]byte, size)
	rand.Read(big)
	errc := make(chan error, 1)
	go func() { errc <- st.Write(ctx, websocket.MessageBinary, big) }()
	if typ, b := readMsg(t, ctx, ts); typ != websocket.MessageBinary || !bytes.Equal(b, big) {
		t.Fatalf("big message: %v, %d bytes", typ, len(b))
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	// The established stream outlives the holder's control session.
	holder.Close()
	if err := ts.Write(ctx, websocket.MessageText, []byte("still here")); err != nil {
		t.Fatal(err)
	}
	if _, b := readMsg(t, ctx, st); string(b) != "still here" {
		t.Fatalf("got %q", b)
	}
	// The close code and reason reach the other side.
	if err := ts.Close(3001, "bye"); err != nil {
		t.Fatal(err)
	}
	expectClose(t, ctx, st, 3001, "bye")
}

func TestStreamHolderAttachesLate(t *testing.T) {
	bs := startCluster(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	holder, holderKey, _ := dial(t, bs[0].url)
	target, _, targetIn := dial(t, bs[1].url)
	granted := grant(t, ctx, target, holderKey, true)
	st, err := holder.OpenStream(ctx, granted, client.StreamOptions{Detached: true})
	if err != nil {
		t.Fatal(err)
	}
	ts, err := target.AcceptStream(ctx, offer(t, targetIn))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.WaitAccepted(ctx); err != nil {
		t.Fatal(err)
	}
	// The target writes before the holder has a socket.
	if err := ts.Write(ctx, websocket.MessageText, []byte("first")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if err := holder.Attach(ctx, st); err != nil {
		t.Fatal(err)
	}
	if _, b := readMsg(t, ctx, st); string(b) != "first" {
		t.Fatalf("got %q", b)
	}
	st.Close(1000, "")
	expectClose(t, ctx, ts, 1000, "")
}

func TestStreamRefusals(t *testing.T) {
	var logs lockedBuffer
	bs := startClusterOpts(t, 3, false, func(c *node.Config) {
		c.Tunables.StreamAttachTTL = 2 * time.Second
	}, &logs)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	holder, holderKey, _ := dial(t, bs[0].url)
	target, _, targetIn := dial(t, bs[1].url)
	var tokens []string

	// A cap without allow_stream.
	plain := grant(t, ctx, target, holderKey, false)
	if _, err := holder.OpenStream(ctx, plain, client.StreamOptions{}); err == nil || !strings.Contains(err.Error(), wire.ErrStreamNotAllowed) {
		t.Fatalf("want stream_not_allowed, got %v", err)
	}
	granted := grant(t, ctx, target, holderKey, true)

	// Refusal: attach and close with 4901.
	st, err := holder.OpenStream(ctx, granted, client.StreamOptions{})
	if err != nil {
		t.Fatal(err)
	}
	d := offer(t, targetIn)
	tokens = append(tokens, d.StreamToken)
	if err := target.RejectStream(ctx, d, "busy"); err != nil {
		t.Fatal(err)
	}
	expectClose(t, ctx, st, wire.CloseStreamRejected, "busy")
	// The token is single use.
	var ae *client.AttachError
	if _, err := target.AcceptStream(ctx, d); !errors.As(err, &ae) || ae.Status != 401 {
		t.Fatalf("reused token: want 401, got %v", err)
	}
	// A token is valid only on its issuer: the holder's Broker does not
	// know the target's token.
	other, _, _ := dial(t, bs[0].url)
	if _, err := other.AcceptStream(ctx, client.Delivery{Stream: d.Stream, StreamToken: d.StreamToken}); !errors.As(err, &ae) || ae.Status != 401 {
		t.Fatalf("token on another Broker: want 401, got %v", err)
	}
	// A made-up token.
	if _, err := target.AcceptStream(ctx, client.Delivery{Stream: "x", StreamToken: "nope"}); !errors.As(err, &ae) || ae.Status != 401 {
		t.Fatalf("bad token: want 401, got %v", err)
	}

	// Ignored offer: the holder half expires (TTL + attach window).
	st, err = holder.OpenStream(ctx, granted, client.StreamOptions{TTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	d = offer(t, targetIn)
	tokens = append(tokens, d.StreamToken)
	var se *client.StreamError
	if err := st.WaitAccepted(ctx); !errors.As(err, &se) || se.Reason != wire.ReasonPeerAttachTimeout {
		t.Fatalf("ignored offer: want peer_attach_timeout, got %v", err)
	}
	expectClose(t, ctx, st, wire.ClosePeerAttachTimeout, "")
	// The target's token expired with its half.
	if _, err := target.AcceptStream(ctx, d); !errors.As(err, &ae) || ae.Status != 401 {
		t.Fatalf("expired token: want 401, got %v", err)
	}

	// An offer that cannot be delivered bounces: the stream fails.
	gone, goneKey, _ := dial(t, bs[2].url)
	g2 := grant(t, ctx, gone, holderKey, true)
	_ = goneKey
	gone.Close()
	time.Sleep(200 * time.Millisecond)
	st, err = holder.OpenStream(ctx, g2, client.StreamOptions{TTL: 500 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.WaitAccepted(ctx); !errors.As(err, &se) || se.Reason != wire.BounceTTLExpired {
		t.Fatalf("undeliverable offer: want ttl_expired, got %v", err)
	}
	expectClose(t, ctx, st, wire.CloseTTLExpired, "")

	if bytes.Contains(logs.Bytes(), []byte(st.ID)) {
		t.Log("stream IDs may appear in logs; tokens must not")
	}
	for _, tok := range tokens {
		if bytes.Contains(logs.Bytes(), []byte(tok)) {
			t.Fatal("a stream token appears in a Broker log")
		}
	}
}

// Killing the holder's Broker closes the target's socket with peer_lost,
// and draining closes established streams after the grace period.
func TestStreamBrokerLoss(t *testing.T) {
	bs := startClusterOpts(t, 3, false, func(c *node.Config) {
		c.Tunables.StreamDrainGrace = time.Second
	}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	open := func(hb, tb *broker) (*client.Stream, *client.Stream) {
		holder, holderKey, _ := dial(t, hb.url)
		target, _, targetIn := dial(t, tb.url)
		granted := grant(t, ctx, target, holderKey, true)
		st, err := holder.OpenStream(ctx, granted, client.StreamOptions{})
		if err != nil {
			t.Fatal(err)
		}
		ts, err := target.AcceptStream(ctx, offer(t, targetIn))
		if err != nil {
			t.Fatal(err)
		}
		if err := st.WaitAccepted(ctx); err != nil {
			t.Fatal(err)
		}
		st.Write(ctx, websocket.MessageText, []byte("ping"))
		readMsg(t, ctx, ts)
		return st, ts
	}
	// Drain the target's Broker: both sides get 1001 after the grace.
	st, ts := open(bs[1], bs[2])
	start := time.Now()
	bs[2].h.Drain()
	// Still usable during the grace period.
	if err := st.Write(ctx, websocket.MessageText, []byte("during grace")); err != nil {
		t.Fatal(err)
	}
	if _, b := readMsg(t, ctx, ts); string(b) != "during grace" {
		t.Fatalf("got %q", b)
	}
	expectClose(t, ctx, st, wire.CloseDraining, "")
	expectClose(t, ctx, ts, wire.CloseDraining, "")
	if time.Since(start) < 900*time.Millisecond {
		t.Fatalf("closed %v after the drain started, before the grace", time.Since(start))
	}

	// Kill the holder's Broker: the target sees peer_lost.
	st, ts = open(bs[0], bs[1])
	bs[0].h.Close()
	expectClose(t, ctx, ts, wire.ClosePeerLost, "")
	_ = st
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) Bytes() []byte {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]byte(nil), l.b.Bytes()...)
}
