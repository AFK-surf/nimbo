package host_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"golang.org/x/crypto/curve25519"

	"github.com/afk-surf/nimbo/internal/ids"
	"github.com/afk-surf/nimbo/internal/node"
	"github.com/afk-surf/nimbo/pkg/client"
	"github.com/afk-surf/nimbo/pkg/wire"
)

// TestDelegation: an orchestrator holding a sandbox's key gets a cap to the
// sandbox before the sandbox connects (design §8.6).
func TestDelegation(t *testing.T) {
	bs := startCluster(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	orch, orchKey, orchIn := dial(t, bs[0].url)
	sandboxKey, _ := client.GenerateKey()

	var capID string
	var err error
	for {
		capID, err = orch.Delegate(ctx, sandboxKey, client.MintOptions{
			Schema:    json.RawMessage(`{"type":"object","required":["q"]}`),
			Base:      json.RawMessage(`{"from":"orch"}`),
			AutoReply: true,
			Durable:   true,
		})
		if err == nil || !strings.Contains(err.Error(), "unavailable") || ctx.Err() != nil {
			break
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	info, err := orch.Inspect(ctx, capID)
	if err != nil {
		t.Fatal(err)
	}
	if info.Kind != "mint" || info.Target != sandboxKey.PublicString() || info.Holder != orchKey.PublicString() || info.Lineage != capID || !info.AutoReply {
		t.Fatalf("inspect %+v", info)
	}

	sbIn := make(chan client.Delivery, 16)
	sandbox, err := client.Dial(ctx, client.Options{URL: bs[1].url, Key: sandboxKey, OnDeliver: func(d client.Delivery) { sbIn <- d }})
	if err != nil {
		t.Fatal(err)
	}
	defer sandbox.Close()
	if _, err := orch.Send(ctx, capID, map[string]any{"x": 1}, client.SendOptions{}); err == nil || !strings.Contains(err.Error(), "schema_violation") {
		t.Fatalf("want schema_violation, got %v", err)
	}
	sent, err := orch.Send(ctx, capID, map[string]any{"q": "hi"}, client.SendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	d := recv(t, sbIn)
	var p map[string]string
	json.Unmarshal(d.Payload, &p)
	if d.On != capID || d.Msg != sent.Msg || d.Reply == "" || p["q"] != "hi" || p["from"] != "orch" {
		t.Fatalf("bad delivery %+v", d)
	}
	if _, err := sandbox.Send(ctx, d.Reply, map[string]any{"a": "ok"}, client.SendOptions{}); err != nil {
		t.Fatal(err)
	}
	if r := recv(t, orchIn); r.On != sent.ReplyID {
		t.Fatalf("reply on %s, want %s", r.On, sent.ReplyID)
	}
	// The cap is the orchestrator's alone.
	if _, err := sandbox.Send(ctx, capID, map[string]any{"q": "x"}, client.SendOptions{}); err == nil || !strings.Contains(err.Error(), "not_holder") {
		t.Fatalf("want not_holder, got %v", err)
	}
}

// forger proves for Key, then breaks the proof with tamper.
type forger struct {
	client.Key
	other  client.Key
	tamper func(f *forger, g *wire.Grant) client.Key
}

func (f *forger) ProveDelegation(ctx context.Context, g *wire.Grant) ([]byte, error) {
	return f.tamper(f, g).ProveDelegation(ctx, g)
}

func TestDelegationBadProofs(t *testing.T) {
	bs := startCluster(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	a, _, _ := dial(t, bs[0].url)
	b, _ := client.GenerateKey()
	other, _ := client.GenerateKey()
	for name, tamper := range map[string]func(*forger, *wire.Grant) client.Key{
		"wrong key":   func(f *forger, _ *wire.Grant) client.Key { return f.other },
		"other grant": func(f *forger, g *wire.Grant) client.Key { g.AllowStream = !g.AllowStream; return f.Key },
		"no schema":   func(f *forger, g *wire.Grant) client.Key { g.Schema = nil; return f.Key },
		"holder":      func(f *forger, g *wire.Grant) client.Key { g.Holder = f.other.Public; return f.Key },
		"broker":      func(f *forger, g *wire.Grant) client.Key { g.Broker[0] ^= 1; return f.Key },
	} {
		_, err := a.Delegate(ctx, &forger{Key: b, other: other, tamper: tamper}, client.MintOptions{Schema: json.RawMessage(`{"type":"object"}`)})
		var we *client.Error
		if !errors.As(err, &we) || we.Code != wire.ErrBadProof {
			t.Errorf("%s: want bad_proof, got %v", name, err)
		}
	}
	// A low-order target key never verifies.
	if _, err := a.Delegate(ctx, client.Key{}, client.MintOptions{}); err == nil {
		t.Error("zero key: delegation succeeded")
	}
}

// rawSession is an endpoint session driven frame by frame.
type rawSession struct {
	t    *testing.T
	conn *websocket.Conn
	key  client.Key
	rid  uint64
}

func rawDial(t *testing.T, u string, key client.Key) *rawSession {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, u, &websocket.DialOptions{Subprotocols: []string{wire.Subprotocol}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.CloseNow() })
	s := &rawSession{t: t, conn: conn, key: key}
	ch := s.read()
	eph, _ := ids.B32.DecodeString(ch.Eph)
	nonce, _ := ids.B32.DecodeString(ch.Nonce)
	ss, err := curve25519.X25519(key.Private[:], eph)
	if err != nil {
		t.Fatal(err)
	}
	pu, _ := url.Parse(u)
	proof := wire.Proof(ss, nonce, pu.Host, key.Public[:], eph)
	s.write(wire.In{Op: "hello", Key: key.PublicString(), Authority: pu.Host, Proof: ids.B32.EncodeToString(proof)})
	if w := s.read(); w.Op != "welcome" {
		t.Fatalf("handshake: %+v", w)
	}
	return s
}

func (s *rawSession) read() *wire.Out {
	s.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, b, err := s.conn.Read(ctx)
	if err != nil {
		s.t.Fatal(err)
	}
	var o wire.Out
	if err := json.Unmarshal(b, &o); err != nil {
		s.t.Fatal(err)
	}
	return &o
}

func (s *rawSession) write(in wire.In) {
	s.t.Helper()
	b, _ := json.Marshal(in)
	if err := s.conn.Write(context.Background(), websocket.MessageText, b); err != nil {
		s.t.Fatal(err)
	}
}

func (s *rawSession) req(in wire.In) *wire.Out {
	s.t.Helper()
	s.rid++
	in.Rid = s.rid
	s.write(in)
	for {
		if o := s.read(); o.Op == "result" && o.Rid == in.Rid {
			return o
		}
	}
}

// proveFor answers a delegate_begin result with target's key.
func (s *rawSession) proveFor(target client.Key, begin *wire.Out) wire.In {
	s.t.Helper()
	if !begin.OK {
		s.t.Fatalf("delegate_begin: %+v", begin.Error)
	}
	broker, _ := ids.ParseBrokerID(begin.Broker)
	eph, _ := ids.ParseKey(begin.Eph)
	proof, err := target.ProveDelegation(context.Background(), &wire.Grant{Broker: broker, Eph: eph, Target: target.Public, Holder: s.key.Public})
	if err != nil {
		s.t.Fatal(err)
	}
	return wire.In{Op: "delegate", Eph: begin.Eph, Proof: ids.B32.EncodeToString(proof)}
}

func errCode(o *wire.Out) string {
	if o.OK || o.Error == nil {
		return ""
	}
	return o.Error.Code
}

func TestDelegationChallenges(t *testing.T) {
	bs := startClusterOpts(t, 1, false, func(c *node.Config) { c.Tunables.DelegationTTL = 200 * time.Millisecond }, nil)
	ak, _ := client.GenerateKey()
	bk, _ := client.GenerateKey()
	s1 := rawDial(t, bs[0].url, ak)
	s2 := rawDial(t, bs[0].url, ak)
	begin := wire.In{Op: "delegate_begin", Target: bk.PublicString()}

	// Single use.
	fin := s1.proveFor(bk, s1.req(begin))
	if o := s1.req(fin); !o.OK || o.Cap == "" {
		t.Fatalf("delegate: %+v", o.Error)
	}
	if c := errCode(s1.req(fin)); c != wire.ErrInvalidChallenge {
		t.Fatalf("replay: %q", c)
	}
	// Bound to the session that asked, even for the same endpoint key.
	fin = s1.proveFor(bk, s1.req(begin))
	if c := errCode(s2.req(fin)); c != wire.ErrInvalidChallenge {
		t.Fatalf("other session: %q", c)
	}
	// Spent by a failed attempt too.
	fin = s1.proveFor(bk, s1.req(begin))
	bad := fin
	bad.Proof = ids.B32.EncodeToString(make([]byte, 32))
	if c := errCode(s1.req(bad)); c != wire.ErrBadProof {
		t.Fatalf("bad proof: %q", c)
	}
	if c := errCode(s1.req(fin)); c != wire.ErrInvalidChallenge {
		t.Fatalf("after a failed attempt: %q", c)
	}
	// Expires.
	fin = s1.proveFor(bk, s1.req(begin))
	time.Sleep(300 * time.Millisecond)
	if c := errCode(s1.req(fin)); c != wire.ErrInvalidChallenge {
		t.Fatalf("expired: %q", c)
	}
	// At most 8 outstanding per session; expired ones make room.
	for i := range 9 {
		o := s2.req(begin)
		if want := i < 8; o.OK != want {
			t.Fatalf("challenge %d: ok=%v %+v", i, o.OK, o.Error)
		}
	}
	time.Sleep(300 * time.Millisecond)
	if o := s2.req(begin); !o.OK {
		t.Fatalf("after expiry: %+v", o.Error)
	}
	// Bad requests fail before a challenge is issued.
	if c := errCode(s1.req(wire.In{Op: "delegate_begin", Target: "nope"})); c != wire.ErrBadRequest {
		t.Fatalf("bad target: %q", c)
	}
	if c := errCode(s1.req(wire.In{Op: "delegate_begin", Target: bk.PublicString(), Schema: json.RawMessage(`{"type":7}`)})); c != wire.ErrBadRequest {
		t.Fatalf("bad schema: %q", c)
	}
}
