package host_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/afk-surf/nimbo/internal/config"
	"github.com/afk-surf/nimbo/internal/host"
	"github.com/afk-surf/nimbo/internal/ids"
	"github.com/afk-surf/nimbo/internal/mesh"
	"github.com/afk-surf/nimbo/internal/node"
	"github.com/afk-surf/nimbo/internal/s3store"
	"github.com/afk-surf/nimbo/pkg/client"
)

type broker struct {
	h   *host.Host
	srv *httptest.Server
	url string
}

func startCluster(t *testing.T, n int) []*broker { return startClusterMesh(t, n, false) }

func startClusterMesh(t *testing.T, n int, tailscale bool) []*broker {
	t.Helper()
	return startClusterOpts(t, n, tailscale, nil, nil)
}

// startClusterOpts starts a cluster; tweak adjusts each Broker's config and
// logs, if set, also receives every Broker's log.
func startClusterOpts(t *testing.T, n int, tailscale bool, tweak func(*node.Config), logs io.Writer) []*broker {
	t.Helper()
	store := s3store.NewMem()
	psk := make([]byte, 32)
	var out []*broker
	for i := 0; i < n; i++ {
		k, err := config.GenerateNodeKey()
		if err != nil {
			t.Fatal(err)
		}
		id := ids.BrokerIDFromPub(k.Public[:])
		var m host.Mesh
		var endpoints []string
		var ts *mesh.TS
		var disco string
		var info struct {
			sync.Mutex
			disco string
			eps   []string
			h     *host.Host
		}
		if tailscale {
			ts, err = mesh.NewTS(mesh.TSConfig{
				NodePrivate: k.Private,
				PSK:         psk,
				OnUnknownPeer: func(id ids.BrokerID) {
					info.Lock()
					h := info.h
					info.Unlock()
					if h != nil {
						h.UnknownPeer(id)
					}
				},
				OnLocalInfo: func(disco string, eps []string, _ json.RawMessage) {
					// As nimbod does: publish the disco key and the
					// endpoints magicsock reports (it may rebind ports).
					info.Lock()
					info.disco = disco
					info.eps = eps
					h := info.h
					info.Unlock()
					if h != nil {
						h.Do(func() {
							h.Node().UpdateMemberInfo(func(mi *node.MemberInfo) {
								mi.DiscoPub = disco
								if len(eps) > 0 {
									mi.Endpoints = eps
								}
							})
						})
					}
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			m = ts
			info.Lock()
			endpoints, disco = info.eps, info.disco
			info.Unlock()
			if len(endpoints) == 0 {
				endpoints = []string{fmt.Sprintf("127.0.0.1:%d", ts.LocalPort())}
			}
		} else {
			tm, err := mesh.NewTCP(id, psk, "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			m = tm
			endpoints = []string{tm.Addr().String()}
		}
		b := &broker{}
		b.srv = httptest.NewUnstartedServer(nil)
		authority := b.srv.Listener.Addr().String()
		b.url = "ws://" + authority + "/v1/ws"
		cfg := node.Config{
			NodePub:     k.Public[:],
			Member:      node.MemberInfo{WSURLs: []string{b.url}, Endpoints: endpoints, DiscoPub: disco},
			PublicNames: []string{authority},
			Tunables:    node.DefaultTunables(),
			Limits:      node.DefaultLimits(),
		}
		if tweak != nil {
			tweak(&cfg)
		}
		var lw io.Writer = testWriter{t}
		if logs != nil {
			lw = io.MultiWriter(lw, logs)
		}
		b.h = host.New(host.Options{
			Node:   cfg,
			Store:  store,
			Mesh:   m,
			Logger: slog.New(slog.NewTextHandler(lw, nil)).With("broker", i),
		})
		info.Lock()
		info.h = b.h
		info.Unlock()
		b.srv.Config.Handler = b.h
		b.srv.Start()
		b.h.Start()
		t.Cleanup(func() { b.h.Close(); b.srv.Close() })
		out = append(out, b)
	}
	deadline := time.Now().Add(45 * time.Second)
	for _, b := range out {
		for {
			var members int
			var ready bool
			b.h.Call(func() { members = len(b.h.Node().Members()); ready = b.h.Node().Ready() })
			if members == n && ready {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("cluster did not form: %d members", members)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	return out
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

type inbox struct {
	mu sync.Mutex
	ch chan client.Delivery
}

func dial(t *testing.T, url string) (*client.Client, client.Key, chan client.Delivery) {
	t.Helper()
	k, _ := client.GenerateKey()
	ch := make(chan client.Delivery, 16)
	c, err := client.Dial(context.Background(), client.Options{URL: url, Key: k, OnDeliver: func(d client.Delivery) { ch <- d }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c, k, ch
}

func recv(t *testing.T, ch chan client.Delivery) client.Delivery {
	t.Helper()
	select {
	case d := <-ch:
		return d
	case <-time.After(10 * time.Second):
		t.Fatal("no delivery")
	}
	return client.Delivery{}
}

func TestEndToEnd(t *testing.T) { testEndToEnd(t, startCluster(t, 3)) }

func TestEndToEndTailscaleMesh(t *testing.T) {
	if testing.Short() {
		t.Skip("starts userspace WireGuard engines")
	}
	testEndToEnd(t, startClusterMesh(t, 3, true))
}

func testEndToEnd(t *testing.T, bs []*broker) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	svc, _, svcIn := dial(t, bs[0].url)
	agent, agentKey, agentIn := dial(t, bs[1].url)

	// The service mints a cap with a schema and a base, and transfers it to
	// the agent's key (durable, out-of-band bootstrap).
	// Durable writes wait for 2 placement members; on a fresh mesh the
	// first WireGuard handshakes can take a few seconds, so retry
	// "unavailable" (it is retryable by definition).
	var capID, granted string
	var err error
	for {
		capID, err = svc.Mint(ctx, client.MintOptions{
			Schema:    json.RawMessage(`{"type":"object","required":["q"]}`),
			Base:      json.RawMessage(`{"tenant":"t1"}`),
			AutoReply: true,
			Durable:   true,
		})
		if err == nil {
			granted, err = svc.Transfer(ctx, capID, agentKey.PublicString(), true)
		}
		if err == nil || !strings.Contains(err.Error(), "unavailable") || ctx.Err() != nil {
			break
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	if _, err := agent.Send(ctx, granted, map[string]any{"x": 1}, client.SendOptions{}); err == nil || !strings.Contains(err.Error(), "schema_violation") {
		t.Fatalf("want schema_violation, got %v", err)
	}
	sent, err := agent.Send(ctx, granted, map[string]any{"q": "hello"}, client.SendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	d := recv(t, svcIn)
	if d.On != capID || d.Msg != sent.Msg || d.Reply == "" {
		t.Fatalf("bad delivery %+v", d)
	}
	var p map[string]string
	json.Unmarshal(d.Payload, &p)
	if p["q"] != "hello" || p["tenant"] != "t1" {
		t.Fatalf("payload %s", d.Payload)
	}
	// Reply on the single-use reply cap; a second use must fail.
	if _, err := svc.Send(ctx, d.Reply, map[string]any{"a": "world"}, client.SendOptions{}); err != nil {
		t.Fatal(err)
	}
	r := recv(t, agentIn)
	if r.On != sent.ReplyID {
		t.Fatalf("reply on %s, want %s", r.On, sent.ReplyID)
	}
	if _, err := svc.Send(ctx, d.Reply, map[string]any{"a": "again"}, client.SendOptions{}); err == nil || !strings.Contains(err.Error(), "consumed") {
		t.Fatalf("second reply: want consumed, got %v", err)
	}
	// The agent's ID is useless to anyone else.
	if _, err := svc.Send(ctx, granted, map[string]any{"q": "x"}, client.SendOptions{}); err == nil || !strings.Contains(err.Error(), "not_holder") {
		t.Fatalf("want not_holder, got %v", err)
	}
	items, err := agent.Renew(ctx, granted)
	if err != nil || !items[0].OK {
		t.Fatalf("renew: %v %+v", err, items)
	}
}
