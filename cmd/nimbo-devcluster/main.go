// Command nimbo-devcluster runs a local Nimbo cluster for SDK development
// and tests: in-process Brokers (internal/host) on loopback, joined by the
// TCP mesh, with an in-memory S3. Nothing is persisted.
//
//	nimbo-devcluster -brokers 3 -port 7700
//
// Once every Broker is ready it prints one JSON line to stdout,
// {"urls":["ws://127.0.0.1:7700/v1/ws",...]}, and runs until SIGINT,
// SIGTERM or the end of stdin. Commands on stdin, one per line, each
// answered by one JSON line:
//
//	stop N    stop Broker N (0-based); its sessions drop
//	start N   start Broker N again on the same address, with a new key
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/afk-surf/nimbo/internal/config"
	"github.com/afk-surf/nimbo/internal/host"
	"github.com/afk-surf/nimbo/internal/ids"
	"github.com/afk-surf/nimbo/internal/mesh"
	"github.com/afk-surf/nimbo/internal/node"
	"github.com/afk-surf/nimbo/internal/s3store"
)

var (
	flagBrokers = flag.Int("brokers", 3, "number of Brokers")
	flagPort    = flag.Int("port", 0, "WebSocket port of Broker 0; Broker i listens on port+i (0: random ports)")
	flagVerbose = flag.Bool("v", false, "log at info level (default: warnings)")
)

type broker struct {
	addr string // host:port, kept across restarts
	h    *host.Host
	srv  *http.Server
}

type cluster struct {
	store  s3store.Store
	psk    []byte
	logger *slog.Logger
	bs     []*broker
}

func main() {
	flag.Parse()
	level := slog.LevelWarn
	if *flagVerbose {
		level = slog.LevelInfo
	}
	c := &cluster{
		store:  s3store.NewMem(),
		psk:    make([]byte, 32),
		logger: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})),
	}
	var urls []string
	for i := 0; i < *flagBrokers; i++ {
		addr := "127.0.0.1:0"
		if *flagPort != 0 {
			addr = fmt.Sprintf("127.0.0.1:%d", *flagPort+i)
		}
		b := &broker{addr: addr}
		if err := c.start(b); err != nil {
			fatal(err)
		}
		c.bs = append(c.bs, b)
		urls = append(urls, url(b))
	}
	if err := c.waitReady(); err != nil {
		fatal(err)
	}
	reply(map[string]any{"urls": urls})

	done := make(chan struct{})
	go func() {
		defer close(done)
		sc := bufio.NewScanner(os.Stdin)
		for sc.Scan() {
			if strings.TrimSpace(sc.Text()) == "" {
				continue
			}
			if err := c.command(sc.Text()); err != nil {
				reply(map[string]any{"error": err.Error()})
			} else {
				reply(map[string]any{"ok": true})
			}
		}
	}()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sig:
	case <-done:
	}
	for _, b := range c.bs {
		c.stop(b)
	}
}

func url(b *broker) string { return "ws://" + b.addr + "/v1/ws" }

func (c *cluster) start(b *broker) error {
	k, err := config.GenerateNodeKey()
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", b.addr)
	if err != nil {
		return err
	}
	b.addr = ln.Addr().String()
	tm, err := mesh.NewTCP(ids.BrokerIDFromPub(k.Public[:]), c.psk, "127.0.0.1:0")
	if err != nil {
		ln.Close()
		return err
	}
	lim := node.DefaultLimits()
	// Every test endpoint comes from one IP.
	lim.SessionsPerIP, lim.HandshakesPerSec, lim.HandshakeBurst = 1<<20, 1e6, 1e6
	b.h = host.New(host.Options{
		Node: node.Config{
			NodePub:     k.Public[:],
			Member:      node.MemberInfo{WSURLs: []string{url(b)}, Endpoints: []string{tm.Addr().String()}},
			PublicNames: []string{b.addr},
			Tunables:    node.DefaultTunables(),
			Limits:      lim,
		},
		Store:  c.store,
		Mesh:   tm,
		Logger: c.logger.With("broker", b.addr),
	})
	mux := http.NewServeMux()
	mux.Handle("/v1/ws", b.h)
	mux.Handle("/v1/stream", b.h)
	b.srv = &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go b.srv.Serve(ln)
	b.h.Start()
	return nil
}

func (c *cluster) stop(b *broker) {
	if b.h == nil {
		return
	}
	b.srv.Close()
	b.h.Close()
	b.h, b.srv = nil, nil
}

// waitReady waits until every running Broker is ready and sees at least
// every running Broker (a stopped one stays listed until its lease runs out).
func (c *cluster) waitReady() error {
	n := 0
	for _, b := range c.bs {
		if b.h != nil {
			n++
		}
	}
	deadline := time.Now().Add(45 * time.Second)
	for _, b := range c.bs {
		if b.h == nil {
			continue
		}
		for {
			var members int
			var ready bool
			b.h.Call(func() { members = len(b.h.Node().Members()); ready = b.h.Node().Ready() })
			if members >= n && ready {
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("broker %s not ready: sees %d of %d members", b.addr, members, n)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	return nil
}

func (c *cluster) command(line string) error {
	f := strings.Fields(line)
	if len(f) != 2 {
		return errors.New("usage: stop N | start N")
	}
	i, err := strconv.Atoi(f[1])
	if err != nil || i < 0 || i >= len(c.bs) {
		return fmt.Errorf("no broker %q", f[1])
	}
	b := c.bs[i]
	switch f[0] {
	case "stop":
		c.stop(b)
		return nil
	case "start":
		if b.h != nil {
			return fmt.Errorf("broker %d is running", i)
		}
		if err := c.start(b); err != nil {
			return err
		}
		return c.waitReady()
	}
	return fmt.Errorf("unknown command %q", f[0])
}

func reply(v any) {
	b, _ := json.Marshal(v)
	fmt.Println(string(b))
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "nimbo-devcluster:", err)
	os.Exit(1)
}
