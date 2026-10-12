// Command nimbod is the Nimbo Broker.
//
//	nimbod keygen                       print a new node key (privkey:<hex>)
//	nimbod init  [s3 flags] [-mesh m]   create cluster.json in the bucket prefix
//	nimbod run   [flags]                run a Broker
//	nimbod ctl members [s3 flags]       list members from S3
package main

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"text/tabwriter"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pires/go-proxyproto"

	"github.com/afk-surf/nimbo/internal/config"
	"github.com/afk-surf/nimbo/internal/derpsrv"
	"github.com/afk-surf/nimbo/internal/host"
	"github.com/afk-surf/nimbo/internal/ids"
	"github.com/afk-surf/nimbo/internal/mesh"
	"github.com/afk-surf/nimbo/internal/node"
	"github.com/afk-surf/nimbo/internal/s3store"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	var err error
	switch os.Args[1] {
	case "keygen":
		var k config.NodeKey
		k, err = config.GenerateNodeKey()
		if err == nil {
			fmt.Println(k.String())
		}
	case "init":
		err = cmdInit(os.Args[2:])
	case "run":
		err = cmdRun(os.Args[2:])
	case "ctl":
		err = cmdCtl(os.Args[2:])
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "nimbod:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: nimbod keygen | init | run | ctl members   (-h for flags)")
	os.Exit(2)
}

type s3Flags struct {
	bucket, prefix, endpoint, region string
	pathStyle                        bool
}

func (f *s3Flags) register(fs *flag.FlagSet) {
	fs.StringVar(&f.bucket, "bucket", os.Getenv("NIMBOD_S3_BUCKET"), "S3 bucket")
	fs.StringVar(&f.prefix, "prefix", envOr("NIMBOD_S3_PREFIX", "nimbo"), "cluster prefix in the bucket")
	fs.StringVar(&f.endpoint, "s3-endpoint", os.Getenv("NIMBOD_S3_ENDPOINT"), "S3-compatible endpoint URL (MinIO, R2, GCS)")
	fs.StringVar(&f.region, "s3-region", envOr("AWS_REGION", "us-east-1"), "S3 region")
	fs.BoolVar(&f.pathStyle, "s3-path-style", false, "use path-style S3 addressing")
}

func (f *s3Flags) store(ctx context.Context) (s3store.Store, error) {
	if f.bucket == "" {
		return nil, errors.New("-bucket is required")
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(f.region))
	if err != nil {
		return nil, err
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if f.endpoint != "" {
			o.BaseEndpoint = &f.endpoint
		}
		o.UsePathStyle = f.pathStyle
	})
	return &s3store.AWS{Client: client, Bucket: f.bucket, Prefix: strings.Trim(f.prefix, "/")}, nil
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func cmdInit(args []string) error {
	fs := flag.NewFlagSet("init", flag.ExitOnError)
	var sf s3Flags
	sf.register(fs)
	meshKind := fs.String("mesh", "tailscale", "broker mesh: tailscale, or tcp (development only: not encrypted)")
	fs.Parse(args)
	ctx := context.Background()
	st, err := sf.store(ctx)
	if err != nil {
		return err
	}
	c, err := config.NewCluster(*meshKind)
	if err != nil {
		return err
	}
	b, _ := json.MarshalIndent(c, "", "  ")
	ok, err := st.PutIfAbsent(ctx, "cluster.json", b)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("cluster.json already exists")
	}
	fmt.Println("created cluster", c.ClusterID)
	return nil
}

func cmdCtl(args []string) error {
	if len(args) == 0 || args[0] != "members" {
		return errors.New("usage: nimbod ctl members [s3 flags]")
	}
	fs := flag.NewFlagSet("ctl", flag.ExitOnError)
	var sf s3Flags
	sf.register(fs)
	fs.Parse(args[1:])
	ctx := context.Background()
	st, err := sf.store(ctx)
	if err != nil {
		return err
	}
	l := st.Do(ctx, node.S3Request{Op: node.S3List, Key: "members/"})
	if l.Err != nil {
		return l.Err
	}
	now := l.Date
	if now.IsZero() {
		now = time.Now()
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "BROKER\tINCARNATION\tDRAINING\tHEARTBEAT AGE\tREGION\tWS URLS")
	for _, o := range l.Objects {
		g := st.Do(ctx, node.S3Request{Op: node.S3Get, Key: o.Key})
		var mi node.MemberInfo
		if g.Err != nil || json.Unmarshal(g.Body, &mi) != nil {
			continue
		}
		id := strings.TrimSuffix(strings.TrimPrefix(o.Key, "members/"), ".json")
		fmt.Fprintf(tw, "%s\t%s\t%v\t%v\t%s\t%s\n", id[:16], mi.Incarnation, mi.Draining, now.Sub(o.LastModified).Round(time.Second), mi.Region, strings.Join(mi.WSURLs, ","))
	}
	return tw.Flush()
}

func cmdRun(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	var sf s3Flags
	sf.register(fs)
	stateDir := fs.String("state-dir", os.Getenv("NIMBOD_STATE_DIR"), "directory for node.key (optional)")
	listen := fs.String("listen", ":8443", "HTTP(S) listen address for /v1/ws and /derp")
	tlsCert := fs.String("tls-cert", "", "TLS certificate (omit when TLS terminates at a load balancer)")
	tlsKey := fs.String("tls-key", "", "TLS key")
	proxyProto := fs.Bool("proxy-protocol", false, "require PROXY protocol v2 on -listen (behind a load balancer)")
	publicNames := fs.String("public-name", "", "comma-separated host[:port] names endpoints dial (handshake authority)")
	wsURL := fs.String("ws-url", "", "advertised endpoint URL (default wss://<first public name>/v1/ws)")
	region := fs.String("region", "", "region label, used to suggest nearby Brokers")
	meshPort := fs.Int("mesh-port", 41641, "UDP port for the Tailscale mesh (tcp mesh: TCP port)")
	meshAdvertise := fs.String("mesh-advertise", "", "comma-separated ip:port endpoints to advertise for the mesh")
	derpRegion := fs.Int("derp-region", 0, "serve DERP as this region ID (needs a public hostname and TLS)")
	derpCode := fs.String("derp-region-code", "nimbo", "DERP region code")
	derpHost := fs.String("derp-host", "", "public hostname of this DERP relay")
	derpPort := fs.Int("derp-port", 443, "public HTTPS port of this DERP relay")
	stunListen := fs.String("stun-listen", ":3478", "STUN listen address when serving DERP")
	metricsListen := fs.String("metrics-listen", "127.0.0.1:9090", "listen address for /metrics, /healthz and /debug/stats")
	verbose := fs.Bool("v", false, "verbose mesh logging")
	fs.Parse(args)

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx := context.Background()
	st, err := sf.store(ctx)
	if err != nil {
		return err
	}
	cg := st.Do(ctx, node.S3Request{Op: node.S3Get, Key: "cluster.json"})
	if cg.Err != nil {
		return fmt.Errorf("reading cluster.json: %w", cg.Err)
	}
	if cg.NotFound {
		return errors.New("cluster.json not found: run nimbod init first")
	}
	cluster, err := config.ParseCluster(cg.Body)
	if err != nil {
		return err
	}
	psk, _ := cluster.PSK()
	key, src, err := config.LoadNodeKey(*stateDir)
	if err != nil {
		return err
	}
	self := ids.BrokerIDFromPub(key.Public[:])
	logger.Info("node key loaded", "source", src, "broker", self.String())

	names := splitList(*publicNames)
	if len(names) == 0 {
		return errors.New("-public-name is required")
	}
	url := *wsURL
	if url == "" {
		url = "wss://" + names[0] + "/v1/ws"
	}
	member := node.MemberInfo{WSURLs: []string{url}, Region: *region}
	if *derpRegion > 0 {
		if *derpHost == "" {
			return errors.New("-derp-host is required with -derp-region")
		}
		member.DERP = &node.DERPInfo{RegionID: *derpRegion, RegionCode: *derpCode, HostName: *derpHost, DERPPort: *derpPort, STUNPort: 3478}
	}

	var h *host.Host
	bridge := &meshBridge{}
	var meshT host.Mesh
	var derp *derpsrv.Server
	var advertise []netip.AddrPort
	for _, a := range splitList(*meshAdvertise) {
		ap, err := netip.ParseAddrPort(a)
		if err != nil {
			return fmt.Errorf("-mesh-advertise: %w", err)
		}
		advertise = append(advertise, ap)
	}
	switch cluster.Mesh {
	case "tcp":
		logger.Warn("using the tcp development mesh: peer traffic is NOT encrypted")
		tm, err := mesh.NewTCP(self, psk, fmt.Sprintf(":%d", *meshPort))
		if err != nil {
			return err
		}
		for _, ap := range advertise {
			member.Endpoints = append(member.Endpoints, ap.String())
		}
		if len(member.Endpoints) == 0 {
			return errors.New("-mesh-advertise is required with the tcp mesh")
		}
		meshT = tm
	case "tailscale":
		tsLog := logger.With("component", "mesh")
		ts, err := mesh.NewTS(mesh.TSConfig{
			NodePrivate:     key.Private,
			PSK:             psk,
			Port:            uint16(*meshPort),
			StaticEndpoints: advertise,
			Logf: func(f string, a ...any) {
				if *verbose {
					tsLog.Info(fmt.Sprintf(f, a...))
				}
			},
			OnUnknownPeer: func(id ids.BrokerID) { bridge.unknownPeer(id) },
			OnLocalInfo:   func(disco string, eps []string, home int) { bridge.localInfo(disco, eps, home) },
		})
		if err != nil {
			return fmt.Errorf("mesh: %w", err)
		}
		meshT = ts
	default:
		return fmt.Errorf("cluster.json: unknown mesh %q", cluster.Mesh)
	}
	if *derpRegion > 0 {
		derp, err = derpsrv.New(self, *derpRegion, cluster.DERPMeshKey, func(f string, a ...any) { logger.Info("derp: " + fmt.Sprintf(f, a...)) })
		if err != nil {
			return err
		}
		if err := derpsrv.ServeSTUN(ctx, *stunListen); err != nil {
			return err
		}
		meshT = withDERP{meshT, derp}
	}

	left := make(chan struct{})
	h = host.New(host.Options{
		Node: node.Config{
			NodePub:     key.Public[:],
			Member:      member,
			PublicNames: names,
			Tunables:    node.DefaultTunables(),
			Limits:      node.DefaultLimits(),
		},
		Store:  st,
		Mesh:   meshT,
		Logger: logger,
		OnLeft: func() { close(left) },
	})

	bridge.attach(h)

	mux := http.NewServeMux()
	mux.Handle("/v1/ws", h)
	if derp != nil {
		derp.Register(mux)
	}
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	if *proxyProto {
		ln = &proxyproto.Listener{Listener: ln, ConnPolicy: func(proxyproto.ConnPolicyOptions) (proxyproto.Policy, error) { return proxyproto.REQUIRE, nil }}
	}
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	if *tlsCert != "" {
		cert, err := tls.LoadX509KeyPair(*tlsCert, *tlsKey)
		if err != nil {
			return err
		}
		srv.TLSConfig = &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{"http/1.1"}}
		ln = tls.NewListener(ln, srv.TLSConfig)
	}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("serve", "err", err)
		}
	}()
	if *metricsListen != "" {
		go http.ListenAndServe(*metricsListen, h.MetricsHandler())
	}
	h.Start()
	logger.Info("nimbod running", "listen", *listen, "ws", url, "mesh", cluster.Mesh, "cluster", cluster.ClusterID, "id", hex.EncodeToString(self[:8]))

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sig:
		logger.Info("draining")
		h.Drain()
		select {
		case <-left:
		case <-time.After(60 * time.Second):
		case <-sig:
		}
	case <-h.Done():
	}
	sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	srv.Shutdown(sctx)
	h.Close()
	return nil
}

// withDERP also reconfigures the DERP mesh on membership changes.
type withDERP struct {
	host.Mesh
	d *derpsrv.Server
}

func (w withDERP) SetPeers(ms []node.Member) {
	w.Mesh.SetPeers(ms)
	w.d.SetPeers(ms)
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// meshBridge forwards mesh callbacks to the host once it exists; the mesh
// starts first and may report its local info before.
type meshBridge struct {
	mu      sync.Mutex
	h       *host.Host
	pending func(*host.Host)
}

func (b *meshBridge) attach(h *host.Host) {
	b.mu.Lock()
	b.h = h
	p := b.pending
	b.pending = nil
	b.mu.Unlock()
	if p != nil {
		p(h)
	}
}

func (b *meshBridge) localInfo(disco string, eps []string, home int) {
	apply := func(h *host.Host) {
		h.Do(func() {
			h.Node().UpdateMemberInfo(func(mi *node.MemberInfo) {
				mi.DiscoPub, mi.Endpoints, mi.HomeDERP = disco, eps, home
			})
		})
	}
	b.mu.Lock()
	h := b.h
	if h == nil {
		b.pending = apply // only the latest matters
	}
	b.mu.Unlock()
	if h != nil {
		apply(h)
	}
}

func (b *meshBridge) unknownPeer(id ids.BrokerID) {
	b.mu.Lock()
	h := b.h
	b.mu.Unlock()
	if h != nil {
		h.UnknownPeer(id)
	}
}
