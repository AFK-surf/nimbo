# nimbo

Nimbo is a distributed, capability-based messaging fabric connecting AI agents, isolated runtimes and external devices.

- **Brokers** (`nimbod`) form a cluster with no leader and no consensus: membership is a prefix in S3, and Brokers talk over an embedded Tailscale data plane (WireGuard with a cluster pre-shared key, NAT traversal, external DERP relays: Tailscale's public ones by default, or each Broker's own choice) with no control plane.
- **Endpoints** connect over WebSocket with an ephemeral X25519 key. Connecting grants nothing: all authority comes from **capabilities**, 128-bit IDs bound to one holder, which can be minted, transferred, carried in messages and replied to (single-use reply caps). A cap minted with `allow_stream` also lets its holders open a full-duplex WebSocket **stream** to its target: each side attaches a data socket to its own Broker with a one-time token, and the Brokers relay over the mesh.
- Control state (caps, presence, schemas) lives on the 3 Brokers nearest its key in XOR space, refreshed by its writers; nothing is replicated cluster-wide.

Read [docs/design.md](docs/design.md) for the design, [docs/protocol.md](docs/protocol.md) for the endpoint protocol, and [docs/dst.md](docs/dst.md) for how it is tested.

## Layout

| Path | |
|---|---|
| `internal/node` | the Broker: one deterministic state machine (membership, replication, caps, presence, routing, sessions) |
| `internal/sim`, `cmd/nimbo-sim` | deterministic simulator and invariant checker |
| `internal/host` | production runtime: event loop, peer links, WebSocket sessions, metrics |
| `internal/mesh` | Tailscale data-plane mesh; TCP development mesh |
| `internal/s3store` | S3 (aws-sdk-go-v2) and in-memory stores |
| `pkg/client`, `pkg/wire` | Go endpoint SDK and protocol types |
| `sdk/typescript` | TypeScript endpoint SDK for Bun, Node and Cloudflare Workers |
| `cmd/nimbod` | the Broker binary |
| `cmd/nimbo-devcluster` | a local in-memory cluster for SDK development and tests |

## Running

```sh
go build ./cmd/nimbod
nimbod keygen                                   # privkey:<hex>, or let nimbod create --state-dir/node.key
nimbod init -bucket my-bucket -prefix prod      # writes cluster.json (WireGuard PSK)
NIMBOD_NODE_KEY=privkey:... nimbod run -bucket my-bucket -prefix prod \
    -public-name broker1.example.com -listen :443 -tls-cert cert.pem -tls-key key.pem
nimbod ctl members -bucket my-bucket -prefix prod
```

Behind a TLS-terminating load balancer, omit `-tls-*` and add `-proxy-protocol` (the load balancer must send PROXY protocol v2). Metrics, `/healthz` and `/debug/stats` are on `-metrics-listen` (default `127.0.0.1:9090`). Brokers relay through Tailscale's public DERP relays by default; `-derp-relay host[:port]` (e.g. a self-hosted `derper`, with `-derp-insecure` for a self-signed certificate) or `-derp-map url|file` chooses others per Broker. For local development without UDP, `nimbod init -mesh tcp` selects an unencrypted TCP mesh (use `-mesh-advertise ip:port`).

## Using it from Go

```go
key, _ := client.GenerateKey()
c, _ := client.Dial(ctx, client.Options{URL: "wss://broker1.example.com/v1/ws", Key: key,
    OnDeliver: func(d client.Delivery) { /* d.On, d.Payload, d.Reply */ }})
capID, _ := c.Mint(ctx, client.MintOptions{AutoReply: true})
granted, _ := c.Transfer(ctx, capID, otherEndpointKey, true) // hand `granted` to the other endpoint
```

Streams: the target mints with `AllowStream: true`; the holder opens, the target accepts the offer it receives:

```go
st, _ := holder.OpenStream(ctx, granted, client.StreamOptions{Payload: map[string]any{"proto": "ssh"}})
// target, in OnDeliver: if d.Stream != "" { go target.AcceptStream(ctx, d) } (or RejectStream, or ignore it)
st.Write(ctx, websocket.MessageBinary, data) // st.Read, st.Reader/Writer, st.NetConn, st.Close(code, reason)
```

## Using it from TypeScript

[`sdk/typescript`](sdk/typescript) runs in Bun, Node ≥ 22 and Cloudflare Workers. The client connects and reconnects in the background:

```ts
const client = new Client({ url: "wss://broker1.example.com/v1/ws", key: generateKey() });
client.on("deliver", (d) => { /* d.on, d.payload, d.respond(...) */ });
const cap = await client.mint({ autoReply: true });
const granted = await client.transfer(cap, otherEndpointKey, { durable: true });
```

Streams work the same way: `client.openStream(cap, payload)` on the holder, `d.acceptStream()` (or `d.rejectStream(reason)`) on an offer at the target.

For SDK development, `go run ./cmd/nimbo-devcluster` starts a 3-Broker cluster on loopback with in-memory storage and prints its URLs.

## Testing

```sh
go test -short ./...           # unit, end-to-end over the TCP mesh, a few simulation seeds
go test ./...                  # also the Tailscale-mesh end-to-end test and all DST scenarios
go run ./cmd/nimbo-sim -n 500  # 500 random fault schedules
```

Requires Go 1.27.1 (pinned by the `tailscale.com` module).

CI (`.github/workflows/ci.yml`) runs gofmt, vet and a tidy check; the TypeScript SDK's tests in Node, Bun and workerd; all tests except the (single-threaded) simulator with the race detector; the Tailscale-mesh end-to-end tests; builds with cgo disabled for linux/amd64, linux/arm64, darwin/arm64, openbsd/amd64, openbsd/arm64 and windows/amd64; and the simulation scenarios plus 16 random fault schedules on fresh seeds each run, which keeps the job to about 4 minutes. A nightly run sweeps 400 seeds plus 100 at a 3× fault rate, and a manual run takes any number (`seeds` input). A failing seed is printed and replays exactly with `go run ./cmd/nimbo-sim -seed N -n 1 -v`.
