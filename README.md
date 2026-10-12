# nimbo

Nimbo is a distributed, capability-based messaging fabric connecting AI agents, isolated runtimes and external devices.

- **Brokers** (`nimbod`) form a cluster with no leader and no consensus: membership is a prefix in S3, and Brokers talk over an embedded Tailscale data plane (WireGuard with a cluster pre-shared key, NAT traversal, embedded DERP relays) with no control plane.
- **Endpoints** connect over WebSocket with an ephemeral X25519 key. Connecting grants nothing: all authority comes from **capabilities**, 128-bit IDs bound to one holder, which can be minted, transferred, carried in messages and replied to (single-use reply caps).
- Control state (caps, presence, schemas) lives on the 3 Brokers nearest its key in XOR space, refreshed by its writers; nothing is replicated cluster-wide.

Read [docs/design.md](docs/design.md) for the design, [docs/protocol.md](docs/protocol.md) for the endpoint protocol, and [docs/dst.md](docs/dst.md) for how it is tested.

## Layout

| Path | |
|---|---|
| `internal/node` | the Broker: one deterministic state machine (membership, replication, caps, presence, routing, sessions) |
| `internal/sim`, `cmd/nimbo-sim` | deterministic simulator and invariant checker |
| `internal/host` | production runtime: event loop, peer links, WebSocket sessions, metrics |
| `internal/mesh` | Tailscale data-plane mesh; TCP development mesh |
| `internal/derpsrv` | embedded DERP relay and STUN |
| `internal/s3store` | S3 (aws-sdk-go-v2) and in-memory stores |
| `pkg/client`, `pkg/wire` | Go endpoint SDK and protocol types |
| `cmd/nimbod` | the Broker binary |

## Running

```sh
go build ./cmd/nimbod
nimbod keygen                                   # privkey:<hex>, or let nimbod create --state-dir/node.key
nimbod init -bucket my-bucket -prefix prod      # writes cluster.json (WireGuard PSK, DERP mesh key)
NIMBOD_NODE_KEY=privkey:... nimbod run -bucket my-bucket -prefix prod \
    -public-name broker1.example.com -listen :443 -tls-cert cert.pem -tls-key key.pem \
    -derp-region 1 -derp-host broker1.example.com
nimbod ctl members -bucket my-bucket -prefix prod
```

Behind a TLS-terminating load balancer, omit `-tls-*` and add `-proxy-protocol` (the load balancer must send PROXY protocol v2). Metrics, `/healthz` and `/debug/stats` are on `-metrics-listen` (default `127.0.0.1:9090`). For local development without UDP, `nimbod init -mesh tcp` selects an unencrypted TCP mesh (use `-mesh-advertise ip:port`).

## Using it from Go

```go
key, _ := client.GenerateKey()
c, _ := client.Dial(ctx, client.Options{URL: "wss://broker1.example.com/v1/ws", Key: key,
    OnDeliver: func(d client.Delivery) { /* d.On, d.Payload, d.Reply */ }})
capID, _ := c.Mint(ctx, client.MintOptions{AutoReply: true})
granted, _ := c.Transfer(ctx, capID, otherEndpointKey, true) // hand `granted` to the other endpoint
```

## Testing

```sh
go test -short ./...           # unit, end-to-end over the TCP mesh, a few simulation seeds
go test ./...                  # also the Tailscale-mesh end-to-end test and all DST scenarios
go run ./cmd/nimbo-sim -n 500  # 500 random fault schedules
```

Requires Go 1.27.1 (pinned by the `tailscale.com` module).
