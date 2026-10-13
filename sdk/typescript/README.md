# @afk-surf/nimbo

The TypeScript endpoint SDK for Nimbo. One ESM build runs unchanged in **Bun**, **Node ≥ 22** and **Cloudflare Workers**. It implements [docs/protocol.md](../../docs/protocol.md) with the same features as the Go SDK (`pkg/client`), plus `request`, `keepAlive` and `Delivery.respond`.

```ts
import { Client, generateKey } from "@afk-surf/nimbo";

const client = new Client({ url: "wss://broker1.example.com/v1/ws", key: generateKey() });

client.on("deliver", async (d) => {
  console.log(d.on, d.payload, d.caps);
  if (d.reply) await d.respond({ ok: true }); // send on the single-use reply cap
});

const inbox = await client.mint({ autoReply: true, durable: true });
const granted = await client.transfer(inbox, otherEndpointKey, { durable: true }); // hand `granted` to the other endpoint

// On the other endpoint:
const { msg, replyId } = await other.send(granted, { text: "hi" });
const reply = await other.request(granted, { text: "hi" }, { timeoutMs: 5000 }); // send, then await the reply
```

## Connections

`new Client(options)` returns at once. The client connects in the background and keeps a session up until `close()`:

- Every failure (network error, refused or dropped connection, failed handshake, a Broker that is starting or draining) is retried for ever, with jittered exponential backoff from 250ms to 30s (`backoffBaseMs`, `backoffMaxMs`). A Broker's `closing.retry_after_ms` replaces the delay. An attempt without a session after 15s (`connectTimeoutMs`) is abandoned and retried.
- Requests made while there is no session wait for the next one and are sent in the order they were made. Pass `signal` (an `AbortSignal`, e.g. `AbortSignal.timeout(5000)`) to stop waiting.
- A request already sent when its session drops rejects with `SessionClosedError`. The Broker may or may not have carried it out, and it is not resent: sends are at most once.
- Only invalid options throw, synchronously from the constructor: a URL that is not `ws://` or `wss://`, a malformed key, or no `WebSocket` implementation.

`client.state` is `connecting`, `open` or `closed`. The `connect` event reports each new session (its ID, the Broker's limits and the other Brokers it suggests). The `disconnect` event reports each ended session or failed attempt, with the error, the Broker's `closing` reason and the delay before the next attempt.

## API

| | |
|---|---|
| `generateKey()`, `importKey(priv)`, `exportKey(key)` | Endpoint keys (X25519). `key.publicKey` is the base32 endpoint key others transfer caps to. `exportKey` gives the private key in base32 for `importKey`. |
| `new Client(options)` | `url`, `key`, and optionally `authority` (the `host[:port]` in the handshake proof; default: the URL's), `maxMessageBytes`, `clientName`, `WebSocket`, `backoffBaseMs`, `backoffMaxMs`, `connectTimeoutMs`, `renewIntervalMs`. |
| `mint({ schema?, base?, autoReply?, durable? })` | Returns a new cap ID that targets and is held by this endpoint. The ID is also the lineage ID that deliveries on the cap report as `on`. |
| `transfer(cap, to, { durable? })` | Returns a new cap ID held by endpoint `to`. Use `durable` whenever the ID leaves the session. |
| `send(cap, payload, { caps?, ttlMs?, reply? })` | Returns `{ msg, replyId }`. `replyId` is set when the cap has auto-reply. Resolving means the message was accepted for routing, not that it was delivered. |
| `request(cap, payload, { timeoutMs?, ...send options })` | Sends, then returns the reply `Delivery` (default timeout 30s). The reply is not also emitted as `deliver`. |
| `renew(caps)` | Returns `RenewItem[]`. Batches of more than 256 caps are split. |
| `inspect(cap)` | Returns `CapInfo`. Only the holder may inspect a cap. |
| `keepAlive(...caps)`, `release(...caps)` | Renews caps every 30s until released. A cap that cannot be renewed is dropped and reported by a `capLost` event. |
| `on(event, listener)` | Events: `deliver`, `bounce`, `connect`, `disconnect` and `capLost`. Returns a function that removes the listener. |
| `close()` | Ends the client. Requests still outstanding reject with `ClientClosedError`. `using client = new Client(...)` also closes it. |

Every request method accepts `{ signal }`. Field names are camelCase versions of the protocol's names (`autoReply`, `ttlMs`, `replyId`, …).

### Errors

- **`NimboError`**: the Broker rejected the request. `code` is a protocol error code (`invalid_cap`, `schema_violation`, `unavailable`, …), and `detail` explains it. `retryable` is true for `unavailable` and `rate_limited`. `request` adds two codes: `no_reply` (the cap has no auto-reply) and `timeout`.
- **`SessionClosedError`**: the session ended before the result arrived.
- **`ClientClosedError`**: the client was closed.
- An aborted `signal` rejects with the signal's reason.

## Runtimes

- **Bun** and **Node ≥ 22** use the global `WebSocket`. On older Node, pass the `ws` package: `new Client({ ..., WebSocket })`.
- **Cloudflare Workers**: no `nodejs_compat` flag is needed (tested with compatibility date 2025-09-01). Two limits apply:
  - A Worker cannot open sockets at global scope, so create the client inside a handler.
  - A plain Worker's I/O belongs to the request that created it. For a session that outlives one request, keep the client in a Durable Object:

```ts
import { DurableObject } from "cloudflare:workers";
import { Client, importKey } from "@afk-surf/nimbo";

export class Endpoint extends DurableObject<Env> {
  client = new Client({ url: this.env.NIMBO_URL, key: importKey(this.env.NIMBO_KEY) });

  async fetch(req: Request) {
    const { cap, payload } = await req.json<{ cap: string; payload: Record<string, unknown> }>();
    const reply = await this.client.request(cap, payload);
    return Response.json(reply.payload);
  }
}
```

The SDK uses only `WebSocket`, `crypto.subtle`, `crypto.getRandomValues` and timers. X25519 comes from [`@noble/curves`](https://github.com/paulmillr/noble-curves), its only dependency.

## Development

```sh
npm install
npm run build
npm run typecheck
npm test            # Node, Bun and workerd (needs Go, Bun)
node test/node.ts   # or one runtime: bun test/node.ts, node test/workers.ts
```

The tests build and start [`cmd/nimbo-devcluster`](../../cmd/nimbo-devcluster), a 3-Broker cluster in one process with in-memory storage. Set `NIMBO_DEVCLUSTER` to the path of a prebuilt binary to skip the build. Unit tests run the client against an in-memory fake Broker (`test/fakebroker.ts`). The handshake proof is checked against the same vector as `pkg/wire/proof_test.go`. The workerd run uses Miniflare and lends the Worker the cluster's stop/start commands through a service binding.
