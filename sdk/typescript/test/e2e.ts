// End-to-end tests against a real cluster (cmd/nimbo-devcluster).

import {
  Client,
  NimboError,
  StreamAttachError,
  StreamCloseCode,
  StreamError,
  delegationProof,
  encodeBase32,
  generateKey,
} from "../dist/index.js";
import type { Bounce, Delivery, DisconnectEvent, EndpointKey, Grant, Stream, StreamMessage } from "../dist/index.js";
import { Inbox, assert, eq, rejects, until } from "./harness.ts";
import type { Test } from "./harness.ts";

interface Ep {
  client: Client;
  key: EndpointKey;
  deliveries: Inbox<Delivery>;
  bounces: Inbox<Bounce>;
}

function endpoint(url: string, opts: { backoffMaxMs?: number } = {}): Ep {
  const key = generateKey();
  const client = new Client({ url, key, backoffBaseMs: 50, ...opts });
  const deliveries = new Inbox<Delivery>();
  const bounces = new Inbox<Bounce>();
  client.on("deliver", deliveries.push);
  client.on("bounce", bounces.push);
  return { client, key, deliveries, bounces };
}

/** Runs `f` with endpoints on the given Brokers, closing them afterwards. */
async function withEndpoints(urls: string[], f: (...eps: Ep[]) => Promise<void>): Promise<void> {
  const eps = urls.map((u) => endpoint(u));
  try {
    await f(...eps);
  } finally {
    for (const e of eps) e.client.close();
  }
}

/**
 * a grants b a cap to a (with allow_stream unless told otherwise). Durable
 * writes are retried while a restarted Broker is still suspected.
 */
async function grantStream(a: Ep, b: Ep, allowStream = true): Promise<string> {
  for (let i = 0; ; i++) {
    try {
      const cap = await a.client.mint({ durable: true, allowStream });
      return await a.client.transfer(cap, b.key.publicKey, { durable: true });
    } catch (err) {
      if (!(err instanceof NimboError && err.code === "unavailable") || i >= 10) throw err;
    }
  }
}

/** The next offer delivered to e. */
async function nextOffer(e: Ep): Promise<Delivery> {
  const d = await e.deliveries.next();
  assert(d.stream && d.streamToken, `not a stream offer: ${JSON.stringify(d.payload)}`);
  return d;
}

const bytes = (...b: number[]) => new Uint8Array(b);

export const e2eTests: Test[] = [
  {
    name: "e2e: delegate a cap to a sandbox before it connects",
    async fn({ urls }) {
      await withEndpoints([urls[0]!], async (orch) => {
        const sandboxKey = generateKey();
        let cap = "";
        for (let i = 0; ; i++) {
          try {
            cap = await orch.client.delegate(sandboxKey, { base: { from: "orch" }, autoReply: true, durable: true });
            break;
          } catch (err) {
            if (!(err instanceof NimboError && err.code === "unavailable") || i >= 10) throw err;
          }
        }
        const info = await orch.client.inspect(cap);
        eq([info.kind, info.target, info.holder, info.lineage], ["mint", sandboxKey.publicKey, orch.key.publicKey, cap], "inspect");

        const sandbox = new Client({ url: urls[1]!, key: sandboxKey, backoffBaseMs: 50 });
        try {
          sandbox.on("deliver", (d) => {
            if (d.on === cap && d.reply) void d.respond({ pong: d.payload.ping as number });
          });
          const r = await orch.client.request(cap, { ping: 1 }, { timeoutMs: 10_000 });
          eq(r.payload, { pong: 1 }, "the sandbox answers on the delegated cap");
        } finally {
          sandbox.close();
        }

        // A Prover keeps the key elsewhere; a proof for another grant fails.
        const prover = (tamper?: (g: Grant) => void) => ({
          publicKey: sandboxKey.publicKey,
          prove(g: Grant) {
            tamper?.(g);
            return delegationProof(sandboxKey.privateKey, g);
          },
        });
        const viaProver = await orch.client.delegate(prover(), { allowStream: true });
        eq((await orch.client.inspect(viaProver)).allowStream, true, "delegated through a Prover");
        const bad = await rejects(orch.client.delegate(prover((g) => (g.allowStream = false)), { allowStream: true }));
        assert(bad instanceof NimboError && bad.code === "bad_proof", `got ${String(bad)}`);
        const wrong = await rejects(orch.client.delegate({ publicKey: sandboxKey.publicKey, prove: (g) => delegationProof(generateKey().privateKey, g) }));
        assert(wrong instanceof NimboError && wrong.code === "bad_proof", `got ${String(wrong)}`);
      });
    },
  },
  {
    name: "e2e: mint, durable transfer, send and reply across Brokers",
    async fn({ urls }) {
      await withEndpoints([urls[0]!, urls[1]!], async (a, b) => {
        const inbox = await a.client.mint({ autoReply: true, durable: true });
        const granted = await a.client.transfer(inbox, b.key.publicKey, { durable: true });
        assert(granted !== inbox, "transfer returns a new cap ID");

        // a answers every message on inbox.
        a.client.on("deliver", (d) => {
          if (d.on === inbox && d.reply) void d.respond({ echo: d.payload.text as string });
        });
        const sent = await b.client.send(granted, { text: "hello" });
        assert(sent.msg && sent.replyId, "send result has msg and replyId");
        const d = await a.deliveries.next();
        eq([d.on, d.payload], [inbox, { text: "hello" }], "delivery at a");
        assert(d.reply, "auto_reply gives a reply cap");
        const reply = await b.deliveries.next();
        eq([reply.on, reply.payload], [sent.replyId, { echo: "hello" }], "reply at b");

        const r = await b.client.request(granted, { text: "again" }, { timeoutMs: 10_000 });
        eq(r.payload, { echo: "again" }, "request()");
      });
    },
  },
  {
    name: "e2e: schema, base and their errors",
    async fn({ urls }) {
      await withEndpoints([urls[0]!, urls[2]!], async (a, b) => {
        const cap = await a.client.mint({
          schema: { type: "object", properties: { n: { type: "integer" } }, required: ["n"] },
          base: { from: "base" },
          durable: true,
        });
        const g = await a.client.transfer(cap, b.key.publicKey, { durable: true });
        await b.client.send(g, { n: 7 });
        eq((await a.deliveries.next()).payload, { n: 7, from: "base" }, "base merged into the payload");

        const bad = await rejects(b.client.send(g, { n: "seven" }));
        assert(bad instanceof NimboError && bad.code === "schema_violation", `got ${String(bad)}`);
        const conflict = await rejects(b.client.send(g, { n: 1, from: "me" }));
        assert(conflict instanceof NimboError && conflict.code === "base_conflict", `got ${String(conflict)}`);
        const noReply = await rejects(b.client.request(g, { n: 1 }));
        assert(noReply instanceof NimboError && noReply.code === "no_reply", `got ${String(noReply)}`);
        await a.deliveries.next(); // the request's message still went out
      });
    },
  },
  {
    name: "e2e: carried caps, inspect and renew",
    async fn({ urls }) {
      await withEndpoints([urls[1]!, urls[2]!], async (a, b) => {
        const toA = await a.client.transfer(await a.client.mint({ durable: true }), b.key.publicKey, { durable: true });
        const bOwn = await b.client.mint({ autoReply: true });
        await b.client.send(toA, { here: "a cap" }, { caps: [bOwn] });
        const d = await a.deliveries.next();
        eq(d.caps.length, 1, "one carried cap");
        const carried = d.caps[0]!;
        assert(carried !== bOwn, "the carried cap has a new ID");

        const info = await a.client.inspect(carried);
        eq([info.holder, info.target, info.lineage, info.autoReply], [a.key.publicKey, b.key.publicKey, bOwn, true], "inspect");
        assert(info.ttlMs > 0, "ttl");

        const renewed = await a.client.renew([carried, "a".repeat(26)]);
        eq(renewed.length, 2, "renew items");
        assert(renewed[0]!.ok && renewed[0]!.ttlMs! > 0, "the carried cap renews");
        assert(!renewed[1]!.ok && renewed[1]!.error, "an unknown cap fails");

        // a uses the carried cap to message b.
        await a.client.send(carried, { back: true });
        eq((await b.deliveries.next()).on, bOwn, "message on the carried cap arrives on b's lineage");
      });
    },
  },
  {
    name: "e2e: unknown and foreign caps",
    async fn({ urls }) {
      await withEndpoints([urls[0]!, urls[0]!], async (a, b) => {
        const random = encodeBase32(globalThis.crypto.getRandomValues(new Uint8Array(16)));
        const err = await rejects(a.client.send(random, {}));
        assert(err instanceof NimboError && err.code === "invalid_cap", `got ${String(err)}`);
        const mine = await a.client.mint({ durable: true });
        const foreign = await rejects(b.client.inspect(mine));
        assert(foreign instanceof NimboError && foreign.code === "not_holder", `got ${String(foreign)}`);
      });
    },
  },
  {
    name: "e2e: a message to an endpoint that left bounces with ttl_expired",
    async fn({ urls }) {
      await withEndpoints([urls[0]!, urls[1]!], async (a, b) => {
        const g = await a.client.transfer(await a.client.mint({ durable: true }), b.key.publicKey, { durable: true });
        a.client.close();
        await until(() => a.client.state === "closed", "a closed");
        // close() returns before a's Broker has seen the close frame (WebKit
        // sends it late), and a message that reaches a's session before then
        // is written to it, not bounced. So send until one bounces.
        const sent = new Set<string>();
        let bounce: Bounce | undefined;
        for (let i = 0; !bounce; i++) {
          sent.add((await b.client.send(g, { x: i }, { ttlMs: 300 })).msg);
          bounce = await b.bounces.next(3000).catch((err: unknown) => {
            if (i >= 4) throw err;
            return undefined;
          });
        }
        eq(bounce.reason, "ttl_expired", "bounce reason");
        assert(sent.has(bounce.msg), `bounce for ${bounce.msg}, which b did not send`);
      });
    },
  },
  {
    name: "e2e: streams across Brokers carry text and binary both ways and pass the close through",
    async fn({ urls }) {
      await withEndpoints([urls[0]!, urls[1]!], async (a, b) => {
        const granted = await grantStream(a, b);
        // b (the holder) opens; a (the target) accepts.
        const hs = await b.client.openStream(granted, { proto: "echo/1" });
        hs.send("early"); // waits in the socket until a accepts
        const offer = await nextOffer(a);
        eq([offer.stream, offer.payload], [hs.id, { proto: "echo/1" }], "offer");
        const ts = await offer.acceptStream();
        await hs.accepted;
        eq(await ts.receive(), "early", "early message");

        for (let i = 0; i < 10; i++) {
          const m: StreamMessage = i % 2 ? bytes(i, 255, 0) : `text ${i}`;
          ts.send(m);
          eq(await hs.receive(), m, `to the holder ${i}`);
          hs.send(m);
          eq(await ts.receive(), m, `to the target ${i}`);
        }
        // A message larger than a relay chunk (32 KiB) and than a payload (64 KiB).
        const big = globalThis.crypto.getRandomValues(new Uint8Array(65536)).slice();
        const large = new Uint8Array(1 << 20);
        for (let o = 0; o < large.length; o += big.length) large.set(big, o);
        hs.send(large);
        const got = await ts.receive();
        assert(got instanceof Uint8Array && got.length === large.length && got.every((x, i) => x === large[i]), "1 MiB message");

        // The stream outlives the holder's session.
        b.client.close();
        ts.send("still here");
        eq(await hs.receive(), "still here", "after the session closed");
        ts.close(3001, "bye");
        eq(await hs.closed, { code: 3001, reason: "bye" }, "close passed through");
        eq(await hs.receive(), undefined, "no more messages");
      });
    },
  },
  {
    name: "e2e: streams on one Broker, the holder attaching after the target",
    async fn({ urls }) {
      await withEndpoints([urls[2]!, urls[2]!], async (a, b) => {
        const granted = await grantStream(a, b);
        const hs = await b.client.openStream(granted, {}, { detached: true });
        eq(hs.state, "detached", "detached");
        const ts = await (await nextOffer(a)).acceptStream();
        await hs.accepted;
        ts.send(bytes(1, 2, 3));
        await hs.attach();
        eq(await hs.receive(), bytes(1, 2, 3), "written before the holder attached");
        hs.close();
        eq((await ts.closed).code, 1000, "normal close");
      });
    },
  },
  {
    name: "e2e: stream refusals and failures",
    async fn({ urls }) {
      await withEndpoints([urls[0]!, urls[1]!, urls[2]!], async (a, b, c) => {
        const plain = await grantStream(a, b, false);
        const notAllowed = await rejects(b.client.openStream(plain));
        assert(notAllowed instanceof NimboError && notAllowed.code === "stream_not_allowed", `got ${String(notAllowed)}`);

        // The target refuses: the holder's socket closes with 4901 and the reason.
        const granted = await grantStream(a, b);
        const hs = await b.client.openStream(granted);
        const offer = await nextOffer(a);
        await offer.rejectStream("busy");
        eq(await hs.closed, { code: StreamCloseCode.rejected, reason: "busy" }, "refusal");
        // The token was used.
        assert((await rejects(offer.acceptStream())) instanceof StreamAttachError, "token reuse is refused");
        // A token is valid only on the Broker that issued it.
        const other = await rejects(c.client.acceptStream(offer));
        assert(other instanceof StreamAttachError, `token on another Broker: ${String(other)}`);

        // An offer that cannot be delivered: the stream fails with the bounce reason.
        // As in the bounce test, an offer that reaches c's session before c's
        // Broker has seen the close frame is delivered there, and that stream
        // would wait for an attach: try again until one bounces.
        const gone = await grantStream(c, b);
        c.client.close();
        await until(() => c.client.state === "closed", "c closed");
        let failed: Stream | undefined;
        let err: unknown;
        for (let i = 0; !failed; i++) {
          const st = await b.client.openStream(gone, {}, { ttlMs: 300 });
          const late = new Promise<"late">((r) => setTimeout(() => r("late"), 3000));
          const r = await Promise.race([st.accepted.then(() => "accepted" as const, (e: unknown) => e), late]);
          if (r === "late" && i < 4) {
            st.close();
            continue;
          }
          failed = st;
          err = r;
        }
        assert(err instanceof StreamError && err.reason === "ttl_expired", `got ${String(err)}`);
        eq((await failed.closed).code, StreamCloseCode.ttlExpired, "the holder's socket closes with 4902");
      });
    },
  },
  {
    name: "e2e: requests wait for a Broker that is down, and sessions come back after a restart",
    needs: "ctl",
    async fn({ urls, ctl }) {
      await ctl!("stop 2");
      const a = endpoint(urls[2]!, { backoffMaxMs: 500 });
      const b = endpoint(urls[0]!);
      try {
        const ds = new Inbox<DisconnectEvent>();
        a.client.on("disconnect", ds.push);
        // a is created while its Broker is down: it keeps trying, and the request waits.
        const minted = a.client.mint({ durable: true });
        await ds.next();
        await ds.next();
        eq(a.client.state, "connecting", "state while the Broker is down");
        await ctl!("start 2");
        const cap = await minted;
        eq(a.client.state, "open", "state after the Broker started");
        const g = await a.client.transfer(cap, b.key.publicKey, { durable: true });

        // The Broker goes away under an open session.
        await ctl!("stop 2");
        const d = await ds.next();
        assert(d.wasOpen, "the open session ended");
        await ctl!("start 2");
        await until(() => a.client.state === "open", "a reconnected", 20_000);
        // Caps live on their placement set, not the Broker that minted them.
        await b.client.send(g, { after: "restart" });
        eq((await a.deliveries.next()).payload, { after: "restart" }, "delivery after reconnecting");
      } finally {
        a.client.close();
        b.client.close();
      }
    },
  },
  {
    name: "e2e: a stream ends with peer_lost when the other side's Broker stops",
    needs: "ctl",
    async fn({ urls, ctl }) {
      const a = endpoint(urls[0]!);
      const b = endpoint(urls[2]!);
      let hs: Stream | undefined;
      let stopped = false;
      try {
        const granted = await grantStream(a, b);
        hs = await b.client.openStream(granted);
        const ts = await (await nextOffer(a)).acceptStream();
        await hs.accepted;
        hs.send("ping");
        eq(await ts.receive(), "ping", "flowing");
        await ctl!("stop 2"); // the holder's Broker
        stopped = true;
        eq((await ts.closed).code, StreamCloseCode.peerLost, "the target sees peer_lost");
      } finally {
        hs?.close();
        a.client.close();
        b.client.close();
        if (stopped) await ctl!("start 2");
      }
    },
  },
];
