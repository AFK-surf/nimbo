// End-to-end tests against a real cluster (cmd/nimbo-devcluster).

import { Client, NimboError, encodeBase32, generateKey } from "../dist/index.js";
import type { Bounce, Delivery, DisconnectEvent, EndpointKey } from "../dist/index.js";
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

export const e2eTests: Test[] = [
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
        const sent = await b.client.send(g, { x: 1 }, { ttlMs: 300 });
        const bounce = await b.bounces.next();
        eq(bounce, { msg: sent.msg, reason: "ttl_expired" }, "bounce");
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
];
