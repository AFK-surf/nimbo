// Runs the unit and end-to-end tests inside workerd (the Cloudflare Workers
// runtime) through Miniflare: `node test/workers.ts`.

import { build } from "esbuild";
import { Miniflare } from "miniflare";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { startCluster } from "./cluster.ts";
import type { Report } from "./harness.ts";

const here = dirname(fileURLToPath(import.meta.url));
const bundle = await build({
  entryPoints: [join(here, "worker.ts")],
  bundle: true,
  format: "esm",
  platform: "neutral",
  target: "es2022",
  write: false,
});
const cluster = await startCluster();
const mf = new Miniflare({
  modules: true,
  script: bundle.outputFiles[0]!.text,
  // No nodejs_compat: the SDK must not need it.
  compatibilityDate: "2025-09-01",
  serviceBindings: {
    async CTL(req: { text(): Promise<string> }) {
      try {
        await cluster.ctl(await req.text());
        return new Response("ok");
      } catch (err) {
        return new Response(String(err), { status: 500 });
      }
    },
  },
});
let failed = 0;
try {
  const q = cluster.urls.map((u) => `url=${encodeURIComponent(u)}`).join("&");
  const res = await mf.dispatchFetch(`http://localhost/?${q}`);
  if (!res.ok) throw new Error(`worker answered ${res.status}: ${await res.text()}`);
  const { unit, e2e } = (await res.json()) as { unit: Report; e2e: Report };
  for (const [name, r] of [["unit", unit], ["end to end", e2e]] as const) {
    console.log(`# workerd: ${name}`);
    for (const l of r.lines) console.log(l);
    failed += r.failed;
  }
} finally {
  await mf.dispose();
  cluster.stop();
}
process.exit(failed > 0 ? 1 : 0);
