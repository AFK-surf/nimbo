// Runs the unit and end-to-end tests in Node (`node test/node.ts`) or Bun
// (`bun test/node.ts`).

import { startCluster } from "./cluster.ts";
import { e2eTests } from "./e2e.ts";
import { run } from "./harness.ts";
import { unitTests } from "./unit.ts";

const runtime = typeof (globalThis as { Bun?: unknown }).Bun !== "undefined" ? `bun ${process.versions.bun}` : `node ${process.version}`;
console.log(`# ${runtime}: unit`);
const unit = await run(unitTests, { urls: [] }, console.log);
console.log(`# ${runtime}: end to end`);
const cluster = await startCluster();
let failed = unit.failed;
try {
  const e2e = await run(e2eTests, { urls: cluster.urls, ctl: cluster.ctl }, console.log);
  failed += e2e.failed;
} finally {
  cluster.stop();
}
process.exit(failed > 0 ? 1 : 0);
