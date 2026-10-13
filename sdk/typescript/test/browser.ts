// Runs the unit and end-to-end tests in headless browsers through Playwright:
// `node test/browser.ts [chromium] [firefox] [webkit]` (default: chromium).
//
// Chromium loads the page from http://nimbo.test (mapped to 127.0.0.1),
// which is not a secure context, so it also checks that the SDK needs
// nothing secure-context-only (crypto.subtle). Firefox and WebKit load it
// from http://127.0.0.1.

import { build } from "esbuild";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { chromium, firefox, webkit } from "playwright";
import type { Browser } from "playwright";
import { startCluster } from "./cluster.ts";
import type { Report } from "./harness.ts";

interface Target {
  launch(): Promise<Browser>;
  host: string;
  secure: boolean;
}

const targets: Record<string, Target> = {
  chromium: {
    launch: () => chromium.launch({ args: ["--host-resolver-rules=MAP nimbo.test 127.0.0.1"] }),
    host: "nimbo.test",
    secure: false,
  },
  firefox: { launch: () => firefox.launch(), host: "127.0.0.1", secure: true },
  webkit: { launch: () => webkit.launch(), host: "127.0.0.1", secure: true },
};
const names = process.argv.slice(2);
if (names.length === 0) names.push("chromium");
for (const n of names) if (!targets[n]) throw new Error(`unknown browser ${n}`);

const here = dirname(fileURLToPath(import.meta.url));
const bundle = await build({
  entryPoints: [join(here, "browser-page.ts")],
  bundle: true,
  format: "esm",
  platform: "browser",
  target: "es2022",
  write: false,
});
const script = bundle.outputFiles[0]!.text;
const server = createServer((req, res) => {
  if (req.url === "/tests.js") {
    res.writeHead(200, { "content-type": "text/javascript" }).end(script);
    return;
  }
  res.writeHead(200, { "content-type": "text/html" }).end(`<!doctype html><script type="module" src="/tests.js"></script>`);
});
await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
const port = (server.address() as AddressInfo).port;

let failed = 0;
try {
  for (const name of names) {
    const t = targets[name]!;
    // A fresh cluster per browser: the tests stop and restart Brokers.
    const cluster = await startCluster();
    const browser = await t.launch();
    try {
      const p = await browser.newPage();
      console.log(`# ${name} ${browser.version()} (${t.secure ? "secure" : "insecure"} context)`);
      // Only the test log: the browser's own messages (failed connections
      // while a Broker is stopped) are expected.
      p.on("console", (m) => m.type() === "log" && console.log(m.text()));
      p.on("pageerror", (err) => console.log(`page error: ${err.stack ?? err.message}`));
      await p.exposeFunction("nimboCtl", (cmd: string) => cluster.ctl(cmd));
      await p.goto(`http://${t.host}:${port}/`);
      await p.waitForFunction(() => typeof window.runNimboTests === "function");
      const secure = await p.evaluate(() => window.isSecureContext);
      if (secure !== t.secure) throw new Error(`${name}: isSecureContext is ${secure}, want ${t.secure}`);
      const { unit, e2e } = (await p.evaluate((urls) => window.runNimboTests(urls), cluster.urls)) as { unit: Report; e2e: Report };
      failed += unit.failed + e2e.failed;
    } finally {
      await browser.close();
      cluster.stop();
    }
  }
} finally {
  server.close();
}
process.exit(failed > 0 ? 1 : 0);
