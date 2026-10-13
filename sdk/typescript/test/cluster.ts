// Starts cmd/nimbo-devcluster for the tests (Node and Bun).

import { spawn, spawnSync } from "node:child_process";
import { createInterface } from "node:readline";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

export interface Cluster {
  urls: string[];
  ctl(cmd: string): Promise<void>;
  stop(): void;
}

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "../../..");

/** Builds the helper unless NIMBO_DEVCLUSTER names a binary. */
function binary(): string {
  if (process.env.NIMBO_DEVCLUSTER) return process.env.NIMBO_DEVCLUSTER;
  const out = join(mkdtempSync(join(tmpdir(), "nimbo-devcluster-")), "nimbo-devcluster");
  const r = spawnSync("go", ["build", "-o", out, "./cmd/nimbo-devcluster"], { cwd: repoRoot, stdio: "inherit" });
  if (r.status !== 0) throw new Error("go build ./cmd/nimbo-devcluster failed");
  return out;
}

export async function startCluster(brokers = 3): Promise<Cluster> {
  const p = spawn(binary(), ["-brokers", String(brokers)], { stdio: ["pipe", "pipe", "inherit"] });
  const lines = createInterface({ input: p.stdout! });
  const waiting: ((line: string) => void)[] = [];
  lines.on("line", (l) => waiting.shift()?.(l));
  const nextLine = () =>
    new Promise<Record<string, unknown>>((resolve, reject) => {
      const onExit = () => reject(new Error("nimbo-devcluster exited"));
      p.once("exit", onExit);
      waiting.push((l) => {
        p.off("exit", onExit);
        resolve(JSON.parse(l) as Record<string, unknown>);
      });
    });
  const first = await nextLine();
  return {
    urls: first.urls as string[],
    async ctl(cmd) {
      const answer = nextLine();
      p.stdin!.write(cmd + "\n");
      const r = await answer;
      if (r.error) throw new Error(`nimbo-devcluster ${cmd}: ${String(r.error)}`);
    },
    stop() {
      p.stdin!.end();
      p.kill();
    },
  };
}
