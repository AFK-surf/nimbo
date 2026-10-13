// A test runner small enough to run unchanged in Node, Bun and workerd.

export interface Ctx {
  /** Broker URLs of the dev cluster (end-to-end tests). */
  urls: string[];
  /** Sends a command to nimbo-devcluster (`stop N`, `start N`); absent in workerd. */
  ctl?: (cmd: string) => Promise<void>;
}

export interface Test {
  name: string;
  /** "ctl": needs `ctx.ctl`, so it is skipped where that is unavailable. */
  needs?: "ctl";
  fn: (ctx: Ctx) => Promise<void>;
}

export interface Report {
  passed: number;
  failed: number;
  skipped: number;
  lines: string[];
}

export async function run(tests: Test[], ctx: Ctx, log: (line: string) => void = () => {}): Promise<Report> {
  const r: Report = { passed: 0, failed: 0, skipped: 0, lines: [] };
  const out = (l: string) => {
    r.lines.push(l);
    log(l);
  };
  for (const t of tests) {
    if (t.needs === "ctl" && !ctx.ctl) {
      r.skipped++;
      out(`skip ${t.name}`);
      continue;
    }
    const start = Date.now();
    try {
      await withTimeout(t.fn(ctx), 60_000, t.name);
      r.passed++;
      out(`ok   ${t.name} (${Date.now() - start}ms)`);
    } catch (err) {
      r.failed++;
      out(`FAIL ${t.name}: ${describe(err)}`);
    }
  }
  out(`${r.passed} passed, ${r.failed} failed, ${r.skipped} skipped`);
  return r;
}

/** The stack, with the message first (Firefox's and Safari's stacks leave it out). */
function describe(err: unknown): string {
  if (!(err instanceof Error)) return String(err);
  const head = String(err);
  if (!err.stack) return head;
  return err.stack.startsWith(head) ? err.stack : `${head}\n${err.stack}`;
}

function withTimeout<T>(p: Promise<T>, ms: number, name: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout>;
  return Promise.race([
    p.finally(() => clearTimeout(timer)),
    new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`${name}: timed out after ${ms}ms`)), ms);
    }),
  ]);
}

export function assert(cond: unknown, msg = "assertion failed"): asserts cond {
  if (!cond) throw new Error(msg);
}

export function eq<T>(got: T, want: T, what = "value"): void {
  const g = canonical(got);
  const w = canonical(want);
  if (g !== w) throw new Error(`${what}: got ${g}, want ${w}`);
}

/** JSON with object keys sorted, so that key order does not matter. */
function canonical(v: unknown): string {
  return JSON.stringify(v, (_, x: unknown) =>
    x && typeof x === "object" && !Array.isArray(x) ? Object.fromEntries(Object.entries(x).sort(([a], [b]) => (a < b ? -1 : 1))) : x,
  );
}

/** Awaits `p` and returns its rejection; fails if it resolves. */
export async function rejects(p: Promise<unknown>, what = "promise"): Promise<unknown> {
  try {
    await p;
  } catch (err) {
    return err;
  }
  throw new Error(`${what} resolved, want a rejection`);
}

export const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/** Polls `cond` until it holds. */
export async function until(cond: () => boolean, what: string, ms = 10_000): Promise<void> {
  const deadline = Date.now() + ms;
  while (!cond()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await sleep(5);
  }
}

/** Collects events of one kind; `next()` resolves with the next unread one. */
export class Inbox<T> {
  #items: T[] = [];
  #waiters: ((v: T) => void)[] = [];
  readonly push = (v: T): void => {
    const w = this.#waiters.shift();
    if (w) w(v);
    else this.#items.push(v);
  };
  next(ms = 10_000): Promise<T> {
    const v = this.#items.shift();
    if (v !== undefined) return Promise.resolve(v);
    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.#waiters.splice(this.#waiters.indexOf(w), 1);
        reject(new Error(`no event within ${ms}ms`));
      }, ms);
      const w = (v: T) => {
        clearTimeout(timer);
        resolve(v);
      };
      this.#waiters.push(w);
    });
  }
  get size(): number {
    return this.#items.length;
  }
}
