// The page test/browser.ts loads in each browser: it runs the tests in the
// page and borrows nimbo-devcluster's stop/start commands through a function
// the runner exposes.

import { e2eTests } from "./e2e.ts";
import { run } from "./harness.ts";
import type { Report } from "./harness.ts";
import { unitTests } from "./unit.ts";

declare global {
  interface Window {
    /** Exposed by test/browser.ts. */
    nimboCtl(cmd: string): Promise<void>;
    runNimboTests(urls: string[]): Promise<{ unit: Report; e2e: Report }>;
  }
}

window.runNimboTests = async (urls) => {
  const log = (l: string) => console.log(l);
  log("# unit");
  const unit = await run(unitTests, { urls: [] }, log);
  log("# end to end");
  const e2e = await run(e2eTests, { urls, ctl: (cmd) => window.nimboCtl(cmd) }, log);
  return { unit, e2e };
};
