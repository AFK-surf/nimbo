// A Worker that runs the tests inside workerd; test/workers.ts drives it and
// lends it nimbo-devcluster's stop/start commands through the CTL binding.

import { e2eTests } from "./e2e.ts";
import { run } from "./harness.ts";
import { unitTests } from "./unit.ts";

interface Env {
  CTL: { fetch(url: string, init: { method: string; body: string }): Promise<Response> };
}

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const urls = new URL(req.url).searchParams.getAll("url");
    const ctl = async (cmd: string) => {
      const r = await env.CTL.fetch("http://ctl/", { method: "POST", body: cmd });
      if (!r.ok) throw new Error(await r.text());
    };
    const unit = await run(unitTests, { urls: [] });
    const e2e = await run(e2eTests, { urls, ctl });
    return Response.json({ unit, e2e });
  },
};
