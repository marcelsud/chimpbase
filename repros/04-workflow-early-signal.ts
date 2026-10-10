import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { createChimpbase } from "../packages/bun/src/library.ts";
import { action, v, workflow } from "../packages/runtime/index.ts";

interface State { phase: string; marker: string }
const definition = workflow<unknown, State>({
  name: "repro.early-signal",
  version: 1,
  initialState: () => ({ phase: "initial", marker: "old" }),
  run(wf) {
    if (wf.state.phase === "done") return wf.complete();
    return wf.waitForSignal("ready", {
      state: { phase: "waiting", marker: "new" },
      onSignal: ({ state }) => ({ ...state, phase: "done" }),
    });
  },
});
const instanceValidator = v.object({
  status: v.string(),
  state: v.object({ phase: v.string(), marker: v.string() }),
});
const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-repro-04-"));
try {
  const host = await createChimpbase({
    projectDir,
    storage: { engine: "memory" },
    secrets: { get: () => null },
    registrations: [
      definition,
      action("start", async (ctx, id: string, early: boolean) => {
        await ctx.workflow.start(definition, {}, { workflowId: id });
        if (early) await ctx.workflow.signal(id, "ready", {});
      }),
      action("signal", async (ctx, id: string) => ctx.workflow.signal(id, "ready", {})),
      action("inspect", async (ctx, id: string) => ctx.workflow.get(id)),
    ],
  });
  try {
    await host.executeAction("start", ["late", false]);
    await host.drain();
    await host.executeAction("signal", ["late"]);
    await host.drain();
    const late = instanceValidator.parse((await host.executeAction("inspect", ["late"])).result);
    assert.equal(late.status, "completed");
    assert.equal(late.state.marker, "new", "the late-signal control must keep the new state");

    await host.executeAction("start", ["early", true]);
    await host.drain();
    const early = instanceValidator.parse((await host.executeAction("inspect", ["early"])).result);
    console.log({ issue: 4, lateSignalState: late.state, earlySignalState: early.state });
    assert.equal(early.status, "completed");
    assert.deepEqual(early.state, late.state, "signal timing must not discard waitForSignal's state");
  } finally {
    await host.close();
  }
} finally {
  await rm(projectDir, { recursive: true, force: true });
}
