import { afterEach, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { createChimpbase } from "../packages/bun/src/library.ts";
import { ChimpbaseBunHost } from "../packages/bun/src/runtime.ts";
import {
  createDefaultChimpbasePlatformShim, normalizeProjectConfig, defineChimpbaseApp,
  defineChimpbaseModuleInterface, defineChimpbaseModuleImplementation,
  ensureSqliteInternalTables,
} from "../packages/core/index.ts";
import {
  action, v, workflow, workflowActionStep, workflowSleepStep, workflowWaitForSignalStep,
  type ChimpbaseWorkflowInstance,
} from "../packages/runtime/index.ts";

const dirs: string[] = [];
afterEach(async () => {
  for (const dir of dirs.splice(0)) await rm(dir, { recursive: true, force: true });
});

async function createHost(engine: "memory" | "sqlite", now: () => number = Date.now) {
  const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-workflows-api-"));
  dirs.push(projectDir);
  return await ChimpbaseBunHost.create({
    projectDir,
    config: normalizeProjectConfig({ storage: { engine, path: "workflows.sqlite" },
      worker: { maxAttempts: 2, retryDelayMs: 0 } }),
    platform: { ...createDefaultChimpbasePlatformShim(), now },
  });
}

test("SQLite upgrades signal receipt timestamps without changing saved data", async () => {
  const db = new Database(":memory:");
  try {
    db.exec(`CREATE TABLE _chimpbase_workflow_signals (
      id INTEGER PRIMARY KEY, workflow_id TEXT NOT NULL, signal_name TEXT NOT NULL,
      payload_json TEXT NOT NULL, consumed_at TEXT, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    INSERT INTO _chimpbase_workflow_signals (workflow_id, signal_name, payload_json, created_at)
      VALUES ('legacy', 'approve', '{"saved":true}', '2026-10-11 12:00:00')`);
    await ensureSqliteInternalTables(db);
    expect(db.query("SELECT payload_json, received_at_ms, consumed_at FROM _chimpbase_workflow_signals").all()).toEqual([
      { payload_json: '{"saved":true}', received_at_ms: Date.parse("2026-10-11T12:00:00Z"), consumed_at: null },
    ]);
    db.query("UPDATE _chimpbase_workflow_signals SET received_at_ms = ?").run(Date.parse("2026-10-11T12:00:00.123Z"));
    await ensureSqliteInternalTables(db);
    expect(db.query("SELECT received_at_ms FROM _chimpbase_workflow_signals").get()).toEqual({
      received_at_ms: Date.parse("2026-10-11T12:00:00.123Z"),
    });
  } finally { db.close(); }
});

for (const engine of ["memory", "sqlite"] as const) {
  test(`immediate signal timeout saves the run directive state (${engine})`, async () => {
    const host = await createHost(engine);
    const definition = workflow<{}, { saved: number }>({ name: "timeout-state", version: 1,
      initialState: () => ({ saved: 0 }), run(wf) {
        return wf.waitForSignal("approve", { timeoutMs: 0, onTimeout: "fail", state: { saved: 5 }, stepId: "approval" });
      },
    });
    host.register(definition,
      action("launch", async (ctx) => await ctx.workflow.start(definition, {}, { workflowId: "timeout-state" })),
      action("inspect", async (ctx) => await ctx.workflow.get("timeout-state")),
    );
    try {
      await host.executeAction("launch");
      await host.processNextQueueJob();
      expect((await host.executeAction("inspect")).result).toMatchObject({
        status: "failed", state: { saved: 5 }, currentStepId: "approval", wakeAtMs: null,
        lastError: "workflow signal timed out: timeout-state/approve",
      });
      expect((await host.listJobs())[0]).toMatchObject({ status: "completed", attempt: 1 });
    } finally { await host.close(); }
  });

  for (const style of ["steps", "run"] as const) {
    for (const receiptOffsetMs of [-1, 0, 1]) {
      for (const policy of ["fail", "continue", "callback"] as const) {
        test(`signal receipt determines timeout with a delayed worker (${engine}, ${style}, ${receiptOffsetMs}, ${policy})`, async () => {
          let now = Date.parse("2026-10-11T12:00:00.123Z");
          const host = await createHost(engine, () => now);
          type State = { outcome: string };
          const onTimeout = policy === "callback" ? () => ({ outcome: "timeout" }) : policy;
          const definition = style === "steps"
            ? workflow<{}, State>({ name: "receipt", version: 1, initialState: () => ({ outcome: "waiting" }), steps: [
              workflowWaitForSignalStep<{}, State, { outcome: string }>("approval", "approve", {
                timeoutMs: 10, onTimeout, onSignal: ({ payload }) => ({ outcome: payload.outcome }),
              }),
            ] })
            : workflow<{}, State>({ name: "receipt", version: 1, initialState: () => ({ outcome: "waiting" }), run(wf) {
              if (wf.state.outcome !== "waiting") return wf.complete();
              return wf.waitForSignal<{ outcome: string }>("approve", { timeoutMs: 10, stepId: "approval",
                onTimeout: policy === "continue" ? () => ({ outcome: "continued" }) : onTimeout,
                onSignal: ({ payload }) => ({ outcome: payload.outcome }),
              });
            } });
          host.register(definition,
            action("launch", async (ctx) => await ctx.workflow.start(definition, {}, { workflowId: "receipt" })),
            action("send", async (ctx) => await ctx.workflow.signal("receipt", "approve", { outcome: "signal" })),
            action("inspect", async (ctx) => await ctx.workflow.get("receipt")),
          );
          try {
            await host.executeAction("launch");
            await host.processNextQueueJob();
            const deadline = now + 10;
            expect((await host.executeAction("inspect")).result).toMatchObject({ status: "waiting_signal", wakeAtMs: deadline });
            now = deadline + receiptOffsetMs;
            await host.executeAction("send");
            now = deadline + 100;
            await host.processNextQueueJob();
            const inTime = receiptOffsetMs < 0;
            expect((await host.executeAction("inspect")).result).toMatchObject({
              status: !inTime && policy === "fail" ? "failed" : "completed",
              state: { outcome: inTime ? "signal" : policy === "callback" ? "timeout" : style === "run" && policy === "continue" ? "continued" : "waiting" },
              wakeAtMs: null,
            });
          } finally { await host.close(); }
        });
      }
    }

    for (const timeoutMs of [0, 10]) {
      test(`signal failure is terminal and commits preceding writes (${engine}, ${style}, ${timeoutMs})`, async () => {
        let now = Date.now();
        const host = await createHost(engine, () => now);
        const mark = action("mark", async (ctx) => { await ctx.kv.set("marked", true); });
        const definition = style === "steps"
          ? workflow<{}, { approved: boolean }>({ name: "timeout", version: 1, initialState: () => ({ approved: false }), steps: [
            workflowActionStep("mark", mark),
            workflowWaitForSignalStep("approval", "approve", { timeoutMs, onTimeout: "fail" }),
          ] })
          : workflow<{}, { approved: boolean }>({ name: "timeout", version: 1, initialState: () => ({ approved: false }), async run(wf) {
            if (wf.state.approved) return wf.complete();
            await wf.action(mark);
            return wf.waitForSignal("approve", { stepId: "approval", timeoutMs, onTimeout: "fail",
              onSignal: () => ({ approved: true }),
            });
          } });
        host.register(mark, definition,
          action("launch", async (ctx) => await ctx.workflow.start(definition, {}, { workflowId: "timeout" })),
          action("inspect", async (ctx) => ({ instance: await ctx.workflow.get("timeout"), marked: await ctx.kv.get("marked") })),
          action("late", async (ctx) => await ctx.workflow.signal("timeout", "approve", {})),
          action("retry", async (ctx) => await ctx.workflow.retry("timeout")),
        );
        try {
          await host.executeAction("launch");
          await host.processNextQueueJob();
          if (timeoutMs > 0) {
            expect((await host.executeAction("inspect")).result).toMatchObject({ instance: { status: "waiting_signal" } });
            now += timeoutMs;
            await host.processNextQueueJob();
          }
          expect((await host.executeAction("inspect")).result).toMatchObject({
            instance: { status: "failed", currentStepId: "approval", wakeAtMs: null,
              lastError: "workflow signal timed out: timeout/approve" }, marked: true,
          });
          const jobs = await host.listJobs({ queue: "__chimpbase.workflow.run" });
          expect(jobs.every((job) => job.status === "completed" && job.attempt === 1)).toBe(true);
          await expect(host.executeAction("late")).rejects.toThrow("workflow is terminal");
          expect(await host.processNextQueueJob()).toBeNull();
          if (timeoutMs > 0) {
            await host.executeAction("retry");
            await host.processNextQueueJob();
            expect((await host.executeAction("inspect")).result).toMatchObject({
              instance: { status: "waiting_signal", wakeAtMs: now + timeoutMs, lastError: null },
            });
            await host.executeAction("late");
            await host.processNextQueueJob();
            expect((await host.executeAction("inspect")).result).toMatchObject({ instance: { status: "completed" } });
          }
        } finally { await host.close(); }
      });
    }
  }

  test(`persisted instances remain readable without their original definition (${engine})`, async () => {
    const host = await createHost(engine);
    host.register(
      workflow({ name: "historical", version: 1, initialState: () => ({ saved: 1 }), steps: [] }),
      workflow({ name: "waiting", version: 1, initialState: () => ({ saved: 2 }), steps: [
        workflowWaitForSignalStep("approval", "approve"),
      ] }),
      action("launch", async (ctx) => {
        await ctx.workflow.start("historical", { input: 1 }, { workflowId: "old" });
        await ctx.workflow.start("waiting", {}, { workflowId: "waiting" });
      }),
      action("inspect", async (ctx) => await ctx.workflow.get("old")),
      action("inspectWaiting", async (ctx) => await ctx.workflow.get("waiting")),
    );
    try {
      await host.executeAction("launch");
      await host.drain();
      host.registry.workflows.delete("historical");
      host.registry.workflows.delete("waiting");
      expect((await host.executeAction("inspect")).result).toMatchObject({
        status: "completed", currentStepId: null, state: { saved: 1 }, input: { input: 1 }, workflowVersion: 1,
      });
      expect((await host.executeAction("inspectWaiting")).result).toMatchObject({
        status: "waiting_signal", currentStepId: "approval", state: { saved: 2 },
      });
      await host.engine.getBlobsAdapter().query("UPDATE _chimpbase_workflow_instances SET current_step_id = NULL WHERE workflow_id = ?1",
        ["waiting"], v.object({}));
      expect((await host.executeAction("inspectWaiting")).result).toMatchObject({ status: "waiting_signal", currentStepId: null });
    } finally { await host.close(); }
  });

  test(`retry resumes a saved checkpoint and fences stale wake jobs (${engine})`, async () => {
    let now = Date.now();
    let firstCalls = 0;
    let failureCalls = 0;
    let fixed = false;
    const host = await createHost(engine, () => now);
    const first = action("first", async () => { firstCalls++; return 7; });
    const later = action("later", async () => { failureCalls++; if (!fixed) throw new Error("fix me"); });
    const definition = workflow<{}, { saved: number }>({ name: "recovery", version: 1,
      initialState: () => ({ saved: 0 }), steps: [
        workflowActionStep<{}, { saved: number }, typeof first>("first", first, {
          onResult: ({ result }) => ({ saved: result }),
        }),
        workflowSleepStep("checkpoint", 10),
        workflowActionStep("later", later),
      ],
    });
    host.register(first, later, definition,
      action("launch", async (ctx) => await ctx.workflow.start(definition, {}, { workflowId: "recovery" })),
      action("inspect", async (ctx) => await ctx.workflow.get("recovery")),
      action("retry", async (ctx) => await ctx.workflow.retry("recovery")),
      action("rollback", async (ctx) => { await ctx.workflow.retry("recovery"); throw new Error("rollback retry"); }),
      action("caughtRetry", async (ctx) => {
        await ctx.kv.set("retry-parent", true);
        try { await ctx.workflow.retry("recovery"); } catch { return "caught"; }
        return "retried";
      }),
    );
    const inspect = async () => (await host.executeAction("inspect")).result as ChimpbaseWorkflowInstance;
    try {
      await host.executeAction("launch");
      await expect(host.executeAction("retry")).rejects.toThrow("only failed");
      await host.processNextQueueJob();
      expect((await inspect()).status).toBe("sleeping");
      await expect(host.executeAction("retry")).rejects.toThrow("only failed");
      now += 10;
      await expect(host.processNextQueueJob()).rejects.toThrow("fix me");
      await expect(host.processNextQueueJob()).rejects.toThrow("fix me");
      expect(await inspect()).toMatchObject({ status: "failed", state: { saved: 7 }, currentStepId: "later" });
      const adapter = host.engine.getBlobsAdapter();
      const stalePending = await adapter.queueEnqueue("__chimpbase.workflow.run", { workflowId: "recovery" });
      const staleClaimed = await adapter.queueEnqueue("__chimpbase.workflow.run", { workflowId: "recovery" });
      await adapter.query("UPDATE _chimpbase_queue_jobs SET attempt_count = 2 WHERE id = ?1", [stalePending], v.object({}));
      await adapter.query(`UPDATE _chimpbase_queue_jobs SET status = 'processing', attempt_count = 2,
        lease_expires_at_ms = ?2 WHERE id = ?1`, [staleClaimed, now + 30_000], v.object({}));
      const before = await host.listJobs();
      const retire = adapter.retireWorkflowJobs!.bind(adapter);
      adapter.retireWorkflowJobs = async (id) => { await retire(id); throw new Error("retirement failed"); };
      try {
        await expect(host.executeAction("caughtRetry")).rejects.toThrow("retirement failed");
      } finally { adapter.retireWorkflowJobs = retire; }
      expect(await host.listJobs()).toEqual(before);
      expect(await adapter.kvGet("retry-parent", v.boolean())).toBeNull();
      await expect(host.executeAction("rollback")).rejects.toThrow("rollback retry");
      expect(await host.listJobs()).toEqual(before);
      expect((await inspect()).status).toBe("failed");
      host.registry.workflows.delete("recovery");
      await expect(host.executeAction("retry")).rejects.toThrow("workflow definition not found: recovery@1");
      host.register(definition);
      expect((await host.executeAction("retry")).result).toMatchObject({ status: "running", workflowVersion: 1 });
      expect(await inspect()).toMatchObject({ status: "running", state: { saved: 7 }, currentStepId: "later", lastError: null });
      expect(await adapter.lockQueueJob!(staleClaimed, 2)).toBe(false);
      expect((await host.listJobs()).filter((job) => [stalePending, staleClaimed].includes(job.id))
        .every((job) => job.status === "completed")).toBe(true);
      // The first fresh attempt must retain the full retry budget despite old exhausted messages.
      await expect(host.processNextQueueJob()).rejects.toThrow("fix me");
      expect((await inspect()).status).toBe("running");
      fixed = true;
      await host.processNextQueueJob();
      expect((await inspect()).status).toBe("completed");
      expect(firstCalls).toBe(1);
      expect(failureCalls).toBe(4);
      await expect(host.executeAction("retry")).rejects.toThrow("only failed");
      expect(await host.processNextQueueJob()).toBeNull();
    } finally { await host.close(); }
  });
}

test("unregistered typed starts fail locally and explicitly dispatch to a remote worker", async () => {
  const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-workflows-remote-"));
  dirs.push(projectDir);
  const storage = { engine: "sqlite" as const, path: join(projectDir, "shared.sqlite") };
  const definition = workflow({ name: "remote", version: 1, initialState: () => ({}), run(wf) { return wf.complete(); } });
  const sender = await createChimpbase({ projectDir, storage, registrations: [
    action("local", async (ctx) => await ctx.workflow.start(definition, {}, { workflowId: "local" })),
    action("remote", async (ctx) => await ctx.workflow.start(definition, {}, { workflowId: "remote", allowUnregistered: true })),
    action("inspect", async (ctx) => await ctx.workflow.get("remote")),
  ] });
  const worker = await createChimpbase({ projectDir, storage, registrations: [definition] });
  try {
    await expect(sender.executeAction("local")).rejects.toThrow("allowUnregistered: true");
    expect(await sender.listJobs()).toEqual([]);
    await sender.executeAction("remote");
    expect(await sender.processNextQueueJob()).toBeNull();
    await worker.processNextQueueJob();
    expect((await sender.executeAction("inspect")).result).toMatchObject({ status: "completed" });
  } finally { await sender.close(); await worker.close(); }
});

test("workflow retries retain module identity and cannot target a foreign instance", async () => {
  let fixed = false;
  const definitions = ["left", "right"].map((name) => {
    const moduleInterface = defineChimpbaseModuleInterface({ name, version: 1, calls: {
      start: { input: v.object({}), output: v.string() },
      retry: { input: v.string(), output: v.string() },
      status: { input: v.string(), output: v.string() },
    }, events: {} });
    const flow = workflow({ name: "recover", version: 1, initialState: () => ({}), run(wf) {
      expect(wf.module?.name).toBe(name);
      return fixed ? wf.complete() : wf.fail("fix me");
    } });
    return { moduleInterface, implementation: defineChimpbaseModuleImplementation({
      interface: moduleInterface, registrations: [flow], calls: {
        async start(ctx) { return (await ctx.workflow.start(flow, {}, { workflowId: "same" })).workflowId; },
        async retry(ctx, workflowId) { return (await ctx.workflow.retry(workflowId)).workflowId; },
        async status(ctx, workflowId) { return (await ctx.workflow.get(workflowId))?.status ?? "missing"; },
      },
    }) };
  });
  const host = await createChimpbase({ storage: { engine: "memory" },
    app: defineChimpbaseApp({ modules: definitions.map(({ implementation }) => implementation) }),
  });
  const left = definitions[0]!.moduleInterface.calls;
  const right = definitions[1]!.moduleInterface.calls;
  try {
    const leftId = (await host.executeAction(left.start.id, {})).result;
    const rightId = (await host.executeAction(right.start.id, {})).result;
    await host.drain();
    await expect(host.executeAction(left.retry.id, rightId)).rejects.toThrow("workflow not found");
    await host.executeAction(left.retry.id, leftId);
    expect((await host.executeAction(left.status.id, "same")).result).toBe("running");
    expect((await host.executeAction(right.status.id, "same")).result).toBe("failed");
    fixed = true;
    await host.drain();
    expect((await host.executeAction(left.status.id, leftId)).result).toBe("completed");
    expect((await host.executeAction(right.status.id, rightId)).result).toBe("failed");
  } finally { await host.close(); }
});
