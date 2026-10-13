import { describe, expect, test } from "bun:test";
import { Pool } from "pg";

import { createChimpbase } from "../packages/bun/src/library.ts";
import { defineChimpbaseApp, defineChimpbaseModuleImplementation, defineChimpbaseModuleInterface } from "../packages/core/index.ts";
import { action, v, worker, Worker, type ChimpbaseContext, type ChimpbaseWorkerExecution } from "../packages/runtime/index.ts";

const typed = worker("typed", (_ctx, _payload: { to: string }) => {});
// Checked by typecheck; payload inference must come from the worker, never the producer.
function checkWorkerTypes(ctx: ChimpbaseContext) {
  const id: Promise<number> = ctx.enqueue(typed, { to: "a@example.test" });
  void id;
  void ctx.queue.enqueue(typed, { to: "a@example.test" });
  void ctx.enqueue("remote.worker", { arbitrary: true });
  // @ts-expect-error A numeric recipient must not widen the worker's payload.
  void ctx.enqueue(typed, { to: 42 });
  // @ts-expect-error Deprecated queue client preserves the same payload check.
  void ctx.queue.enqueue(typed, { to: 42 });
  // @ts-expect-error Required worker fields cannot be omitted.
  void ctx.enqueue(typed, {});
}
void checkWorkerTypes;

test("decorated workers receive execution metadata", async () => {
  class Tasks {
    execution?: ChimpbaseWorkerExecution;
    @Worker("decorated")
    run(_ctx: ChimpbaseContext, _payload: unknown, execution?: ChimpbaseWorkerExecution) {
      this.execution = execution;
    }
  }
  const tasks = new Tasks();
  const host = await createChimpbase({ storage: { engine: "memory" },
    registrations: [action("launch", (ctx) => ctx.enqueue("decorated", {}))] });
  host.registerFrom(tasks);
  try {
    await host.executeAction("launch");
    await host.processNextQueueJob();
    expect(tasks.execution?.attempt).toBe(1);
  } finally { await host.close(); }
});

const pgUrl = process.env.CHIMPBASE_TEST_PG_URL;
(pgUrl ? test : test.skip)("three PostgreSQL worker lanes never overlap themselves and all stop before cleanup", async () => {
  const queue = `workers.lanes.${crypto.randomUUID()}`;
  const entered = Promise.withResolvers<void>();
  const aborted = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  const seen = new Set<number>();
  let active = 0;
  let maxActive = 0;
  let aborts = 0;
  let hooks = 0;
  const host = await createChimpbase({
    storage: { engine: "postgres", url: pgUrl }, workerRuntime: { concurrency: 3, pollIntervalMs: 1 },
    registrations: [
      action("enqueue", async (ctx) => { for (let i = 0; i < 6; i += 1) await ctx.enqueue(queue, {}); }),
      worker(queue, async (_ctx, _payload, execution) => {
        if (!execution) throw new Error("worker metadata missing");
        expect(seen.has(execution.jobId)).toBe(false);
        seen.add(execution.jobId);
        active += 1;
        maxActive = Math.max(maxActive, active);
        execution.signal.addEventListener("abort", () => { if (++aborts === 3) aborted.resolve(); }, { once: true });
        if (active === 3) entered.resolve();
        await release.promise;
        active -= 1;
      }),
    ],
  });
  host.registerOnStop("cleanup", () => { expect(active).toBe(0); hooks += 1; });
  await host.executeAction("enqueue");
  const started = await host.start({ serve: false, runWorker: true });
  let stopping: Promise<void> | undefined;
  try {
    await entered.promise;
    await Bun.sleep(15);
    expect(seen.size).toBe(3);
    expect(maxActive).toBe(3);
    stopping = started.stop();
    await aborted.promise;
    expect(hooks).toBe(0);
    release.resolve();
    await stopping;
    expect(hooks).toBe(1);
    expect(seen.size).toBe(3);
    const jobs = await host.listJobs({ queue });
    expect(jobs.filter((job) => job.status === "completed")).toHaveLength(3);
    expect(jobs.filter((job) => job.status === "pending" && job.attempt === 0)).toHaveLength(3);
  } finally {
    release.resolve();
    await (stopping ?? started.stop());
    await host.engine.getBlobsAdapter().query("DELETE FROM _chimpbase_queue_jobs WHERE queue_name = ?1", [queue], v.object({}));
    await host.close();
  }
});

for (const engine of ["memory", "postgres"] as const) {
  const suite = engine === "postgres" && !pgUrl ? describe.skip : describe;
  suite(`worker API (${engine})`, () => {
    test("typed references return inspectable numeric IDs and leave unsupported queues untouched", async () => {
      const prefix = `workers.api.${crypto.randomUUID()}`;
      const calls: string[] = [];
      const send = worker(`${prefix}.send`, (_ctx, payload: { to: string }) => { calls.push(payload.to); });
      const counterfeit = worker(send.name, () => {});
      const missing = worker(`${prefix}.missing`, () => {});
      const host = await createChimpbase({ storage: engine === "memory" ? { engine } : { engine, url: pgUrl! }, registrations: [
        send, action("launch", (ctx) => ctx.enqueue(send, { to: "recipient" })),
        action("legacy", (ctx) => ctx.queue.enqueue(send, { to: "legacy" })),
        action("unknown", (ctx) => ctx.enqueue(`${prefix}.typo`, {})),
        action("counterfeit", (ctx) => ctx.enqueue(counterfeit, {})),
        action("missing", (ctx) => ctx.enqueue(missing, {})),
        action("malformed", (ctx) => ctx.enqueue({ name: send.name, kind: "action", handler: send.handler } as never, {})),
        action("null-reference", (ctx) => ctx.enqueue(null as never, {})),
      ] });
      try {
        const id = (await host.executeAction("launch")).result as number;
        expect(typeof id).toBe("number");
        expect(await host.inspectJob(id)).toMatchObject({ id, queue: send.name, payload: { to: "recipient" }, status: "pending", attempt: 0, registered: true });
        await expect(host.executeAction("counterfeit")).rejects.toThrow("reference is not registered");
        await expect(host.executeAction("missing")).rejects.toThrow("reference is not registered");
        await expect(host.executeAction("malformed")).rejects.toThrow("reference is not registered");
        await expect(host.executeAction("null-reference")).rejects.toThrow("enqueue expects");
        expect(await host.listJobs({ queue: send.name })).toHaveLength(1);
        expect(await host.listJobs({ queue: missing.name })).toEqual([]);
        const unsupported = (await host.executeAction("unknown")).result as number;
        const legacy = (await host.executeAction("legacy")).result as number;
        const outcome = await host.processNextQueueJob();
        expect(outcome?.jobId).toBe(id);
        expect(typeof outcome?.jobId).toBe("number");
        await host.drain();
        expect(calls).toEqual(["recipient", "legacy"]);
        expect(await host.inspectJob(legacy)).toMatchObject({ status: "completed" });
        expect(await host.listJobs({ queue: `${prefix}.typo`, status: "pending", limit: 1 })).toMatchObject([
          { id: unsupported, attempt: 0, registered: false },
        ]);
        expect(await host.inspectJob(Number.MAX_SAFE_INTEGER)).toBeNull();
        await expect(host.listJobs({ limit: Infinity })).rejects.toThrow("limit");
        await expect(host.inspectJob(NaN)).rejects.toThrow("job id");
      } finally {
        await host.engine.getBlobsAdapter().query("DELETE FROM _chimpbase_queue_jobs WHERE queue_name LIKE ?1", [`${prefix}%`], v.object({}));
        await host.close();
      }
    });

    test("retry resets only terminal user jobs and retires only the source's pending DLQ", async () => {
      const prefix = `workers.api.${crypto.randomUUID()}`;
      const queue = `${prefix}.send`;
      let fail = true;
      const send = worker(queue, () => { if (fail) throw new Error("provider down"); });
      const noDlq = worker(`${prefix}.plain`, () => { throw new Error("plain failure"); }, { dlq: false });
      const host = await createChimpbase({ storage: engine === "memory" ? { engine } : { engine, url: pgUrl! },
        worker: { maxAttempts: 1, retryDelayMs: 0 }, registrations: [send, noDlq,
          action("launch", (ctx) => ctx.enqueue(send, {})),
          action("plain", (ctx) => ctx.enqueue(noDlq, {})),
        ] });
      const adapter = host.engine.getBlobsAdapter();
      try {
        const plain = (await host.executeAction("plain")).result as number;
        await expect(host.processNextQueueJob()).rejects.toThrow("plain failure");
        expect(await host.retryJob(plain)).toMatchObject({ id: plain, status: "pending", attempt: 0, lastError: null });
        await expect(host.retryJob(plain)).rejects.toThrow("only failed or dlq");
        // Leave this job terminal so it cannot interfere with the next claimed job.
        await expect(host.processNextQueueJob()).rejects.toThrow("plain failure");
        const id = (await host.executeAction("launch")).result as number;
        await expect(host.processNextQueueJob()).rejects.toThrow("provider down");
        expect(await host.inspectJob(id)).toMatchObject({ status: "dlq", lastError: "provider down", attempt: 1 });
        const [envelope] = await host.listJobs({ queue: `${queue}.dlq` });
        expect(envelope).toMatchObject({ payload: { jobId: id, queue }, registered: false });
        const unrelated = await adapter.queueEnqueue(`${prefix}.audit`, { queue, jobId: id });
        const otherSource = await adapter.queueEnqueue(`${queue}.dlq`, { queue, jobId: id + 1000 });
        await adapter.query("UPDATE _chimpbase_queue_jobs SET status = 'processing' WHERE id = ?1", [envelope!.id], v.object({}));
        await expect(host.retryJob(id)).rejects.toThrow("Complete processing/failed DLQ jobs");
        expect(await host.inspectJob(id)).toMatchObject({ status: "dlq" });
        expect(await host.inspectJob(envelope!.id)).toMatchObject({ status: "processing" });
        await adapter.query("UPDATE _chimpbase_queue_jobs SET status = 'pending' WHERE id = ?1", [envelope!.id], v.object({}));
        const legacy = await adapter.queueEnqueue(`${queue}.dlq`, { queue, attempts: 1, error: "old envelope" });
        await expect(host.retryJob(id)).rejects.toThrow("legacy envelopes without jobId");
        expect(await host.inspectJob(envelope!.id)).toMatchObject({ status: "pending" });
        await adapter.completeQueueJob(legacy);
        expect(await host.retryJob(id)).toMatchObject({ id, status: "pending", attempt: 0, lastError: null });
        expect(await host.inspectJob(envelope!.id)).toMatchObject({ status: "completed", lastError: `superseded by retry of job ${id}` });
        expect(await host.inspectJob(unrelated)).toMatchObject({ status: "pending" });
        expect(await host.inspectJob(otherSource)).toMatchObject({ status: "pending" });
        fail = false;
        await host.processNextQueueJob();
        expect(await host.inspectJob(id)).toMatchObject({ status: "completed" });
        await expect(host.retryJob(id)).rejects.toThrow("only failed or dlq");
        const internal = await adapter.queueEnqueue("__chimpbase.workflow.run", { workflowId: "internal" });
        await adapter.query("UPDATE _chimpbase_queue_jobs SET status = 'failed' WHERE id = ?1", [internal], v.object({}));
        await expect(host.retryJob(internal)).rejects.toThrow("internal coordination jobs");
        await adapter.query("DELETE FROM _chimpbase_queue_jobs WHERE id = ?1", [internal], v.object({}));
      } finally {
        await adapter.query("DELETE FROM _chimpbase_queue_jobs WHERE queue_name LIKE ?1", [`${prefix}%`], v.object({}));
        await host.close();
      }
    });

    if (engine === "postgres") test("retry fences a DLQ envelope claimed concurrently", async () => {
      const queue = `workers.api.${crypto.randomUUID()}`;
      const send = worker(queue, () => { throw new Error("provider down"); });
      const host = await createChimpbase({ storage: { engine: "postgres", url: pgUrl! }, worker: { maxAttempts: 1 },
        registrations: [send, action("launch", (ctx) => ctx.enqueue(send, {}))] });
      const pool = new Pool({ connectionString: pgUrl });
      const client = await pool.connect();
      try {
        const id = (await host.executeAction("launch")).result as number;
        await expect(host.processNextQueueJob()).rejects.toThrow("provider down");
        const [envelope] = await host.listJobs({ queue: `${queue}.dlq` });
        await client.query("BEGIN");
        await client.query("SELECT id FROM _chimpbase_queue_jobs WHERE id = $1 FOR UPDATE", [envelope!.id]);
        const retry = host.retryJob(id).then(() => null, (error: unknown) => error);
        let waiting = false;
        const deadline = Date.now() + 2000;
        while (!waiting && Date.now() < deadline) {
          const result = await pool.query<{ waiting: boolean }>(`SELECT EXISTS (
            SELECT 1 FROM pg_stat_activity WHERE wait_event_type = 'Lock'
            AND query LIKE '%AS job_id%FOR UPDATE%') AS waiting`);
          waiting = result.rows[0]!.waiting;
          if (!waiting) await new Promise((resolve) => setTimeout(resolve, 5));
        }
        expect(waiting).toBe(true);
        // Commit a claim made while retry was waiting to acquire the envelope lock.
        await client.query("UPDATE _chimpbase_queue_jobs SET status = 'processing' WHERE id = $1", [envelope!.id]);
        await client.query("COMMIT");
        expect(await retry).toBeInstanceOf(Error);
        expect(await host.inspectJob(id)).toMatchObject({ status: "dlq" });
        expect(await host.inspectJob(envelope!.id)).toMatchObject({ status: "processing" });
      } finally {
        await client.query("ROLLBACK");
        client.release();
        await pool.query("DELETE FROM _chimpbase_queue_jobs WHERE queue_name IN ($1, $2)", [queue, `${queue}.dlq`]);
        await pool.end();
        await host.close();
      }
    });
  });
}

test("module worker references qualify the local queue and reject cross-module references", async () => {
  const local = worker("notify", () => {});
  const foreign = worker("notify", () => {});
  const owner = defineChimpbaseModuleInterface({ name: "owner", version: 1, calls: {
    enqueue: { input: v.object({}), output: v.number(), errors: [], guarantees: [] },
    foreign: { input: v.object({}), output: v.number(), errors: [], guarantees: [] },
  }, events: {} });
  const neighbor = defineChimpbaseModuleInterface({ name: "neighbor", version: 1, calls: {}, events: {} });
  const host = await createChimpbase({ storage: { engine: "memory" }, app: defineChimpbaseApp({ modules: [
    defineChimpbaseModuleImplementation({ interface: owner, calls: {
      enqueue: (ctx) => ctx.enqueue(local, {}), foreign: (ctx) => ctx.enqueue(foreign, {}),
    }, registrations: [local] }),
    defineChimpbaseModuleImplementation({ interface: neighbor, calls: {}, registrations: [foreign] }),
  ] }) });
  try {
    const id = (await host.executeAction(owner.calls.enqueue.id, {})).result as number;
    expect(await host.inspectJob(id)).toMatchObject({ queue: "module:owner:queue:notify", registered: true });
    await expect(host.executeAction(owner.calls.foreign.id, {})).rejects.toThrow("reference is not registered");
    expect(await host.listJobs({ queue: "module:neighbor:queue:notify" })).toEqual([]);
  } finally { await host.close(); }
});
