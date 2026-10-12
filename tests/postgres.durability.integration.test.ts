import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { Pool } from "pg";

import { createChimpbase } from "../packages/bun/src/library.ts";
import { ChimpbaseBunHost } from "../packages/bun/src/runtime.ts";
import { createDefaultChimpbasePlatformShim, normalizeProjectConfig } from "../packages/core/index.ts";
import { applyPostgresSqlMigrations } from "../packages/postgres/src/index.ts";
import {
  action,
  cron,
  subscription,
  v,
  worker,
  workflow,
  workflowActionStep,
  type ChimpbaseDlqEnvelope,
} from "../packages/runtime/index.ts";

const PG_URL = process.env.CHIMPBASE_TEST_PG_URL;
const describeIfPg = (PG_URL !== undefined && PG_URL.length > 0) ? describe : describe.skip;
function postgresUrl(): string {
  if (PG_URL === undefined || PG_URL.length === 0) {
    throw new Error("PostgreSQL integration URL is unavailable");
  }
  return PG_URL;
}


function uniqueName(label: string): string {
  return `stability.${label}.${Date.now()}.${Math.floor(Math.random() * 1e6)}`;
}

describeIfPg("PostgreSQL queue durability", () => {
  let pool: Pool;
  const queueNames = new Set<string>();

  beforeAll(() => {
    pool = new Pool({ connectionString: PG_URL });
  });

  afterAll(async () => {
    if (queueNames.size > 0) {
      await pool.query(
        "DELETE FROM _chimpbase_queue_jobs WHERE queue_name = ANY($1::text[])",
        [[...queueNames]],
      );
    }
    await pool.end();
  });

  test("concurrent workflow retries resume once and fence stale claimed jobs", async () => {
    const workflowName = uniqueName("retry");
    const workflowId = uniqueName("retry-instance");
    let recovering = false;
    let fixed = false;
    let recoveryCalls = 0;
    const definition = workflow<{}, { saved: number }>({
      name: workflowName, version: 1, initialState: () => ({ saved: 7 }),
      run(wf) {
        if (!recovering) return wf.fail("original failure", { stepId: "resume" });
        recoveryCalls++;
        if (!fixed) throw new Error("still broken");
        return wf.complete();
      },
    });
    const registrations = [definition,
      action("launch", async (ctx) => await ctx.workflow.start(definition, {}, { workflowId })),
      action("retry", async (ctx) => await ctx.workflow.retry(workflowId)),
      action("inspect", async (ctx) => await ctx.workflow.get(workflowId)),
    ];
    const options = { storage: { engine: "postgres" as const, url: postgresUrl() },
      worker: { maxAttempts: 2, retryDelayMs: 0 }, registrations };
    const a = await createChimpbase(options);
    const b = await createChimpbase(options);
    const staleWorker = await createChimpbase(options);
    let releaseClaim!: () => void;
    const claimPaused = new Promise<void>((resolve) => { releaseClaim = resolve; });
    let claimReached!: () => void;
    const claimed = new Promise<void>((resolve) => { claimReached = resolve; });
    let staleExecution: Promise<unknown> | undefined;
    try {
      await a.executeAction("launch");
      await a.processNextQueueJob();
      const stale = await pool.query<{ id: string }>(`INSERT INTO _chimpbase_queue_jobs
        (queue_name, payload_json, status, available_at_ms, attempt_count)
        VALUES ('__chimpbase.workflow.run', $1::jsonb, 'pending', 0, 2) RETURNING id`, [JSON.stringify({ workflowId })]);
      await pool.query(`INSERT INTO _chimpbase_queue_jobs
        (queue_name, payload_json, status, available_at_ms, attempt_count)
        VALUES ('__chimpbase.workflow.run', $1::jsonb, 'pending', 0, 2)`, [JSON.stringify({ workflowId })]);
      const staleAdapter = staleWorker.engine.getBlobsAdapter();
      const lock = staleAdapter.lockQueueJob!.bind(staleAdapter);
      staleAdapter.lockQueueJob = async (jobId, attempt) => {
        claimReached();
        await claimPaused;
        return await lock(jobId, attempt);
      };
      staleExecution = staleWorker.processNextQueueJob();
      await claimed;
      let bothReached!: () => void;
      const retryBarrier = new Promise<void>((resolve) => { bothReached = resolve; });
      let retryCount = 0;
      for (const host of [a, b]) {
        const adapter = host.engine.getBlobsAdapter();
        const retire = adapter.retireWorkflowJobs!.bind(adapter);
        adapter.retireWorkflowJobs = async (id) => {
          if (++retryCount === 2) bothReached();
          await retryBarrier;
          await retire(id);
        };
      }
      const retries = await Promise.allSettled([a.executeAction("retry"), b.executeAction("retry")]);
      expect(retries.filter((result) => result.status === "fulfilled")).toHaveLength(1);
      expect(retries.filter((result) => result.status === "rejected")).toHaveLength(1);
      releaseClaim();
      expect(await staleExecution).toBeNull();
      expect(await staleAdapter.lockQueueJob!(Number(stale.rows[0]!.id), 3)).toBe(false);
      expect(recoveryCalls).toBe(0);
      const jobs = await pool.query<{ status: string; attempt_count: number }>(`SELECT status, attempt_count
        FROM _chimpbase_queue_jobs WHERE payload_json->>'workflowId' = $1 ORDER BY id`, [workflowId]);
      expect(jobs.rows).toEqual([
        { status: "completed", attempt_count: 1 }, { status: "completed", attempt_count: 3 },
        { status: "completed", attempt_count: 2 }, { status: "pending", attempt_count: 0 },
      ]);
      recovering = true;
      await expect(a.processNextQueueJob()).rejects.toThrow("still broken");
      expect((await a.executeAction("inspect")).result).toMatchObject({
        status: "running", state: { saved: 7 }, currentStepId: "resume", lastError: null,
      });
      fixed = true;
      await a.processNextQueueJob();
      expect((await b.executeAction("inspect")).result).toMatchObject({ status: "completed", state: { saved: 7 } });
      expect(recoveryCalls).toBe(2);
    } finally {
      releaseClaim();
      await staleExecution?.catch(() => undefined);
      await a.close(); await b.close(); await staleWorker.close();
      await pool.query("DELETE FROM _chimpbase_queue_jobs WHERE payload_json->>'workflowId' = $1", [workflowId]);
      await pool.query("DELETE FROM _chimpbase_workflow_instances WHERE workflow_id = $1", [workflowId]);
    }
  });

  test("worker retention deletes only old completions in PostgreSQL", async () => {
    const schema = `retention_test_${crypto.randomUUID().replaceAll("-", "")}`;
    const scopedUrl = new URL(postgresUrl());
    scopedUrl.searchParams.set("options", `-c search_path=${schema}`);
    await pool.query(`CREATE SCHEMA ${schema}`);
    let host: ChimpbaseBunHost | undefined;
    try {
      const now = Date.parse("2026-10-11T12:00:00.000Z");
      const cutoff = now - 86_400_000;
      host = await ChimpbaseBunHost.create({
        config: normalizeProjectConfig({
          storage: { engine: "postgres", url: scopedUrl.href }, worker: { retention: { maxAgeDays: 1 } },
        }),
        platform: { ...createDefaultChimpbasePlatformShim(), now: () => now },
      });
      const cleanup = host.registry.crons.get("__chimpbase.worker.cleanup");
      if (!cleanup) throw new Error("missing worker cleanup cron");
      host.registerAction("cleanup", async (ctx) => await cleanup.handler(ctx, {
        fireAt: new Date(now).toISOString(), fireAtMs: now, name: cleanup.name, schedule: cleanup.schedule,
      }));
      const adapter = host.engine.getBlobsAdapter();
      for (const [id, status, completedAt] of [
        [1, "completed", new Date(cutoff - 1).toISOString()],
        [2, "completed", new Date(cutoff).toISOString()],
        [3, "pending", "2000-01-01T00:00:00.000Z"],
        [4, "processing", "2000-01-01T00:00:00.000Z"],
        [5, "failed", "2000-01-01T00:00:00.000Z"],
        [6, "dlq", "2000-01-01T00:00:00.000Z"],
        [7, "completed", "2000-01-01T00:00:00.000Z"],
      ] as const) {
        await adapter.query(
          "INSERT INTO _chimpbase_queue_jobs (id, queue_name, payload_json, status, available_at_ms, completed_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
          [id, "retention-fixture", JSON.stringify({ id }), status, now + 60_000, completedAt], v.object({}),
        );
      }
      await host.executeAction("cleanup");
      const rows = await adapter.query("SELECT id::int AS id FROM _chimpbase_queue_jobs ORDER BY id", [], v.object({ id: v.number() }));
      expect(rows.map((row) => row.id)).toEqual([2, 3, 4, 5, 6]);
    } finally {
      await host?.close();
      await pool.query(`DROP SCHEMA ${schema} CASCADE`);
    }
  });

  test("a failing job does not abandon later jobs in its claimed batch", async () => {
    const queueName = uniqueName("batch-failure");
    queueNames.add(queueName);
    const calls: number[] = [];
    const host = await createChimpbase({
      storage: { engine: "postgres", url: postgresUrl() },
      worker: { retryDelayMs: 0 },
      workerRuntime: { leaseMs: 30_000 },
      registrations: [
        worker(queueName, async (ctx, payload: { id: number }, execution) => {
          calls.push(payload.id);
          await ctx.kv.set(`${queueName}:${payload.id}`, execution?.attempt);
          if (payload.id === 0 || payload.id === 3) throw new Error(`job ${payload.id} failed`);
        }),
        action("enqueue", async (ctx) => {
          for (let id = 0; id < 8; id++) await ctx.enqueue(queueName, { id });
        }),
      ],
    });
    try {
      await host.executeAction("enqueue");
      await expect(host.engine.processNextQueueJobs(8)).rejects.toThrow("job 0 failed");
      expect(calls).toEqual([0, 1, 2, 3, 4, 5, 6, 7]);
      const rows = await pool.query<{ attempt_count: number; lease_expires_at_ms: number | null; status: string }>(
        "SELECT attempt_count, lease_expires_at_ms, status FROM _chimpbase_queue_jobs WHERE queue_name = $1 ORDER BY id",
        [queueName],
      );
      expect(rows.rows).toEqual(Array.from({ length: 8 }, (_, id) => ({
        attempt_count: 1, lease_expires_at_ms: null, status: id === 0 || id === 3 ? "pending" : "completed",
      })));
      expect((await pool.query<{ key: string }>(
        "SELECT key FROM _chimpbase_kv WHERE key = ANY($1::text[]) ORDER BY key",
        [Array.from({ length: 8 }, (_, id) => `${queueName}:${id}`)],
      )).rows.map((row) => row.key)).toEqual([1, 2, 4, 5, 6, 7].map((id) => `${queueName}:${id}`));
    } finally {
      await host.close();
      await pool.query("DELETE FROM _chimpbase_kv WHERE key = ANY($1::text[])", [
        Array.from({ length: 8 }, (_, id) => `${queueName}:${id}`),
      ]);
    }
  });

  test("shutdown releases unstarted batch jobs without consuming attempts or replacing newer claims", async () => {
    const queueName = uniqueName("batch-shutdown");
    queueNames.add(queueName);
    const shutdown = new AbortController();
    const calls: number[] = [];
    const host = await createChimpbase({
      storage: { engine: "postgres", url: postgresUrl() }, registrations: [
        worker(queueName, (_ctx, payload: { id: number }) => {
          calls.push(payload.id);
          if (payload.id === 0) shutdown.abort();
        }),
        action("enqueue", async (ctx) => {
          for (let id = 0; id < 3; id += 1) await ctx.enqueue(queueName, { id });
        }),
      ],
    });
    try {
      await host.executeAction("enqueue");
      expect(await host.engine.processNextQueueJobs(3, shutdown.signal)).toHaveLength(1);
      expect(calls).toEqual([0]);
      const rows = await pool.query<{ id: number; status: string; attempt_count: number; lease_expires_at_ms: number | null }>(
        "SELECT id, status, attempt_count, lease_expires_at_ms FROM _chimpbase_queue_jobs WHERE queue_name = $1 ORDER BY id", [queueName],
      );
      expect(rows.rows.map(({ id: _id, ...state }) => state)).toEqual([
        { status: "completed", attempt_count: 1, lease_expires_at_ms: null },
        { status: "pending", attempt_count: 0, lease_expires_at_ms: null },
        { status: "pending", attempt_count: 0, lease_expires_at_ms: null },
      ]);
      expect(await host.engine.processNextQueueJobs(3)).toHaveLength(2);
      expect(calls).toEqual([0, 1, 2]);
      const newerId = rows.rows[2]?.id;
      if (newerId === undefined) throw new Error("batch job missing");
      await pool.query("UPDATE _chimpbase_queue_jobs SET status = 'processing', attempt_count = 2, lease_expires_at_ms = $2 WHERE id = $1",
        [newerId, Date.now() + 60_000]);
      const adapter = host.engine.getBlobsAdapter();
      if (!adapter.releaseQueueJob) throw new Error("PostgreSQL claim release unavailable");
      await adapter.releaseQueueJob(newerId, 1);
      expect((await pool.query<{ status: string; attempt_count: number }>(
        "SELECT status, attempt_count FROM _chimpbase_queue_jobs WHERE id = $1", [newerId],
      )).rows).toEqual([{ status: "processing", attempt_count: 2 }]);
    } finally { await host.close(); }
  });

  test("concurrent first boot initializes an empty internal schema on every node", async () => {
    const schema = `boot_test_${crypto.randomUUID().replaceAll("-", "")}`;
    const scopedUrl = new URL(postgresUrl());
    scopedUrl.searchParams.set("options", `-c search_path=${schema}`);
    await pool.query(`CREATE SCHEMA ${schema}`);
    let outcomes: PromiseSettledResult<Awaited<ReturnType<typeof createChimpbase>>>[] = [];
    try {
      outcomes = await Promise.allSettled(Array.from({ length: 5 }, () =>
        createChimpbase({ storage: { engine: "postgres", url: scopedUrl.href } })));
      expect(outcomes.filter((outcome) => outcome.status === "rejected")).toEqual([]);
      expect((await pool.query<{ table_name: string }>(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = $1", [schema],
      )).rows.map((row) => row.table_name)).toContain("_chimpbase_queue_jobs");
    } finally {
      for (const outcome of outcomes) {
        if (outcome.status === "fulfilled") await outcome.value.close();
      }
      await pool.query(`DROP SCHEMA ${schema} CASCADE`);
    }
  });

  test("named migrations serialize concurrent startup and roll back failed batches", async () => {
    const id = crypto.randomUUID().replaceAll("-", "");
    const table = `migration_test_${id}`;
    const create = { name: `${id}:001_create`, sql: `CREATE TABLE ${table} (value TEXT)` };
    const insert = { name: `${id}:002_insert`, sql: `INSERT INTO ${table} VALUES ('once')` };
    const pending = { name: `${id}:003_update`, sql: `UPDATE ${table} SET value = 'updated'` };
    const invalid = { name: `${id}:004_bad`, sql: `INSERT INTO missing_${id} VALUES (1)` };
    try {
      await Promise.all([
        applyPostgresSqlMigrations(pool, [create, insert]),
        applyPostgresSqlMigrations(pool, [create, insert]),
      ]);
      expect((await pool.query<{ value: string }>(`SELECT value FROM ${table}`)).rows).toEqual([{ value: "once" }]);
      await expect(applyPostgresSqlMigrations(pool, [pending, invalid])).rejects.toThrow(`missing_${id}`);
      expect((await pool.query<{ value: string }>(`SELECT value FROM ${table}`)).rows).toEqual([{ value: "once" }]);
      expect((await pool.query<{ name: string }>("SELECT name FROM _chimpbase_migrations WHERE name = ANY($1::text[]) ORDER BY name", [
        [create.name, insert.name, pending.name, invalid.name],
      ])).rows).toEqual([{ name: create.name }, { name: insert.name }]);
      await applyPostgresSqlMigrations(pool, [create, insert, pending]);
      expect((await pool.query<{ value: string }>(`SELECT value FROM ${table}`)).rows).toEqual([{ value: "updated" }]);
    } finally {
      await pool.query(`DROP TABLE IF EXISTS ${table}`);
      await pool.query("DELETE FROM _chimpbase_migrations WHERE name = ANY($1::text[])", [
        [create.name, insert.name, pending.name, invalid.name],
      ]);
    }
  });

  for (const scenario of ["single failure", "exhausted retries", "recovered retry", "SQL failure"] as const) {
    test(`workflow ${scenario} persists status after rollback`, async () => {
      const workflowName = uniqueName("failure");
      const workflowId = uniqueName("failure-instance");
      const stepName = uniqueName("failure-step");
      let attempts = 0;
      const maxAttempts = scenario === "single failure" || scenario === "SQL failure" ? 1 : 2;
      const step = action(stepName, async (ctx) => {
        attempts += 1;
        await ctx.kv.set(workflowId, { attempt: attempts });
        if (scenario === "SQL failure") {
          await ctx.db.query("SELECT 1 / 0", [], v.object({ value: v.number() }));
        }
        if (scenario !== "recovered retry" || attempts === 1) throw new Error(`failure ${attempts}`);
      });
      const definition = workflow({
        name: workflowName, version: 1, initialState: () => ({}),
        steps: [workflowActionStep("execute", stepName)],
      });
      const host = await createChimpbase({
        project: { name: uniqueName("workflow-failure") },
        storage: { engine: "postgres", url: postgresUrl() },
        worker: { maxAttempts, retryDelayMs: 0 },
        registrations: [definition, step,
          action("launchFailure", async (ctx) => await ctx.workflow.start(definition, {}, { workflowId })),
          action("inspectFailure", async (ctx) => await ctx.workflow.get(workflowId)),
          action("inspectSideEffect", async (ctx) => await ctx.kv.get(workflowId)),
        ],
      });
      const inspect = async () => (await host.executeAction("inspectFailure")).result;
      const firstError = scenario === "SQL failure" ? "division by zero" : "failure 1";
      try {
        await host.executeAction("launchFailure");
        await expect(host.processNextQueueJob()).rejects.toThrow(firstError);
        expect(await inspect()).toMatchObject({
          status: maxAttempts === 1 ? "failed" : "running", lastError: maxAttempts === 1 ? firstError : null,
        });
        expect((await host.executeAction("inspectSideEffect")).result).toBeNull();
        const first = await pool.query<{ status: string; last_error: string; attempt_count: number; lease_expires_at_ms: number | null }>(
          "SELECT status, last_error, attempt_count, lease_expires_at_ms FROM _chimpbase_queue_jobs WHERE payload_json->>'workflowId' = $1",
          [workflowId],
        );
        expect(first.rows).toEqual([{
          status: maxAttempts === 1 ? "failed" : "pending", last_error: firstError,
          attempt_count: 1, lease_expires_at_ms: null,
        }]);
        if (maxAttempts === 2) {
          if (scenario === "recovered retry") await host.processNextQueueJob();
          else await expect(host.processNextQueueJob()).rejects.toThrow("failure 2");
          expect(await inspect()).toMatchObject({
            status: scenario === "recovered retry" ? "completed" : "failed",
            lastError: scenario === "recovered retry" ? null : "failure 2",
          });
          expect((await host.executeAction("inspectSideEffect")).result)
            .toEqual(scenario === "recovered retry" ? { attempt: 2 } : null);
          const finalJob = await pool.query<{ status: string; last_error: string; attempt_count: number; lease_expires_at_ms: number | null }>(
            "SELECT status, last_error, attempt_count, lease_expires_at_ms FROM _chimpbase_queue_jobs WHERE payload_json->>'workflowId' = $1",
            [workflowId],
          );
          expect(finalJob.rows).toEqual([{
            status: scenario === "recovered retry" ? "completed" : "failed",
            last_error: scenario === "recovered retry" ? "failure 1" : "failure 2",
            attempt_count: 2, lease_expires_at_ms: null,
          }]);
        }
        const lease = await pool.query<{ lease_token: string | null; lease_expires_at_ms: number | null }>(
          "SELECT lease_token, lease_expires_at_ms FROM _chimpbase_workflow_instances WHERE workflow_id = $1", [workflowId],
        );
        expect(lease.rows).toEqual([{ lease_token: null, lease_expires_at_ms: null }]);
        expect(await host.processNextQueueJob()).toBeNull();
      } finally {
        await host.close();
        await pool.query("DELETE FROM _chimpbase_queue_jobs WHERE payload_json->>'workflowId' = $1", [workflowId]);
        await pool.query("DELETE FROM _chimpbase_workflow_instances WHERE workflow_id = $1", [workflowId]);
        await pool.query("DELETE FROM _chimpbase_kv WHERE key = $1", [workflowId]);
      }
    });
  }

  test("a queued job survives one host stopping and is processed by another", async () => {
    const queueName = uniqueName("restart");
    queueNames.add(queueName);
    const processed: string[] = [];

    const enqueueJob = action({
      name: "enqueueRestartJob",
      args: v.object({ id: v.string() }),
      async handler(ctx, input) {
        await ctx.enqueue(queueName, input);
      },
    });

    const hostA = await createChimpbase({
      project: { name: uniqueName("producer") },
      projectDir: process.cwd(),
      registrations: [enqueueJob, worker(queueName, async () => {})],
      storage: { engine: "postgres", url: postgresUrl() },
    });
    const startedA = await hostA.start({ serve: false, runWorker: false });

    try {
      await hostA.executeAction("enqueueRestartJob", { id: "job-after-restart" });
    } finally {
      await startedA.stop();
      await hostA.close();
    }

    const hostB = await createChimpbase({
      project: { name: uniqueName("consumer") },
      projectDir: process.cwd(),
      registrations: [
        worker(queueName, async (_ctx, payload: { id: string }) => {
          processed.push(payload.id);
        }),
      ],
      storage: { engine: "postgres", url: postgresUrl() },
    });
    const startedB = await hostB.start({ serve: false, runWorker: false });

    try {
      await hostB.drain({ maxDurationMs: 5_000 });
      expect(processed).toEqual(["job-after-restart"]);
    } finally {
      await startedB.stop();
      await hostB.close();
    }
  });

  test("failed jobs retry and reach the configured DLQ", async () => {
    const queueName = uniqueName("retry");
    const dlqName = `${queueName}.dlq`;
    queueNames.add(queueName);
    queueNames.add(dlqName);
    let attempts = 0;
    const deadLetters: Array<ChimpbaseDlqEnvelope<{ id: string }>> = [];

    const enqueueJob = action({
      name: "enqueueRetryJob",
      args: v.object({ id: v.string() }),
      async handler(ctx, input) {
        return await ctx.enqueue(queueName, input);
      },
    });

    const host = await createChimpbase({
      project: { name: uniqueName("retry-host") },
      projectDir: process.cwd(),
      registrations: [
        enqueueJob,
        worker(queueName, async () => {
          attempts += 1;
          throw new Error("expected retry failure");
        }, { dlq: dlqName }),
        worker(dlqName, async (_ctx, envelope: ChimpbaseDlqEnvelope<{ id: string }>) => {
          deadLetters.push(envelope);
        }, { dlq: false }),
      ],
      storage: { engine: "postgres", url: postgresUrl() },
      worker: { maxAttempts: 2, retryDelayMs: 0 },
    });
    const started = await host.start({ serve: false, runWorker: false });

    try {
      const jobId = (await host.executeAction("enqueueRetryJob", { id: "retry-me" })).result;
      await expect(host.processNextQueueJob()).rejects.toThrow("expected retry failure");
      await expect(host.processNextQueueJob()).rejects.toThrow("expected retry failure");
      await host.processNextQueueJob();

      expect(attempts).toBe(2);
      expect(deadLetters).toHaveLength(1);
      expect(deadLetters[0]).toMatchObject({
        attempts: 2,
        jobId,
        payload: { id: "retry-me" },
        queue: queueName,
      });
    } finally {
      await started.stop();
      await host.close();
    }
  });

  for (const hookFailure of ["throw", "SQL failure"] as const) {
    for (const terminalStatus of ["failed", "dlq"] as const) {
      test(`a failure hook ${hookFailure} cannot prevent retries ending in ${terminalStatus}`, async () => {
        const queueName = uniqueName("failure-hook");
        const dlqName = `${queueName}.dlq`;
        queueNames.add(queueName);
        queueNames.add(dlqName);
        const workerError = new Error("original worker failure");
        let attempts = 0;
        let hooks = 0;
        const host = await createChimpbase({
          storage: { engine: "postgres", url: postgresUrl() },
          worker: { maxAttempts: 2, retryDelayMs: 0 },
          registrations: [
            worker(queueName, async () => { attempts += 1; throw workerError; }, {
              dlq: terminalStatus === "dlq" ? dlqName : false,
              async onFailure(ctx, error) {
                expect(error).toBe(workerError);
                hooks += 1;
                await ctx.kv.set(queueName, { hook: hooks });
                ctx.pubsub.publish(queueName, { hook: hooks });
                if (hookFailure === "SQL failure") {
                  await ctx.db.query("SELECT 1 / 0", [], v.object({ value: v.number() }));
                }
                throw new Error("failure hook unavailable");
              },
            }),
            action("enqueue", async (ctx) => await ctx.enqueue(queueName, { id: "keep-me" })),
          ],
        });
        try {
          await host.executeAction("enqueue");
          for (const attempt of [1, 2]) {
            expect(await host.processNextQueueJob().catch((error: unknown) => error)).toBe(workerError);
            expect((await pool.query<{
              attempt_count: number; status: string; last_error: string; lease_expires_at_ms: number | null;
            }>(
              "SELECT attempt_count, status, last_error, lease_expires_at_ms FROM _chimpbase_queue_jobs WHERE queue_name = $1",
              [queueName],
            )).rows).toEqual([{
              attempt_count: attempt, status: attempt === 1 ? "pending" : terminalStatus,
              last_error: workerError.message, lease_expires_at_ms: null,
            }]);
          }
          expect(attempts).toBe(2);
          expect(hooks).toBe(2);
          expect(await host.processNextQueueJob()).toBeNull();
          expect((await pool.query<{ key: string }>("SELECT key FROM _chimpbase_kv WHERE key = $1", [queueName])).rows).toEqual([]);
          expect((await pool.query<{ id: string }>("SELECT id FROM _chimpbase_events WHERE event_name = $1", [queueName])).rows).toEqual([]);
          const deadLetters = await pool.query<{ payload_json: ChimpbaseDlqEnvelope }>(
            "SELECT payload_json FROM _chimpbase_queue_jobs WHERE queue_name = $1", [dlqName],
          );
          expect(deadLetters.rows).toHaveLength(terminalStatus === "dlq" ? 1 : 0);
          if (terminalStatus === "dlq") {
            expect(deadLetters.rows[0]?.payload_json).toMatchObject({
              attempts: 2, queue: queueName, payload: { id: "keep-me" }, error: workerError.message,
            });
          }
        } finally {
          await host.close();
          await pool.query("DELETE FROM _chimpbase_kv WHERE key = $1", [queueName]);
          await pool.query("DELETE FROM _chimpbase_events WHERE event_name = $1", [queueName]);
        }
      });
    }
  }

  test("a successful failure hook commits with the terminal job state", async () => {
    const queueName = uniqueName("successful-failure-hook");
    queueNames.add(queueName);
    const host = await createChimpbase({
      storage: { engine: "postgres", url: postgresUrl() }, worker: { maxAttempts: 1 },
      registrations: [
        worker(queueName, async () => { throw new Error("worker failed"); }, {
          dlq: false,
          async onFailure(ctx) {
            await ctx.kv.set(queueName, { recorded: true });
            ctx.pubsub.publish(queueName, { recorded: true });
          },
        }),
        action("enqueue", async (ctx) => await ctx.enqueue(queueName, {})),
      ],
    });
    try {
      await host.executeAction("enqueue");
      await expect(host.processNextQueueJob()).rejects.toThrow("worker failed");
      expect((await pool.query<{ key: string }>("SELECT key FROM _chimpbase_kv WHERE key = $1", [queueName])).rows).toHaveLength(1);
      expect((await pool.query<{ id: string }>("SELECT id FROM _chimpbase_events WHERE event_name = $1", [queueName])).rows).toHaveLength(1);
      expect((await pool.query<{ status: string }>(
        "SELECT status FROM _chimpbase_queue_jobs WHERE queue_name = $1", [queueName],
      )).rows).toEqual([{ status: "failed" }]);
    } finally {
      await host.close();
      await pool.query("DELETE FROM _chimpbase_kv WHERE key = $1", [queueName]);
      await pool.query("DELETE FROM _chimpbase_events WHERE event_name = $1", [queueName]);
    }
  });

  test("failure state SQL errors roll back successful hooks and remain observable", async () => {
    const queueName = uniqueName("failure-state-sql");
    queueNames.add(queueName);
    let hooks = 0;
    const host = await createChimpbase({
      storage: { engine: "postgres", url: postgresUrl() }, worker: { maxAttempts: 1 },
      registrations: [
        worker(queueName, async () => { throw new Error("worker failed"); }, {
          dlq: false,
          async onFailure(ctx) { hooks += 1; await ctx.kv.set(queueName, { recorded: true }); },
        }),
        action("enqueue", async (ctx) => await ctx.enqueue(queueName, {})),
      ],
    });
    const adapter = host.engine.getBlobsAdapter();
    const markFailure = adapter.markQueueJobFailure.bind(adapter);
    adapter.markQueueJobFailure = async () => { await adapter.query("SELECT 1 / 0", [], v.object({ value: v.number() })); };
    try {
      await host.executeAction("enqueue");
      await expect(host.processNextQueueJob()).rejects.toThrow("division by zero");
      expect(hooks).toBe(1);
      expect((await pool.query<{ key: string }>("SELECT key FROM _chimpbase_kv WHERE key = $1", [queueName])).rows).toEqual([]);
      expect((await pool.query<{ status: string; attempt_count: number; last_error: string | null }>(
        "SELECT status, attempt_count, last_error FROM _chimpbase_queue_jobs WHERE queue_name = $1", [queueName],
      )).rows).toEqual([{ status: "processing", attempt_count: 1, last_error: null }]);
      adapter.markQueueJobFailure = markFailure;
      await pool.query("UPDATE _chimpbase_queue_jobs SET lease_expires_at_ms = 0 WHERE queue_name = $1", [queueName]);
      await expect(host.processNextQueueJob()).rejects.toThrow("worker failed");
      expect(hooks).toBe(2);
      expect((await pool.query<{ status: string }>(
        "SELECT status FROM _chimpbase_queue_jobs WHERE queue_name = $1", [queueName],
      )).rows).toEqual([{ status: "failed" }]);
    } finally {
      adapter.markQueueJobFailure = markFailure;
      await host.close();
      await pool.query("DELETE FROM _chimpbase_kv WHERE key = $1", [queueName]);
    }
  });

  test("hook warning sink and persistence failures cannot mask the original worker error", async () => {
    const queueName = uniqueName("failure-hook-warning");
    queueNames.add(queueName);
    const workerError = new Error("original worker failure");
    let warnings = 0;
    let persistenceAttempts = 0;
    const host = await createChimpbase({
      storage: { engine: "postgres", url: postgresUrl() }, worker: { maxAttempts: 1 },
      telemetry: { persist: { log: true, metric: false, trace: false } },
      sinks: [{
        onLog(_scope, _level, message) {
          if (message === "queue failure hook failed") { warnings += 1; throw new Error("warning sink failed"); }
        },
        onMetric() {},
        startSpan() { return { setAttribute() {}, end() {} }; },
        startHandlerSpan() { return { setAttribute() {}, end() {} }; },
      }],
      registrations: [
        worker(queueName, async () => { throw workerError; }, {
          dlq: false, onFailure() { throw new Error("hook failed"); },
        }),
        action("enqueue", async (ctx) => await ctx.enqueue(queueName, {})),
      ],
    });
    const adapter = host.engine.getBlobsAdapter();
    const append = adapter.streamAppend.bind(adapter);
    try {
      await host.executeAction("enqueue");
      adapter.streamAppend = async () => { persistenceAttempts += 1; throw new Error("warning persistence failed"); };
      expect(await host.processNextQueueJob().catch((error: unknown) => error)).toBe(workerError);
      expect(warnings).toBe(1);
      expect(persistenceAttempts).toBe(1);
      expect((await pool.query<{ status: string; last_error: string }>(
        "SELECT status, last_error FROM _chimpbase_queue_jobs WHERE queue_name = $1", [queueName],
      )).rows).toEqual([{ status: "failed", last_error: workerError.message }]);
      expect(await host.processNextQueueJob()).toBeNull();
    } finally {
      adapter.streamAppend = append;
      await host.close();
    }
  });

  test("two hosts claim each queued job at most once", async () => {
    const queueName = uniqueName("concurrency");
    queueNames.add(queueName);
    const processed: string[] = [];

    const enqueueJob = action({
      name: "enqueueConcurrentJob",
      args: v.object({ id: v.string() }),
      async handler(ctx, input) {
        await ctx.enqueue(queueName, input);
      },
    });
    const consume = worker(queueName, async (_ctx, payload: { id: string }) => {
      processed.push(payload.id);
    });

    const hostA = await createChimpbase({
      project: { name: uniqueName("worker-a") },
      projectDir: process.cwd(),
      registrations: [enqueueJob, consume],
      storage: { engine: "postgres", url: postgresUrl() },
    });
    const hostB = await createChimpbase({
      project: { name: uniqueName("worker-b") },
      projectDir: process.cwd(),
      registrations: [consume],
      storage: { engine: "postgres", url: postgresUrl() },
    });
    const startedA = await hostA.start({ serve: false, runWorker: false });
    const startedB = await hostB.start({ serve: false, runWorker: false });

    try {
      for (let index = 0; index < 20; index += 1) {
        await hostA.executeAction("enqueueConcurrentJob", { id: `job-${index}` });
      }

      for (let index = 0; index < 12; index += 1) {
        await Promise.all([
          hostA.processNextQueueJob(),
          hostB.processNextQueueJob(),
        ]);
      }

      expect(processed).toHaveLength(20);
      expect(new Set(processed).size).toBe(20);
    } finally {
      await startedA.stop();
      await startedB.stop();
      await hostA.close();
      await hostB.close();
    }
  });

  test("an expired worker lease is recovered by another host", async () => {
    const queueName = uniqueName("expired-lease");
    queueNames.add(queueName);
    const processed: string[] = [];

    const enqueueJob = action({
      name: "enqueueLeasedJob",
      args: v.object({ id: v.string() }),
      async handler(ctx, input) {
        await ctx.enqueue(queueName, input);
      },
    });

    const hostA = await createChimpbase({
      project: { name: uniqueName("lease-owner") },
      projectDir: process.cwd(),
      registrations: [enqueueJob, worker(queueName, async () => {})],
      storage: { engine: "postgres", url: postgresUrl() },
    });
    const startedA = await hostA.start({ serve: false, runWorker: false });

    try {
      await hostA.executeAction("enqueueLeasedJob", { id: "recover-me" });
      await pool.query(
        `UPDATE _chimpbase_queue_jobs
         SET status = 'processing', attempt_count = 1, lease_expires_at_ms = $1
         WHERE queue_name = $2 AND status = 'pending'`,
        [Date.now() - 1, queueName],
      );
    } finally {
      await startedA.stop();
      await hostA.close();
    }

    const hostB = await createChimpbase({
      project: { name: uniqueName("lease-replacement") },
      projectDir: process.cwd(),
      registrations: [
        worker(queueName, async (_ctx, payload: { id: string }) => {
          processed.push(payload.id);
        }),
      ],
      storage: { engine: "postgres", url: postgresUrl() },
    });
    const startedB = await hostB.start({ serve: false, runWorker: false });

    try {
      await hostB.processNextQueueJob();
      expect(processed).toEqual(["recover-me"]);

      const result = await pool.query<{ attempt_count: number; status: string }>(
        "SELECT attempt_count, status FROM _chimpbase_queue_jobs WHERE queue_name = $1",
        [queueName],
      );
      expect(result.rows[0]).toMatchObject({ attempt_count: 2, status: "completed" });
    } finally {
      await startedB.stop();
      await hostB.close();
    }
  });

  for (const batchSize of [1, 2]) {
    test(`a live worker keeps ownership after lease expiry (batch ${batchSize})`, async () => {
      const queueName = uniqueName("live-lease");
      queueNames.add(queueName);
      const seen: string[] = [];
      let started!: () => void;
      let release!: () => void;
      const entered = new Promise<void>((resolve) => { started = resolve; });
      const blocked = new Promise<void>((resolve) => { release = resolve; });
      const consume = worker(queueName, async (ctx, payload: { id: string }, execution) => {
        seen.push(payload.id);
        if (payload.id === "first" && execution?.attempt === 1) {
          started();
          await blocked;
        }
        await ctx.stream.append(queueName, "processed", payload);
      });
      const a = await createChimpbase({
        storage: { engine: "postgres", url: postgresUrl() },
        workerRuntime: { leaseMs: 30 },
        registrations: [consume, action("enqueue", async (ctx) => {
          await ctx.enqueue(queueName, { id: "first" });
          if (batchSize === 2) await ctx.enqueue(queueName, { id: "second" });
        })],
      });
      const b = await createChimpbase({
        storage: { engine: "postgres", url: postgresUrl() },
        workerRuntime: { leaseMs: 30 }, registrations: [consume],
      });
      let running: Promise<unknown> | undefined;
      try {
        await a.executeAction("enqueue");
        running = a.engine.processNextQueueJobs(batchSize);
        await entered;
        await Bun.sleep(60);
        const other = await b.processNextQueueJob();
        release();
        await running;
        expect(other === null).toBe(batchSize === 1);
        expect(seen).toEqual(batchSize === 1 ? ["first"] : ["first", "second"]);
        const effects = await pool.query<{ count: number }>(
          "SELECT count(*)::int AS count FROM _chimpbase_stream_events WHERE stream_name = $1", [queueName],
        );
        expect(effects.rows[0]?.count).toBe(batchSize);
        const rows = await pool.query<{ attempt_count: number; status: string }>(
          "SELECT attempt_count, status FROM _chimpbase_queue_jobs WHERE queue_name = $1 ORDER BY id", [queueName],
        );
        expect(rows.rows).toEqual(batchSize === 1
          ? [{ attempt_count: 1, status: "completed" }]
          : [{ attempt_count: 1, status: "completed" }, { attempt_count: 2, status: "completed" }]);
      } finally {
        release();
        await running;
        await a.close();
        await b.close();
        await pool.query("DELETE FROM _chimpbase_stream_events WHERE stream_name = $1", [queueName]);
      }
    });
  }

  for (const pauseAfter of ["handler rollback", "hook rollback"] as const) {
    test(`a stale failure after ${pauseAfter} cannot overwrite a recovered job`, async () => {
      const queueName = uniqueName("stale-failure");
      const dlqName = `${queueName}.dlq`;
      queueNames.add(queueName);
      queueNames.add(dlqName);
      let rolledBack!: () => void;
      let release!: () => void;
      const entered = new Promise<void>((resolve) => { rolledBack = resolve; });
      const blocked = new Promise<void>((resolve) => { release = resolve; });
      let hooks = 0;
      const a = await createChimpbase({
        storage: { engine: "postgres", url: postgresUrl() }, workerRuntime: { leaseMs: 30 }, worker: { maxAttempts: 1 },
        registrations: [worker(queueName, async () => { throw new Error("old attempt failed"); }, {
          dlq: dlqName, onFailure: async () => { hooks += 1; throw new Error("hook failed"); },
        }), action("enqueue", async (ctx) => await ctx.enqueue(queueName, {}))],
      });
      const b = await createChimpbase({
        storage: { engine: "postgres", url: postgresUrl() }, registrations: [worker(queueName, async () => {})],
      });
      const adapter = a.engine.getBlobsAdapter();
      const rollback = adapter.rollbackTransaction.bind(adapter);
      let rollbacks = 0;
      adapter.rollbackTransaction = async () => {
        await rollback();
        rollbacks += 1;
        if (rollbacks === (pauseAfter === "handler rollback" ? 1 : 2)) {
          rolledBack();
          await blocked;
        }
      };
      let running: Promise<unknown> | undefined;
      try {
        await a.executeAction("enqueue");
        running = a.processNextQueueJob().catch((error: unknown) => error);
        await entered;
        await Bun.sleep(60);
        expect(await b.processNextQueueJob()).not.toBeNull();
        release();
        expect(await running).toMatchObject({ message: "old attempt failed" });
        expect(hooks).toBe(pauseAfter === "handler rollback" ? 0 : 1);
        const rows = await pool.query<{ attempt_count: number; status: string; queue_name: string }>(
          "SELECT attempt_count, status, queue_name FROM _chimpbase_queue_jobs WHERE queue_name = ANY($1::text[])", [[queueName, dlqName]],
        );
        expect(rows.rows).toEqual([{ attempt_count: 2, status: "completed", queue_name: queueName }]);
      } finally {
        release();
        await running;
        adapter.rollbackTransaction = rollback;
        await a.close();
        await b.close();
      }
    });
  }

  test("a workflow waiting for a signal resumes on a replacement host", async () => {
    const workflowName = uniqueName("workflow");
    const workflowId = uniqueName("workflow-instance");

    type State = { phase: "waiting" | "done" };
    const durableWorkflow = workflow<{}, State>({
      name: workflowName,
      version: 1,
      initialState: () => ({ phase: "waiting" }),
      run(ctx) {
        if (ctx.state.phase === "done") return ctx.complete(ctx.state);
        return ctx.waitForSignal("continue", {
          state: ctx.state,
          onSignal: () => ({ phase: "done" }),
        });
      },
    });
    const launch = action("launchDurableWorkflow", async (ctx) =>
      await ctx.workflow.start(durableWorkflow, {}, { workflowId })
    );
    const signal = action("signalDurableWorkflow", async (ctx) => {
      await ctx.workflow.signal(workflowId, "continue", {});
    });
    const inspect = action("inspectDurableWorkflow", async (ctx) =>
      await ctx.workflow.get<{}, State>(workflowId)
    );

    const hostA = await createChimpbase({
      project: { name: uniqueName("workflow-a") },
      projectDir: process.cwd(),
      registrations: [durableWorkflow, launch, signal, inspect],
      storage: { engine: "postgres", url: postgresUrl() },
    });
    const startedA = await hostA.start({ serve: false, runWorker: false });

    try {
      await hostA.executeAction("launchDurableWorkflow", []);
      await hostA.processNextQueueJob();
      const waiting = v.object({ status: v.string() }).parse(
        (await hostA.executeAction("inspectDurableWorkflow", [])).result,
        "waiting workflow",
      );
      expect(waiting.status).toBe("waiting_signal");
    } finally {
      await startedA.stop();
      await hostA.close();
    }

    const hostB = await createChimpbase({
      project: { name: uniqueName("workflow-b") },
      projectDir: process.cwd(),
      registrations: [durableWorkflow, launch, signal, inspect],
      storage: { engine: "postgres", url: postgresUrl() },
    });
    const startedB = await hostB.start({ serve: false, runWorker: false });

    try {
      await hostB.executeAction("signalDurableWorkflow", []);
      await hostB.processNextQueueJob();
      const completed = v.object({
        state: v.object({ phase: v.enum(["waiting", "done"] as const) }),
        status: v.string(),
      }).parse(
        (await hostB.executeAction("inspectDurableWorkflow", [])).result,
        "completed workflow",
      );
      expect(completed.status).toBe("completed");
      expect(completed.state.phase).toBe("done");
    } finally {
      await startedB.stop();
      await hostB.close();
      await pool.query("DELETE FROM _chimpbase_workflow_signals WHERE workflow_id = $1", [workflowId]);
      await pool.query("DELETE FROM _chimpbase_workflow_instances WHERE workflow_id = $1", [workflowId]);
    }
  });

  test("two hosts schedule a due cron slot only once", async () => {
    const scheduleName = uniqueName("cron");
    let executions = 0;
    const registration = cron(scheduleName, "* * * * *", async () => {
      executions += 1;
    });

    const hostA = await createChimpbase({
      project: { name: uniqueName("cron-a") },
      projectDir: process.cwd(),
      registrations: [registration],
      storage: { engine: "postgres", url: postgresUrl() },
    });
    const hostB = await createChimpbase({
      project: { name: uniqueName("cron-b") },
      projectDir: process.cwd(),
      registrations: [registration],
      storage: { engine: "postgres", url: postgresUrl() },
    });
    const startedA = await hostA.start({ serve: false, runWorker: false });
    const startedB = await hostB.start({ serve: false, runWorker: false });

    try {
      await hostA.processNextCronSchedule();
      await hostB.processNextCronSchedule();
      await pool.query(
        "UPDATE _chimpbase_cron_schedules SET next_fire_at_ms = $1 WHERE schedule_name = $2",
        [Date.now() - 60_000, scheduleName],
      );

      const scheduled = await Promise.all([
        hostA.processNextCronSchedule(),
        hostB.processNextCronSchedule(),
      ]);
      expect(scheduled.filter(Boolean)).toHaveLength(1);

      await Promise.all([
        hostA.processNextQueueJob(),
        hostB.processNextQueueJob(),
      ]);
      expect(executions).toBe(1);
    } finally {
      await startedA.stop();
      await startedB.stop();
      await hostA.close();
      await hostB.close();
      await pool.query("DELETE FROM _chimpbase_cron_schedules WHERE schedule_name = $1", [scheduleName]);
    }
  });

  test("heterogeneous hosts preserve foreign crons and leave their schedules and jobs for a replacement host", async () => {
    const scheduleA = uniqueName("cron-owner-a");
    const scheduleB = uniqueName("cron-owner-b");
    let executions = 0;
    const registrationA = cron(scheduleA, "* * * * *", () => { executions += 1; });
    const a = await createChimpbase({
      storage: { engine: "postgres", url: postgresUrl() }, registrations: [registrationA],
    });
    const b = await createChimpbase({
      storage: { engine: "postgres", url: postgresUrl() },
      registrations: [cron(scheduleB, "* * * * *", () => { throw new Error("foreign cron ran"); })],
    });
    let replacement: typeof a | undefined;
    let aClosed = false;
    try {
      await a.syncCronSchedules();
      await b.syncCronSchedules();
      await a.close();
      aClosed = true;
      await pool.query("UPDATE _chimpbase_cron_schedules SET next_fire_at_ms = $1 WHERE schedule_name = $2", [Date.now() - 120_000, scheduleA]);
      expect(await b.processNextCronSchedule()).toBeNull();
      const schedules = await pool.query<{ schedule_name: string; lease_token: string | null }>(
        "SELECT schedule_name, lease_token FROM _chimpbase_cron_schedules WHERE schedule_name = ANY($1::text[]) ORDER BY schedule_name",
        [[scheduleA, scheduleB]],
      );
      expect(schedules.rows).toEqual([
        { schedule_name: scheduleA, lease_token: null }, { schedule_name: scheduleB, lease_token: null },
      ]);
      replacement = await createChimpbase({
        storage: { engine: "postgres", url: postgresUrl() }, registrations: [registrationA],
      });
      expect((await replacement.processNextCronSchedule())?.scheduleName).toBe(scheduleA);
      expect(await b.processNextQueueJob()).toBeNull();
      const jobs = await pool.query<{ status: string; attempt_count: number }>(
        "SELECT status, attempt_count FROM _chimpbase_queue_jobs WHERE payload_json->>'scheduleName' = $1", [scheduleA],
      );
      expect(jobs.rows).toEqual([{ status: "pending", attempt_count: 0 }]);
      expect(await replacement.processNextQueueJob()).not.toBeNull();
      expect(executions).toBe(1);
    } finally {
      if (!aClosed) await a.close();
      await b.close();
      await replacement?.close();
      await pool.query("DELETE FROM _chimpbase_queue_jobs WHERE payload_json->>'scheduleName' = ANY($1::text[])", [[scheduleA, scheduleB]]);
      await pool.query("DELETE FROM _chimpbase_cron_runs WHERE schedule_name = ANY($1::text[])", [[scheduleA, scheduleB]]);
      await pool.query("DELETE FROM _chimpbase_cron_schedules WHERE schedule_name = ANY($1::text[])", [[scheduleA, scheduleB]]);
    }
  });

  for (const claimMode of ["single", "batch"] as const) {
    test(`async deliveries target exact subscriptions and skip unsupported legacy events (${claimMode})`, async () => {
      const eventName = uniqueName(`subscription-${claimMode}`);
      const legacyEvent = uniqueName(`legacy-${claimMode}`);
      const calls: string[] = [];
      const a = await createChimpbase({
        storage: { engine: "postgres", url: postgresUrl() },
        registrations: [
          subscription(eventName, () => { calls.push("left"); }, { dispatch: "async", name: "left" }),
          subscription(eventName, () => { throw new Error("right belongs to the replacement"); }, { dispatch: "async", name: "right" }),
          action("emit", (ctx) => { ctx.pubsub.publish(eventName, { value: 1 }); }),
        ],
      });
      const b = await createChimpbase({
        storage: { engine: "postgres", url: postgresUrl() },
        registrations: [
          subscription(eventName, () => { calls.push("right"); }, { dispatch: "async", name: "right" }),
          subscription(legacyEvent, () => { calls.push("legacy"); }, { dispatch: "async", name: "legacy" }),
        ],
      });
      if (claimMode === "single") {
        a.engine.getBlobsAdapter().claimNextQueueJobs = undefined;
        b.engine.getBlobsAdapter().claimNextQueueJobs = undefined;
      }
      try {
        await a.executeAction("emit");
        a.registry.subscriptions.set(eventName, (a.registry.subscriptions.get(eventName) ?? []).filter((entry) => entry.name === "left"));
        await pool.query(
          "INSERT INTO _chimpbase_queue_jobs (queue_name, payload_json, status, available_at_ms, attempt_count) VALUES ($1, $2::jsonb, 'pending', 0, 0)",
          ["__chimpbase.subscription.run", JSON.stringify({ eventName: legacyEvent, payload: {}, payloadJson: "{}" })],
        );
        expect(await a.processNextQueueJob()).not.toBeNull();
        expect(calls).toEqual(["left"]);
        expect(await a.processNextQueueJob()).toBeNull();
        const pending = await pool.query<{ subscription: string | null; attempt_count: number }>(
          "SELECT payload_json->>'subscriptionName' AS subscription, attempt_count FROM _chimpbase_queue_jobs WHERE payload_json->>'eventName' = ANY($1::text[]) AND status = 'pending' ORDER BY id",
          [[eventName, legacyEvent]],
        );
        expect(pending.rows).toEqual([{ subscription: "right", attempt_count: 0 }, { subscription: null, attempt_count: 0 }]);
        expect(await b.engine.processNextQueueJobs(5)).toHaveLength(claimMode === "single" ? 1 : 2);
        if (claimMode === "single") expect(await b.processNextQueueJob()).not.toBeNull();
        expect(calls).toEqual(["left", "right", "legacy"]);
        expect(await b.processNextQueueJob()).toBeNull();
        const completed = await pool.query<{ status: string; attempt_count: number }>(
          "SELECT status, attempt_count FROM _chimpbase_queue_jobs WHERE payload_json->>'eventName' = ANY($1::text[]) ORDER BY id", [[eventName, legacyEvent]],
        );
        expect(completed.rows).toEqual(Array.from({ length: 3 }, () => ({ status: "completed", attempt_count: 1 })));
      } finally {
        await a.close();
        await b.close();
        await pool.query("DELETE FROM _chimpbase_queue_jobs WHERE payload_json->>'eventName' = ANY($1::text[])", [[eventName, legacyEvent]]);
        await pool.query("DELETE FROM _chimpbase_events WHERE event_name = $1", [eventName]);
      }
    });

    test(`workflow claims respect exact persisted definitions across restarts and expired leases (${claimMode})`, async () => {
      const workflowName = uniqueName(`workflow-version-${claimMode}`);
      const workflowA = uniqueName("workflow-a");
      const workflowB = uniqueName("workflow-b");
      const executions: number[] = [];
      const definition = (version: number) => workflow({
        name: workflowName, version, initialState: () => ({}),
        run(ctx) { executions.push(version); return ctx.complete(); },
      });
      const a = await createChimpbase({
        storage: { engine: "postgres", url: postgresUrl() },
        registrations: [definition(1), action("launch", async (ctx) => await ctx.workflow.start(workflowName, {}, { workflowId: workflowA }))],
      });
      const b = await createChimpbase({
        storage: { engine: "postgres", url: postgresUrl() },
        registrations: [definition(2), action("launch", async (ctx) => await ctx.workflow.start(workflowName, {}, { workflowId: workflowB }))],
      });
      let replacement: typeof a | undefined;
      let aClosed = false;
      if (claimMode === "single") b.engine.getBlobsAdapter().claimNextQueueJobs = undefined;
      try {
        await a.executeAction("launch");
        await a.close();
        aClosed = true;
        await pool.query(
          "UPDATE _chimpbase_queue_jobs SET status = 'processing', attempt_count = 1, lease_expires_at_ms = 0 WHERE payload_json->>'workflowId' = $1",
          [workflowA],
        );
        expect(await b.processNextQueueJob()).toBeNull();
        const untouched = await pool.query<{ status: string; attempt_count: number; lease_expires_at_ms: number }>(
          "SELECT status, attempt_count, lease_expires_at_ms::double precision AS lease_expires_at_ms FROM _chimpbase_queue_jobs WHERE payload_json->>'workflowId' = $1",
          [workflowA],
        );
        expect(untouched.rows).toEqual([{ status: "processing", attempt_count: 1, lease_expires_at_ms: 0 }]);
        await b.executeAction("launch");
        expect(await b.processNextQueueJob()).not.toBeNull();
        expect(executions).toEqual([2]);
        replacement = await createChimpbase({ storage: { engine: "postgres", url: postgresUrl() }, registrations: [definition(1)] });
        if (claimMode === "single") replacement.engine.getBlobsAdapter().claimNextQueueJobs = undefined;
        expect(await replacement.processNextQueueJob()).not.toBeNull();
        expect(executions).toEqual([2, 1]);
        const completed = await pool.query<{ status: string; attempt_count: number }>(
          "SELECT status, attempt_count FROM _chimpbase_queue_jobs WHERE payload_json->>'workflowId' = ANY($1::text[]) ORDER BY id",
          [[workflowA, workflowB]],
        );
        expect(completed.rows).toEqual([{ status: "completed", attempt_count: 2 }, { status: "completed", attempt_count: 1 }]);
      } finally {
        if (!aClosed) await a.close();
        await b.close();
        await replacement?.close();
        await pool.query("DELETE FROM _chimpbase_queue_jobs WHERE payload_json->>'workflowId' = ANY($1::text[])", [[workflowA, workflowB]]);
        await pool.query("DELETE FROM _chimpbase_workflow_instances WHERE workflow_id = ANY($1::text[])", [[workflowA, workflowB]]);
      }
    });
  }
});
