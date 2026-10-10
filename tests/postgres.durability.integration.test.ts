import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { Pool } from "pg";

import { createChimpbase } from "../packages/bun/src/library.ts";
import { applyPostgresSqlMigrations } from "../packages/postgres/src/index.ts";
import {
  action,
  cron,
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
        await ctx.enqueue(queueName, input);
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
      await host.executeAction("enqueueRetryJob", { id: "retry-me" });
      await expect(host.processNextQueueJob()).rejects.toThrow("expected retry failure");
      await expect(host.processNextQueueJob()).rejects.toThrow("expected retry failure");
      await host.processNextQueueJob();

      expect(attempts).toBe(2);
      expect(deadLetters).toHaveLength(1);
      expect(deadLetters[0]).toMatchObject({
        attempts: 2,
        payload: { id: "retry-me" },
        queue: queueName,
      });
    } finally {
      await started.stop();
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
});
