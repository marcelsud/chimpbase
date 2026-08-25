import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { Pool } from "pg";

import { createChimpbase } from "../packages/bun/src/library.ts";
import {
  action,
  cron,
  v,
  worker,
  workflow,
  type ChimpbaseDlqEnvelope,
} from "../packages/runtime/index.ts";

const PG_URL = process.env.CHIMPBASE_TEST_PG_URL;
const describeIfPg = PG_URL ? describe : describe.skip;

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
      storage: { engine: "postgres", url: PG_URL! },
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
      storage: { engine: "postgres", url: PG_URL! },
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
      storage: { engine: "postgres", url: PG_URL! },
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
      storage: { engine: "postgres", url: PG_URL! },
    });
    const hostB = await createChimpbase({
      project: { name: uniqueName("worker-b") },
      projectDir: process.cwd(),
      registrations: [consume],
      storage: { engine: "postgres", url: PG_URL! },
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
      storage: { engine: "postgres", url: PG_URL! },
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
      storage: { engine: "postgres", url: PG_URL! },
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
      storage: { engine: "postgres", url: PG_URL! },
    });
    const startedA = await hostA.start({ serve: false, runWorker: false });

    try {
      await hostA.executeAction("launchDurableWorkflow", []);
      await hostA.processNextQueueJob();
      const waiting = (await hostA.executeAction("inspectDurableWorkflow", [])).result as {
        status: string;
      };
      expect(waiting.status).toBe("waiting_signal");
    } finally {
      await startedA.stop();
      await hostA.close();
    }

    const hostB = await createChimpbase({
      project: { name: uniqueName("workflow-b") },
      projectDir: process.cwd(),
      registrations: [durableWorkflow, launch, signal, inspect],
      storage: { engine: "postgres", url: PG_URL! },
    });
    const startedB = await hostB.start({ serve: false, runWorker: false });

    try {
      await hostB.executeAction("signalDurableWorkflow", []);
      await hostB.processNextQueueJob();
      const completed = (await hostB.executeAction("inspectDurableWorkflow", [])).result as {
        state: State;
        status: string;
      };
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
      storage: { engine: "postgres", url: PG_URL! },
    });
    const hostB = await createChimpbase({
      project: { name: uniqueName("cron-b") },
      projectDir: process.cwd(),
      registrations: [registration],
      storage: { engine: "postgres", url: PG_URL! },
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
