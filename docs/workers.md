# Workers & Queues

Workers process durable background jobs from queues. Jobs survive process restarts, retry on failure, and move to a dead letter queue (DLQ) after exhausting attempts.

Start with SQLite and `bunx chimpbase dev`: HTTP handlers and workers run in the same process. Use `npx chimpbase-node dev` for Node.

## Enqueuing Jobs

From any handler with a `ChimpbaseContext`:

```ts
const jobId = await ctx.enqueue(sendEmail, {
  to: "user@example.com",
  subject: "Welcome!",
  body: "Thanks for signing up.",
});
```

Passing the worker registration checks its payload in TypeScript. `ctx.queue.enqueue(...)` accepts the same arguments; both return a numeric job ID scoped to the current database. String queue names remain available for dynamic producers, but their payloads cannot be checked against a handler. Unregistered destinations stay pending; use the job commands below to find them.

### Delayed jobs

```ts
await ctx.enqueue("reminder.send", { userId: 42 }, {
  delayMs: 60_000, // process after 1 minute
});
```

`delayMs` must be a nonnegative safe integer; negative, fractional, nonfinite or unsafe delays fail before persistence. Jobs enqueued inside an action commit with its database writes and roll back if the action fails.

## Defining Workers

Save this complete example as `chimpbase.app.ts`. It queues a delivery request, then records its recipient when the worker runs:

```ts chimpbase-check:worker-email
import type { ChimpbaseAppDefinitionInput } from "chimpbase/core";
import { action, v, worker } from "chimpbase/runtime";

type EmailPayload = { to: string; subject: string; body: string };

export const sendEmail = worker("email.send", async (ctx, payload: EmailPayload) => {
  await ctx.kv.set("email:last-recipient", payload.to);
  ctx.log.info("email request recorded", { to: payload.to });
  ctx.metric("email.requests.recorded", 1);
});

const requestEmail = action("requestEmail", async (ctx, payload: EmailPayload) => {
  return await ctx.enqueue(sendEmail, payload);
});
const lastRecipient = action("lastRecipient", async (ctx) => {
  return await ctx.kv.get("email:last-recipient", v.string());
});

export default {
  project: { name: "email-worker" },
  registrations: [requestEmail, lastRecipient, sendEmail],
} satisfies ChimpbaseAppDefinitionInput;
```

Annotate the worker's payload type. Registrations can be imported from other files; duplicate worker names fail when the application loads.

## Handler Signature

```ts
(ctx: ChimpbaseContext, payload: TPayload, execution?: ChimpbaseWorkerExecution) => TResult | Promise<TResult>
```

If the handler throws, the job is retried according to the worker configuration.

Execution metadata contains `attempt`, `jobId`, `queue` and `signal`. The signal aborts when the worker stops. Bound external calls and combine their deadline with this signal:

```ts
const sendWebhook = worker("webhook.send", async (_ctx, payload: { url: string }, execution) => {
  const timeout = AbortSignal.timeout(5_000);
  const signal = execution ? AbortSignal.any([execution.signal, timeout]) : timeout;
  const response = await fetch(payload.url, { method: "POST", signal });
  if (!response.ok) throw new Error(`webhook returned ${response.status}`);
});
```

SQLite serializes database operations for the entire handler transaction. A worker awaiting network I/O delays HTTP actions that need the database. Increasing concurrency does not change this SQLite limit; a lease does not interrupt execution. Stop waits for running handlers to finish, so handlers that ignore cancellation and have no deadline can delay shutdown indefinitely.

Worker database writes, published events, and job completion commit in one transaction.
A completion or commit failure rolls them back before retrying. Telemetry or event bus
failures after commit do not reopen a completed job. External effects such as sending
email still need idempotency because they cannot roll back with the database.
Use an application idempotency key for external effects; a database job ID alone is insufficient.

PostgreSQL holds the claimed job's row lock throughout execution, so an expired lease cannot reclaim a job while its handler transaction is active. After a host disconnects, the lock releases and the expired lease can be recovered. Batched claims check their attempt number before execution; a stale claim or failure callback cannot overwrite a newer attempt's state.

A failed job does not abandon the remaining jobs already claimed in its batch. Each job keeps its own transaction and retry handling; the batch reports its first failure after attempting the remaining jobs.

An `onFailure` hook runs after the handler transaction rolls back. Successful hook writes and events commit with the job's retry or terminal state. If the hook throws, its writes and events roll back, the runtime still records the original worker error and advances retry/DLQ handling, and a warning identifies the hook failure. A recovered claim cannot be overwritten during this fallback. Database failures while recording the job state still propagate and leave the claim recoverable after lease expiry.

Its third argument contains the execution metadata and original `payload`.

Nodes sharing a database may register different handlers. Internal subscription jobs are claimed only by nodes registering their event and subscription name, cron jobs only by nodes registering their schedule, and workflow jobs only by nodes registering the persisted workflow name and version. Unsupported work stays queued without consuming a retry attempt. Use unique subscription names for different handler sets; unnamed or duplicate-name handlers remain a shared group. See [Event bus transports](/advanced/event-bus) for existing unnamed jobs and deployment guidance.

## Dead Letter Queue (DLQ)

When a job fails after all retry attempts, it's moved to a DLQ. Register a DLQ worker to handle failed jobs:

The default destination is `<queue>.dlq`. Without a DLQ handler, its envelope remains pending and visible through job inspection. Setting `{ dlq: false }` leaves the source as terminal `failed` instead.

```ts
import { worker, type ChimpbaseDlqEnvelope } from "chimpbase/runtime";

const emailWorker = worker("email.send", sendEmailHandler);

const emailDlq = worker(
  "email.send.dlq",
  async (ctx, envelope: ChimpbaseDlqEnvelope<EmailPayload>) => {
    ctx.log.error("email delivery failed permanently", {
      to: envelope.payload.to,
      jobId: envelope.jobId,
      attempts: envelope.attempts,
      error: envelope.error,
      failedAt: envelope.failedAt,
    });
  },
  { dlq: false }, // DLQ workers should not have their own DLQ
);
```

Link a worker to its DLQ:

```ts
const emailWorker = worker("email.send", sendEmailHandler, {
  dlq: "email.send.dlq",
});
```

In a business module, a custom DLQ name resolves within that module's queue namespace. Imported actions invoked by module workers and crons use the owning module's visibility rules.

### DLQ Envelope

```ts
interface ChimpbaseDlqEnvelope<TPayload> {
  jobId?: number;     // source job ID; older persisted envelopes may omit it
  attempts: number;   // total attempts made
  error: string;      // last error message
  failedAt: string;   // ISO 8601 timestamp
  payload: TPayload;  // original job payload
  queue: string;      // original queue name
}
```

## Inspect and retry jobs

Run from the app directory, or add `--project-dir PATH`:

```bash
bunx chimpbase jobs list
bunx chimpbase jobs list --queue email.send --status pending --limit 20
bunx chimpbase jobs inspect 42
bunx chimpbase jobs retry 42
```

For Node, use `npx chimpbase-node`. Inspection shows queue, payload, status, attempt, availability, lease expiration, last error and whether the current host registers its destination. IDs belong to one database; pruned completed jobs are no longer inspectable.

The programmatic equivalents are `host.listJobs({ queue?, status?, limit? })`, `host.inspectJob(id)` and `host.retryJob(id)`. Filter pending jobs with `registered: false` to find unsupported destinations; `drain()` considers only locally supported work. Retry accepts terminal `failed` or `dlq` jobs, resets attempts and makes them immediately available. Retrying a source marks its correlated pending DLQ envelope completed with a `superseded by retry` diagnostic in the same transaction. It rejects while the envelope is processing or has itself failed; handle that envelope first. Legacy envelopes without a source `jobId` must be handled before retrying. Register the source worker with its original DLQ configuration before retrying; a DLQ handler is optional. Completed envelopes remain for inspection. Internal coordination jobs cannot be retried through this API.

## Configuration

Worker behavior is configured in the app definition:

```ts
export default {
  worker: {
    maxAttempts: 5,     // retries before DLQ
    retryDelayMs: 1000, // delay between retries
  },
} satisfies ChimpbaseAppDefinitionInput;
```

Set runtime worker settings through environment variables. See the [environment-variable reference](/configuration#environment-variables) for defaults:

```bash
export CHIMPBASE_WORKER_CONCURRENCY=4
export CHIMPBASE_WORKER_POLL_INTERVAL_MS=250
export CHIMPBASE_WORKER_LEASE_MS=30000
```

All worker numeric settings must be safe integers. `maxAttempts`, `concurrency` and `leaseMs` must be positive; `retryDelayMs` may be zero. `pollIntervalMs` must be from `1` to `2147483647`. Invalid settings fail at startup, including explicitly empty or malformed worker environment variables.

Completed jobs are pruned daily at 03:00 UTC after 30 days by default. Pending, processing, failed and DLQ jobs remain available for recovery. SQLite preserves its highest job ID until a newer row exists, so cleanup may temporarily keep one older completed row. Cleanup runs only while a worker processes schedules.

Change retention through the host options in a [custom entry point](/advanced/app-composition#custom-entry-point):

```ts
const chimpbase = await createChimpbase({
  app,
  workerRuntime: {
    retention: { enabled: true, maxAgeDays: 30, schedule: "0 3 * * *" },
  },
});
```

Set `workerRuntime.retention.enabled: false` to keep all completed jobs. `maxAgeDays` must be an integer from `1` to `100000000`.

## Registration

```ts
export default {
  registrations: [
    worker("email.send", sendEmailHandler, { dlq: "email.send.dlq" }),
    worker("email.send.dlq", emailDlqHandler, { dlq: false }),
  ],
};
```
