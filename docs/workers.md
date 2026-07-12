# Workers & Queues

Workers process durable background jobs from queues. Jobs survive process restarts, retry on failure, and move to a dead letter queue (DLQ) after exhausting attempts.

## Enqueuing Jobs

From any handler with a `ChimpbaseContext`:

```ts
await ctx.enqueue("email.send", {
  to: "user@example.com",
  subject: "Welcome!",
  body: "Thanks for signing up.",
});
```

### Delayed jobs

```ts
await ctx.enqueue("reminder.send", { userId: 42 }, {
  delayMs: 60_000, // process after 1 minute
});
```

## Defining Workers

```ts
import { worker } from "@chimpbase/runtime";

const sendEmail = worker("email.send", async (ctx, payload) => {
  ctx.log.info("sending email", { to: payload.to });
  // ... send the email
  ctx.metric("emails.sent", 1);
});
```

## Handler Signature

```ts
(ctx: ChimpbaseContext, payload: TPayload) => TResult | Promise<TResult>
```

If the handler throws, the job is retried according to the worker configuration.

## Dead Letter Queue (DLQ)

When a job fails after all retry attempts, it's moved to a DLQ. Register a DLQ worker to handle failed jobs:

```ts
import { worker, type ChimpbaseDlqEnvelope } from "@chimpbase/runtime";

const emailWorker = worker("email.send", sendEmailHandler);

const emailDlq = worker(
  "email.send.dlq",
  async (ctx, envelope: ChimpbaseDlqEnvelope<EmailPayload>) => {
    ctx.log.error("email delivery failed permanently", {
      to: envelope.payload.to,
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

### DLQ Envelope

```ts
interface ChimpbaseDlqEnvelope<TPayload> {
  attempts: number;   // total attempts made
  error: string;      // last error message
  failedAt: string;   // ISO 8601 timestamp
  payload: TPayload;  // original job payload
  queue: string;      // original queue name
}
```

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

Additional settings via environment variables:

```
CHIMPBASE_WORKER_CONCURRENCY=4
CHIMPBASE_WORKER_POLL_INTERVAL_MS=250
CHIMPBASE_WORKER_LEASE_MS=30000
```

## Registration

```ts
export default {
  registrations: [
    worker("email.send", sendEmailHandler, { dlq: "email.send.dlq" }),
    worker("email.send.dlq", emailDlqHandler, { dlq: false }),
  ],
};
```
