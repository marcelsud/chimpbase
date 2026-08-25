# chimpbase

## Build complex backends with fewer moving parts. Chimpbase + PostgreSQL

PostgreSQL-backed backend primitives for software that needs more than request-response, without adding a distributed systems stack before it is necessary.

Chimpbase gives you a small runtime for the parts of backend software that usually force teams to adopt extra infrastructure too early:

- actions for business operations
- workers for background jobs
- durable queues
- durable cron schedules
- workflows that survive time, retries and restarts
- HTTP handlers when you need them

The goal is simple:

Use PostgreSQL as both your application database and your coordination layer, so you can build serious software without starting with a broker, a scheduler and a workflow engine.

## What You Don't Need On Day One

With Chimpbase + PostgreSQL, you do not need separate tools just to get started with:

- background jobs
- recurring jobs
- durable retries
- long-running business processes
- internal event handling
- operational state

That means fewer moving parts, fewer integration boundaries and less accidental complexity while the product is still taking shape.

## 30-Second Example

```ts
import { createChimpbase } from "chimpbase/runtime/bun";
import { action, subscription, v, worker } from "chimpbase/runtime";

const chimpbase = await createChimpbase({
  storage: { engine: "postgres", url: process.env.DATABASE_URL! },
});

const createCustomer = action({
  args: v.object({
    email: v.string(),
    name: v.string(),
    plan: v.string(),
  }),
  async handler(ctx, input) {
    const [customer] = await ctx.db.query<{ id: number }>(
      "insert into customers (email, name, plan) values (?1, ?2, ?3) returning id",
      [input.email, input.name, input.plan],
    );

    ctx.pubsub.publish("customer.created", {
      customerId: customer.id,
      email: input.email,
    });

    return customer;
  },
});

chimpbase.register({ createCustomer });

chimpbase.register(
  subscription("customer.created", async (ctx, event) => {
    await ctx.enqueue("customer.sync", event);
  }, { idempotent: true, name: "enqueueCustomerSync" }),
  worker("customer.sync", async (ctx, event) => {
    ctx.log.info("syncing customer", { customerId: event.customerId });
    await ctx.db.query(
      "update customers set synced_at = now() where id = ?1",
      [event.customerId],
    );
  }),
);

await chimpbase.start();
```

This is the shape Chimpbase is optimizing for:

- one runtime
- one database
- explicit backend primitives
- no infrastructure ceremony by default

## Why Chimpbase Feels Simpler

Most backend complexity is real:

- state changes
- side effects
- retries
- delayed work
- recurring work
- processes that survive time

What usually makes systems harder is spreading that complexity across too many tools too early.

Chimpbase keeps those concerns close together:

- `action(...)` for business operations
- `subscription(...)` for internal choreography
- `enqueue(...)` + `worker(...)` for durable background work
- `cron(...)` for recurring work
- `workflow(...)` for long-running processes

All of them run on the same runtime and can share the same PostgreSQL storage story.

## PostgreSQL First

Chimpbase supports SQLite and memory for local development and tests.

SQLite is available across the Bun, Deno and Node hosts. It is a good fit for:

- local development
- tests
- isolated single-runtime deployments

If multiple runtimes or containers need to coordinate through the same durable queue, subscription or workflow state, use PostgreSQL.

But the default recommendation is: 

> Just use PostgreSQL

That gives you a clean baseline:

- application data in PostgreSQL
- queue state in PostgreSQL
- cron schedule state in PostgreSQL
- workflow state in PostgreSQL
- fewer operational dependencies on day one

If later you need more infrastructure, add it because the workload demands it, not because the framework required it from the start.

## Quick Start

Install the single public package:

```bash
npm install chimpbase
```

Import the portable runtime DSL from `chimpbase/runtime`, then import the adapter for the host that runs the process:

```ts
import { action, cron, subscription, v, worker, workflow } from "chimpbase/runtime";
import { createChimpbase } from "chimpbase/runtime/bun";

const chimpbase = await createChimpbase({
  storage: { engine: "postgres", url: process.env.DATABASE_URL! },
});
```

The root `chimpbase` export and `chimpbase/runtime` contain only the portable runtime DSL. They do not auto-detect a host or re-export any adapter. Each process must explicitly select exactly one adapter:

- Bun: `chimpbase/runtime/bun`
- Node: `chimpbase/runtime/node`
- Deno: `chimpbase/runtime/deno`

Register actions, workers, subscriptions and cron jobs explicitly:

```ts
chimpbase.register({ createCustomer });

chimpbase.register(
  worker("customer.sync", syncCustomer),
  cron("billing.rollup", "0 * * * *", runBillingRollup),
);
```

Then start the runtime:

```ts
await chimpbase.start();
```

## The Primitives

### `action`

Use `action(...)` for business operations that may be called from HTTP, CLI, workflows or other actions.

### `subscription`

Use `subscription(...)` for internal pub/sub reactions. Mark handlers as idempotent when replay safety matters.

### `enqueue` + `worker`

Use queues and workers for durable background execution and retries.

### `cron`

Use `cron(...)` for recurring work such as rollups, reminders and cleanup jobs.

Missed cron backlog is skipped after downtime. The runtime resumes from the current slot instead of replaying every missed interval.

### `workflow`

Use `workflow(...)` when a business process has to survive time, restarts and retries.

### `ctx.blobs`

Use `ctx.blobs` (from `chimpbase/blobs`) for binary object storage with S3-like semantics: buckets, keys, metadata, multipart uploads, signed URLs, copy, listing. The default driver writes files under a configurable root so plain `rsync` can mirror them for backup.

### `service` + `ctx.mesh`

Use `service()` and `chimpbaseMesh()` (from `chimpbase/mesh`) to group actions into versioned services and discover peers across nodes automatically. Each node advertises into a Postgres registry table and reaches remote actions via `ctx.mesh.call(...)` — no extra broker.

### `ctx.db`

Use `ctx.db.query(...)` for raw SQL and `ctx.db.kysely<T>()` for type-safe queries via Kysely.

## Examples

The `examples/` ladder under [`examples/bun`](examples/bun), [`examples/node`](examples/node), and [`examples/deno`](examples/deno) covers three rungs per runtime:

- `basic` — actions + `route()` over SQLite. The smallest runnable app.
- `intermediate` — adds subscriptions, workers, cron, and Postgres.
- `advanced` — adds `workflow`, collections, KV, streams, plugins (`chimpbase/auth`, `chimpbase/webhooks`, `chimpbase/rest-collections`, `chimpbase/otel`), and multi-replica Docker Compose.

## When Chimpbase Is A Good Fit

Chimpbase is a good fit when you want:

- PostgreSQL as the main system of record
- explicit backend primitives instead of a large framework abstraction
- background work without adopting a broker immediately
- durable workflows without introducing a separate workflow platform
- a small operational footprint while the product is still evolving

