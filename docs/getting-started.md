# Getting Started

Chimpbase is a PostgreSQL-backed runtime for building backends that need more than request-response — background jobs, durable queues, cron schedules, workflows — without adding a distributed systems stack before it's necessary.

## Install

Choose your runtime:

::: code-group

```bash [Bun]
bun add @chimpbase/bun
```

```bash [Deno]
deno add npm:@chimpbase/deno
```

```bash [Node]
npm install @chimpbase/node
```

:::

## Create Your App

Create a `chimpbase.app.ts`. This is the only file you need:

```ts
import type { ChimpbaseAppDefinitionInput } from "@chimpbase/bun";
import { action, subscription, worker, v } from "@chimpbase/runtime";
import { Hono } from "hono";
import type { ChimpbaseRouteEnv } from "@chimpbase/runtime";

// ── Actions ─────────────────────────────────────────────────────────────

const createCustomer = action({
  name: "createCustomer",
  args: v.object({
    email: v.string(),
    name: v.string(),
  }),
  async handler(ctx, input) {
    const [customer] = await ctx.db.query<{ id: number }>(
      "INSERT INTO customers (email, name) VALUES (?1, ?2) RETURNING id",
      [input.email, input.name],
    );

    ctx.pubsub.publish("customer.created", {
      customerId: customer.id,
      email: input.email,
    });

    return customer;
  },
});

// ── Subscriptions ───────────────────────────────────────────────────────

const onCustomerCreated = subscription(
  "customer.created",
  async (ctx, payload) => {
    await ctx.enqueue("customer.welcome", payload);
  },
  { idempotent: true, name: "enqueueWelcome" },
);

// ── Workers ─────────────────────────────────────────────────────────────

const sendWelcome = worker("customer.welcome", async (ctx, payload) => {
  ctx.log.info("sending welcome email", { email: payload.email });
});

// ── HTTP Routes ─────────────────────────────────────────────────────────

const api = new Hono<{ Bindings: ChimpbaseRouteEnv }>();

api.post("/customers", async (c) => {
  const body = await c.req.json();
  const customer = await c.env.action(createCustomer, body);
  return c.json(customer, 201);
});

// ── App Definition ──────────────────────────────────────────────────────

export default {
  project: { name: "my-app" },
  httpHandler: api,
  migrations: {
    sqlite: [{
      name: "001_init",
      sql: "CREATE TABLE customers (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT NOT NULL, name TEXT NOT NULL)",
    }],
  },
  registrations: [
    createCustomer,
    onCustomerCreated,
    sendWelcome,
  ],
} satisfies ChimpbaseAppDefinitionInput;
```

## Run It

```bash
bun run chimpbase.app.ts
```

The server starts on port 3000 with:
- A `/health` endpoint
- Your HTTP routes (via Hono)
- A background worker processing queued jobs

## Choose a Storage Engine

### SQLite (development)

Default — no configuration needed. Data stored in `data/{project-name}.db`.

### PostgreSQL (production)

Set the environment variable:

```bash
CHIMPBASE_STORAGE_ENGINE=postgres
DATABASE_URL=postgresql://localhost/mydb
```

PostgreSQL supports concurrent workers and coordination across multiple instances.

### Memory (testing)

```bash
CHIMPBASE_STORAGE_ENGINE=memory
```

Data is lost on restart. Used for unit tests.

## What's Next

- [Actions](/actions) — define business operations
- [Subscriptions](/subscriptions) — react to events
- [Workers & Queues](/workers) — durable background jobs
- [Cron](/cron) — scheduled tasks
- [Workflows](/workflows) — long-running processes
- [HTTP Routes](/routes) — handle HTTP requests with Hono
- [Database](/database) — raw SQL and Kysely
- [Configuration](/configuration) — environment variables, custom entry points, and advanced options

### Plugins

- [Auth](/auth) — API key authentication, user management, scopes, rate limiting
- [Webhooks](/webhooks) — outbound + inbound webhooks with HMAC
- [REST Collections](/rest-collections) — expose collections as REST APIs
