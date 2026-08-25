# Context Overview

Every handler in Chimpbase — actions, subscriptions, workers, cron jobs, and workflow steps — receives a `ChimpbaseContext` as its first argument. The context provides access to all runtime capabilities.

## Context API

| Category | API | Purpose |
|----------|-----|---------|
| **Database** | `ctx.db.query(sql, params)` | Raw SQL queries |
| | `ctx.db.kysely<T>()` | Typed Kysely query builder |
| **Pub/Sub** | `ctx.pubsub.publish(event, payload)` | Publish ephemeral events |
| **Queues** | `ctx.enqueue(name, payload, opts?)` | Enqueue durable background jobs |
| **Key-Value** | `ctx.kv.get/set/delete/list` | Key-value storage |
| **Collections** | `ctx.collection.find/insert/update/delete` | Schemaless JSON documents |
| **Streams** | `ctx.stream.append/read` | Append-only event streams |
| **Workflows** | `ctx.workflow.start/get/signal` | Workflow management |
| **Actions** | `ctx.action(name, args)` | Call other registered actions |
| **Secrets** | `ctx.secret(name)` | Access preloaded secrets |
| **Logging** | `ctx.log.debug/info/warn/error` | Structured logging |
| **Metrics** | `ctx.metric(name, value, labels)` | Record metrics |
| **Tracing** | `ctx.trace(name, callback, attrs?)` | Distributed tracing spans |

## Usage

Every primitive handler receives the context as the first argument:

```ts
import { action, v } from "@chimpbase/runtime";

const createOrder = action("createOrder", async (ctx, input) => {
  // Database
  const [order] = await ctx.db.query(
    "insert into orders (total) values (?1) returning id",
    [input.total],
    v.object({ id: v.number() }),
  );

  // Pub/Sub
  ctx.pubsub.publish("order.created", { orderId: order.id });

  // Queue
  await ctx.enqueue("order.fulfill", { orderId: order.id });

  // Logging
  ctx.log.info("order created", { orderId: order.id });

  return order;
});
```

See the individual primitive pages for detailed API documentation:

- [Database](/database)
- [Collections](/collections)
- [KV Store](/kv)
- [Streams](/streams)
- [Telemetry](/telemetry)
