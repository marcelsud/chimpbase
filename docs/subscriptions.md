# Subscriptions

Subscriptions react to events published via `ctx.pubsub.publish()`. Use them for internal choreography — audit logging, notifications, data denormalization, or enqueuing background work.

## Publishing Events

Any action, subscription, or worker handler can publish events:

```ts
ctx.pubsub.publish("order.created", { orderId: 42, total: 99.99 });
```

## Subscribing to Events

```ts
import { subscription } from "@chimpbase/runtime";

const onOrderCreated = subscription(
  "order.created",
  async (ctx, payload) => {
    await ctx.db.query(
      "INSERT INTO order_audit (order_id, event) VALUES (?1, ?2)",
      [payload.orderId, "created"],
    );
  },
  { idempotent: true, name: "auditOrderCreated" },
);
```

## Handler Signature

```ts
(ctx: ChimpbaseContext, payload: TPayload) => TResult | Promise<TResult>
```

The handler receives the full `ChimpbaseContext` and the event payload.

## Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `idempotent` | `boolean` | `false` | When `true`, the handler runs at most once per event (dedup via KV) |
| `name` | `string` | — | Required when `idempotent: true`. Used as the dedup key. |
| `telemetry` | `boolean \| object` | — | Control logging/metrics/tracing |

## Idempotency

Mark subscriptions as idempotent when replay safety matters. The framework atomically reserves a KV marker before invoking the handler, in the same database transaction. Concurrent deliveries wait for that transaction and skip the handler after a successful commit; a rollback removes the reservation so delivery can retry. External effects still need their own idempotency key when a handler can fail after sending them.

```ts
subscription("payment.captured", handlePayment, {
  idempotent: true,
  name: "processPaymentCapture",
});
```

Idempotency markers are cleaned up automatically when retention is enabled in the app configuration.

## Dispatch Mode

Subscriptions can be dispatched synchronously or asynchronously:

- **sync** (default) — subscriptions run within the same transaction as the publisher
- **async** — subscriptions run asynchronously after the publisher completes

## Cross-Process Fanout

By default, subscriptions dispatch **in the same process** that published the event. A publish in container A does **not** reach subscribers in container B unless one of the following is wired up:

1. **Async dispatch + shared storage** — with `subscriptions.dispatch: "async"`, the engine enqueues dispatch jobs on the internal `__chimpbase.subscription.run` queue. Any worker consuming that queue — including a different container backed by the same storage (e.g. shared Postgres) — can pick up and run the subscription.
2. **`ChimpbaseEventBus` transport** — a pluggable interface on the engine that publishes committed events to a broker and delivers them to peer processes that call `engine.startEventBus()`.

The default `eventBus` is `NoopEventBus` — publish is a no-op, nothing crosses process boundaries.

### Event bus transports

`@chimpbase/postgres` ships two implementations:

| Transport | Delivery | Notes |
|-----------|----------|-------|
| `PostgresPollingEventBus` | Polls `_chimpbase_events` table | No payload cap. Higher latency (poll interval). No extra infra beyond the existing Postgres adapter. |
| `PostgresListenEventBus` | `LISTEN`/`NOTIFY` | Push-based, sub-ms latency. Payload cap ~7800B per event — throws `PayloadTooLargeError` above the limit. |

### Wiring `PostgresListenEventBus`

```ts
import { ChimpbaseEngine } from "@chimpbase/core";
import { PostgresListenEventBus, openPostgresPool } from "@chimpbase/postgres";

const pool = openPostgresPool(config);
const eventBus = new PostgresListenEventBus({ pool });

const engine = new ChimpbaseEngine({
  adapter,
  eventBus,
  // ...
});

engine.startEventBus();
```

Each process gets a unique `originId` — events published by a process are filtered out of its own `LISTEN` stream, so in-process subscriptions are not double-dispatched.

Hosts serialize bus deliveries with their actions, routes, and worker drains. When using `ChimpbaseEngine` directly, pass the same operation scheduler used for those calls to `startEventBus(runOperation)`. Nested `ctx.action` calls keep using the current transaction.

The polling transport filters the IDs committed by its own process without skipping earlier peer events. It advances its cursor after successful delivery, so failed callbacks retry on the next poll.

### Payload size

`PostgresListenEventBus` sends the full event envelope through `pg_notify`. Postgres caps `NOTIFY` payloads at 8000 bytes; the transport leaves headroom at 7800 bytes and throws `PayloadTooLargeError` when an envelope exceeds it. The event is still persisted (commit already happened) — only the fanout is skipped. Switch to `PostgresPollingEventBus` or a transport built on Redis Streams / NATS JetStream for larger payloads.

### Choosing a transport

- **Single container** — leave `eventBus` unset (`NoopEventBus`). In-process dispatch is all you need.
- **Multiple containers, small payloads, low latency** — `PostgresListenEventBus`.
- **Multiple containers, large payloads, tolerant of poll latency** — `PostgresPollingEventBus`.
- **High throughput, replay, or durable subscriber groups** — implement `ChimpbaseEventBus` against Redis Streams, NATS JetStream, or Kafka.

## Common Patterns

### Enqueue background work

```ts
subscription("todo.completed", async (ctx, todo) => {
  await ctx.enqueue("todo.completed.notify", todo);
}, { idempotent: true, name: "enqueueTodoNotification" });
```

### Append to a stream

```ts
subscription("todo.created", async (ctx, todo) => {
  await ctx.stream.append("todo.activity", "todo.created", {
    todoId: todo.id,
    title: todo.title,
  });
}, { idempotent: true, name: "streamTodoCreated" });
```

### Multiple subscriptions per event

You can register multiple subscriptions for the same event:

```ts
subscription("order.created", auditOrder, { idempotent: true, name: "auditOrder" }),
subscription("order.created", notifyWarehouse, { idempotent: true, name: "notifyWarehouse" }),
subscription("order.created", updateAnalytics, { idempotent: true, name: "updateAnalytics" }),
```
