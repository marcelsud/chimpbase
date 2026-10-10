# Subscriptions

Subscriptions react to events published via `ctx.pubsub.publish()`. Use them for internal choreography — audit logging, notifications, data denormalization, or enqueuing background work.

## Publishing Events

Any action, subscription, or worker handler can publish events:

```ts
ctx.pubsub.publish("order.created", { orderId: 42, total: 99.99 });
```

## Subscribing to Events

```ts
import { subscription } from "chimpbase/runtime";

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
| `idempotent` | `boolean` | `false` | Skips duplicate handling after a successful commit (dedup via KV) |
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

## Multiple Processes

PostgreSQL hosts include a polling event bus that delivers committed events to peers. With async dispatch, subscription work is queued durably and can run on any host sharing the database and registrations.

Use a stable subscription name with `idempotent: true` when several hosts may receive the same event. External effects still need their own idempotency key.

See [Event bus transports](/advanced/event-bus) for transport limits and direct engine integrations.

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
