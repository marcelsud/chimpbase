# examples/node/intermediate

Node 22+ port of the async-primitives ladder rung: `subscription`, `worker`, `enqueue`, `cron`, telemetry. Same orders domain as `basic`, extended with a status lifecycle and a completion notification pipeline.

Order migrations, handlers, and HTTP routes live in `examples/shared/orders`. Each runtime keeps its own bootstrap, registrations, and native tests; advanced examples also share the fulfilment workflow.

## Run

```bash
bun install                              # at repo root
export DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/chimpbase
bun run dev:node:intermediate
```

SQLite (`node:sqlite`) works out of the box for local play; the `DATABASE_URL` export switches the host to Postgres once you have one running.

## Primitives introduced on top of `basic`

- **`ctx.pubsub.publish(event, payload)`** — action publishes a domain event after the DB write commits.
- **`subscription(event, handler, { idempotent, name })`** — cross-process-safe event handler. The runtime wraps each delivery in a transaction; duplicates are deduped by event id + subscription name.
- **`ctx.enqueue(queue, payload)`** — durable background job handed to a worker.
- **`worker(queue, handler)`** and a DLQ sibling (`worker("...dlq", ...)`) — queue consumers. Postgres queues lock rows with `FOR UPDATE SKIP LOCKED` so multiple replicas can share a queue.
- **`cron(name, "5-field cron", handler)`** — UTC-only scheduler that runs through the same worker path as queues.
- **`ctx.log / ctx.metric / ctx.trace`** — telemetry on the worker side.

## Domain

`pending → assigned → in_progress → completed | rejected`

Events: `order.created`, `order.assigned`, `order.started`, `order.completed`, `order.rejected`. Completion also pushes a job onto `order.completed.notify`, which `notifyOrderCompleted` processes (writes a row to `order_notifications`). Failures cascade to `order.completed.notify.dlq`.

## Tests

```bash
bun run --filter @chimpbase/example-node-intermediate test:app    # node:test, pure domain
bun run --filter @chimpbase/example-node-intermediate test:e2e    # node:test, full flow
```

Tests use Node's native `node:test` runner under the tsx loader. E2E runs against in-memory storage with `subscriptions: { dispatch: "sync" }` so the event → audit → queue → worker pipeline settles inside one `drain()` call.
