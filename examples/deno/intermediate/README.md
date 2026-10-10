# examples/deno/intermediate

Deno 2+ port of the async-primitives ladder rung: `subscription`, `worker`, `enqueue`, `cron`, telemetry. Same orders domain as `basic`, extended with a status lifecycle and a completion notification pipeline.

Order migrations, handlers, and HTTP routes live in `examples/shared/orders`. Each runtime keeps its own bootstrap, registrations, and native tests; advanced examples also share the fulfilment workflow.

## Run

```bash
cd examples/deno/intermediate
export DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/chimpbase
deno task dev
```

SQLite (`node:sqlite`) works out of the box; the `DATABASE_URL` export switches the host to Postgres once you have one running.

## Primitives introduced on top of `basic`

- **`ctx.pubsub.publish(event, payload)`** — action publishes a domain event after the DB write commits.
- **`subscription(event, handler, { idempotent, name })`** — cross-process-safe event handler. Runtime wraps each delivery in a transaction; duplicates dedupe by event id + subscription name.
- **`ctx.enqueue(queue, payload)`** — durable background job handed to a worker.
- **`worker(queue, handler)`** and a DLQ sibling — queue consumers. Postgres queues lock rows with `FOR UPDATE SKIP LOCKED` so multiple replicas share safely.
- **`cron(name, "5-field cron", handler)`** — UTC-only scheduler; same worker execution path.
- **`ctx.log / ctx.metric / ctx.trace`** — telemetry on the worker side.

## Tests

```bash
deno task test
```

Deno's native `Deno.test` runner. In-memory storage + `subscriptions: { dispatch: "sync" }` so pubsub → audit → queue → worker settles inside `host.drain()`.

Hono (`npm:hono`) handles HTTP, identical to the Bun/Node rungs — the `httpHandler: orderApiApp` shape is runtime-agnostic.
