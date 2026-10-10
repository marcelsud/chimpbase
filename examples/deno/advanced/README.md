# examples/deno/advanced

Production-shape Chimpbase app on Deno 2+. Mirrors `examples/bun/advanced`: fulfilment **workflow**, four first-party plugins, multi-replica Docker Compose topology.

Order migrations, handlers, and HTTP routes live in `examples/shared/orders`. Each runtime keeps its own bootstrap, registrations, and native tests; advanced examples also share the fulfilment workflow.

## Run

```bash
cd examples/deno/advanced
export DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/chimpbase
echo "CHIMPBASE_BOOTSTRAP_API_KEY=dev-key" > .env
deno task dev
```

Mutations require `X-API-Key: dev-key`. Requires Deno 2.0+ (for stable `Deno.serve`, `node:sqlite`, npm compatibility).

## Multi-replica via Docker Compose

```bash
docker compose up --build
```

Three replicas share Postgres + OTel collector. Coordination rules are identical to the Bun/Node advanced rungs — queues with `FOR UPDATE SKIP LOCKED`, cron slot leasing, idempotent subscriptions, workflow state in Postgres.

## Plugins

- `@chimpbase/auth` — API-key guard.
- `@chimpbase/webhooks` — outbound delivery on `order.completed` / `order.rejected`.
- `@chimpbase/rest-collections` — auto-exposes `quality_reports` under `/api/quality_reports`.
- `@chimpbase/otel` — `createOtelSink` wired only when `OTEL_EXPORTER_OTLP_ENDPOINT` is set.

## Tests

```bash
deno task test
```

In-memory storage + `subscriptions: { dispatch: "sync" }` so pubsub → audit → queue → worker → workflow settles inside `host.drain()`.
