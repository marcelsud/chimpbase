# examples/node/advanced

Production-shape Chimpbase app on Node 22+. Mirrors `examples/bun/advanced`: fulfilment **workflow**, four first-party plugins, multi-replica Docker Compose topology, Node-native test runner.

## What's new on top of `intermediate`

- **`workflow(...)`** — `order.fulfilment` orchestrates `assign → start → [wait for quality.decision signal] → complete | reject`. Imperative `run(ctx)` form; state persists in Postgres.
- **`ctx.workflow.start / signal / get`** — used by the HTTP routes `POST /orders/:id/fulfilment`, `POST /fulfilments/:workflowId/quality`, and `GET /fulfilments/:workflowId`.
- **`@chimpbase/auth`** — API-key middleware on every path except `/health`. Bootstrap key read from the `CHIMPBASE_BOOTSTRAP_API_KEY` secret.
- **`@chimpbase/webhooks`** — outbound delivery for `order.completed` and `order.rejected`. Management endpoints under `/_webhooks`.
- **`@chimpbase/rest-collections`** — auto-exposes the `quality_reports` collection under `/api/quality_reports`.
- **`@chimpbase/otel`** — wired via `createOtelSink({...})` only when `OTEL_EXPORTER_OTLP_ENDPOINT` is set. Local `node --import tsx/esm app.ts` without an endpoint keeps `sinks: []` to avoid retry noise.

Order migrations, handlers, and HTTP routes live in `examples/shared/orders`. Each runtime keeps its own bootstrap, registrations, and native tests; advanced examples also share the fulfilment workflow.

## Run

```bash
bun install                                 # at repo root
export DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/chimpbase
echo "CHIMPBASE_BOOTSTRAP_API_KEY=dev-key" > examples/node/advanced/.env
bun run dev:node:advanced
```

Mutations require `X-API-Key: dev-key`. Requires Node 22+ (for `node:sqlite` fallback, stable `fetch`, native `node --test`).

## Multi-replica via Docker Compose

```bash
cd examples/node/advanced
docker compose up --build
```

Three replicas share a Postgres + OTel collector:

- Queues (`order.completed.notify`) use `SELECT … FOR UPDATE SKIP LOCKED` — exactly one replica processes each job.
- Cron (`orders.backlog.snapshot`) uses slot leasing — exactly one replica fires each 15-minute slot.
- Idempotent subscriptions (`auditOrderCreated`, etc.) dedupe by `event_id + subscription name` across replicas.
- Workflow state persists in Postgres — any replica can resume a running fulfilment instance.

The Docker image is Node 22, with Bun installed just to hydrate the workspace at build time. Runtime is `node --import tsx/esm app.ts`.

## Tests

```bash
bun run --filter @chimpbase/example-node-advanced test:app    # node:test, domain only
bun run --filter @chimpbase/example-node-advanced test:e2e    # node:test, lifecycle + workflow
```

In-memory storage + `subscriptions: { dispatch: "sync" }` so pubsub → audit → queue → worker → workflow settles inside `host.drain()` calls.
